package io.horizontalsystems.bitcoincore.network.peer

import io.horizontalsystems.bitcoincore.core.IConnectionManager
import io.horizontalsystems.bitcoincore.core.IPeerAddressManager
import io.horizontalsystems.bitcoincore.core.IPeerAddressManagerListener
import io.horizontalsystems.bitcoincore.network.Network
import io.horizontalsystems.bitcoincore.network.messages.*
import io.horizontalsystems.bitcoincore.network.peer.task.PeerTask
import java.net.InetAddress
import java.util.concurrent.Executors
import java.util.logging.Logger

class PeerGroup(
        private val hostManager: IPeerAddressManager,
        private val network: Network,
        private val peerManager: PeerManager,
        peerSize: Int,
        private val networkMessageParser: NetworkMessageParser,
        private val networkMessageSerializer: NetworkMessageSerializer,
        private val connectionManager: IConnectionManager,
        private val localDownloadedBestBlockHeight: Int)
    : Peer.Listener, IPeerAddressManagerListener {

    interface Listener {
        fun onStart() = Unit
        fun onStop() = Unit
        fun onPeerCreate(peer: Peer) = Unit
        fun onPeerConnect(peer: Peer) = Unit
        fun onPeerDisconnect(peer: Peer, e: Exception?) = Unit
        fun onPeerReady(peer: Peer) = Unit
    }

    var inventoryItemsHandler: IInventoryItemsHandler? = null
    var peerTaskHandler: IPeerTaskHandler? = null

    private var running = false
    private val logger = Logger.getLogger("PeerGroup")
    private val peerGroupListeners = mutableListOf<Listener>()
    private val executorService = Executors.newCachedThreadPool()
    private val peerThreadPool = Executors.newCachedThreadPool()

    private val acceptableBlockHeightDifference = 50_000
    private var peerCountToConnectMax = 100
    private var peerCountToConnect: Int? = null // number of peers to connect to
    private val peerCountToHold = peerSize      // number of peers held
    private var peerCountConnected = 0          // number of peers connected to

    fun start() {
        if (running || !connectionManager.isConnected) {
            return
        }

        running = true
        peerCountConnected = 0
        peerGroupListeners.forEach { it.onStart() }
        connectPeersIfRequired()
    }

    fun stop() {
        running = false
        peerManager.disconnectAll()
        peerGroupListeners.forEach { it.onStop() }
    }

    fun addPeerGroupListener(listener: Listener) {
        peerGroupListeners.add(listener)
    }

    fun someReadyPeers(): List<Peer> {
        return peerManager.someReadyPeers()
    }

    @Throws
    fun checkPeersSynced() {
        if (peerManager.peersCount < 1) {
            throw Error("No peers connected")
        }

        if (!peerManager.hasSyncedPeer()) {
            throw Error("Peers not synced yet")
        }
    }

    //
    // PeerListener implementations
    //
    override fun onConnect(peer: Peer) {
        hostManager.markConnected(peer)
        peerGroupListeners.forEach { it.onPeerConnect(peer) }

        peerCountToConnect?.let { disconnectSlowestPeer(it) } ?: setPeerCountToConnect(peer)
    }

    override fun onReady(peer: Peer) {
        peerGroupListeners.forEach { it.onPeerReady(peer) }
    }

    override fun onDisconnect(peer: Peer, e: Exception?) {
        peerManager.remove(peer)

        if (e == null) {
            logger.info("Peer ${peer.host} disconnected.")
            hostManager.markSuccess(peer.host)
        } else {
            logger.warning("Peer ${peer.host} disconnected with error ${e.javaClass.simpleName}, ${e.message}.")
            hostManager.markFailed(peer.host)
        }

        peerGroupListeners.forEach { it.onPeerDisconnect(peer, e) }
        connectPeersIfRequired()
    }

    override fun onReceiveMessage(peer: Peer, message: IMessage) {
        if (message is AddrMessage) {
            val addrs = message.addresses
            val peerIps = mutableListOf<String>()
            for (address in addrs) {
                val addr = InetAddress.getByAddress(address.address)
                peerIps.add(addr.hostAddress)
            }

            hostManager.addIps(peerIps)
        } else if (message is InvMessage) {
            inventoryItemsHandler?.handleInventoryItems(peer, message.inventory)
        }
    }

    override fun onTaskComplete(peer: Peer, task: PeerTask) {
        peerTaskHandler?.handleCompletedTask(peer, task)
    }

    //
    // PeerAddressManager Listener
    //
    override fun onAddAddress() {
        connectPeersIfRequired()
    }

    //
    // Private methods
    //

    private fun setPeerCountToConnect(peer: Peer) {
        peerCountToConnect = if (peer.announcedLastBlockHeight - localDownloadedBestBlockHeight > acceptableBlockHeightDifference) {
            peerCountToConnectMax
        } else {
            0
        }
    }

    private fun disconnectSlowestPeer(peerCountToConnect: Int) {
        if (peerCountToConnect > peerCountConnected && peerCountToHold > 1 && hostManager.hasFreshIps) {
            val sortedPeers = peerManager.sorted()
            if (sortedPeers.size >= peerCountToHold) {
                sortedPeers.lastOrNull()?.close()
            }
        }
    }

    @Synchronized
    private fun connectPeersIfRequired() {
        if (!running || !connectionManager.isConnected) {
            return
        }

        for (i in peerManager.peersCount until peerCountToHold) {
            val ip = hostManager.getIp() ?: break
            val peer = Peer(ip, network, this, networkMessageParser, networkMessageSerializer, executorService)
            peerCountConnected += 1
            peerGroupListeners.forEach { it.onPeerCreate(peer) }
            peerManager.add(peer)
            peer.start(peerThreadPool)
        }
    }

    //
    // PeerGroup Exceptions
    //
    class Error(message: String) : Exception(message)
}
