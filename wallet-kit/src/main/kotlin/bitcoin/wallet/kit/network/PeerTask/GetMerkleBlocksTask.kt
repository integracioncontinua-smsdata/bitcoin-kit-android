package bitcoin.wallet.kit.network.PeerTask

import bitcoin.wallet.kit.blocks.BlockSyncer
import bitcoin.wallet.kit.core.toHexString
import bitcoin.wallet.kit.models.InventoryItem
import bitcoin.wallet.kit.models.MerkleBlock
import bitcoin.wallet.kit.models.Transaction

class GetMerkleBlocksTask(hashes: List<ByteArray>) : PeerTask() {

    private var hashes = hashes.toMutableList()
    private var pendingMerkleBlocks = mutableListOf<MerkleBlock>()
    private var nextBlockFull = true

    override fun start() {
        val items = hashes.map { hash ->
            InventoryItem(InventoryItem.MSG_FILTERED_BLOCK, hash)
        }

        requester?.getData(items)
    }

    override fun handleMerkleBlock(merkleBlock: MerkleBlock): Boolean {
        if (hashes.firstOrNull { merkleBlock.blockHash.contentEquals(it) } == null) {
            return false
        }

        if (merkleBlock.complete) {
            handleCompletedMerkleBlock(merkleBlock)
        } else {
            pendingMerkleBlocks.add(merkleBlock)
        }

        return true
    }

    override fun handleTransaction(transaction: Transaction): Boolean {
        val block = pendingMerkleBlocks.firstOrNull { it.associatedTransactionHexes.contains(transaction.hash.toHexString()) }
                ?: return false

        block.associatedTransactions.add(transaction)

        if (block.complete) {
            pendingMerkleBlocks.remove(block)
            handleCompletedMerkleBlock(block)
        }

        return true
    }

    private fun handleCompletedMerkleBlock(merkleBlock: MerkleBlock) {
        hashes.firstOrNull { it.contentEquals(merkleBlock.blockHash) }?.let {
            hashes.remove(it)
        }

        try {
            delegate?.handleMerkleBlock(merkleBlock, nextBlockFull)
        } catch (e: BlockSyncer.Error.NextBlockNotFull) {
            nextBlockFull = false
        }

        if (hashes.isEmpty()) {
            delegate?.onTaskCompleted(this)
        }
    }

//    override func isRequestingInventory(hash: Data) -> Bool {
//        return hashes.contains(hash)
//    }

}
