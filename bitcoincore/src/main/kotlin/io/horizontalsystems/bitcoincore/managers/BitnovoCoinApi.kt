package io.horizontalsystems.bitcoincore.managers

import android.util.Base64
import com.eclipsesource.json.Json
import com.eclipsesource.json.JsonObject
import io.horizontalsystems.bitcoincore.core.IInitialSyncApi
import io.horizontalsystems.bitcoincore.utils.HashUtils
import java.util.*
import java.util.logging.Logger

class BitnovoCoinApi(
        host: String,
        private val coin: String,
        private val accessToken: String
) : IInitialSyncApi {
    private val apiManager = ApiManager(host)
    private val logger = Logger.getLogger("BitnovoCoinApi")

    override fun getTransactions(addresses: List<String>): List<TransactionItem> {
        logger.info("Request transactions for ${addresses.size} addresses: [${addresses.first()}, ...]")

        fun min(a: Int, b: Int) = if (a < b) a else b
        val transactions = mutableListOf<TransactionItem>()
        var current = 0
        val maxSize = 20
        while (current < addresses.size) {
            val size = min(maxSize, addresses.size - current)
            val currentAddresses = addresses.slice(current until size)
            val requestData = JsonObject().apply {
                this["addresses"] = Json.array(*currentAddresses.toTypedArray())
            }
            val response = apiManager.post("api/v1/wallets/$coin/tx/", requestData.toString(), authToken()).asArray()

            logger.info("Got ${response.size()} transactions for requested addresses")

            for (txItem in response) {
                val tx = txItem.asObject()

                val blockHashJson = tx["block"] ?: continue
                val blockHash = if (blockHashJson.isString) blockHashJson.asString() else continue

                val outputs = mutableListOf<TransactionOutputItem>()
                for (outputItem in tx["outputs"].asArray()) {
                    val outputJson = outputItem.asObject()

                    val scriptJson = outputJson["script"] ?: continue
                    val addressJson = outputJson["address"] ?: continue

                    if (scriptJson.isString && addressJson.isString) {
                        outputs.add(TransactionOutputItem(scriptJson.asString(), addressJson.asString()))
                    }
                }

                transactions.add(TransactionItem(blockHash, tx["height"].asInt(), outputs))
            }
            current += size
        }
        return transactions
    }

    private fun authToken(): String {
        val now = Date().time / 1000
        val secretHash = HashUtils.sha256("$accessToken$now".toByteArray())
        val base64Hash = Base64.encodeToString(secretHash, Base64.NO_WRAP)
        return "Token $now:$base64Hash"
    }

}
