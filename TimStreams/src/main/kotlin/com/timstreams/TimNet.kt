package com.timstreams

import android.util.Base64
import java.net.HttpURLConnection
import java.net.URL

internal class Fetched(val code: Int, val finalUrl: String, val body: ByteArray)

internal object TimNet {
    private val urlRegex = Regex("""https?://[^\s"'<>\\]+?\.m3u8[^\s"'<>\\]*""")

    fun get(url: String, headers: Map<String, String>, timeoutMs: Int = 7000): Fetched? {
        return try {
            val c = URL(url).openConnection() as HttpURLConnection
            try {
                c.connectTimeout = timeoutMs
                c.readTimeout = timeoutMs
                c.instanceFollowRedirects = true
                headers.forEach { (k, v) -> c.setRequestProperty(k, v) }
                val code = c.responseCode
                val stream = if (code in 200..299) c.inputStream else c.errorStream
                val body = stream?.use { it.readBytes() } ?: ByteArray(0)
                Fetched(code, c.url.toString(), body)
            } finally {
                c.disconnect()
            }
        } catch (e: Exception) {
            null
        }
    }

    fun hex(b: ByteArray, n: Int): String =
        b.take(n).joinToString("") { "%02x".format(it) }

    private fun find(bytes: ByteArray): String? {
        val s = String(bytes, Charsets.ISO_8859_1)
        if (s.contains("http")) urlRegex.find(s)?.let { return it.value }
        val r = s.reversed()
        if (r.contains("http")) urlRegex.find(r)?.let { return it.value }
        return null
    }

    /** Tries common ways a playlist address might be hidden. Returns (url, method) or null. */
    fun decode(raw: ByteArray): Pair<String, String>? {
        if (raw.isEmpty()) return null
        find(raw)?.let { return it to "plain" }

        val txt = String(raw, Charsets.ISO_8859_1).trim()
        for ((flag, label) in listOf(Base64.DEFAULT to "b64", Base64.URL_SAFE to "b64url")) {
            val b = runCatching { Base64.decode(txt, flag) }.getOrNull()
            if (b != null) find(b)?.let { return it to label }
        }
        if (txt.length % 2 == 0 && txt.isNotEmpty() &&
            txt.all { it.isDigit() || it in 'a'..'f' || it in 'A'..'F' }
        ) {
            val b = ByteArray(txt.length / 2) { txt.substring(it * 2, it * 2 + 2).toInt(16).toByte() }
            find(b)?.let { return it to "hex" }
        }

        // single-byte XOR families, with and without a leading header byte
        for (offset in 0..2) {
            if (raw.size <= offset + 16) continue
            for (mode in 0..3) {
                for (key in 0..255) {
                    val out = ByteArray(raw.size - offset)
                    for (i in out.indices) {
                        val k = when (mode) {
                            0 -> key
                            1 -> (key + i) and 255
                            2 -> key xor (i and 255)
                            else -> (key - i) and 255
                        }
                        out[i] = (raw[i + offset].toInt() xor k).toByte()
                    }
                    find(out)?.let { return it to "xor(offset=$offset,mode=$mode,key=$key)" }
                }
            }
        }

        // repeating text keys
        val keys = listOf(
            "plum", "plum.wasm", "tims", "timstreams", "tim-streams", "tim-streams.com",
            "ehgembxe", "ehgembxe.cfd", "tims-001", "sytes", "secure", "main"
        )
        for (offset in 0..2) {
            if (raw.size <= offset + 16) continue
            for (ks in keys) {
                val kb = ks.toByteArray(Charsets.UTF_8)
                val out = ByteArray(raw.size - offset)
                for (i in out.indices) {
                    out[i] = (raw[i + offset].toInt() xor kb[i % kb.size].toInt()).toByte()
                }
                find(out)?.let { return it to "xorkey($ks,offset=$offset)" }
            }
        }
        return null
    }
}
