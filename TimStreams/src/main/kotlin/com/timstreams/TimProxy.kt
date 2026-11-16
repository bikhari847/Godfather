package com.timstreams

import android.util.Log
import java.io.OutputStream
import java.net.HttpURLConnection
import java.net.InetAddress
import java.net.ServerSocket
import java.net.Socket
import java.net.URL
import java.net.URLDecoder
import java.net.URLEncoder
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors

/**
 * Tiny local HTTP proxy. The site disguises video chunks as PNG images,
 * so we fetch them, strip the wrapper and serve clean video to the player.
 */
internal object TimProxy {
    private const val TAG = "TimStreams"
    private val pool = Executors.newCachedThreadPool()
    private val sessions = ConcurrentHashMap<String, Map<String, String>>()
    @Volatile private var server: ServerSocket? = null
    @Volatile private var port = 0
    @Volatile private var segLogged = 0
    @Volatile private var errLogged = 0

    private class Up(val code: Int, val body: ByteArray)

    @Synchronized
    private fun ensureStarted(): Boolean {
        if (server?.isClosed == false) return true
        return try {
            val s = ServerSocket(0, 50, InetAddress.getByName("127.0.0.1"))
            server = s
            port = s.localPort
            val t = Thread {
                while (!s.isClosed) {
                    try {
                        val c = s.accept()
                        pool.execute { handle(c) }
                    } catch (e: Exception) {
                        if (s.isClosed) break
                    }
                }
            }
            t.isDaemon = true
            t.name = "TimProxy-accept"
            t.start()
            Log.i(TAG, "proxy listening on $port")
            true
        } catch (e: Exception) {
            Log.e(TAG, "proxy start failed: $e")
            false
        }
    }

    /** Returns a local URL that plays the given playlist through the proxy. */
    fun wrap(playlistUrl: String, headers: Map<String, String>): String? {
        if (!ensureStarted()) return null
        val id = UUID.randomUUID().toString().replace("-", "").take(12)
        if (sessions.size > 200) sessions.clear()
        sessions[id] = headers
        return "http://127.0.0.1:$port/m/$id?u=" + URLEncoder.encode(playlistUrl, "UTF-8")
    }

    private fun handle(sock: Socket) {
        try {
            sock.soTimeout = 30_000
            sock.tcpNoDelay = true
            val reader = sock.getInputStream().bufferedReader(Charsets.ISO_8859_1)
            val requestLine = reader.readLine() ?: return
            while (true) {
                val l = reader.readLine() ?: break
                if (l.isEmpty()) break
            }
            val method = requestLine.substringBefore(" ")
            val head = method.equals("HEAD", true)
            val target = requestLine.split(" ").getOrNull(1) ?: return
            val path = target.substringBefore("?")
            val query = target.substringAfter("?", "")
            val parts = path.trim('/').split("/")
            val kind = parts.getOrNull(0) ?: ""
            val session = parts.getOrNull(1) ?: ""
            val url = query.split("&").firstOrNull { it.startsWith("u=") }
                ?.substring(2)
                ?.let { URLDecoder.decode(it, "UTF-8") }
            val headers = sessions[session]
            val out = sock.getOutputStream()
            if (url == null || headers == null) {
                respond(out, 404, "text/plain", ByteArray(0), head)
                return
            }

            val up = try {
                fetch(url, headers)
            } catch (e: Exception) {
                if (errLogged < 8) {
                    errLogged++
                    Log.w(TAG, "upstream failed for ${url.take(90)}: $e")
                }
                respond(out, 502, "text/plain", ByteArray(0), head)
                return
            }
            if (up.code !in 200..299) {
                if (errLogged < 8) {
                    errLogged++
                    Log.w(TAG, "upstream ${up.code} for ${url.take(90)}")
                }
                respond(out, if (up.code > 0) up.code else 502, "text/plain", ByteArray(0), head)
                return
            }

            when (kind) {
                "m" -> {
                    val text = String(up.body, Charsets.UTF_8)
                    if (!text.trimStart().startsWith("#EXTM3U")) {
                        Log.w(TAG, "not a playlist: ${text.take(80)}")
                        respond(out, 502, "text/plain", ByteArray(0), head)
                        return
                    }
                    val body = rewrite(text, url, session).toByteArray(Charsets.UTF_8)
                    respond(out, 200, "application/vnd.apple.mpegurl", body, head)
                }
                "s" -> {
                    val (data, type) = clean(up.body)
                    respond(out, 200, type, data, head)
                }
                else -> respond(out, 200, "application/octet-stream", up.body, head)
            }
        } catch (e: Exception) {
            Log.w(TAG, "proxy error: $e")
        } finally {
            try {
                sock.close()
            } catch (_: Exception) {
            }
        }
    }

    private fun fetch(url: String, headers: Map<String, String>): Up {
        var last: Exception? = null
        for (attempt in 0..1) {
            try {
                val c = URL(url).openConnection() as HttpURLConnection
                try {
                    c.connectTimeout = 8_000
                    c.readTimeout = 15_000
                    c.instanceFollowRedirects = true
                    c.setRequestProperty("Accept", "*/*")
                    headers.forEach { (k, v) -> c.setRequestProperty(k, v) }
                    val code = c.responseCode
                    val stream = if (code in 200..299) c.inputStream else c.errorStream
                    val bytes = stream?.use { it.readBytes() } ?: ByteArray(0)
                    if (code >= 500 && attempt == 0) continue
                    return Up(code, bytes)
                } finally {
                    c.disconnect()
                }
            } catch (e: Exception) {
                last = e
            }
        }
        throw last ?: java.io.IOException("fetch failed")
    }

    private fun rewrite(text: String, baseUrl: String, session: String): String {
        val base = URL(baseUrl)
        fun proxied(kind: String, ref: String): String =
            "http://127.0.0.1:$port/$kind/$session?u=" +
                URLEncoder.encode(URL(base, ref).toString(), "UTF-8")

        val uriAttr = Regex("""URI="([^"]+)"""")
        return text.lineSequence().joinToString("\n") { line ->
            val l = line.trim()
            when {
                l.isEmpty() -> line
                l.startsWith("#") -> uriAttr.replace(line) { m ->
                    val ref = m.groupValues[1]
                    val kind = when {
                        ref.contains(".m3u8") -> "m"
                        line.startsWith("#EXT-X-KEY") -> "k"
                        else -> "s"
                    }
                    "URI=\"" + proxied(kind, ref) + "\""
                }
                else -> proxied(if (l.contains(".m3u8")) "m" else "s", l)
            }
        }
    }

    // ---- fake-PNG / junk stripping ----------------------------------------

    private fun isPng(b: ByteArray) =
        b.size > 8 && b[0] == 0x89.toByte() && b[1] == 0x50.toByte() &&
            b[2] == 0x4E.toByte() && b[3] == 0x47.toByte()

    private fun pngEnd(b: ByteArray): Int {
        var p = 8
        while (p + 12 <= b.size) {
            val len = ((b[p].toInt() and 0xFF) shl 24) or ((b[p + 1].toInt() and 0xFF) shl 16) or
                ((b[p + 2].toInt() and 0xFF) shl 8) or (b[p + 3].toInt() and 0xFF)
            if (len < 0 || p.toLong() + 12L + len > b.size) return -1
            val type = String(b, p + 4, 4, Charsets.ISO_8859_1)
            p += 12 + len
            if (type == "IEND") return p
        }
        return -1
    }

    private fun findTs(b: ByteArray, from: Int): Int {
        val last = minOf(b.size - 377, from + 65536)
        var i = from
        while (i <= last) {
            if (b[i] == 0x47.toByte() && b[i + 188] == 0x47.toByte() && b[i + 376] == 0x47.toByte()) return i
            i++
        }
        return -1
    }

    private fun findMp4(b: ByteArray, from: Int): Int {
        val last = minOf(b.size - 8, from + 8192)
        var i = from
        while (i <= last) {
            val t = String(b, i + 4, 4, Charsets.ISO_8859_1)
            if (t == "ftyp" || t == "styp" || t == "moof" || t == "moov") return i
            i++
        }
        return -1
    }

    private fun slice(b: ByteArray, from: Int) = if (from <= 0) b else b.copyOfRange(from, b.size)

    private fun clean(b: ByteArray): Pair<ByteArray, String> {
        var start = 0
        val png = isPng(b)
        if (png) {
            val e = pngEnd(b)
            if (e > 0 && e < b.size) start = e
        }
        val ts = findTs(b, start)
        val result: Pair<ByteArray, String>
        val how: String
        if (ts >= 0) {
            result = slice(b, ts) to "video/mp2t"
            how = "ts@$ts"
        } else {
            val mp4 = findMp4(b, start)
            if (mp4 >= 0) {
                result = slice(b, mp4) to "video/mp4"
                how = "mp4@$mp4"
            } else {
                result = slice(b, start) to "application/octet-stream"
                how = "unknown(start=$start)"
            }
        }
        if (segLogged < 4) {
            segLogged++
            val headHex = b.take(24).joinToString("") { "%02x".format(it) }
            Log.i(TAG, "segment size=${b.size} png=$png head=$headHex -> $how")
        }
        return result
    }

    private fun respond(out: OutputStream, code: Int, contentType: String, body: ByteArray, head: Boolean) {
        val reason = if (code == 200) "OK" else "Error"
        val header = "HTTP/1.1 $code $reason\r\n" +
            "Content-Type: $contentType\r\n" +
            "Content-Length: ${body.size}\r\n" +
            "Cache-Control: no-store\r\n" +
            "Access-Control-Allow-Origin: *\r\n" +
            "Connection: close\r\n\r\n"
        out.write(header.toByteArray(Charsets.ISO_8859_1))
        if (!head) out.write(body)
        out.flush()
    }
}
