package com.timstreams

import android.annotation.SuppressLint
import android.app.Application
import android.content.Context
import android.net.Uri
import android.os.Handler
import android.os.Looper
import android.util.Log
import android.webkit.ConsoleMessage
import android.webkit.CookieManager
import android.webkit.WebChromeClient
import android.webkit.WebResourceError
import android.webkit.WebResourceRequest
import android.webkit.WebResourceResponse
import android.webkit.WebSettings
import android.webkit.WebView
import android.webkit.WebViewClient
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withTimeoutOrNull
import java.io.ByteArrayInputStream
import java.util.Collections
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

internal class Sniffed(val url: String, val headers: Map<String, String>)

/** Our own hidden browser: loads the player and catches the stream playlist request. */
internal object TimWeb {
    private const val TAG = "TimStreams"

    @Volatile var context: Context? = null
    private val main = Handler(Looper.getMainLooper())
    private val slots = Semaphore(3)
    private val pRegex = Regex("""/p/[0-9a-f]{32}""")
    private val blockedHosts = listOf(
        "doubleclick", "googlesyndication", "google-analytics", "googletagmanager",
        "adservice", "histats", "popads", "propeller", "adsterra", "clarity.ms"
    )

    private fun appContext(): Context? {
        context?.let { return it }
        return runCatching {
            val at = Class.forName("android.app.ActivityThread")
            val app = at.getMethod("currentApplication").invoke(null) as? Application
            app?.applicationContext
        }.getOrNull()?.also { context = it }
    }

    private fun isPlaylist(u: String): Boolean {
        val path = u.substringBefore("?").lowercase()
        return path.contains(".m3u8") || path.contains("/main/secure/")
    }

    private fun emptyResponse(mime: String): WebResourceResponse =
        WebResourceResponse(
            mime, "utf-8", 200, "OK",
            mapOf("Access-Control-Allow-Origin" to "*"),
            ByteArrayInputStream(ByteArray(0))
        )

    private fun blocked(u: String): WebResourceResponse? {
        val l = u.lowercase()
        return if (blockedHosts.any { l.contains(it) }) emptyResponse("text/javascript") else null
    }

    private fun sameSite(a: String?, b: String?): Boolean {
        if (a == null || b == null) return true
        fun key(h: String) = h.split(".").takeLast(2).joinToString(".")
        return key(a) == key(b)
    }

    @SuppressLint("SetJavaScriptEnabled")
    private fun setup(wv: WebView) {
        val s = wv.settings
        s.javaScriptEnabled = true
        s.domStorageEnabled = true
        s.mediaPlaybackRequiresUserGesture = false
        s.blockNetworkImage = true
        s.javaScriptCanOpenWindowsAutomatically = false
        s.mixedContentMode = WebSettings.MIXED_CONTENT_ALWAYS_ALLOW
        runCatching { CookieManager.getInstance().setAcceptThirdPartyCookies(wv, true) }
        runCatching {
            wv.onResume()
            wv.resumeTimers()
            wv.layout(0, 0, 1280, 720)
        }
    }

    private fun destroy(wv: WebView?) {
        if (wv == null) return
        runCatching {
            wv.stopLoading()
            wv.webViewClient = WebViewClient()
            wv.loadUrl("about:blank")
            wv.destroy()
        }
    }

    /**
     * If [html] is given it is shown as a page whose address is [pageUrl] (no network needed);
     * otherwise [pageUrl] is loaded normally.
     */
    suspend fun sniff(
        pageUrl: String,
        referer: String?,
        html: String?,
        script: String?,
        timeoutMs: Long,
        tag: String,
        notes: MutableList<String>
    ): Sniffed? = slots.withPermit { sniffInner(pageUrl, referer, html, script, timeoutMs, tag, notes) }

    private suspend fun sniffInner(
        pageUrl: String,
        referer: String?,
        html: String?,
        script: String?,
        timeoutMs: Long,
        tag: String,
        notes: MutableList<String>
    ): Sniffed? {
        val c = appContext()
        if (c == null) {
            notes.add("$tag:no-context")
            return null
        }
        val started = System.currentTimeMillis()
        val result = CompletableDeferred<Sniffed?>()
        val seen = Collections.synchronizedList(ArrayList<String>())
        val errs = Collections.synchronizedList(ArrayList<String>())
        val reqCount = AtomicInteger(0)
        val finished = AtomicInteger(0)
        val sawP = AtomicBoolean(false)
        val sawWasm = AtomicBoolean(false)
        val holder = arrayOfNulls<WebView>(1)
        val topHost = runCatching { Uri.parse(pageUrl).host }.getOrNull()

        main.post {
            try {
                val wv = WebView(c)
                holder[0] = wv
                setup(wv)

                wv.webChromeClient = object : WebChromeClient() {
                    override fun onConsoleMessage(cm: ConsoleMessage?): Boolean {
                        if (cm != null && cm.messageLevel() == ConsoleMessage.MessageLevel.ERROR && errs.size < 3) {
                            errs.add("js:" + cm.message().take(50))
                        }
                        return true
                    }
                }

                wv.webViewClient = object : WebViewClient() {
                    override fun shouldInterceptRequest(
                        view: WebView?,
                        request: WebResourceRequest?
                    ): WebResourceResponse? {
                        val r = request ?: return null
                        val u = r.url.toString()
                        reqCount.incrementAndGet()
                        if (seen.size < 24) seen.add(u.take(110))
                        if (pRegex.containsMatchIn(u)) sawP.set(true)
                        if (u.contains("plum.wasm")) sawWasm.set(true)
                        if (isPlaylist(u)) {
                            if (!result.isCompleted) {
                                result.complete(Sniffed(u, r.requestHeaders ?: emptyMap()))
                            }
                            return emptyResponse("application/vnd.apple.mpegurl")
                        }
                        return blocked(u)
                    }

                    // Only the top page is protected from ad redirects. Frames (the player) load freely.
                    override fun shouldOverrideUrlLoading(
                        view: WebView?,
                        request: WebResourceRequest?
                    ): Boolean {
                        val r = request ?: return false
                        if (!r.isForMainFrame) return false
                        return !sameSite(r.url.host, topHost)
                    }

                    override fun onPageFinished(view: WebView?, url: String?) {
                        if (finished.incrementAndGet() == 1 && script != null) {
                            view?.evaluateJavascript(script, null)
                        }
                    }

                    override fun onReceivedHttpError(
                        view: WebView?,
                        request: WebResourceRequest?,
                        errorResponse: WebResourceResponse?
                    ) {
                        if (request != null && errorResponse != null && errs.size < 3) {
                            val host = request.url.host?.take(14) ?: "?"
                            errs.add("${errorResponse.statusCode}@$host")
                        }
                    }

                    override fun onReceivedError(
                        view: WebView?,
                        request: WebResourceRequest?,
                        error: WebResourceError?
                    ) {
                        if (request != null && error != null && request.isForMainFrame && errs.size < 3) {
                            errs.add("neterr${error.errorCode}")
                        }
                    }
                }

                if (html != null) {
                    wv.loadDataWithBaseURL(pageUrl, html, "text/html", "utf-8", null)
                } else {
                    val extra = HashMap<String, String>()
                    if (referer != null) extra["Referer"] = referer
                    wv.loadUrl(pageUrl, extra)
                }
            } catch (e: Throwable) {
                Log.w(TAG, "sniff[$tag] webview error: $e")
                errs.add("webview:" + e.javaClass.simpleName)
                result.complete(null)
            }
        }

        try {
            val r = withTimeoutOrNull(timeoutMs) { result.await() }
            val secs = "%.1f".format((System.currentTimeMillis() - started) / 1000.0)
            if (r != null) {
                notes.add("$tag:HIT ${secs}s")
                Log.i(TAG, "sniff[$tag] HIT in ${secs}s ${r.url.take(110)}")
            } else {
                val e = synchronized(errs) { errs.joinToString(",") }
                notes.add(
                    "$tag:miss req=${reqCount.get()} p=${if (sawP.get()) "Y" else "N"} " +
                        "w=${if (sawWasm.get()) "Y" else "N"} fin=${finished.get()}" +
                        (if (e.isNotEmpty()) " $e" else "")
                )
                Log.i(TAG, "sniff[$tag] MISS after ${secs}s")
                synchronized(seen) { seen.forEach { Log.i(TAG, "  seen[$tag] $it") } }
            }
            return r
        } finally {
            main.post { destroy(holder[0]) }
        }
    }
}
