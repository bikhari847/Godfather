package com.timstreams

import android.annotation.SuppressLint
import android.app.Application
import android.content.Context
import android.net.Uri
import android.os.Handler
import android.os.Looper
import android.util.Log
import android.webkit.CookieManager
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

internal class Sniffed(val url: String, val headers: Map<String, String>)

/** Our own hidden browser: blocks junk, catches the stream playlist request. */
internal object TimWeb {
    private const val TAG = "TimStreams"

    @Volatile var context: Context? = null
    private val main = Handler(Looper.getMainLooper())
    private val slots = Semaphore(3)

    private val blockedExt = listOf(".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".ico", ".woff", ".woff2", ".ttf")
    private val blockedHosts = listOf(
        "doubleclick", "googlesyndication", "google-analytics", "googletagmanager",
        "adservice", "histats", "popads", "propeller", "adsterra", "clarity.ms", "yandex"
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

    private fun emptyResponse(mime: String, body: String = ""): WebResourceResponse =
        WebResourceResponse(
            mime, "utf-8", 200, "OK",
            mapOf("Access-Control-Allow-Origin" to "*"),
            ByteArrayInputStream(body.toByteArray(Charsets.UTF_8))
        )

    private fun blocked(u: String): WebResourceResponse? {
        val l = u.lowercase()
        val path = l.substringBefore("?")
        if (path.contains("disable-devtool")) {
            return emptyResponse(
                "text/javascript",
                "window.DisableDevtool=Object.assign(function(){},{isRunning:false,isSuspend:true," +
                    "version:'0',md5:function(){return ''},isDevToolOpened:function(){return false}," +
                    "close:function(){}});"
            )
        }
        if (blockedHosts.any { l.contains(it) }) return emptyResponse("text/javascript")
        if (blockedExt.any { path.endsWith(it) }) return emptyResponse("image/png")
        return null
    }

    private fun sameSite(a: String?, b: String?): Boolean {
        if (a == null || b == null) return true
        fun key(h: String) = h.split(".").takeLast(2).joinToString(".")
        return key(a) == key(b)
    }

    @SuppressLint("SetJavaScriptEnabled")
    private fun setup(wv: WebView, ua: String) {
        val s = wv.settings
        s.javaScriptEnabled = true
        s.domStorageEnabled = true
        s.mediaPlaybackRequiresUserGesture = false
        s.userAgentString = ua
        s.loadsImagesAutomatically = false
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

    suspend fun sniff(
        pageUrl: String,
        referer: String?,
        script: String?,
        userAgent: String,
        timeoutMs: Long,
        tag: String
    ): Sniffed? = slots.withPermit { sniffInner(pageUrl, referer, script, userAgent, timeoutMs, tag) }

    private suspend fun sniffInner(
        pageUrl: String,
        referer: String?,
        script: String?,
        userAgent: String,
        timeoutMs: Long,
        tag: String
    ): Sniffed? {
        val c = appContext()
        if (c == null) {
            Log.w(TAG, "sniff[$tag]: no context")
            return null
        }
        val started = System.currentTimeMillis()
        val result = CompletableDeferred<Sniffed?>()
        val seen = java.util.Collections.synchronizedList(ArrayList<String>())
        val holder = arrayOfNulls<WebView>(1)
        val topHost = runCatching { Uri.parse(pageUrl).host }.getOrNull()

        main.post {
            try {
                val wv = WebView(c)
                holder[0] = wv
                setup(wv, userAgent)
                var scriptDone = false
                wv.webViewClient = object : WebViewClient() {
                    override fun shouldInterceptRequest(
                        view: WebView?,
                        request: WebResourceRequest?
                    ): WebResourceResponse? {
                        val r = request ?: return null
                        val u = r.url.toString()
                        if (seen.size < 30) seen.add(u.take(120))
                        if (isPlaylist(u)) {
                            if (!result.isCompleted) {
                                result.complete(Sniffed(u, r.requestHeaders ?: emptyMap()))
                            }
                            return emptyResponse("application/vnd.apple.mpegurl")
                        }
                        return blocked(u)
                    }

                    override fun shouldOverrideUrlLoading(
                        view: WebView?,
                        request: WebResourceRequest?
                    ): Boolean {
                        val host = request?.url?.host
                        // never let ads or anti-embed code navigate us away
                        return !sameSite(host, topHost)
                    }

                    override fun onPageFinished(view: WebView?, url: String?) {
                        if (script != null && !scriptDone) {
                            scriptDone = true
                            view?.evaluateJavascript(script, null)
                        }
                    }
                }
                val extra = HashMap<String, String>()
                if (referer != null) extra["Referer"] = referer
                wv.loadUrl(pageUrl, extra)
            } catch (e: Throwable) {
                Log.w(TAG, "sniff[$tag] webview error: $e")
                result.complete(null)
            }
        }

        try {
            val r = withTimeoutOrNull(timeoutMs) { result.await() }
            val ms = System.currentTimeMillis() - started
            if (r != null) {
                Log.i(TAG, "sniff[$tag] HIT in ${ms}ms ${r.url.take(110)}")
            } else {
                Log.i(TAG, "sniff[$tag] MISS after ${ms}ms, requests seen=${seen.size}")
                synchronized(seen) { seen.take(25).forEach { Log.i(TAG, "  seen[$tag] $it") } }
            }
            return r
        } finally {
            main.post { destroy(holder[0]) }
        }
    }
}
