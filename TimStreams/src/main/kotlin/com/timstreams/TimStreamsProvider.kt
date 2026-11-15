package com.timstreams

import com.fasterxml.jackson.annotation.JsonProperty
import com.lagradost.cloudstream3.*
import com.lagradost.cloudstream3.network.WebViewResolver
import com.lagradost.cloudstream3.utils.AppUtils.toJson
import com.lagradost.cloudstream3.utils.AppUtils.tryParseJson
import com.lagradost.cloudstream3.utils.ExtractorLink
import com.lagradost.cloudstream3.utils.ExtractorLinkType
import com.lagradost.cloudstream3.utils.Qualities
import com.lagradost.cloudstream3.utils.newExtractorLink
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withTimeoutOrNull
import java.net.URLEncoder
import java.text.SimpleDateFormat
import java.util.Locale
import java.util.TimeZone
import java.util.concurrent.ConcurrentHashMap

private const val CONFIG_URL = "https://raw.githubusercontent.com/YOUR_GITHUB_REPO/main/config.json"
private val DEFAULT_DOMAINS = listOf("https://tim-streams.com")
private const val USER_AGENT =
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Mobile Safari/537.36"
private const val BASE_TTL = 10 * 60_000L
private const val ITEMS_TTL = 30_000L
private const val LINK_TTL = 90_000L
private const val LIVE_WINDOW = 270 * 60_000L
private const val KIND_EVENTS = "events"
private const val KIND_CHANNELS = "channels"
private val KEEP_HEADERS = setOf("origin", "referer", "user-agent", "cookie")

data class StreamDto(
    val name: String? = null,
    val url: String? = null,
    val vip: Boolean? = false,
)

data class ItemDto(
    val url: String? = null,
    val name: String? = null,
    val logo: String? = null,
    val genre: Int? = null,
    val time: String? = null,
    val vip: Boolean? = false,
    val streams: List<StreamDto>? = null,
)

data class ItemsResponse(
    val events: List<ItemDto>? = null,
    val channels: List<ItemDto>? = null,
)

data class ConfigDto(
    val domains: List<String>? = null,
    @JsonProperty("eventTimezone") val eventTimezone: String? = null,
)

private data class Resolved(val url: String, val referer: String, val headers: Map<String, String>)

class TimStreamsProvider : MainAPI() {
    override var name = "TimStreams"
    override var mainUrl = DEFAULT_DOMAINS.first()
    override var lang = "en"
    override val hasMainPage = true
    override val supportedTypes = setOf(TvType.Live)

    override val mainPage = mainPageOf(
        "events:live" to "🔴 Live Now",
        "events:upcoming" to "Upcoming",
        "events:g1" to "Soccer",
        "events:g2" to "Motorsport",
        "events:g3" to "Fighting",
        "events:g6" to "Wrestling",
        "events:g7" to "Basketball",
        "events:g8" to "American Football",
        "events:g9" to "Baseball",
        "events:g10" to "Tennis",
        "events:g11" to "Hockey",
        "events:g13" to "Cricket",
        "events:g15,16" to "Rugby",
        "events:other" to "Other Events",
        "channels:g2" to "Sports Channels",
        "channels:g1" to "Entertainment",
        "channels:g3" to "Cartoons",
        "channels:g4" to "Movies",
        "channels:g5" to "News",
    )

    private val cache = ConcurrentHashMap<String, Pair<Long, List<ItemDto>>>()
    private val linkCache = ConcurrentHashMap<String, Pair<Long, Resolved>>()
    @Volatile private var baseUrl: String? = null
    @Volatile private var baseCheckedAt = 0L
    @Volatile private var eventTz = "America/New_York"
    private val webViewLock = Mutex()

    private fun apiHeaders(base: String) = mapOf(
        "Accept" to "application/json",
        "Referer" to "$base/",
        "User-Agent" to USER_AGENT,
    )

    private suspend fun loadConfig(): ConfigDto? = runCatching {
        if (CONFIG_URL.contains("YOUR_GITHUB_REPO")) null
        else tryParseJson<ConfigDto>(app.get(CONFIG_URL, timeout = 8L).text)
    }.getOrNull()

    private suspend fun probe(domain: String): String? = runCatching {
        val res = app.get("$domain/api/channels", headers = apiHeaders(domain), timeout = 8L)
        if (res.isSuccessful && res.text.contains("\"channels\"")) {
            val idx = res.url.indexOf("/api/")
            if (idx > 0) res.url.substring(0, idx).trimEnd('/') else domain
        } else null
    }.getOrNull()

    private suspend fun resolveBase(force: Boolean = false): String {
        val now = System.currentTimeMillis()
        val current = baseUrl
        if (!force && current != null && now - baseCheckedAt < BASE_TTL) return current

        val cfg = loadConfig()
        cfg?.eventTimezone?.takeIf { it.isNotBlank() }?.let { eventTz = it }

        val candidates = (listOfNotNull(current) + cfg?.domains.orEmpty() + DEFAULT_DOMAINS)
            .map { it.trim().trimEnd('/') }
            .filter { it.startsWith("http") }
            .distinct()

        for (candidate in candidates) {
            val alive = probe(candidate) ?: continue
            baseUrl = alive
            baseCheckedAt = now
            mainUrl = alive
            return alive
        }
        return current ?: candidates.firstOrNull() ?: DEFAULT_DOMAINS.first()
    }

    private suspend fun fetchItems(kind: String, base: String): List<ItemDto>? = runCatching {
        val path = if (kind == KIND_EVENTS) "/api/live-upcoming" else "/api/channels"
        val text = app.get(base + path, headers = apiHeaders(base), timeout = 20L).text
        val parsed = tryParseJson<ItemsResponse>(text)
        if (kind == KIND_EVENTS) parsed?.events else parsed?.channels
    }.getOrNull()

    private suspend fun getItems(kind: String): List<ItemDto> {
        val now = System.currentTimeMillis()
        cache[kind]?.let { if (now - it.first < ITEMS_TTL) return it.second }

        var result = fetchItems(kind, resolveBase())
        if (result == null) result = fetchItems(kind, resolveBase(force = true))
        if (result != null) {
            cache[kind] = now to result
            return result
        }
        return cache[kind]?.second ?: emptyList()
    }

    private fun clean(text: String?): String? {
        var s = text?.trim() ?: return null
        while (s.contains("&amp;")) s = s.replace("&amp;", "&")
        return s.ifBlank { null }
    }

    private fun poster(logo: String?, base: String, kind: String): String? {
        val s = clean(logo) ?: return null
        val abs = when {
            s.startsWith("//") -> "https:$s"
            s.startsWith("/") -> base + s
            else -> s
        }
        // Channel logos come from many random hosts: route them through an image proxy
        return if (kind == KIND_CHANNELS && abs.startsWith("http") && !abs.startsWith(base)) {
            "https://wsrv.nl/?url=" + URLEncoder.encode(abs, "UTF-8") + "&w=500&output=png"
        } else abs
    }

    private fun ItemDto.startMillis(): Long? {
        val t = time ?: return null
        return runCatching {
            SimpleDateFormat("yyyy-MM-dd'T'HH:mm", Locale.US).apply {
                timeZone = TimeZone.getTimeZone(eventTz)
            }.parse(t)?.time
        }.getOrNull()
    }

    private fun ItemDto.isLive(): Boolean {
        val start = startMillis() ?: return false
        val now = System.currentTimeMillis()
        return now >= start - 10 * 60_000L && now <= start + LIVE_WINDOW
    }

    private fun ItemDto.isUpcoming(): Boolean {
        val start = startMillis() ?: return false
        return start > System.currentTimeMillis() + 10 * 60_000L
    }

    private fun localTime(item: ItemDto): String? {
        val start = item.startMillis() ?: return null
        return SimpleDateFormat("EEE d MMM, HH:mm", Locale.getDefault()).format(start)
    }

    private fun ItemDto.toResponse(kind: String, base: String): SearchResponse? {
        val slug = url?.takeIf { it.isNotBlank() } ?: return null
        val title = clean(name) ?: return null
        val live = kind == KIND_EVENTS && isLive()
        val segment = if (kind == KIND_EVENTS) "event" else "channel"
        return newLiveSearchResponse(
            if (live) "🔴 $title" else title,
            "$base/$segment/$slug",
            TvType.Live
        ) {
            this.posterUrl = poster(logo, base, kind)
        }
    }

    private fun sortEvents(list: List<ItemDto>) =
        list.sortedWith(
            compareByDescending<ItemDto> { it.isLive() }.thenBy { it.startMillis() ?: Long.MAX_VALUE }
        )

    override suspend fun getMainPage(page: Int, request: MainPageRequest): HomePageResponse {
        val empty = newHomePageResponse(request.name, emptyList<SearchResponse>(), false)
        if (page > 1) return empty
        return runCatching {
            val kind = request.data.substringBefore(":")
            val filter = request.data.substringAfter(":")
            val base = resolveBase()
            val all = getItems(kind)

            val picked: List<ItemDto> = when {
                filter == "live" -> all.filter { it.isLive() }
                filter == "upcoming" -> all.filter { it.isUpcoming() }
                filter == "other" -> {
                    val known = setOf(1, 2, 3, 6, 7, 8, 9, 10, 11, 13, 15, 16)
                    all.filter { it.genre !in known }
                }
                filter.startsWith("g") -> {
                    val ids = filter.drop(1).split(",").mapNotNull { it.trim().toIntOrNull() }.toSet()
                    all.filter { it.genre in ids }
                }
                else -> all
            }
            val ordered = if (kind == KIND_EVENTS) sortEvents(picked) else picked
            val results = ordered.mapNotNull { it.toResponse(kind, base) }
            newHomePageResponse(HomePageList(request.name, results, isHorizontalImages = true), false)
        }.getOrDefault(empty)
    }

    override suspend fun search(query: String): List<SearchResponse> = runCatching {
        val q = query.trim().lowercase()
        if (q.isEmpty()) return@runCatching emptyList<SearchResponse>()
        val base = resolveBase()
        val events = sortEvents(getItems(KIND_EVENTS).filter { clean(it.name)?.lowercase()?.contains(q) == true })
            .mapNotNull { it.toResponse(KIND_EVENTS, base) }
        val channels = getItems(KIND_CHANNELS).filter { clean(it.name)?.lowercase()?.contains(q) == true }
            .mapNotNull { it.toResponse(KIND_CHANNELS, base) }
        events + channels
    }.getOrDefault(emptyList())

    override suspend fun load(url: String): LoadResponse {
        val kind = if (url.contains("/channel/")) KIND_CHANNELS else KIND_EVENTS
        val slug = url.trimEnd('/').substringAfterLast('/')
        val base = resolveBase()

        val item = getItems(kind).firstOrNull { it.url == slug }
            ?: throw ErrorLoadingException("This ${if (kind == KIND_EVENTS) "event" else "channel"} is no longer listed.")

        val all = item.streams.orEmpty().filter { !it.url.isNullOrBlank() }
        val free = all.filter { it.vip != true }
        if (free.isEmpty()) throw ErrorLoadingException("No free streams are available yet. Try again closer to start time.")

        val title = clean(item.name) ?: "TimStreams"
        val plot = buildString {
            localTime(item)?.let { append("🕒 $it (your time)\n") }
            append("Streams: ").append(free.joinToString { clean(it.name) ?: "Stream" })
            if (all.size > free.size) append("\n(${all.size - free.size} VIP-only stream(s) hidden)")
        }

        return newLiveStreamLoadResponse(title, url, free.toJson()) {
            this.posterUrl = poster(item.logo, base, kind)
            this.plot = plot
        }
    }

    // -----------------------------------------------------------------------
    // Stream resolving
    // -----------------------------------------------------------------------
    private val m3u8InText = Regex("""https?://[^"'\s<>\\]+?\.m3u8[^"'\s<>\\]*""")
    private val m3u8InUrl = Regex("""\.m3u8""")

    private fun originOf(url: String): String =
        Regex("""^(https?://[^/]+)""").find(url)?.groupValues?.get(1) ?: url

    private fun buildHeaders(captured: Map<String, String>, origin: String): Map<String, String> {
        val h = LinkedHashMap<String, String>()
        captured.forEach { (k, v) -> if (k.lowercase() in KEEP_HEADERS) h[k] = v }
        if (h.keys.none { it.equals("Origin", true) }) h["Origin"] = origin
        if (h.keys.none { it.equals("Referer", true) }) h["Referer"] = "$origin/"
        if (h.keys.none { it.equals("User-Agent", true) }) h["User-Agent"] = USER_AGENT
        return h
    }

    /** Quick attempt: sometimes the playlist URL is right in the embed page. */
    private suspend fun findInHtml(embedUrl: String, referer: String): String? =
        withTimeoutOrNull(4_000L) {
            runCatching {
                app.get(
                    embedUrl,
                    referer = referer,
                    headers = mapOf("User-Agent" to USER_AGENT),
                    timeout = 4L
                ).text.replace("\\/", "/").let { m3u8InText.find(it)?.value }
            }.getOrNull()
        }

    /** Loads a page in a hidden WebView and catches the first .m3u8 request it makes. */
    private suspend fun sniff(
        pageUrl: String,
        referer: String,
        script: String?
    ): Pair<String, Map<String, String>>? =
        withTimeoutOrNull(22_000L) {
            runCatching {
                val resolver = WebViewResolver(
                    interceptUrl = m3u8InUrl,
                    useOkhttp = false,
                    script = script
                )
                val (req, _) = resolver.resolveUsingWebView(pageUrl, referer)
                req?.let { it.url.toString() to it.headers.toMap() }
            }.getOrNull()
        }

    private fun injectScript(embedUrl: String): String {
        val safe = embedUrl.replace("\\", "\\\\").replace("'", "\\'")
        return """
            (function(){
              try {
                document.documentElement.innerHTML = '<head></head><body style="margin:0;background:#000"></body>';
                var f = document.createElement('iframe');
                f.src = '$safe';
                f.allow = 'autoplay; encrypted-media; fullscreen';
                f.setAttribute('allowfullscreen', 'true');
                f.style.cssText = 'border:0;width:100vw;height:100vh';
                document.body.appendChild(f);
              } catch (e) {}
            })();
        """.trimIndent()
    }

    private suspend fun resolveStream(embedUrl: String): Resolved? {
        val now = System.currentTimeMillis()
        linkCache[embedUrl]?.let { if (now - it.first < LINK_TTL) return it.second }

        val base = baseUrl ?: mainUrl
        val origin = originOf(embedUrl)

        // 1) Fast: playlist visible in the embed HTML
        findInHtml(embedUrl, "$base/")?.let {
            val r = Resolved(it, "$origin/", buildHeaders(emptyMap(), origin))
            linkCache[embedUrl] = now to r
            return r
        }

        // 2) Reliable: player inside a tim-streams.com page (like a real browser), then 3) player on its own
        val captured = webViewLock.withLock {
            linkCache[embedUrl]?.let { return@withLock null to it.second }
            val hit = sniff("$base/api/settings", "$base/", injectScript(embedUrl))
                ?: sniff(embedUrl, "$base/", null)
            hit to null
        }
        captured.second?.let { return it }
        val (url, hdrs) = captured.first ?: return null
        val r = Resolved(url, hdrs["Referer"] ?: "$origin/", buildHeaders(hdrs, origin))
        linkCache[embedUrl] = System.currentTimeMillis() to r
        return r
    }

    override suspend fun loadLinks(
        data: String,
        isCasting: Boolean,
        subtitleCallback: (SubtitleFile) -> Unit,
        callback: (ExtractorLink) -> Unit
    ): Boolean {
        val streams = tryParseJson<List<StreamDto>>(data).orEmpty()
            .filter { it.vip != true && !it.url.isNullOrBlank() }
        if (streams.isEmpty()) return false

        var found = false
        streams.amap { stream ->
            val resolved = runCatching { resolveStream(stream.url!!) }.getOrNull()
            if (resolved != null) {
                runCatching {
                    callback(
                        newExtractorLink(
                            source = name,
                            name = "$name - ${clean(stream.name) ?: "Stream"}",
                            url = resolved.url,
                            type = ExtractorLinkType.M3U8
                        ) {
                            this.referer = resolved.referer
                            this.headers = resolved.headers
                            this.quality = Qualities.Unknown.value
                        }
                    )
                    found = true
                }
            }
        }
        return found
    }
}
