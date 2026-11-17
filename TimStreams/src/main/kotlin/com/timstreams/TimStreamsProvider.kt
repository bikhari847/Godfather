package com.timstreams

import android.util.Log
import com.fasterxml.jackson.annotation.JsonProperty
import com.lagradost.cloudstream3.*
import com.lagradost.cloudstream3.utils.AppUtils.toJson
import com.lagradost.cloudstream3.utils.AppUtils.tryParseJson
import com.lagradost.cloudstream3.utils.ExtractorLink
import com.lagradost.cloudstream3.utils.ExtractorLinkType
import com.lagradost.cloudstream3.utils.Qualities
import com.lagradost.cloudstream3.utils.newExtractorLink
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withTimeoutOrNull
import org.json.JSONObject
import java.net.URLEncoder
import java.text.SimpleDateFormat
import java.util.Collections
import java.util.Locale
import java.util.TimeZone
import java.util.concurrent.ConcurrentHashMap

private const val TAG = "TimStreams"
private const val CONFIG_URL = "https://raw.githubusercontent.com/YOUR_GITHUB_REPO/main/config.json"
private val DEFAULT_DOMAINS = listOf("https://tim-streams.com")
private const val USER_AGENT =
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Mobile Safari/537.36"
private const val BASE_TTL = 10 * 60_000L
private const val ITEMS_TTL = 30_000L
private const val LINK_TTL_DEFAULT = 90_000L
private const val LINK_TTL_MAX = 60 * 60_000L
private const val LIVE_WINDOW = 270 * 60_000L
private const val RESOLVE_WAIT = 32_000L
private const val KIND_EVENTS = "events"
private const val KIND_CHANNELS = "channels"
private val KEEP_HEADERS = setOf("origin", "referer", "user-agent", "cookie")
private val GENERIC_LABELS = setOf("timstreams", "timstream", "stream", "stream 1", "default", "live", "")
private val KNOWN_GENRES = setOf(1, 2, 3, 6, 7, 8, 9, 10, 11, 13, 15, 16)

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

data class ConfigDto(
    val domains: List<String>? = null,
    @JsonProperty("eventTimezone") val eventTimezone: String? = null,
    @JsonProperty("debug") val debug: Boolean? = null,
)

data class LinkData(
    val page: String? = null,
    val streams: List<StreamDto>? = null,
    val title: String? = null,
)

private data class Resolved(
    val url: String,
    val referer: String,
    val headers: Map<String, String>,
    val expiresAt: Long,
)

private class Feed(val items: List<ItemDto>, val at: Long)

private fun JSONObject.str(key: String): String? =
    if (isNull(key)) null else optString(key, "").trim().ifBlank { null }

class TimStreamsProvider : MainAPI() {
    override var name = "TimStreams"
    override var mainUrl = DEFAULT_DOMAINS.first()
    override var lang = "en"
    override val hasMainPage = true
    override val supportedTypes = setOf(TvType.Live)

    override val mainPage = mainPageOf(
        "events:live" to "🔴 Live Now",
        "events:upcoming" to "Upcoming",
        "channels:g2" to "Sports Channels",
        "channels:g5" to "News",
        "channels:g1" to "Entertainment",
        "channels:g3" to "Cartoons",
        "channels:g4" to "Movies",
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
    )

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val feed = ConcurrentHashMap<String, Feed>()
    private val feedLocks = ConcurrentHashMap<String, Mutex>()
    private val known = ConcurrentHashMap<String, ItemDto>()
    private val linkCache = ConcurrentHashMap<String, Resolved>()
    private val inflight = ConcurrentHashMap<String, Deferred<Resolved?>>()
    private val notesFor = ConcurrentHashMap<String, String>()
    private val wanted: MutableSet<String> = ConcurrentHashMap.newKeySet()
    @Volatile private var primed = false
    @Volatile private var debug = true
    @Volatile private var baseUrl: String? = null
    @Volatile private var baseCheckedAt = 0L
    @Volatile private var eventTz = "America/New_York"
    private val baseLock = Mutex()

    // -----------------------------------------------------------------------
    // Domain handling (self-healing)
    // -----------------------------------------------------------------------
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
        val quick = baseUrl
        if (!force && quick != null && System.currentTimeMillis() - baseCheckedAt < BASE_TTL) return quick

        return baseLock.withLock {
            val now = System.currentTimeMillis()
            val current = baseUrl
            if (!force && current != null && now - baseCheckedAt < BASE_TTL) return@withLock current

            val cfg = loadConfig()
            cfg?.eventTimezone?.takeIf { it.isNotBlank() }?.let { eventTz = it }
            cfg?.debug?.let { debug = it }

            val candidates = (listOfNotNull(current) + cfg?.domains.orEmpty() + DEFAULT_DOMAINS)
                .map { it.trim().trimEnd('/') }
                .filter { it.startsWith("http") }
                .distinct()

            for (candidate in candidates) {
                val alive = probe(candidate) ?: continue
                baseUrl = alive
                baseCheckedAt = System.currentTimeMillis()
                mainUrl = alive
                return@withLock alive
            }
            current ?: candidates.firstOrNull() ?: DEFAULT_DOMAINS.first()
        }
    }

    // -----------------------------------------------------------------------
    // Data (single request per kind, stale data is served instantly)
    // -----------------------------------------------------------------------
    private fun parseItems(text: String, key: String): List<ItemDto>? {
        val root = runCatching { JSONObject(text) }.getOrNull() ?: return null
        if (!root.has(key)) return null
        val arr = root.optJSONArray(key) ?: return emptyList()
        val out = ArrayList<ItemDto>()
        for (i in 0 until arr.length()) {
            runCatching {
                val o = arr.getJSONObject(i)
                val streams = ArrayList<StreamDto>()
                o.optJSONArray("streams")?.let { sa ->
                    for (j in 0 until sa.length()) {
                        runCatching {
                            val s = sa.getJSONObject(j)
                            streams.add(StreamDto(s.str("name"), s.str("url"), s.optBoolean("vip", false)))
                        }
                    }
                }
                out.add(
                    ItemDto(
                        url = o.str("url"),
                        name = o.str("name"),
                        logo = o.str("logo"),
                        genre = if (o.isNull("genre")) null else o.optInt("genre", -1).takeIf { it >= 0 },
                        time = o.str("time"),
                        vip = o.optBoolean("vip", false),
                        streams = streams,
                    )
                )
            }
        }
        return out
    }

    private suspend fun fetchItems(kind: String, base: String): List<ItemDto>? = runCatching {
        val path = if (kind == KIND_EVENTS) "/api/live-upcoming" else "/api/channels"
        val text = app.get(base + path, headers = apiHeaders(base), timeout = 20L).text
        val items = parseItems(text, kind)
        Log.i(TAG, "fetch $kind: items=${items?.size} chars=${text.length}")
        items
    }.getOrNull()

    private suspend fun refresh(kind: String): List<ItemDto> {
        val lock = feedLocks.getOrPut(kind) { Mutex() }
        return lock.withLock {
            val c = feed[kind]
            if (c != null && System.currentTimeMillis() - c.at < 5_000L) return@withLock c.items
            var res = fetchItems(kind, resolveBase())
            if (res == null) res = fetchItems(kind, resolveBase(force = true))
            if (res != null) {
                feed[kind] = Feed(res, System.currentTimeMillis())
                res.forEach { item -> item.url?.let { u -> known["$kind:$u"] = item } }
                res
            } else {
                feed[kind]?.items ?: emptyList()
            }
        }
    }

    private suspend fun getItems(kind: String): List<ItemDto> {
        val c = feed[kind]
        if (c == null) return refresh(kind)
        if (System.currentTimeMillis() - c.at >= ITEMS_TTL) {
            scope.launch { runCatching { refresh(kind) } }
        }
        return c.items
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------
    private fun clean(text: String?): String? {
        var s = text?.trim() ?: return null
        while (s.contains("&amp;")) s = s.replace("&amp;", "&")
        return s.ifBlank { null }
    }

    private fun enc(s: String) = URLEncoder.encode(s, "UTF-8")

    private fun placeholder(title: String?): String =
        "https://placehold.co/500x281/1b1b2f/ffffff/png?text=" + enc(title ?: "TimStreams")

    private fun poster(logo: String?, base: String, title: String?, proxy: Boolean): String {
        val ph = placeholder(title)
        val s = clean(logo) ?: return ph
        val abs = when {
            s.startsWith("//") -> "https:$s"
            s.startsWith("/") -> base + s
            else -> s
        }
        if (!proxy || !abs.startsWith("http") || abs.startsWith(base)) return abs
        return "https://wsrv.nl/?url=" + enc(abs) + "&w=500&output=png&default=" + enc(ph)
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
            this.posterUrl = poster(logo, base, title, kind == KIND_CHANNELS)
        }
    }

    private fun sortEvents(list: List<ItemDto>) =
        list.sortedWith(
            compareByDescending<ItemDto> { it.isLive() }.thenBy { it.startMillis() ?: Long.MAX_VALUE }
        )

    private fun prime() {
        if (primed) return
        val items = feed[KIND_CHANNELS]?.items ?: return
        val target = items.firstOrNull { it.genre == 2 && !it.streams.isNullOrEmpty() }
            ?.streams?.firstOrNull { it.vip != true && !it.url.isNullOrBlank() }?.url ?: return
        primed = true
        scope.launch {
            delay(1_200L)
            runCatching { launchResolve(target, null, user = false, first = false) }
        }
    }

    // -----------------------------------------------------------------------
    // Home, search, details
    // -----------------------------------------------------------------------
    override suspend fun getMainPage(page: Int, request: MainPageRequest): HomePageResponse {
        val empty = newHomePageResponse(request.name, emptyList<SearchResponse>(), false)
        if (page > 1) return empty
        return runCatching {
            val kind = request.data.substringBefore(":")
            val filter = request.data.substringAfter(":")
            val base = resolveBase()
            val all = getItems(kind)
            if (kind == KIND_CHANNELS) prime()

            val picked: List<ItemDto> = when {
                filter == "live" -> all.filter { it.isLive() }
                filter == "upcoming" -> all.filter { it.isUpcoming() }
                filter == "other" -> all.filter { it.genre !in KNOWN_GENRES }
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
            ?: known["$kind:$slug"]
            ?: throw ErrorLoadingException("This ${if (kind == KIND_EVENTS) "event" else "channel"} is no longer listed.")

        val all = item.streams.orEmpty().filter { !it.url.isNullOrBlank() }
        val free = all.filter { it.vip != true }
        if (free.isEmpty()) throw ErrorLoadingException("No free streams are available yet. Try again closer to start time.")

        val title = clean(item.name) ?: "TimStreams"
        val plot = buildString {
            localTime(item)?.let { append("🕒 $it (your time)\n") }
            append("Streams: ").append(free.joinToString { displayName(it.name, title) })
            if (all.size > free.size) append("\n(${all.size - free.size} VIP-only stream(s) hidden)")
        }

        val segment = if (kind == KIND_EVENTS) "event" else "channel"
        val pageUrl = "$base/$segment/$slug"

        free.firstOrNull()?.url?.let { first ->
            runCatching { launchResolve(first, pageUrl, user = false, first = true) }
        }

        return newLiveStreamLoadResponse(title, url, LinkData(pageUrl, free, title).toJson()) {
            this.posterUrl = poster(item.logo, base, title, kind == KIND_CHANNELS)
            this.plot = plot
        }
    }

    private fun displayName(label: String?, title: String?): String {
        val l = clean(label)
        return if (l == null || l.lowercase() in GENERIC_LABELS) (title ?: "Live") else l
    }

    // -----------------------------------------------------------------------
    // Stream resolving
    // -----------------------------------------------------------------------
    private val expiryRegex = Regex("""/(\d{10})/[^/]+$""")
    private val pRegex = Regex("""(https?://[^"'\s\\<>]+)?/p/([0-9a-fA-F]{32})""")
    private val m3u8Any = Regex("""https?://[^"'\s<>\\]+?\.m3u8[^"'\s<>\\]*""")
    private val signedAny = Regex("""(?:SIGNED_URL|data-signed-url)["']?\s*[:=]\s*["']([^"']+\.m3u8[^"']*)""")
    private val hostOnly = Regex("""^https?://([^/]+)""")

    private val autoplayScript = """
        (function(){
          try {
            setInterval(function(){
              var v = document.querySelector('video');
              if (v && v.paused) { v.muted = true; if (v.play) { v.play().catch(function(){}); } }
            }, 800);
          } catch (e) {}
        })();
    """.trimIndent()

    private fun frameHtml(embedUrl: String): String {
        val src = embedUrl.replace("&", "&amp;").replace("\"", "&quot;")
        return "<!DOCTYPE html><html><head><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"></head>" +
            "<body style=\"margin:0;background:#000\"><iframe src=\"$src\" " +
            "allow=\"autoplay; encrypted-media; fullscreen\" allowfullscreen " +
            "style=\"border:0;width:100vw;height:100vh\"></iframe></body></html>"
    }

    private fun originOf(url: String): String =
        Regex("""^(https?://[^/]+)""").find(url)?.groupValues?.get(1) ?: url

    private fun hostOf(url: String): String =
        hostOnly.find(url)?.groupValues?.get(1)?.take(18) ?: "?"

    private fun buildHeaders(captured: Map<String, String>, origin: String): Map<String, String> {
        val h = LinkedHashMap<String, String>()
        captured.forEach { (k, v) -> if (k.lowercase() in KEEP_HEADERS) h[k] = v }
        if (h.keys.none { it.equals("Origin", true) }) h["Origin"] = origin
        if (h.keys.none { it.equals("Referer", true) }) h["Referer"] = "$origin/"
        if (h.keys.none { it.equals("User-Agent", true) }) h["User-Agent"] = USER_AGENT
        return h
    }

    private fun expiryOf(url: String): Long {
        val now = System.currentTimeMillis()
        val ts = expiryRegex.find(url.substringBefore("?"))?.groupValues?.get(1)?.toLongOrNull()
            ?: return now + LINK_TTL_DEFAULT
        val exp = ts * 1000L - 5 * 60_000L
        return if (exp > now + 30_000L) minOf(exp, now + LINK_TTL_MAX) else now + LINK_TTL_DEFAULT
    }

    private fun cached(embedUrl: String): Resolved? {
        val r = linkCache[embedUrl] ?: return null
        return if (System.currentTimeMillis() < r.expiresAt) r else null
    }

    // ---- fast path: page -> /p/<token> -> XOR with the token -> playlist -----

    private fun hexBytes(hex: String): ByteArray? = runCatching {
        ByteArray(hex.length / 2) { hex.substring(it * 2, it * 2 + 2).toInt(16).toByte() }
    }.getOrNull()

    private fun keyVariants(token: String): List<ByteArray> {
        val hex = hexBytes(token)
        val out = ArrayList<ByteArray>()
        hex?.let { out.add(it) }
        out.add(token.toByteArray())
        hex?.let { out.add(it.reversedArray()) }
        out.add(token.reversed().toByteArray())
        return out.filter { it.isNotEmpty() }
    }

    private fun printable(bytes: ByteArray): String {
        val sb = StringBuilder()
        for (b in bytes) {
            val v = b.toInt() and 0xFF
            if (v in 0x20..0x7E) sb.append(v.toChar()) else if (sb.isNotEmpty()) break
        }
        return sb.toString().trim()
    }

    private fun isPlaylistUrl(s: String) =
        s.startsWith("http") && s.contains(".m3u8") && !s.contains(' ') && s.length > 25

    private fun xorDecode(raw: ByteArray, token: String): String? {
        if (raw.isEmpty()) return null
        for (key in keyVariants(token)) {
            val out = ByteArray(raw.size)
            for (i in raw.indices) out[i] = (raw[i].toInt() xor key[i % key.size].toInt()).toByte()
            printable(out).let { if (isPlaylistUrl(it)) return it }
            for (skip in 1..4) {
                if (skip >= out.size) break
                printable(out.copyOfRange(skip, out.size)).let { if (isPlaylistUrl(it)) return it }
            }
        }
        return null
    }

    private fun decodeBody(raw: ByteArray, text: String, token: String): String? {
        xorDecode(raw, token)?.let { return it }
        xorDecode(text.toByteArray(Charsets.UTF_8), token)?.let { return it }
        xorDecode(text.toByteArray(Charsets.ISO_8859_1), token)?.let { return it }
        runCatching {
            val b64 = android.util.Base64.decode(text.trim(), android.util.Base64.DEFAULT)
            xorDecode(b64, token)?.let { return it }
        }
        return null
    }

    /** True only when the URL really answers with a playlist (plain or PNG-wrapped). */
    private suspend fun verify(url: String, origin: String): Boolean {
        if (!isPlaylistUrl(url)) return false
        val res = runCatching {
            app.get(
                url,
                referer = "$origin/",
                headers = mapOf("Origin" to origin, "User-Agent" to USER_AGENT, "Accept" to "*/*"),
                timeout = 8L
            )
        }.getOrNull() ?: return false
        if (!res.isSuccessful) return false
        val bytes = runCatching { res.body.bytes() }.getOrNull()
            ?: runCatching { res.text.toByteArray(Charsets.ISO_8859_1) }.getOrNull()
            ?: return false
        if (bytes.isEmpty()) return false
        val head = String(bytes, 0, minOf(bytes.size, 512), Charsets.ISO_8859_1)
        if (head.contains("#EXTM3U")) return true
        // fake-PNG wrapper: 89 50 4E 47
        return bytes.size > 8 && (bytes[0].toInt() and 0xFF) == 0x89 &&
            bytes[1] == 0x50.toByte() && bytes[2] == 0x4E.toByte() && bytes[3] == 0x47.toByte()
    }

    private suspend fun accept(url: String, origin: String, stage: String, notes: MutableList<String>): Resolved? {
        if (verify(url, origin)) {
            notes.add("$stage:ok")
            val streamOrigin = originOf(url)
            return Resolved(
                url,
                "$streamOrigin/",
                buildHeaders(mapOf("Origin" to origin, "Referer" to "$origin/"), origin),
                expiryOf(url)
            )
        }
        notes.add("$stage:bad@${hostOf(url)}")
        return null
    }

    private suspend fun resolveFast(embedUrl: String, pageUrl: String?, notes: MutableList<String>): Resolved? {
        val base = baseUrl ?: mainUrl
        val referer = pageUrl ?: "$base/"
        val origin = originOf(embedUrl)

        if (m3u8Any.containsMatchIn(embedUrl)) {
            accept(embedUrl, origin, "self", notes)?.let { return it }
        }

        var token: String? = null
        var pUrl: String? = null
        pRegex.find(embedUrl)?.let {
            token = it.groupValues[2]
            pUrl = "$origin/p/${it.groupValues[2]}"
        }

        var html: String? = null
        if (token == null) {
            val page = runCatching {
                app.get(
                    embedUrl,
                    referer = referer,
                    headers = mapOf(
                        "User-Agent" to USER_AGENT,
                        "Accept" to "text/html,application/xhtml+xml,*/*",
                    ),
                    timeout = 14L
                )
            }.getOrNull()

            if (page == null || !page.isSuccessful) {
                notes.add("http:page${page?.code ?: 0}")
                return null
            }
            html = page.text.replace("\\/", "/")

            signedAny.find(html)?.groupValues?.get(1)?.let { direct ->
                val abs = if (direct.startsWith("//")) "https:$direct" else direct
                accept(abs, origin, "signed", notes)?.let { return it }
            }
            pRegex.find(html)?.let { m ->
                token = m.groupValues[2]
                pUrl = m.groupValues[1].takeIf { it.startsWith("http") }?.let { "$it/p/${m.groupValues[2]}" }
                    ?: "$origin/p/${m.groupValues[2]}"
            }
        }

        val tok = token
        if (tok != null) {
            val candidates = LinkedHashSet<String>()
            pUrl?.let { candidates.add(it) }
            candidates.add("$origin/p/$tok")
            runCatching { candidates.add("https://tims-001.sytes.net/p/$tok") }

            for (candidate in candidates) {
                val res = runCatching {
                    app.get(
                        candidate,
                        referer = referer,
                        headers = mapOf("User-Agent" to USER_AGENT, "Accept" to "*/*", "Origin" to origin),
                        timeout = 14L
                    )
                }.getOrNull() ?: continue

                if (!res.isSuccessful) {
                    notes.add("http:p${res.code}@${hostOf(candidate)}")
                    continue
                }
                val raw = runCatching { res.body.bytes() }.getOrNull() ?: ByteArray(0)
                val text = runCatching { res.text }.getOrDefault("")
                val decoded = decodeBody(raw, text, tok)
                if (decoded == null) {
                    notes.add("http:xor-fail len=${if (raw.isNotEmpty()) raw.size else text.length}")
                    continue
                }
                accept(decoded, origin, "p", notes)?.let { return it }
            }
        }

        // last resort: any playlist string sitting in the page
        html?.let { h ->
            m3u8Any.findAll(h).map { it.value }.distinct().take(4).forEach { direct ->
                runCatching { accept(direct, origin, "scan", notes) }.getOrNull()?.let { return it }
            }
        }
        return null
    }

    // ---- slow path: hidden browser ------------------------------------------

    private suspend fun race(attempts: List<suspend () -> Sniffed?>): Sniffed? = coroutineScope {
        val result = CompletableDeferred<Sniffed?>()
        val jobs = attempts.map { a ->
            launch {
                val r = runCatching { a() }.getOrNull()
                if (r != null) result.complete(r)
            }
        }
        launch {
            jobs.forEach { it.join() }
            result.complete(null)
        }
        val r = result.await()
        coroutineContext.cancelChildren()
        r
    }

    private suspend fun doResolve(embedUrl: String, pageUrl: String?, first: Boolean): Resolved? {
        cached(embedUrl)?.let { return it }
        val base = baseUrl ?: mainUrl
        val notes = Collections.synchronizedList(ArrayList<String>())

        resolveFast(embedUrl, pageUrl, notes)?.let {
            notesFor[embedUrl] = synchronized(notes) { notes.joinToString(" ") }
            linkCache[embedUrl] = it
            return it
        }

        val origin = originOf(embedUrl)
        val framePage = pageUrl ?: "$base/"
        val attempts = ArrayList<suspend () -> Sniffed?>()
        attempts.add {
            TimWeb.sniff(framePage, "$base/", frameHtml(embedUrl), null, 26_000L, "frame", notes)
        }
        if (first && pageUrl != null) {
            attempts.add {
                delay(2_000L)
                TimWeb.sniff(pageUrl, "$base/", null, null, 26_000L, "page", notes)
            }
        }
        attempts.add {
            delay(3_500L)
            TimWeb.sniff(embedUrl, "$base/", null, autoplayScript, 24_000L, "direct", notes)
        }

        val hit = race(attempts)
        val text = synchronized(notes) { notes.joinToString(" ") }
        notesFor[embedUrl] = text
        if (hit == null) {
            Log.w(TAG, "resolve failed: $text")
            return null
        }
        val r = Resolved(
            hit.url,
            hit.headers["Referer"] ?: "$origin/",
            buildHeaders(hit.headers, origin),
            expiryOf(hit.url)
        )
        linkCache[embedUrl] = r
        return r
    }

    private fun launchResolve(embedUrl: String, pageUrl: String?, user: Boolean, first: Boolean): Deferred<Resolved?> {
        if (user) wanted.add(embedUrl)
        cached(embedUrl)?.let { return CompletableDeferred(it) }
        inflight[embedUrl]?.let { if (it.isActive) return it }
        if (!user) {
            inflight.entries.toList().forEach { e ->
                if (e.key != embedUrl && e.key !in wanted && e.value.isActive) e.value.cancel()
            }
        }
        val d = scope.async(start = CoroutineStart.LAZY) {
            try {
                doResolve(embedUrl, pageUrl, first)
            } finally {
                wanted.remove(embedUrl)
            }
        }
        inflight[embedUrl] = d
        d.start()
        return d
    }

    override suspend fun loadLinks(
        data: String,
        isCasting: Boolean,
        subtitleCallback: (SubtitleFile) -> Unit,
        callback: (ExtractorLink) -> Unit
    ): Boolean {
        val ld = tryParseJson<LinkData>(data)
        val streams = ld?.streams.orEmpty().filter { it.vip != true && !it.url.isNullOrBlank() }
        if (streams.isEmpty()) return false

        val jobs = streams.mapIndexed { i, s -> s to launchResolve(s.url!!, ld?.page, user = true, first = i == 0) }

        var found = false
        jobs.amap { (stream, job) ->
            val shown = displayName(stream.name, ld?.title)
            val resolved = withTimeoutOrNull(RESOLVE_WAIT) { runCatching { job.await() }.getOrNull() }
            if (resolved != null) {
                runCatching {
                    val proxyHeaders = resolved.headers.filterKeys {
                        it.equals("Origin", true) || it.equals("User-Agent", true)
                    }
                    val proxied = TimProxy.wrap(resolved.url, proxyHeaders)
                    if (proxied != null) {
                        callback(
                            newExtractorLink(
                                source = name,
                                name = shown,
                                url = proxied,
                                type = ExtractorLinkType.M3U8
                            ) {
                                this.referer = ""
                                this.headers = emptyMap()
                                this.quality = Qualities.Unknown.value
                            }
                        )
                        found = true
                    }
                    callback(
                        newExtractorLink(
                            source = name,
                            name = if (proxied != null) "$shown · Backup" else shown,
                            url = resolved.url,
                            type = ExtractorLinkType.M3U8
                        ) {
                            this.referer = resolved.referer
                            this.headers = resolved.headers
                            this.quality = Qualities.Unknown.value
                        }
                    )
                    found = true
                    if (debug) {
                        val note = notesFor[stream.url!!] ?: "ok"
                        callback(
                            newExtractorLink(
                                source = name,
                                name = "⚠ ${resolved.url.substringAfterLast('/')} · $note".take(190),
                                url = "https://invalid.invalid/x.m3u8",
                                type = ExtractorLinkType.M3U8
                            ) {
                                this.quality = Qualities.Unknown.value
                            }
                        )
                    }
                }
            } else {
                Log.w(TAG, "no link for $shown")
                if (debug) {
                    runCatching {
                        val note = notesFor[stream.url!!] ?: "timeout"
                        callback(
                            newExtractorLink(
                                source = name,
                                name = "⚠ $shown: $note".take(190),
                                url = "https://invalid.invalid/x.m3u8",
                                type = ExtractorLinkType.M3U8
                            ) {
                                this.quality = Qualities.Unknown.value
                            }
                        )
                        found = true
                    }
                }
            }
        }
        return found
    }
}
