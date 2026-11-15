"""Daily health check. Keeps config.json pointing at a working domain and alerts on Telegram."""
import json, os, urllib.request, urllib.parse

CONFIG = "config.json"
UA = {"User-Agent": "Mozilla/5.0"}


def get_json(url):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=20) as r:
        return r.geturl(), json.loads(r.read().decode("utf-8", "replace"))


def probe(domain):
    """Returns the real base URL (follows redirects) if the API answers sanely, else None."""
    try:
        final, data = get_json(domain.rstrip("/") + "/api/channels")
        if isinstance(data.get("channels"), list) and data["channels"]:
            return final.split("/api/")[0].rstrip("/")
    except Exception as e:
        print("probe failed", domain, e)
    return None


def alert(text):
    token, chat = os.getenv("TG_TOKEN"), os.getenv("TG_CHAT")
    print("ALERT:", text)
    if token and chat:
        data = urllib.parse.urlencode({"chat_id": chat, "text": text}).encode()
        try:
            urllib.request.urlopen(f"https://api.telegram.org/bot{token}/sendMessage", data, timeout=15)
        except Exception as e:
            print("telegram failed", e)


cfg = json.load(open(CONFIG))
domains = cfg.get("domains", [])
working = None
for d in domains:
    working = probe(d)
    if working:
        break

if not working:
    alert("TimStreams: no domain in config.json is responding. Needs a manual fix.")
    raise SystemExit(1)

if working != domains[0].rstrip("/"):
    new_list = [working] + [d for d in domains if d.rstrip("/") != working]
    cfg["domains"] = new_list
    json.dump(cfg, open(CONFIG, "w"), indent=2)
    alert(f"TimStreams: domain moved. config.json now starts with {working}")
else:
    print("All good:", working)

# Schema sanity check: warn if the API shape changed
try:
    _, ev = get_json(working + "/api/live-upcoming")
    _, ch = get_json(working + "/api/channels")
    bad = []
    if "events" not in ev:
        bad.append("live-upcoming has no 'events'")
    for c in ch.get("channels", [])[:20]:
        if not c.get("url") or not c.get("name") or not isinstance(c.get("streams"), list):
            bad.append("channels item missing url/name/streams")
            break
    if bad:
        alert("TimStreams: API shape changed: " + "; ".join(bad))
        raise SystemExit(1)
except SystemExit:
    raise
except Exception as e:
    alert(f"TimStreams: schema check failed: {e}")
    raise SystemExit(1)
