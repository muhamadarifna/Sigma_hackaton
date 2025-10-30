# connector.py
import logging
from datetime import datetime, timezone
from fivetran_connector_sdk import Connector, Operations as op
from google_play_scraper import reviews, Sort

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -----------------------------
# Utils
# -----------------------------
def to_iso_utc(dt):
    """Konversi datetime (naive/aware) ke ISO UTC string."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc).isoformat()
    return dt.astimezone(timezone.utc).isoformat()

def to_aware_utc(dt):
    """Pastikan datetime aware UTC untuk perbandingan."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def parse_iso_utc(s):
    """Parse ISO string (dengan kemungkinan 'Z') menjadi aware UTC datetime."""
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return to_aware_utc(dt)
    except Exception:
        return None

def resolve_sort(name: str):
    """
    Beberapa versi google-play-scraper tidak punya Sort.HELPFUL.
    Di versi lain, nama yang tersedia adalah MOST_RELEVANT.
    Fallback ke NEWEST jika tidak ada.
    """
    key = (name or "NEWEST").upper()
    if key == "RATING":
        return getattr(Sort, "RATING", Sort.NEWEST)
    if key in ("HELPFUL", "MOST_RELEVANT"):
        return getattr(Sort, "HELPFUL", getattr(Sort, "MOST_RELEVANT", Sort.NEWEST))
    return getattr(Sort, "NEWEST", Sort.NEWEST)

# -----------------------------
# Fetcher
# -----------------------------
def fetch_reviews(app_id: str, lang: str, country: str, sort: str, max_count: int, since_iso: str = None):
    """
    Ambil ulasan dengan paging (continuation_token), stop kalau sudah mencapai max_count
    atau (opsional) bertemu review dengan timestamp <= since_iso (incremental).
    """
    logger.info(f"Fetching reviews for app_id={app_id} lang={lang} country={country} sort={sort} max={max_count}")
    sort_enum = resolve_sort(sort)

    token = None
    out = []
    since_dt = parse_iso_utc(since_iso)
    if since_dt:
        logger.info(f"Incremental cutoff (since): {since_dt.isoformat()}")

    while True:
        remaining = max_count - len(out)
        if remaining <= 0:
            break

        batch, token = reviews(
            app_id,
            lang=lang,
            country=country,
            sort=sort_enum,
            count=min(200, remaining),  # lib limit ~200 per call
            continuation_token=token
        )

        stop_incremental = False
        for r in batch:
            r_at_utc = to_aware_utc(r.get("at"))
            if since_dt is not None and r_at_utc is not None and r_at_utc <= since_dt:
                stop_incremental = True
                break
            out.append(r)

        if stop_incremental or token is None:
            break

    logger.info(f"Fetched {len(out)} rows.")
    return out

# -----------------------------
# Mapping
# -----------------------------
def map_record(r: dict, lang: str, country: str, app_id: str):
    """
    Map field raw dari google_play_scraper -> kolom siap upsert Fivetran.
    Hindari dependency pandas; gunakan dict langsung.
    PENTING: Ubah nama kolom 'at' menjadi 'reviewed_at' karena 'at' adalah reserved keyword SQL.
    """
    return {
        # Keys utama
        "review_id": r.get("reviewId"),
        "user_name": r.get("userName"),
        "score": r.get("score"),
        "thumbs_up_count": r.get("thumbsUpCount"),
        "content": r.get("content"),
        "reply_content": r.get("replyContent"),
        "app_version": r.get("reviewCreatedVersion") or r.get("appVersion"),
        "criteria": r.get("criteria"),

        # Timestamps ISO UTC - UBAH 'at' menjadi 'reviewed_at'
        "reviewed_at": to_iso_utc(r.get("at")),
        "replied_at": to_iso_utc(r.get("repliedAt")),

        # Metadata
        "lang": lang,
        "country": country,
        "app_id": app_id,

        # Fivetran metadata
        "_fivetran_synced": datetime.utcnow().replace(tzinfo=timezone.utc).isoformat(),
    }

# -----------------------------
# Entry point untuk Fivetran
# -----------------------------
def update(configuration, state):
    """
    - Baca config (app_id, lang, country, count, sort) -> SEMUA STRING di configuration.json.
    - Incremental: gunakan state['...']['last_at_iso'] untuk stop di batch berikutnya.
    - Upsert ke table 'playstore_reviews'.
    """
    logger.info("=== START FIVETRAN SYNC (Play Store Reviews) ===")
    logger.info(f"Incoming state: {state}")
    logger.info(f"Configuration keys: {list(configuration.keys())}")

    # --- Read config (semua STRING) ---
    app_id  = configuration.get("app_id", "com.telkomsel.telkomselcm")
    lang    = configuration.get("lang", "id")
    country = configuration.get("country", "id")
    count   = int(configuration.get("count", "100"))
    sort    = configuration.get("sort", "NEWEST")  # NEWEST | RATING | HELPFUL | MOST_RELEVANT

    # Incremental marker per app/locale key
    state = state or {}
    state_key = f"{app_id}|{lang}|{country}|{sort}"
    last_at_iso = None
    if isinstance(state.get(state_key), dict):
        last_at_iso = state[state_key].get("last_at_iso")

    # --- Fetch ---
    rows = fetch_reviews(app_id, lang, country, sort, count, since_iso=last_at_iso)

    # --- Upsert per row ---
    processed, errors = 0, 0
    newest_iso = last_at_iso  # track newest for state
    for r in rows:
        try:
            rec = map_record(r, lang, country, app_id)
            op.upsert(table="playstore_reviews", data=rec)
            processed += 1

            # Track newest timestamp (ISO string) - gunakan 'reviewed_at' bukan 'at'
            if rec.get("reviewed_at"):
                if newest_iso is None or rec["reviewed_at"] > newest_iso:
                    newest_iso = rec["reviewed_at"]
        except Exception as e:
            errors += 1
            logger.exception(f"Failed upsert for review_id={r.get('reviewId')}: {e}")

    logger.info(f"Processed={processed}, Errors={errors}")

    # --- Update state (incremental) ---
    new_state = dict(state)
    new_state[state_key] = {
        "last_at_iso": newest_iso or last_at_iso,
        "last_run_utc": datetime.utcnow().replace(tzinfo=timezone.utc).isoformat(),
        "processed": processed,
        "errors": errors
    }

    logger.info(f"New state: {new_state}")
    logger.info("=== END FIVETRAN SYNC ===")
    return new_state

# Wajib: objek connector
connector = Connector(update=update)