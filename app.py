import streamlit as st
import threading
import time
from datetime import datetime, timedelta, timezone
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from isodate import parse_duration
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# --------------------------- Scheduler Thread Logic ---------------------------

def scheduler_loop():
    """
    Runs in a daemon thread. Sleeps until the next top-of-hour boundary, then calls
    run_once_and_append(). After each run, waits for the next hour.
    """
    time.sleep(1)  # Give Streamlit a moment to finish loading

    while True:
        now = datetime.now()
        next_hour = (now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
        time.sleep((next_hour - now).total_seconds())
        run_once_and_append()

_scheduler_thread = None

def start_scheduler_thread():
    global _scheduler_thread
    if _scheduler_thread is None:
        _scheduler_thread = threading.Thread(target=scheduler_loop, daemon=True)
        _scheduler_thread.start()


# --------------------------- Configuration ---------------------------

st.set_page_config(layout="wide")
API_KEY = st.secrets["youtube"]["api_key"]

CHANNEL_IDS = [
    "UCnC8SAZzQiBGYVSKZ_S3y4Q",
    "UCzwCEE_PchiBULMnAJqhGVg",
    "UCz4a7agVFr1TxU-mpAP8hkw",
    "UCkw1tYo7k8t-Y99bOXuZwhg",
]

GOOGLE_SHEET_URL = (
    "https://docs.google.com/spreadsheets/"
    "d/1kOtceVYuUwMDkaDfhNOUfqlKCTNhKQANpzwk0wqwfj0/edit?gid=0"
)

# This must exactly match the sheet's 10 columns
EXPECTED_HEADER = [
    "Video ID",
    "Channel",
    "Upload Date",
    "Cronjob time",
    "Views",
    "Likes",
    "Comment",
    "VPH",
    "Engagement rate",
    "Engagement rate %"
]


# ----------------------- Google Sheets Helpers ---------------------------

@st.cache_resource(ttl=3600)
def get_google_sheet_client():
    try:
        creds_dict = st.secrets["gcp_service_account"]
        scopes = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scopes)
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"❌ Error setting up Google Sheets client: {e}")
        return None

@st.cache_resource(ttl=3600)
def get_worksheet():
    client = get_google_sheet_client()
    if not client:
        return None
    try:
        return client.open_by_url(GOOGLE_SHEET_URL).worksheet("Sheet1")
    except Exception as e:
        st.error(f"❌ Error opening worksheet 'Sheet1': {e}")
        return None


# ----------------------- YouTube Helper Functions ----------------------------

def create_youtube_client():
    return build("youtube", "v3", developerKey=API_KEY)

def iso8601_to_seconds(duration_str: str) -> int:
    try:
        return int(parse_duration(duration_str).total_seconds())
    except:
        return 0

def get_midnight_ist_utc() -> datetime:
    now_utc = datetime.now(timezone.utc)
    ist_tz = timezone(timedelta(hours=5, minutes=30))
    now_ist = now_utc.astimezone(ist_tz)
    today_ist = now_ist.date()
    midnight_ist = datetime(
        year=today_ist.year,
        month=today_ist.month,
        day=today_ist.day,
        hour=0, minute=0, second=0,
        tzinfo=ist_tz
    )
    return midnight_ist.astimezone(timezone.utc)

def is_within_today(published_at_str: str) -> bool:
    try:
        pub_dt = datetime.fromisoformat(published_at_str.replace("Z", "+00:00")).astimezone(timezone.utc)
    except:
        return False
    start = get_midnight_ist_utc()
    return start <= pub_dt < start + timedelta(days=1)

def retry_youtube_call(func_or_request, *args, **kwargs):
    if hasattr(func_or_request, "execute") and not callable(func_or_request):
        req = func_or_request
        try:
            return req.execute()
        except HttpError as e:
            time.sleep(2)
            try:
                return req.execute()
            except HttpError:
                return None
    else:
        try:
            return func_or_request(*args, **kwargs).execute()
        except HttpError:
            time.sleep(2)
            try:
                return func_or_request(*args, **kwargs).execute()
            except HttpError:
                return None

def discover_videos():
    """
    Discover all videos (> 180s) published “today in IST” across CHANNEL_IDS.
    """
    youtube = create_youtube_client()
    video_to_channel = {}
    video_to_published = {}
    logs = []
    all_ids = []

    for idx, ch_id in enumerate(CHANNEL_IDS, start=1):
        ch_resp = retry_youtube_call(
            youtube.channels().list,
            part="snippet,contentDetails",
            id=ch_id
        )
        if not ch_resp or not ch_resp.get("items"):
            logs.append(f"❌ Error fetching channel {ch_id}")
            return {}, {}, logs, True

        title = ch_resp["items"][0]["snippet"]["title"]
        uploads = ch_resp["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
        logs.append(f"🔍 Checking {idx}/{len(CHANNEL_IDS)}: {title}")

        pl_req = youtube.playlistItems().list(
            part="snippet",
            playlistId=uploads,
            maxResults=50
        )
        while pl_req:
            pl_resp = retry_youtube_call(pl_req)
            if not pl_resp:
                logs.append("❌ PlaylistItems error")
                return {}, {}, logs, True

            for item in pl_resp.get("items", []):
                vid = item["snippet"]["resourceId"]["videoId"]
                pub = item["snippet"]["publishedAt"]
                if not is_within_today(pub):
                    continue

                cd = retry_youtube_call(
                    youtube.videos().list,
                    part="contentDetails,snippet",
                    id=vid
                )
                if not cd or not cd.get("items"):
                    continue

                dur = iso8601_to_seconds(cd["items"][0]["contentDetails"]["duration"])
                if dur > 180:
                    pub_dt = datetime.fromisoformat(
                        cd["items"][0]["snippet"]["publishedAt"].replace("Z", "+00:00")
                    ).astimezone(timezone.utc)
                    video_to_channel[vid] = title
                    video_to_published[vid] = pub_dt
                    all_ids.append(vid)

            pl_req = youtube.playlistItems().list_next(pl_req, pl_resp)

        if all_ids:
            logs.append(f"✅ Found {len(all_ids)} so far")

    if not all_ids:
        logs.append("ℹ️ No long videos found today")
        return {}, {}, logs, True

    logs.append(f"ℹ️ Total discovered: {len(all_ids)}")
    return video_to_channel, video_to_published, logs, False

def fetch_statistics(video_ids):
    yt = create_youtube_client()
    out = {}
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i+50]
        resp = retry_youtube_call(
            yt.videos().list,
            part="statistics",
            id=",".join(batch)
        )
        if not resp:
            continue
        for itm in resp.get("items", []):
            stats = itm.get("statistics", {})
            out[itm["id"]] = {
                "viewCount": int(stats.get("viewCount", 0)),
                "likeCount": int(stats.get("likeCount", 0)),
                "commentCount": int(stats.get("commentCount", 0)),
            }
    return out


# ----------------------- Core “Run Now” Function ----------------------------

def run_once_and_append():
    st.info("🔍 Reading sheet…")
    ws = get_worksheet()
    if ws is None:
        st.error("Cannot connect to Google Sheet")
        return

    try:
        data = ws.get_all_values()
    except Exception as e:
        st.error(f"❌ {e}")
        return

    header = data[0] if data else []
    rows = data[1:] if len(data) > 1 else []

    if header != EXPECTED_HEADER:
        try:
            ws.clear()
            ws.append_row(EXPECTED_HEADER, value_input_option="RAW")
            data = ws.get_all_values()
            header = data[0]
            rows = []
            st.success("✔️ Initialized header")
        except Exception as e:
            st.error(f"❌ {e}")
            return

    # index lookups
    idxVid     = header.index("Video ID")
    idxCh      = header.index("Channel")
    idxUp      = header.index("Upload Date")
    idxCron    = header.index("Cronjob time")

    tracked = set()
    vid_to_ch = {}
    vid_to_pub = {}
    ist_tz = timezone(timedelta(hours=5, minutes=30))

    for r in rows:
        vid = r[idxVid]
        if vid not in tracked:
            tracked.add(vid)
            vid_to_ch[vid] = r[idxCh]
            pub_str = r[idxUp]
            try:
                if "T" in pub_str and pub_str.endswith("Z"):
                    dt = datetime.fromisoformat(pub_str.replace("Z","+00:00"))
                else:
                    dt = datetime.strptime(pub_str, "%d/%m/%Y %H:%M:%S").replace(tzinfo=ist_tz)
                    dt = dt.astimezone(timezone.utc)
                vid_to_pub[vid] = dt
            except:
                pass

    st.write(f"➡️ Tracking {len(tracked)} video(s)")

    st.info("🔍 Discovering new long videos…")
    new_map, new_pub, logs, no_flag = discover_videos()
    for msg in logs:
        st.write(msg)

    if not no_flag:
        added = 0
        for v, ch in new_map.items():
            if v not in tracked:
                tracked.add(v)
                vid_to_ch[v] = ch
                vid_to_pub[v] = new_pub[v]
                added += 1
        st.success(f"ℹ️ Added {added} new video(s), now tracking {len(tracked)} total.")
    else:
        st.warning("ℹ️ No new videos; polling existing only.")

    if not tracked:
        st.warning("⚠️ Nothing to track.")
        return

    st.info(f"🕒 Fetching stats for {len(tracked)} videos…")
    stats = fetch_statistics(list(tracked))
    if not stats:
        st.error("❌ Stats fetch failed.")
        return

    # collect existing pairs
    existing = {(r[idxVid], r[idxCron]) for r in rows if len(r) > idxCron}

    new_rows = []
    now_ist = datetime.now(timezone.utc).astimezone(ist_tz)
    cron_str = now_ist.strftime("%d/%m/%Y %H:%M:%S")

    for v in tracked:
        if v not in stats or v not in vid_to_pub:
            continue

        up_utc = vid_to_pub[v]
        up_ist = up_utc.astimezone(ist_tz)
        up_str = up_ist.strftime("%d/%m/%Y %H:%M:%S")
        ch = vid_to_ch.get(v, "N/A")

        vc = stats[v]["viewCount"]
        lc = stats[v]["likeCount"]
        cc = stats[v]["commentCount"]
        hrs = max((datetime.now(timezone.utc) - up_utc).total_seconds() / 3600.0, 1/3600.0)
        vph = vc / hrs
        er = (lc + cc) / vc if vc>0 else 0.0
        er_pct = er*100

        if (v, cron_str) not in existing:
            new_rows.append([
                v, ch, up_str, cron_str,
                str(vc), str(lc), str(cc),
                f"{vph:.2f}", f"{er:.4f}", f"{er_pct:.2f}"
            ])

    st.write(f"➡️ {len(new_rows)} new row(s) to append")

    if new_rows:
        try:
            ws.append_rows(new_rows, value_input_option="RAW")
            st.success(f"✅ Appended {len(new_rows)} row(s).")
        except Exception as e:
            st.error(f"❌ {e}")


# ----------------------- Streamlit Layout ----------------------------

if "scheduler_started" not in st.session_state:
    start_scheduler_thread()
    st.session_state.scheduler_started = True

st.title("📊 YouTube Long Video VPH & Engagement Tracker")

if st.button("▶️ Run Now"):
    run_once_and_append()

st.markdown("---")
st.subheader("Sheet Contents")
ws = get_worksheet()
if ws:
    try:
        all_vals = ws.get_all_values()
        if all_vals:
            df = pd.DataFrame(all_vals[1:], columns=all_vals[0])
            st.dataframe(df, height=600)
        else:
            st.info("ℹ️ Sheet is empty.")
    except Exception as e:
        st.error(f"❌ {e}")
else:
    st.error("❌ Cannot connect to sheet.")
