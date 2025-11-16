"""
FINAL main.py — Save documents in EXACT order/format (website-compatible)
"""

import os
import re
import logging
from datetime import datetime
import requests
from pymongo import MongoClient, ReturnDocument
from telegram import Update
from telegram.ext import ApplicationBuilder, ContextTypes, MessageHandler, filters
from dotenv import load_dotenv

load_dotenv()

# ENV
BOT_TOKEN = os.getenv("BOT_TOKEN")
TMDB_API_KEY = os.getenv("TMDB_API_KEY")
MONGODB_URI = os.getenv("MONGODB_URI")
DB = os.getenv("MONGO_DB_NAME", "moviesdb")
COL = os.getenv("MONGO_COLLECTION", "movies")

if not BOT_TOKEN or not TMDB_API_KEY or not MONGODB_URI:
    raise SystemExit("Missing ENV: BOT_TOKEN / TMDB_API_KEY / MONGODB_URI")

# Logging
logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# Mongo
client = MongoClient(MONGODB_URI)
db = client[DB]
collection = db[COL]

# Helpers
def extract_title_year(caption: str):
    if not caption:
        return {"title": "", "year": None}
    year_match = re.search(r"\b(19|20)\d{2}\b", caption)
    year = year_match.group(0) if year_match else None
    text = caption.lower()
    text = re.sub(r"\[.*?\]|\(.*?\)|\{.*?\}", " ", text)
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    if year:
        text = text.replace(year, "").strip()
    title = " ".join([w.capitalize() for w in text.split()]) if text else ""
    return {"title": title, "year": year}

def detect_category(caption: str):
    c = (caption or "").lower()
    if any(x in c for x in ["tamil", "telugu", "malayalam", "kannada"]):
        return "south"
    if "dubbed" in c or "dual audio" in c:
        return "hollywood"
    if "hindi" in c:
        return "bollywood"
    return "hollywood"

def tmdb_search(query, year=None):
    params = {"api_key": TMDB_API_KEY, "query": query}
    if year: params["year"] = year
    try:
        r = requests.get("https://api.themoviedb.org/3/search/movie", params=params, timeout=10)
        r.raise_for_status()
        return r.json().get("results", [])
    except Exception as e:
        logger.exception("TMDB search failed: %s", e)
        return []

def tmdb_details(id):
    try:
        r = requests.get(
            f"https://api.themoviedb.org/3/movie/{id}",
            params={"api_key": TMDB_API_KEY, "append_to_response": "videos,credits"},
            timeout=10
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.exception("TMDB details failed: %s", e)
        return None

def build_img(path):
    return f"https://image.tmdb.org/t/p/original{path}" if path else ""

# Build document with EXACT order
def build_ordered_doc(tmdb, parsed_title, file_id, category):
    # now timestamp
    now = datetime.utcnow().isoformat() + "Z"

    # actors as comma string
    cast = (tmdb.get("credits", {}) or {}).get("cast", []) if tmdb else []
    actors_str = ", ".join([c.get("name") for c in cast][:10]) if cast else ""

    # director and producer as strings
    crew = (tmdb.get("credits", {}) or {}).get("crew", []) if tmdb else []
    director = ""
    producer = ""
    for member in crew:
        job = (member.get("job") or "").lower()
        if job == "director" and not director:
            director = member.get("name") or ""
        if job == "producer" and not producer:
            producer = member.get("name") or ""

    # trailer
    trailer = ""
    videos = (tmdb.get("videos", {}) or {}).get("results", []) if tmdb else []
    for v in videos:
        if (v.get("type") or "").lower() == "trailer" and (v.get("site") or "").lower() == "youtube":
            key = v.get("key")
            if key:
                trailer = f"https://www.youtube.com/watch?v={key}"
                break

    # genres list
    genres = [g.get("name") for g in (tmdb.get("genres") or [])] if tmdb else []

    # releaseDate and runtime and rating safe
    releaseDate = tmdb.get("release_date") if tmdb and tmdb.get("release_date") else ""
    try:
        runtime = int(tmdb.get("runtime") or 0) if tmdb else 0
    except:
        runtime = 0
    try:
        rating = float(tmdb.get("vote_average") or 0) if tmdb else 0.0
    except:
        rating = 0.0

    # Build ordered dict (regular dict in Python3.7+ preserves insertion)
    doc = {
        # _id will be created by MongoDB automatically when inserting; it will still display above fields in UI
        "title": parsed_title or (tmdb.get("title") if tmdb else "") or "",
        "posterUrl": build_img(tmdb.get("poster_path")) if tmdb else "",
        "backdropUrl": build_img(tmdb.get("backdrop_path")) if tmdb else "",
        "description": tmdb.get("overview") if tmdb else "",
        "category": category or "hollywood",
        "actors": actors_str,
        "director": director,
        "producer": producer,
        "rating": rating,
        "downloadLinks": [],               # empty as screenshot
        "telegramLinks": [file_id or ""],  # ARRAY of STRINGS only
        "seasons": [],
        "trailerLink": trailer,
        "genres": genres,
        "releaseDate": releaseDate,
        "runtime": runtime,
        "tagline": tmdb.get("tagline") if tmdb else "",
        "createdAt": now,
        "updatedAt": now,
        "__v": 0
    }
    return doc

# Handler
async def handle(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not msg:
        return

    media = msg.video or msg.document
    if not media:
        logger.info("Ignoring message without supported media")
        return

    caption = msg.caption or ""
    parsed = extract_title_year(caption)
    title_for_search = parsed["title"] or caption or ""
    year_for_search = parsed["year"]

    # TMDB search attempts
    results = []
    if year_for_search:
        results = tmdb_search(title_for_search, year_for_search)
    if not results:
        results = tmdb_search(title_for_search)
    if not results and caption and caption != title_for_search:
        results = tmdb_search(caption)

    if not results:
        logger.info("No TMDB result for caption: %s", caption)
        # still create minimal safe doc using caption
        now = datetime.utcnow().isoformat() + "Z"
        category = detect_category(caption)
        file_id = media.file_id
        minimal_doc = {
            "title": parsed["title"] or caption,
            "posterUrl": "",
            "backdropUrl": "",
            "description": "",
            "category": category,
            "actors": "",
            "director": "",
            "producer": "",
            "rating": 0.0,
            "downloadLinks": [],
            "telegramLinks": [file_id or ""],
            "seasons": [],
            "trailerLink": "",
            "genres": [{"id": g["id"], "name": g["name"]} for g in tmdb.get("genres", [])],
            "releaseDate": parsed.get("year") or "",
            "runtime": 0,
            "tagline": "",
            "createdAt": now,
            "updatedAt": now,
            "__v": 0
        }
        # upsert via find_one_and_replace to enforce order
        filter_q = {"title": minimal_doc["title"], "releaseDate": minimal_doc["releaseDate"]}
        res = collection.find_one_and_replace(filter_q, minimal_doc, upsert=True, return_document=ReturnDocument.AFTER)
        logger.info("Saved minimal doc (no TMDB) id: %s", res.get("_id") if res else "unknown")
        return

    # we have TMDB id
    tmdb_id = results[0].get("id")
    tmdb = tmdb_details(tmdb_id) if tmdb_id else None
    if not tmdb:
        logger.info("TMDB details not available for id: %s", tmdb_id)
        return

    file_id = media.file_id
    category = detect_category(caption)

    # Build ordered doc
    ordered_doc = build_ordered_doc(tmdb, parsed["title"] or (tmdb.get("title") or ""), file_id, category)

    # Preserve createdAt if document already exists
    filter_q = {"title": ordered_doc["title"], "releaseDate": ordered_doc["releaseDate"]}
    existing = collection.find_one(filter_q)
    if existing:
        ordered_doc["createdAt"] = existing.get("createdAt") or ordered_doc["createdAt"]

    # Upsert by replacing full document (this keeps fields in our insertion order)
    res = collection.find_one_and_replace(filter_q, ordered_doc, upsert=True, return_document=ReturnDocument.AFTER)
    logger.info("Saved/updated movie: %s id: %s", ordered_doc["title"], res.get("_id") if res else "unknown")

# Startup (Pella compatible)
async def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(MessageHandler(filters.ChatType.CHANNEL, handle))

    logger.info("Bot started. Initializing...")
    await app.initialize()
    await app.start()
    await app.updater.start_polling()
    logger.info("Bot running and listening for channel posts...")

if __name__ == "__main__":
    import asyncio
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    loop.create_task(main())
    loop.run_forever()
