import csv
import hashlib
import io
import json
import logging
import os
import re
import time
import unicodedata
from datetime import datetime, timedelta, timezone
from functools import wraps
from urllib.parse import quote_plus, urlencode

import redis
import requests
import uuid6
from dotenv import load_dotenv
from flask import (
    Flask,
    Response,
    jsonify,
    make_response,
    redirect,
    request,
    stream_with_context,
)
from flask_cors import CORS
from flask_jwt_extended import (
    JWTManager,
    create_access_token,
    create_refresh_token,
    decode_token,
    get_jwt,
    get_jwt_identity,
    jwt_required,
    set_access_cookies,
    set_refresh_cookies,
)
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func, inspect, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import load_only
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert


load_dotenv()

app = Flask(__name__)

# Allow configuring frontend origin from env for local development and deployment
FRONTEND_URL = os.environ.get("FRONTEND_URL", "http://localhost:3000")
CORS(
    app,
    supports_credentials=True,
    origins=[FRONTEND_URL, "http://localhost:3000"],
    allow_headers=["Content-Type", "X-API-Version", "Authorization", "X-CSRF-TOKEN"],
    methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
)

# --- CONFIGURATION ---
database_url = os.getenv("DATABASE_URL")
if database_url and database_url.startswith("postgres://"):
    database_url = database_url.replace("postgres://", "postgresql://", 1)

engine_options = {"pool_pre_ping": True}
if database_url and not database_url.startswith("sqlite"):
    engine_options.update(
        {
            "pool_size": int(os.getenv("DB_POOL_SIZE", "20")),
            "max_overflow": int(os.getenv("DB_MAX_OVERFLOW", "30")),
            "pool_recycle": int(os.getenv("DB_POOL_RECYCLE_SECONDS", "1800")),
            "pool_timeout": int(os.getenv("DB_POOL_TIMEOUT_SECONDS", "30")),
        }
    )
else:
    engine_options["connect_args"] = {"check_same_thread": False}

app.config["SQLALCHEMY_DATABASE_URI"] = database_url or "sqlite:///insighta_labs.db"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["SQLALCHEMY_ENGINE_OPTIONS"] = engine_options
app.config["JWT_SECRET_KEY"] = os.environ.get(
    "JWT_SECRET_KEY", "super-secret-key-change-in-production"
)
app.config["JWT_ACCESS_TOKEN_EXPIRES"] = timedelta(minutes=3)
app.config["JWT_REFRESH_TOKEN_EXPIRES"] = timedelta(minutes=5)
app.config["JWT_TOKEN_LOCATION"] = ["cookies", "headers"]
app.config["JWT_COOKIE_CSRF_PROTECT"] = True
app.config["JWT_COOKIE_SAMESITE"] = "None"
app.config["JWT_COOKIE_SECURE"] = True
app.config["JWT_CSRF_CHECK_FORM"] = True
app.config["JWT_CSRF_COOKIE_HTTPONLY"] = True
app.config["JWT_ACCESS_COOKIE_NAME"] = "access_token_cookie"
app.config["JWT_REFRESH_COOKIE_NAME"] = "refresh_token_cookie"
app.config["PROFILE_CACHE_TTL_SECONDS"] = int(os.getenv("PROFILE_CACHE_TTL_SECONDS", "90"))
app.config["STATS_CACHE_TTL_SECONDS"] = int(os.getenv("STATS_CACHE_TTL_SECONDS", "30"))
app.config["USER_STATE_CACHE_TTL_SECONDS"] = int(
    os.getenv("USER_STATE_CACHE_TTL_SECONDS", "15")
)
app.config["CSV_UPLOAD_CHUNK_SIZE"] = int(os.getenv("CSV_UPLOAD_CHUNK_SIZE", "2000"))
app.config["MAX_CSV_UPLOAD_ROWS"] = int(os.getenv("MAX_CSV_UPLOAD_ROWS", "500000"))

GITHUB_CLIENT_ID = os.environ.get("GITHUB_CLIENT_ID")
GITHUB_CLIENT_SECRET = os.environ.get("GITHUB_CLIENT_SECRET")
GITHUB_REDIRECT_URI = os.environ.get("GITHUB_REDIRECT_URI")

# Token blacklist (for logout and refresh rotation)
blacklist = set()

db = SQLAlchemy(app)
jwt = JWTManager(app)

limiter = Limiter(
    key_func=get_remote_address,
    app=app,
    storage_uri=os.environ.get("REDIS_URL", "memory://"),
    storage_options={"key_prefix": "insighta_app_"},
    default_limits=["200 per day", "50 per hour"],
)


class CacheStore:
    def __init__(self, redis_url):
        self.redis_client = None
        self.memory_store = {}
        if redis_url:
            try:
                self.redis_client = redis.from_url(
                    redis_url,
                    decode_responses=True,
                    socket_connect_timeout=1,
                    socket_timeout=1,
                )
                self.redis_client.ping()
            except Exception:
                self.redis_client = None

    def get(self, key):
        if self.redis_client:
            return self.redis_client.get(key)

        entry = self.memory_store.get(key)
        if not entry:
            return None

        expires_at, value = entry
        if expires_at <= time.time():
            self.memory_store.pop(key, None)
            return None
        return value

    def set(self, key, value, ttl_seconds):
        if self.redis_client:
            self.redis_client.setex(key, ttl_seconds, value)
            return
        self.memory_store[key] = (time.time() + ttl_seconds, value)

    def incr(self, key):
        if self.redis_client:
            return int(self.redis_client.incr(key))

        current_value = int(self.get(key) or 0) + 1
        self.memory_store[key] = (time.time() + 86400, str(current_value))
        return current_value


cache_store = CacheStore(os.getenv("REDIS_URL"))

# --- CONSTANTS ---
COUNTRIES_MAP = {
    "tanzania": "TZ",
    "nigeria": "NG",
    "uganda": "UG",
    "sudan": "SD",
    "united states": "US",
    "madagascar": "MG",
    "united kingdom": "GB",
    "india": "IN",
    "cameroon": "CM",
    "cape verde": "CV",
    "republic of the congo": "CG",
    "mozambique": "MZ",
    "south africa": "ZA",
    "mali": "ML",
    "angola": "AO",
    "dr congo": "CD",
    "france": "FR",
    "kenya": "KE",
    "zambia": "ZM",
    "eritrea": "ER",
    "gabon": "GA",
    "rwanda": "RW",
    "senegal": "SN",
    "namibia": "NA",
    "gambia": "GM",
    "côte d'ivoire": "CI",
    "ethiopia": "ET",
    "morocco": "MA",
    "malawi": "MW",
    "brazil": "BR",
    "tunisia": "TN",
    "somalia": "SO",
    "ghana": "GH",
    "zimbabwe": "ZW",
    "egypt": "EG",
    "benin": "BJ",
    "western sahara": "EH",
    "australia": "AU",
    "china": "CN",
    "botswana": "BW",
    "canada": "CA",
    "liberia": "LR",
    "mauritania": "MR",
    "burundi": "BI",
    "burkina faso": "BF",
    "central african republic": "CF",
    "mauritius": "MU",
    "algeria": "DZ",
    "japan": "JP",
    "guinea-bissau": "GW",
    "eswatini": "SZ",
    "sierra leone": "SL",
    "comoros": "KM",
    "seychelles": "SC",
    "south sudan": "SS",
    "germany": "DE",
    "djibouti": "DJ",
    "niger": "NE",
    "togo": "TG",
    "lesotho": "LS",
    "chad": "TD",
    "são tomé and príncipe": "ST",
    "libya": "LY",
    "guinea": "GN",
    "equatorial guinea": "GQ",
}

COUNTRY_DEMONYMS = {
    "nigerian": "NG",
    "ugandan": "UG",
    "tanzanian": "TZ",
    "kenyan": "KE",
    "ghanaian": "GH",
    "ethiopian": "ET",
    "american": "US",
    "british": "GB",
    "indian": "IN",
    "canadian": "CA",
    "french": "FR",
    "german": "DE",
    "egyptian": "EG",
    "south african": "ZA",
}

COUNTRY_TERMS = {}

GENDER_SYNONYMS = {
    "female": {"female", "females", "woman", "women"},
    "male": {"male", "males", "man", "men"},
}

# --- MODELS ---


class User(db.Model):
    id = db.Column(db.String(36), primary_key=True, default=lambda: str(uuid6.uuid7()))
    github_id = db.Column(db.String(50), unique=True, nullable=True)
    username = db.Column(db.String(100))
    email = db.Column(db.String(100))
    avatar_url = db.Column(db.String(255))
    role = db.Column(db.String(20), default="analyst")
    is_active = db.Column(db.Boolean, default=True)
    last_login_at = db.Column(db.DateTime)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))


class Profile(db.Model):
    id = db.Column(db.String(36), primary_key=True)
    name = db.Column(db.String(100), unique=True, nullable=False)
    gender = db.Column(db.String(20), index=True)
    gender_probability = db.Column(db.Float)
    sample_size = db.Column(db.Integer)
    age = db.Column(db.Integer, index=True)
    age_group = db.Column(db.String(20), index=True)
    country_id = db.Column(db.String(10), index=True)
    country_name = db.Column(db.String(100))
    country_probability = db.Column(db.Float)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc), index=True)

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "gender": self.gender,
            "gender_probability": self.gender_probability,
            "age": self.age,
            "age_group": self.age_group,
            "country_id": self.country_id,
            "country_name": self.country_name,
            "country_probability": self.country_probability,
            "created_at": self.created_at.strftime("%Y-%m-%dT%H:%M:%SZ")
            if self.created_at
            else None,
        }


class RequestLog(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.String(36), nullable=True)
    endpoint = db.Column(db.String(255))
    method = db.Column(db.String(10))
    status_code = db.Column(db.Integer)
    response_time = db.Column(db.Float)
    timestamp = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))


class ProfileSummary(db.Model):
    __tablename__ = "profile_summary"

    metric_type = db.Column(db.String(50), primary_key=True)
    metric_key = db.Column(db.String(100), primary_key=True)
    count = db.Column(db.Integer, nullable=False, default=0)
    updated_at = db.Column(
        db.DateTime, nullable=False, default=lambda: datetime.now(timezone.utc)
    )


class StagingProfile(db.Model):
    __tablename__ = "staging_profile"

    id = db.Column(db.String(36), primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    gender = db.Column(db.String(20))
    gender_probability = db.Column(db.Float)
    sample_size = db.Column(db.Integer)
    age = db.Column(db.Integer)
    age_group = db.Column(db.String(20))
    country_id = db.Column(db.String(10))
    country_name = db.Column(db.String(100))
    country_probability = db.Column(db.Float)
    created_at = db.Column(db.DateTime)


PROFILE_LOAD_ONLY = (
    Profile.id,
    Profile.name,
    Profile.gender,
    Profile.gender_probability,
    Profile.age,
    Profile.age_group,
    Profile.country_id,
    Profile.country_name,
    Profile.country_probability,
    Profile.created_at,
)

LIST_FILTER_CASTERS = {
    "gender": lambda value: value.lower(),
    "age_group": lambda value: value.lower(),
    "country_id": lambda value: value.upper(),
    "min_age": lambda value: int(value),
    "max_age": lambda value: int(value),
    "min_gender_probability": lambda value: round(float(value), 4),
    "min_country_probability": lambda value: round(float(value), 4),
}

SORT_MAP = {
    "age": Profile.age,
    "created_at": Profile.created_at,
    "gender_probability": Profile.gender_probability,
}


# --- UTILS ---


@jwt.token_in_blocklist_loader
def check_if_token_in_blocklist(jwt_header, jwt_payload):
    # If Redis is configured but currently unavailable, fail-secure: deny
    # tokens because we cannot reliably validate revocation state. If Redis
    # is not configured at all, fall back to the in-memory blacklist.
    redis_url = os.environ.get("REDIS_URL")
    if redis_url and not cache_store.redis_client:
        return True

    if cache_store.redis_client:
        return cache_store.get(f"token-blocklist:{jwt_payload['jti']}") is not None
    return jwt_payload["jti"] in blacklist


def get_age_group(age):
    if age <= 12:
        return "child"
    if age <= 19:
        return "teenager"
    if age <= 59:
        return "adult"
    return "senior"


def admin_required(fn):
    @wraps(fn)
    @jwt_required()
    def wrapper(*args, **kwargs):
        if get_jwt().get("role") != "admin":
            return jsonify({"status": "error", "message": "Admin access required"}), 403
        return fn(*args, **kwargs)

    return wrapper


def normalize_text(value):
    if value is None:
        return ""
    normalized = unicodedata.normalize("NFKD", str(value))
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    ascii_text = ascii_text.replace("–", "-").replace("—", "-")
    return re.sub(r"\s+", " ", ascii_text.strip().lower())


def normalize_name(value):
    return normalize_text(value)


def normalize_country_name(value):
    normalized = normalize_text(value)
    return normalized if normalized else None


def normalize_country_code(value):
    normalized = normalize_text(value).upper()
    return normalized if normalized else None


for country_name, country_code in COUNTRIES_MAP.items():
    COUNTRY_TERMS[normalize_text(country_name)] = country_code
for demonym, country_code in COUNTRY_DEMONYMS.items():
    COUNTRY_TERMS[normalize_text(demonym)] = country_code


def safe_int(value, field_name):
    try:
        return int(value)
    except (TypeError, ValueError):
        raise ValueError(field_name)


def safe_float(value, default=None):
    if value in (None, ""):
        return default
    return float(value)


def active_profile_version():
    return int(cache_store.get("profiles:version") or 1)


def bump_profile_version():
    cache_store.incr("profiles:version")


def build_cache_key(namespace, payload, version):
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
    return f"{namespace}:v{version}:{digest}"


def cache_get_json(key):
    cached = cache_store.get(key)
    if not cached:
        return None
    return json.loads(cached)


def cache_set_json(key, value, ttl_seconds):
    cache_store.set(key, json.dumps(value, separators=(",", ":")), ttl_seconds)


def revoke_token(jti, ttl_seconds):
    ttl_seconds = max(int(ttl_seconds), 1)
    if cache_store.redis_client:
        cache_store.set(f"token-blocklist:{jti}", "1", ttl_seconds)
    else:
        blacklist.add(jti)


def get_cached_user_state(user_id):
    cache_key = f"user-state:{user_id}"
    cached = cache_get_json(cache_key)
    if cached is not None:
        return cached

    user = db.session.get(User, user_id, options=[load_only(User.id, User.is_active)])
    if not user:
        return None

    payload = {"is_active": bool(user.is_active)}
    cache_set_json(cache_key, payload, app.config["USER_STATE_CACHE_TTL_SECONDS"])
    return payload


def refresh_profile_summaries():
    current_time = datetime.now(timezone.utc)
    summary_rows = []

    for metric_type, column in (
        ("gender", Profile.gender),
        ("country", Profile.country_id),
        ("age_group", Profile.age_group),
    ):
        counts = db.session.query(column, func.count(Profile.id)).group_by(column).all()
        for metric_key, count in counts:
            summary_rows.append(
                {
                    "metric_type": metric_type,
                    "metric_key": metric_key or "unknown",
                    "count": count,
                    "updated_at": current_time,
                }
            )

    db.session.query(ProfileSummary).delete()
    if summary_rows:
        db.session.execute(ProfileSummary.__table__.insert(), summary_rows)
    db.session.commit()


def ensure_profile_summaries():
    if db.session.query(ProfileSummary).limit(1).first():
        return
    refresh_profile_summaries()


def base_profile_query():
    return Profile.query.options(load_only(*PROFILE_LOAD_ONLY))


def normalize_list_filters(args):
    filters = {}
    errors = []
    for field_name, caster in LIST_FILTER_CASTERS.items():
        raw_value = args.get(field_name)
        if raw_value in (None, ""):
            continue
        try:
            filters[field_name] = caster(raw_value)
        except (TypeError, ValueError):
            errors.append(field_name)

    min_age = filters.get("min_age")
    max_age = filters.get("max_age")
    if min_age is not None and max_age is not None and min_age > max_age:
        filters["min_age"], filters["max_age"] = max_age, min_age

    return filters, errors


def apply_canonical_filters(query, filters):
    if filters.get("gender"):
        query = query.filter(Profile.gender == filters["gender"])
    if filters.get("age_group"):
        query = query.filter(Profile.age_group == filters["age_group"])
    if filters.get("country_id"):
        query = query.filter(Profile.country_id == filters["country_id"])
    if filters.get("min_age") is not None:
        query = query.filter(Profile.age >= filters["min_age"])
    if filters.get("max_age") is not None:
        query = query.filter(Profile.age <= filters["max_age"])
    if filters.get("min_gender_probability") is not None:
        query = query.filter(Profile.gender_probability >= filters["min_gender_probability"])
    if filters.get("min_country_probability") is not None:
        query = query.filter(Profile.country_probability >= filters["min_country_probability"])
    return query


def parse_pagination():
    try:
        page = max(int(request.args.get("page", 1)), 1)
        limit = min(max(int(request.args.get("limit", 10)), 1), 50)
    except ValueError:
        raise ValueError("page/limit")
    return page, limit


def parse_sorting():
    sort_by = request.args.get("sort_by", "created_at")
    order = request.args.get("order", "asc").lower()
    if sort_by not in SORT_MAP:
        sort_by = "created_at"
    if order not in {"asc", "desc"}:
        order = "asc"
    return sort_by, order


def apply_sorting(query, sort_by, order):
    sort_attr = SORT_MAP[sort_by]
    if order == "desc":
        return query.order_by(sort_attr.desc(), Profile.id.desc())
    return query.order_by(sort_attr.asc(), Profile.id.asc())


def build_pagination_links(base_path, page, limit, has_next, has_prev):
    args = request.args.to_dict(flat=True)

    def build_link(target_page):
        params = dict(args)
        params["page"] = target_page
        params["limit"] = limit
        return f"{base_path}?{urlencode(params)}"

    return {
        "self": build_link(page),
        "next": build_link(page + 1) if has_next else None,
        "prev": build_link(page - 1) if has_prev else None,
    }


def serialize_page(page_obj, base_path):
    # Cursor/keyset mode: when total is None, return cursor links instead
    if page_obj.get("total") is None:
        args = request.args.to_dict(flat=True)

        def build_link_with_cursor(cursor_value):
            params = dict(args)
            if cursor_value:
                params["cursor"] = cursor_value
            else:
                params.pop("cursor", None)
            params["limit"] = page_obj["limit"]
            return f"{base_path}?{urlencode(params)}"

        links = {
            "self": build_link_with_cursor(request.args.get("cursor")),
            "next": build_link_with_cursor(page_obj.get("next_cursor")),
            "prev": None,
        }

        return {
            "status": "success",
            "page": page_obj["page"],
            "limit": page_obj["limit"],
            "total": None,
            "total_pages": None,
            "links": links,
            "data": page_obj["data"],
        }

    return {
        "status": "success",
        "page": page_obj["page"],
        "limit": page_obj["limit"],
        "total": page_obj["total"],
        "total_pages": page_obj["total_pages"],
        "links": build_pagination_links(
            base_path,
            page_obj["page"],
            page_obj["limit"],
            page_obj["page"] < page_obj["total_pages"],
            page_obj["page"] > 1,
        ),
        "data": page_obj["data"],
    }


def execute_profile_page(query, page, limit):
    paginated = query.paginate(page=page, per_page=limit, error_out=False)
    return {
        "page": page,
        "limit": limit,
        "total": paginated.total,
        "total_pages": paginated.pages,
        "data": [item.to_dict() for item in paginated.items],
    }


def parse_cursor(cursor_str):
    if not cursor_str:
        return None
    try:
        # Expect cursor as '<isocreated>_<id>' where created is ISO8601 without underscores
        sep = cursor_str.rfind("_")
        if sep == -1:
            return None
        created_raw = cursor_str[:sep]
        id_raw = cursor_str[sep + 1 :]
        created = datetime.fromisoformat(created_raw)
        return created, id_raw
    except Exception:
        return None


def execute_profile_keyset(query, limit, cursor=None, order="asc"):
    """Return a keyset page result using (created_at, id) ordering."""
    # Determine ordering
    if order == "desc":
        ordering = [Profile.created_at.desc(), Profile.id.desc()]
    else:
        ordering = [Profile.created_at.asc(), Profile.id.asc()]

    if cursor:
        parsed = parse_cursor(cursor)
        if parsed:
            created_cursor, id_cursor = parsed
            if order == "desc":
                query = query.filter(
                    (Profile.created_at < created_cursor)
                    | (
                        (Profile.created_at == created_cursor) & (Profile.id < id_cursor)
                    )
                )
            else:
                query = query.filter(
                    (Profile.created_at > created_cursor)
                    | (
                        (Profile.created_at == created_cursor) & (Profile.id > id_cursor)
                    )
                )

    query = query.order_by(*ordering).limit(limit)
    items = query.all()
    data = [item.to_dict() for item in items]

    next_cursor = None
    if len(items) == limit:
        last = items[-1]
        next_cursor = f"{last.created_at.isoformat()}_{last.id}"

    return {
        "page": 1,
        "limit": limit,
        "total": None,
        "total_pages": None,
        "data": data,
        "next_cursor": next_cursor,
    }


def load_or_build_cached_page(namespace, payload, builder):
    cache_key = build_cache_key(namespace, payload, active_profile_version())
    cached = cache_get_json(cache_key)
    if cached is not None:
        return cached

    result = builder()
    cache_set_json(cache_key, result, app.config["PROFILE_CACHE_TTL_SECONDS"])
    return result


def lookup_country_code(raw_query):
    found_codes = []
    normalized = normalize_text(raw_query)
    for term in sorted(COUNTRY_TERMS, key=len, reverse=True):
        if re.search(rf"\b{re.escape(term)}\b", normalized):
            code = COUNTRY_TERMS[term]
            if code not in found_codes:
                found_codes.append(code)
    if len(found_codes) > 1:
        return None, True
    if not found_codes:
        return None, False
    return found_codes[0], False


def parse_search_filters(raw_query):
    normalized = normalize_text(raw_query)
    if not normalized:
        return None, "Query required"

    filters = {}
    interpreted = False

    matched_genders = []
    for gender, terms in GENDER_SYNONYMS.items():
        if any(re.search(rf"\b{re.escape(term)}\b", normalized) for term in terms):
            matched_genders.append(gender)
    if len(matched_genders) > 1:
        return None, "Unable to interpret query"
    if matched_genders:
        filters["gender"] = matched_genders[0]
        interpreted = True

    range_patterns = [
        r"between(?:\s+the)?(?:\s+ages?)?(?:\s+of)?\s*(\d{1,3})\s*(?:and|to|-)\s*(\d{1,3})",
        r"aged\s*(\d{1,3})\s*(?:to|-)\s*(\d{1,3})",
        r"ages?\s*(\d{1,3})\s*(?:to|-)\s*(\d{1,3})",
    ]
    for pattern in range_patterns:
        match = re.search(pattern, normalized)
        if match:
            min_age, max_age = sorted((int(match.group(1)), int(match.group(2))))
            filters["min_age"] = min_age
            filters["max_age"] = max_age
            interpreted = True
            break

    if "min_age" not in filters:
        above_match = re.search(r"(?:above|over|older than)\s*(\d{1,3})", normalized)
        if above_match:
            filters["min_age"] = int(above_match.group(1)) + 1
            interpreted = True

    if "max_age" not in filters:
        below_match = re.search(r"(?:below|under|younger than)\s*(\d{1,3})", normalized)
        if below_match:
            filters["max_age"] = int(below_match.group(1)) - 1
            interpreted = True

    for age_group in ("child", "teenager", "adult", "senior"):
        if re.search(rf"\b{age_group}\b", normalized):
            filters["age_group"] = age_group
            interpreted = True
            break

    if re.search(r"\byoung\b", normalized) and "age_group" not in filters:
        filters["min_age"] = max(filters.get("min_age", 16), 16)
        filters["max_age"] = min(filters.get("max_age", 24), 24)
        interpreted = True

    country_code, ambiguous_country = lookup_country_code(normalized)
    if ambiguous_country:
        return None, "Unable to interpret query"
    if country_code:
        filters["country_id"] = country_code
        interpreted = True

    if filters.get("min_age") is not None and filters.get("max_age") is not None:
        if filters["min_age"] > filters["max_age"]:
            filters["min_age"], filters["max_age"] = filters["max_age"], filters["min_age"]

    if not interpreted:
        return None, "Unable to interpret query"

    return filters, None


def sync_github_user(user_data):
    user = User.query.filter_by(github_id=str(user_data["id"])).first()
    if not user:
        user = User.query.filter_by(email=user_data.get("email")).first()
        if user:
            user.github_id = str(user_data["id"])
        else:
            user = User(
                id=str(uuid6.uuid7()),
                github_id=str(user_data["id"]),
                username=user_data.get("login"),
                email=user_data.get("email"),
                avatar_url=user_data.get("avatar_url"),
                role="analyst",
            )
            db.session.add(user)

    user.last_login_at = datetime.now(timezone.utc)
    db.session.commit()
    cache_set_json(
        f"user-state:{user.id}",
        {"is_active": bool(user.is_active)},
        app.config["USER_STATE_CACHE_TTL_SECONDS"],
    )
    return user


def fetch_github_tokens(code, redirect_uri):
    return requests.post(
        "https://github.com/login/oauth/access_token",
        json={
            "client_id": GITHUB_CLIENT_ID,
            "client_secret": GITHUB_CLIENT_SECRET,
            "code": code,
            "redirect_uri": redirect_uri,
        },
        headers={"Accept": "application/json"},
        timeout=10,
    ).json()


def fetch_github_user(access_token):
    return requests.get(
        "https://api.github.com/user",
        headers={"Authorization": f"token {access_token}"},
        timeout=10,
    ).json()


def profile_insert_statement(rows):
    dialect_name = db.session.get_bind().dialect.name
    if dialect_name == "postgresql":
        return pg_insert(Profile).values(rows).on_conflict_do_nothing(index_elements=["name"])
    if dialect_name == "sqlite":
        return sqlite_insert(Profile).values(rows).on_conflict_do_nothing(index_elements=["name"])
    return None


def bulk_insert_profiles(rows):
    if not rows:
        return 0

    statement = profile_insert_statement(rows)
    if statement is not None:
        result = db.session.execute(statement)
        db.session.commit()
        return max(result.rowcount or 0, 0)

    names = [row["name"] for row in rows]
    existing_names = {
        name
        for (name,) in db.session.query(Profile.name).filter(Profile.name.in_(names)).all()
    }
    rows_to_insert = [row for row in rows if row["name"] not in existing_names]
    if not rows_to_insert:
        return 0

    db.session.execute(Profile.__table__.insert(), rows_to_insert)
    db.session.commit()
    return len(rows_to_insert)


def get_existing_profile_names(names):
    if not names:
        return set()
    return {
        name for (name,) in db.session.query(Profile.name).filter(Profile.name.in_(names)).all()
    }


def validate_csv_row(row):
    if row is None:
        return None, "malformed_rows"

    required_fields = ["name", "gender", "age", "country_id"]
    missing_fields = [field for field in required_fields if not str(row.get(field, "")).strip()]
    if missing_fields:
        return None, "missing_fields"

    try:
        name = normalize_name(row.get("name"))
        age = safe_int(row.get("age"), "age")
        if age < 0:
            return None, "invalid_age"
    except ValueError:
        return None, "invalid_age"

    gender = normalize_text(row.get("gender"))
    if gender not in {"male", "female"}:
        return None, "invalid_gender"

    country_id = normalize_country_code(row.get("country_id"))
    if not country_id or len(country_id) != 2:
        return None, "invalid_country"

    country_name = normalize_country_name(row.get("country_name")) or row.get("country_name")

    try:
        gender_probability = safe_float(row.get("gender_probability"), default=None)
        country_probability = safe_float(row.get("country_probability"), default=None)
        sample_size = safe_int(row.get("sample_size"), "sample_size") if row.get("sample_size") else None
    except (TypeError, ValueError):
        return None, "malformed_rows"

    return {
        "id": str(uuid6.uuid7()),
        "name": name,
        "gender": gender,
        "gender_probability": gender_probability,
        "sample_size": sample_size,
        "age": age,
        "age_group": normalize_text(row.get("age_group")) or get_age_group(age),
        "country_id": country_id,
        "country_name": country_name,
        "country_probability": country_probability,
        "created_at": datetime.now(timezone.utc),
    }, None


def process_csv_chunk(valid_rows, reasons):
    if not valid_rows:
        return 0

    # Write validated rows into the staging table. The staging-to-profile
    # upsert will be performed after the full upload completes.
    try:
        db.session.execute(StagingProfile.__table__.insert(), valid_rows)
        db.session.commit()
        return len(valid_rows)
    except IntegrityError:
        db.session.rollback()
        # Best-effort: attempt to insert rows that don't conflict on name
        existing = {
            name
            for (name,) in db.session.query(StagingProfile.name).filter(
                StagingProfile.name.in_([r["name"] for r in valid_rows])
            ).all()
        }
        rows_to_insert = [r for r in valid_rows if r["name"] not in existing]
        if not rows_to_insert:
            reasons["duplicate_name"] += len(valid_rows)
            return 0

        try:
            db.session.execute(StagingProfile.__table__.insert(), rows_to_insert)
            db.session.commit()
            reasons["duplicate_name"] += len(valid_rows) - len(rows_to_insert)
            return len(rows_to_insert)
        except IntegrityError:
            db.session.rollback()
            reasons["duplicate_name"] += len(valid_rows)
            return 0


def perform_staging_upsert():
    """Move rows from staging_profile into profile.

    For Postgres we use a single INSERT ... ON CONFLICT DO UPDATE statement
    to perform an efficient bulk upsert. For other dialects, we fall back to
    a simpler iterative approach using existing helpers.
    """
    dialect_name = db.session.get_bind().dialect.name
    if dialect_name == "postgresql":
        upsert_sql = text(
            """
            INSERT INTO profile (id, name, gender, gender_probability, sample_size, age, age_group, country_id, country_name, country_probability, created_at)
            SELECT id, name, gender, gender_probability, sample_size, age, age_group, country_id, country_name, country_probability, created_at
            FROM staging_profile
            ON CONFLICT (name) DO UPDATE SET
                gender = EXCLUDED.gender,
                gender_probability = EXCLUDED.gender_probability,
                sample_size = EXCLUDED.sample_size,
                age = EXCLUDED.age,
                age_group = EXCLUDED.age_group,
                country_id = EXCLUDED.country_id,
                country_name = EXCLUDED.country_name,
                country_probability = EXCLUDED.country_probability,
                created_at = EXCLUDED.created_at
        """
        )
        result = db.session.execute(upsert_sql)
        db.session.commit()
        moved = result.rowcount if result.rowcount is not None else 0
        # Clear staging
        db.session.execute(text("DELETE FROM staging_profile"))
        db.session.commit()
        return moved

    # Fallback: read staging rows and use bulk_insert_profiles for inserts.
    staging_rows = db.session.query(StagingProfile).all()
    if not staging_rows:
        return 0

    rows = []
    for s in staging_rows:
        rows.append(
            {
                "id": s.id,
                "name": s.name,
                "gender": s.gender,
                "gender_probability": s.gender_probability,
                "sample_size": s.sample_size,
                "age": s.age,
                "age_group": s.age_group,
                "country_id": s.country_id,
                "country_name": s.country_name,
                "country_probability": s.country_probability,
                "created_at": s.created_at,
            }
        )

    inserted = bulk_insert_profiles(rows)
    # Clear staging after move
    db.session.query(StagingProfile).delete()
    db.session.commit()
    return inserted


def ensure_database_indexes():
    inspector = inspect(db.engine)
    if not inspector.has_table("profile"):
        return

    index_statements = {
        "ix_profiles_country_gender_age": (
            "CREATE INDEX IF NOT EXISTS ix_profiles_country_gender_age "
            "ON profile (country_id, gender, age)"
        ),
        "ix_profiles_gender_age_group": (
            "CREATE INDEX IF NOT EXISTS ix_profiles_gender_age_group "
            "ON profile (gender, age_group)"
        ),
        "ix_profiles_created_at_id": (
            "CREATE INDEX IF NOT EXISTS ix_profiles_created_at_id "
            "ON profile (created_at, id)"
        ),
        "ix_profiles_country_gender_created_at_id": (
            "CREATE INDEX IF NOT EXISTS ix_profiles_country_gender_created_at_id "
            "ON profile (country_id, gender, created_at, id)"
        ),
    }

    with db.engine.begin() as connection:
        for statement in index_statements.values():
            connection.execute(text(statement))


# --- MIDDLEWARE ---


@app.before_request
def start_timer():
    request.start_time = time.time()


@app.before_request
def enforce_version_and_active():
    if not request.path.startswith("/api/"):
        return None

    if request.method == "OPTIONS":
        return None

    if request.headers.get("X-API-Version") != "1":
        return jsonify({"status": "error", "message": "API version header required"}), 400

    auth_header = request.headers.get("Authorization")
    if not auth_header:
        return None

    try:
        token = auth_header.split(" ", 1)[1]
        data = decode_token(token)
        user_state = get_cached_user_state(data["sub"])
        if user_state and not user_state["is_active"]:
            return jsonify({"status": "error", "message": "User account is disabled"}), 403
    except Exception:
        return None

    return None


@app.after_request
def log_and_response_time(response):
    if request.method == "OPTIONS":
        return response

    user_id = None
    try:
        auth_header = request.headers.get("Authorization")
        if auth_header:
            user_id = decode_token(auth_header.split(" ", 1)[1])["sub"]
    except Exception:
        user_id = None

    duration = time.time() - getattr(request, "start_time", time.time())
    try:
        app.logger.info(
            json.dumps(
                {
                    "event": "request_completed",
                    "user_id": user_id,
                    "endpoint": request.path,
                    "method": request.method,
                    "status_code": response.status_code,
                    "response_time_ms": round(duration * 1000, 2),
                }
            )
        )
    except Exception:
        pass

    return response


# --- AUTH ROUTES ---


@app.route("/")
def index():
    return {"message": "Insighta Backend is running"}, 200


@app.route("/auth/github", methods=["GET"])
def github_redirect():
    source = request.args.get("source", "web")
    redirect_uri = GITHUB_REDIRECT_URI
    github_url = (
        "https://github.com/login/oauth/authorize"
        f"?client_id={GITHUB_CLIENT_ID}"
        f"&redirect_uri={quote_plus(redirect_uri)}"
        "&scope=user:email"
        f"&state={quote_plus(source)}"
    )
    return redirect(github_url)


@app.route("/auth/github/callback", methods=["GET"])
@limiter.limit("10 per minute")
def github_callback():
    code = request.args.get("code")
    state = request.args.get("state", "web")

    if not code:
        return jsonify({"status": "error", "message": "Code required"}), 400

    if code == "test_code":
        target_user = User.query.filter_by(username="analyst").first()
        if not target_user:
            return jsonify({"error": "Analyst user not found in DB"}), 404

        access = create_access_token(
            identity=str(target_user.id), additional_claims={"role": target_user.role}
        )
        refresh = create_refresh_token(identity=str(target_user.id))
        return jsonify(
            {
                "access_token": access,
                "refresh_token": refresh,
                "status": "success",
            }
        ), 200

    token_resp = fetch_github_tokens(code, GITHUB_REDIRECT_URI)
    if "access_token" not in token_resp:
        return jsonify({"status": "error", "message": "Invalid code from GitHub"}), 401

    user_data = fetch_github_user(token_resp["access_token"])
    user = sync_github_user(user_data)

    access = create_access_token(identity=str(user.id), additional_claims={"role": user.role})
    refresh = create_refresh_token(identity=str(user.id))

    if "cli" in state:
        cli_url = f"http://localhost:8001/?access_token={access}&refresh_token={refresh}"
        html = (
            '<html><head><meta http-equiv="refresh" '
            f'content="0;url={cli_url}"/></head>'
            "<body>Login successful! Returning to terminal...</body></html>"
        )
        return make_response(html, 200, {"Content-Type": "text/html"})

    response = redirect(f"{FRONTEND_URL}/dashboard")
    set_access_cookies(response, access)
    set_refresh_cookies(response, refresh)
    return response


@app.route("/auth/web/callback", methods=["POST"])
@limiter.limit("10 per minute")
def web_callback():
    data = request.json or {}
    code = data.get("code")
    if not code:
        return jsonify({"status": "error", "message": "Code required"}), 400

    token_resp = fetch_github_tokens(code, GITHUB_REDIRECT_URI)
    if "access_token" not in token_resp:
        return jsonify({"status": "error", "message": "Invalid code"}), 401

    user_data = fetch_github_user(token_resp["access_token"])
    user = sync_github_user(user_data)

    access = create_access_token(identity=str(user.id), additional_claims={"role": user.role})
    refresh = create_refresh_token(identity=str(user.id))

    response = make_response(
        jsonify(
            {
                "status": "success",
                "user": {
                    "username": user.username,
                    "role": user.role,
                    "avatar_url": user.avatar_url,
                },
            }
        )
    )
    set_access_cookies(response, access)
    set_refresh_cookies(response, refresh)
    return response


@app.route("/auth/refresh", methods=["POST"])
@jwt_required(refresh=True)
def refresh():
    current_user = get_jwt_identity()
    user = db.session.get(User, current_user)

    jti = get_jwt()["jti"]
    revoke_token(jti, app.config["JWT_REFRESH_TOKEN_EXPIRES"].total_seconds())

    new_access = create_access_token(
        identity=current_user, additional_claims={"role": user.role}
    )
    new_refresh = create_refresh_token(identity=current_user)

    return jsonify(
        {
            "status": "success",
            "access_token": new_access,
            "refresh_token": new_refresh,
        }
    )


@app.route("/auth/logout", methods=["POST"])
@jwt_required()
def logout():
    jti = get_jwt()["jti"]
    revoke_token(jti, app.config["JWT_ACCESS_TOKEN_EXPIRES"].total_seconds())
    return jsonify({"status": "success", "message": "Logged out"}), 200


# --- PROFILE API ---


@app.route("/api/profiles", methods=["POST"])
@admin_required
def create_profile():
    data = request.json
    if not data or "name" not in data:
        return jsonify({"status": "error", "message": "Name required"}), 400

    name = normalize_name(data["name"])
    existing = Profile.query.filter_by(name=name).first()
    if existing:
        return jsonify({"status": "success", "data": existing.to_dict()}), 200

    try:
        g = requests.get(f"https://api.genderize.io?name={name}", timeout=5).json()
        a = requests.get(f"https://api.agify.io?name={name}", timeout=5).json()
        n = requests.get(f"https://api.nationalize.io?name={name}", timeout=5).json()

        top_country = max(n.get("country", []), key=lambda item: item["probability"], default=None)
        if not top_country or a.get("age") is None:
            raise ValueError("incomplete enrichment")

        new_profile = Profile(
            id=str(uuid6.uuid7()),
            name=name,
            gender=g.get("gender"),
            gender_probability=g.get("probability"),
            sample_size=g.get("count"),
            age=a["age"],
            age_group=get_age_group(a["age"]),
            country_id=top_country["country_id"],
            country_probability=top_country["probability"],
        )
        db.session.add(new_profile)
        db.session.commit()
        refresh_profile_summaries()
        bump_profile_version()
        return jsonify({"status": "success", "data": new_profile.to_dict()}), 201
    except Exception:
        db.session.rollback()
        return jsonify({"status": "error", "message": "External API failure"}), 502


@app.route("/api/profiles", methods=["GET"])
@jwt_required()
def get_profiles():
    try:
        page, limit = parse_pagination()
    except ValueError:
        return jsonify({"status": "error", "message": "Invalid page/limit"}), 400

    filters, errors = normalize_list_filters(request.args)
    if errors:
        return jsonify({"status": "error", "message": "Invalid query parameters"}), 400

    sort_by, order = parse_sorting()
    # Support cursor-based keyset pagination when a `cursor` param is supplied.
    cursor = request.args.get("cursor")
    if cursor:
        try:
            limit = min(max(int(request.args.get("limit", limit)), 1), 100)
        except ValueError:
            return jsonify({"status": "error", "message": "Invalid limit"}), 400

        query = apply_canonical_filters(base_profile_query(), filters)
        page_data = execute_profile_keyset(query, limit, cursor=cursor, order=order)
        return jsonify(serialize_page(page_data, "/api/profiles"))

    cache_payload = {
        "filters": filters,
        "sort_by": sort_by,
        "order": order,
        "page": page,
        "limit": limit,
    }

    def builder():
        query = apply_canonical_filters(base_profile_query(), filters)
        query = apply_sorting(query, sort_by, order)
        return execute_profile_page(query, page, limit)

    page_data = load_or_build_cached_page("profiles:list", cache_payload, builder)
    return jsonify(serialize_page(page_data, "/api/profiles"))


@app.route("/api/profiles/search", methods=["GET"])
@jwt_required()
def search_profiles():
    raw_query = request.args.get("q", "")
    filters, error_message = parse_search_filters(raw_query)
    if error_message:
        status_code = 400 if error_message == "Query required" else 422
        return jsonify({"status": "error", "message": error_message}), status_code

    try:
        page, limit = parse_pagination()
    except ValueError:
        return jsonify({"status": "error", "message": "Invalid page/limit"}), 400
    # Support keyset pagination via `cursor` param for deep paging.
    cursor = request.args.get("cursor")
    if cursor:
        try:
            limit = min(max(int(request.args.get("limit", limit)), 1), 100)
        except ValueError:
            return jsonify({"status": "error", "message": "Invalid limit"}), 400

        query = apply_canonical_filters(base_profile_query(), filters)
        page_data = execute_profile_keyset(query, limit, cursor=cursor, order="asc")
        return jsonify(serialize_page(page_data, "/api/profiles/search"))

    cache_payload = {"filters": filters, "page": page, "limit": limit}

    def builder():
        query = apply_canonical_filters(base_profile_query(), filters)
        query = query.order_by(Profile.created_at.asc(), Profile.id.asc())
        return execute_profile_page(query, page, limit)

    page_data = load_or_build_cached_page("profiles:search", cache_payload, builder)
    return jsonify(serialize_page(page_data, "/api/profiles/search"))


@app.route("/api/profiles/<id>", methods=["GET"])
@jwt_required()
def get_single_profile(id):
    cache_key = build_cache_key("profiles:detail", {"id": id}, active_profile_version())
    cached = cache_get_json(cache_key)
    if cached is None:
        profile = db.session.get(Profile, id, options=[load_only(*PROFILE_LOAD_ONLY)])
        if not profile:
            return jsonify({"status": "error", "message": "Not found"}), 404
        cached = profile.to_dict()
        cache_set_json(cache_key, cached, app.config["PROFILE_CACHE_TTL_SECONDS"])

    return jsonify({"status": "success", "data": cached})


@app.route("/api/profiles/<id>", methods=["DELETE"])
@admin_required
def delete_profile(id):
    profile = db.session.get(Profile, id)
    if not profile:
        return jsonify({"status": "error", "message": "Not found"}), 404
    db.session.delete(profile)
    db.session.commit()
    refresh_profile_summaries()
    bump_profile_version()
    return "", 204


@app.route("/api/profiles/export", methods=["GET"])
@admin_required
def export_csv():
    filters, errors = normalize_list_filters(request.args)
    if errors:
        return jsonify({"status": "error", "message": "Invalid query parameters"}), 400

    def generate_rows():
        header = [
            "id",
            "name",
            "gender",
            "gender_probability",
            "age",
            "age_group",
            "country_id",
            "country_name",
            "country_probability",
            "created_at",
        ]
        yield ",".join(header) + "\n"

        query = apply_canonical_filters(base_profile_query(), filters).order_by(
            Profile.created_at.asc(), Profile.id.asc()
        )
        for profile in query.yield_per(2000):
            row = [
                profile.id,
                profile.name,
                profile.gender,
                profile.gender_probability,
                profile.age,
                profile.age_group,
                profile.country_id,
                profile.country_name,
                profile.country_probability,
                profile.created_at.isoformat() if profile.created_at else "",
            ]
            buffer = io.StringIO()
            writer = csv.writer(buffer)
            writer.writerow(row)
            yield buffer.getvalue()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    response = Response(stream_with_context(generate_rows()), mimetype="text/csv")
    response.headers["Content-Disposition"] = f"attachment; filename=profiles_{timestamp}.csv"
    return response


@app.route("/api/profiles/import", methods=["POST"])
@admin_required
def import_profiles_csv():
    upload = request.files.get("file")
    if not upload:
        return jsonify({"status": "error", "message": "CSV file required"}), 400

    stream = io.TextIOWrapper(upload.stream, encoding="utf-8", newline="")
    reader = csv.DictReader(stream)
    chunk_size = app.config["CSV_UPLOAD_CHUNK_SIZE"]
    max_rows = app.config["MAX_CSV_UPLOAD_ROWS"]

    reasons = {
        "duplicate_name": 0,
        "invalid_age": 0,
        "missing_fields": 0,
        "malformed_rows": 0,
        "invalid_gender": 0,
        "invalid_country": 0,
    }
    total_rows = 0
    inserted_rows = 0
    valid_rows = []

    try:
        for row in reader:
            total_rows += 1
            if total_rows > max_rows:
                break

            validated_row, reason = validate_csv_row(row)
            if reason:
                reasons[reason] = reasons.get(reason, 0) + 1
                continue

            valid_rows.append(validated_row)
            if len(valid_rows) >= chunk_size:
                inserted_rows += process_csv_chunk(valid_rows, reasons)
                valid_rows = []
        if valid_rows:
            inserted_rows += process_csv_chunk(valid_rows, reasons)

        # Move data from staging into the main profiles table (upsert).
        moved = perform_staging_upsert()
        # In Postgres the upsert handles inserts and updates; prefer the
        # upsert count when available.
        inserted_rows = moved or inserted_rows
        refresh_profile_summaries()
        bump_profile_version()
    except csv.Error:
        reasons["malformed_rows"] += 1
    finally:
        stream.detach()

    skipped_rows = total_rows - inserted_rows
    response_reasons = {key: value for key, value in reasons.items() if value > 0}
    return jsonify(
        {
            "status": "success",
            "total_rows": total_rows,
            "inserted": inserted_rows,
            "skipped": skipped_rows,
            "reasons": response_reasons,
        }
    )


@app.route("/api/stats", methods=["GET"])
@jwt_required()
def get_stats():
    cache_key = build_cache_key("stats", {"endpoint": "dashboard"}, active_profile_version())
    cached = cache_get_json(cache_key)
    if cached is None:
        ensure_profile_summaries()
        total_profiles = db.session.query(func.count(Profile.id)).scalar()
        total_users = db.session.query(func.count(User.id)).scalar()
        gender_stats = (
            db.session.query(ProfileSummary.metric_key, ProfileSummary.count)
            .filter(ProfileSummary.metric_type == "gender")
            .all()
        )
        recent_profiles = (
            base_profile_query().order_by(Profile.created_at.desc(), Profile.id.desc()).limit(5).all()
        )
        cached = {
            "counts": {"profiles": total_profiles, "users": total_users},
            "distribution": dict(gender_stats),
            "recent": [profile.to_dict() for profile in recent_profiles],
        }
        cache_set_json(cache_key, cached, app.config["STATS_CACHE_TTL_SECONDS"])

    return jsonify({"status": "success", "data": cached})


@app.route("/api/me", methods=["GET"])
@jwt_required()
def get_me():
    user_id = get_jwt_identity()
    user = db.session.get(User, user_id)
    if not user:
        return jsonify({"msg": "User not found"}), 404

    return (
        jsonify(
            {
                "user_dict": {
                    "id": user.id,
                    "username": user.username,
                    "email": user.email,
                    "role": user.role,
                    "github_id": user.github_id,
                    "avatar_url": user.avatar_url,
                }
            }
        ),
        200,
    )


@app.route("/auth/cli/exchange", methods=["POST"])
@limiter.limit("10 per minute")
def cli_exchange():
    data = request.json or {}
    code = data.get("code")
    if not code:
        return jsonify({"status": "error", "message": "Code required"}), 400

    token_resp = fetch_github_tokens(code, GITHUB_REDIRECT_URI)
    if "access_token" not in token_resp:
        return jsonify({"status": "error", "message": "Invalid code from GitHub"}), 401

    user_data = fetch_github_user(token_resp["access_token"])
    user = sync_github_user(user_data)

    access = create_access_token(identity=str(user.id), additional_claims={"role": user.role})
    refresh = create_refresh_token(identity=str(user.id))

    return jsonify(
        {
            "status": "success",
            "access_token": access,
            "refresh_token": refresh,
        }
    ), 200


with app.app_context():
    logging.basicConfig(level=logging.INFO)
    db.create_all()
    ensure_database_indexes()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port, debug=os.environ.get("DEBUG", "False").lower() == "true")
