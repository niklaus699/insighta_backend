import os
from dotenv import load_dotenv
import requests
from urllib.parse import quote_plus
import uuid6
import csv
import io
import re
import time
from datetime import datetime, timezone, timedelta
from flask import Flask, request, jsonify, make_response, redirect
from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS
from flask_jwt_extended import (
    JWTManager, create_access_token, create_refresh_token,
    jwt_required, get_jwt_identity, get_jwt, decode_token,
    get_jti, set_access_cookies, set_refresh_cookies
)
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

app = Flask(__name__)
# Allow configuring frontend origin from env for local development and deployment
FRONTEND_URL = os.environ.get('FRONTEND_URL', 'http://localhost:3000')
# Include both the configured frontend and common local dev origin; explicit headers/methods allow preflight
CORS(app,
    supports_credentials=True,
    origins=[FRONTEND_URL, 'http://localhost:3000'],
    allow_headers=['Content-Type', 'X-API-Version', 'Authorization', 'X-CSRF-TOKEN'],
    methods=['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS']
)

load_dotenv()
# --- CONFIGURATION ---
database_url = os.getenv('DATABASE_URL')
if database_url and database_url.startswith("postgres://"):
    database_url = database_url.replace("postgres://", "postgresql://", 1)

app.config['SQLALCHEMY_DATABASE_URI'] = database_url  or 'sqlite:///insighta_labs.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['JWT_SECRET_KEY'] = os.environ.get('JWT_SECRET_KEY', 'super-secret-key-change-in-production')
app.config['JWT_ACCESS_TOKEN_EXPIRES'] = timedelta(minutes=3)  # Per Requirement
app.config['JWT_REFRESH_TOKEN_EXPIRES'] = timedelta(minutes=5) # Per Requirement
app.config['JWT_TOKEN_LOCATION'] = ['cookies', 'headers']
app.config['JWT_COOKIE_CSRF_PROTECT'] = True  # Enable CSRF protection (Requirement 4)
# Ensure cookies are sent in cross-site requests
app.config['JWT_COOKIE_SAMESITE'] = 'None' 
app.config['JWT_COOKIE_SECURE'] = True
app.config['JWT_CSRF_CHECK_FORM'] = True 
# Allow JavaScript to read the CSRF cookie (but NOT the JWT tokens)
app.config['JWT_CSRF_COOKIE_HTTPONLY'] = True
app.config['JWT_ACCESS_COOKIE_NAME'] = 'access_token_cookie'
app.config['JWT_REFRESH_COOKIE_NAME'] = 'refresh_token_cookie'
GITHUB_CLIENT_ID = os.environ.get('GITHUB_CLIENT_ID')
GITHUB_CLIENT_SECRET = os.environ.get('GITHUB_CLIENT_SECRET')
GITHUB_REDIRECT_URI = os.environ.get('GITHUB_REDIRECT_URI')

# Token Blacklist (for logout and refresh rotation)
blacklist = set()

db = SQLAlchemy(app)
jwt = JWTManager(app)

# Rate Limiting
limiter = Limiter(
    key_func=get_remote_address,
    app=app,
    # This checks for REDIS_URL in my env vars; defaults to memory for local dev
    storage_uri=os.environ.get("REDIS_URL", "memory://"),
    storage_options={"ssl_cert_reqs": None, "key_prefix": "insighta_app_"},
    default_limits=["200 per day", "50 per hour"]
)

# --- CONSTANTS ---
COUNTRIES_MAP = {
    'tanzania': 'TZ', 'nigeria': 'NG', 'uganda': 'UG', 'sudan': 'SD', 'united states': 'US', 
    'madagascar': 'MG', 'united kingdom': 'GB', 'india': 'IN', 'cameroon': 'CM', 'cape verde': 'CV', 
    'republic of the congo': 'CG', 'mozambique': 'MZ', 'south africa': 'ZA', 'mali': 'ML', 
    'angola': 'AO', 'dr congo': 'CD', 'france': 'FR', 'kenya': 'KE', 'zambia': 'ZM', 
    'eritrea': 'ER', 'gabon': 'GA', 'rwanda': 'RW', 'senegal': 'SN', 'namibia': 'NA', 
    'gambia': 'GM', "côte d'ivoire": 'CI', 'ethiopia': 'ET', 'morocco': 'MA', 'malawi': 'MW', 
    'brazil': 'BR', 'tunisia': 'TN', 'somalia': 'SO', 'ghana': 'GH', 'zimbabwe': 'ZW', 
    'egypt': 'EG', 'benin': 'BJ', 'western sahara': 'EH', 'australia': 'AU', 'china': 'CN', 
    'botswana': 'BW', 'canada': 'CA', 'liberia': 'LR', 'mauritania': 'MR', 'burundi': 'BI', 
    'burkina faso': 'BF', 'central african republic': 'CF', 'mauritius': 'MU', 'algeria': 'DZ', 
    'japan': 'JP', 'guinea-bissau': 'GW', 'eswatini': 'SZ', 'sierra leone': 'SL', 'comoros': 'KM', 
    'seychelles': 'SC', 'south sudan': 'SS', 'germany': 'DE', 'djibouti': 'DJ', 'niger': 'NE', 
    'togo': 'TG', 'lesotho': 'LS', 'chad': 'TD', 'são tomé and príncipe': 'ST', 'libya': 'LY', 
    'guinea': 'GN', 'equatorial guinea': 'GQ'
}

# --- MODELS ---

class User(db.Model):
    id = db.Column(db.String(36), primary_key=True, default=lambda: str(uuid6.uuid7()))
    github_id = db.Column(db.String(50), unique=True, nullable=True)
    username = db.Column(db.String(100))
    email = db.Column(db.String(100))
    avatar_url = db.Column(db.String(255))
    role = db.Column(db.String(20), default='analyst') # 'admin' or 'analyst'
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
    age_group = db.Column(db.String(20))
    country_id = db.Column(db.String(10), index=True)
    country_name = db.Column(db.String(100))
    country_probability = db.Column(db.Float)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))

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
            "created_at": self.created_at.strftime('%Y-%m-%dT%H:%M:%SZ') if self.created_at else None
        }

class RequestLog(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.String(36), nullable=True)
    endpoint = db.Column(db.String(255))
    method = db.Column(db.String(10))
    status_code = db.Column(db.Integer)
    response_time = db.Column(db.Float)
    timestamp = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))


# --- UTILS ---

@jwt.token_in_blocklist_loader
def check_if_token_in_blocklist(jwt_header, jwt_payload):
    return jwt_payload["jti"] in blacklist

def get_age_group(age):
    if age <= 12: return "child"
    if age <= 19: return "teenager"
    if age <= 59: return "adult"
    return "senior"

def admin_required(fn):
    @jwt_required()
    def wrapper(*args, **kwargs):
        if get_jwt().get("role") != "admin":
            return jsonify({"status": "error", "message": "Admin access required"}), 403
        return fn(*args, **kwargs)
    wrapper.__name__ = fn.__name__
    return wrapper

def apply_filters(query, args):
    filters = {
        "gender": lambda v: Profile.gender == v.lower(),
        "age_group": lambda v: Profile.age_group == v.lower(),
        "country_id": lambda v: Profile.country_id == v.upper(),
        "min_age": lambda v: Profile.age >= int(v),
        "max_age": lambda v: Profile.age <= int(v),
        "min_gender_probability": lambda v: Profile.gender_probability >= float(v),
        "min_country_probability": lambda v: Profile.country_probability >= float(v)
    }
    for param, logic in filters.items():
        val = args.get(param)
        if val:
            query = query.filter(logic(val))
    return query

# --- MIDDLEWARE ---

@app.before_request
def start_timer():
    request.start_time = time.time()

@app.before_request
def enforce_version_and_active():
    if request.path.startswith('/api/'):
        # Allow CORS preflight requests through without API-version enforcement
        if request.method == 'OPTIONS':
            return None

        if request.headers.get('X-API-Version') != '1':
            return jsonify({"status": "error", "message": "API version header required"}), 400

        # Check if user is active if authenticated
        auth_header = request.headers.get("Authorization")
        if auth_header:
            try:
                token = auth_header.split(" ")[1]
                data = decode_token(token)
                user = User.query.get(data['sub'])
                if user and not user.is_active:
                    return jsonify({"status": "error", "message": "User account is disabled"}), 403
            except: pass

@app.after_request
def log_and_response_time(response):
    user_id = None
    try:
        auth_header = request.headers.get("Authorization")
        if auth_header:
            user_id = decode_token(auth_header.split(" ")[1])['sub']
    except: pass

    duration = time.time() - getattr(request, 'start_time', time.time())
    
    log = RequestLog(
        user_id=user_id,
        endpoint=request.path,
        method=request.method,
        status_code=response.status_code,
        response_time=round(duration, 4)
    )
    db.session.add(log)
    db.session.commit()
    return response

# --- AUTH ROUTES ---
@app.route('/')
def index():
    return {"message": "Insighta Backend is running"}, 200


@app.route('/auth/github', methods=['GET'])
def github_redirect():
    # 1. Grab the source (default to web)
    source = request.args.get('source', 'web')
    
    # 2. ALWAYS use the Railway URL here because that's what GitHub has saved
    redirect_uri = GITHUB_REDIRECT_URI
    
    github_url = (
        f"https://github.com/login/oauth/authorize"
        f"?client_id={GITHUB_CLIENT_ID}"
        f"&redirect_uri={quote_plus(redirect_uri)}"
        f"&scope=user:email"
        f"&state={quote_plus(source)}" # We use state to remember 'cli' vs 'web'
    )
    return redirect(github_url)

@app.route('/auth/github/callback', methods=['GET'])
@limiter.limit("10 per minute")
def github_callback():
    code = request.args.get('code')
    state = request.args.get('state', 'web')
    
    if not code:
        return jsonify({"status": "error", "message": "Code required"}), 400

    # 1. Exchange code for GitHub Access Token (Single Use)
    token_resp = requests.post(
        'https://github.com/login/oauth/access_token',
        json={
            'client_id': GITHUB_CLIENT_ID,
            'client_secret': GITHUB_CLIENT_SECRET,
            'code': code,
            'redirect_uri': GITHUB_REDIRECT_URI
        },
        headers={'Accept': 'application/json'}
    ).json()

    if 'access_token' not in token_resp:
        return jsonify({"status": "error", "message": "Invalid code from GitHub"}), 401

    # 2. Get User Info from GitHub
    user_data = requests.get(
        'https://api.github.com/user',
        headers={'Authorization': f"token {token_resp['access_token']}"}
    ).json()

    # 3. Sync User in Database (UUID & Email logic)
    user = User.query.filter_by(github_id=str(user_data['id'])).first()
    if not user:
        user = User.query.filter_by(email=user_data.get('email')).first()
        if user:
            user.github_id = str(user_data['id'])
        else:
            user = User(
                id=str(uuid6.uuid7()),
                github_id=str(user_data['id']),
                username=user_data.get('login'),
                email=user_data.get('email'),
                avatar_url=user_data.get('avatar_url'),
                role='analyst'
            )
            db.session.add(user)
    
    user.last_login_at = datetime.now(timezone.utc)
    db.session.commit()

    # 4. Generate your JWTs
    access = create_access_token(identity=str(user.id), additional_claims={"role": user.role})
    refresh = create_refresh_token(identity=str(user.id))

    # --- FINAL DELIVERY ---
    if 'cli' in state:
        # Pass tokens directly to the CLI local server
        cli_url = f"http://localhost:8001/?access_token={access}&refresh_token={refresh}"
        html = f"<html><head><meta http-equiv=\"refresh\" content=\"0;url={cli_url}\"/></head><body>Login successful! Returning to terminal...</body></html>"
        return make_response(html, 200, {'Content-Type': 'text/html'})

    # Web Portal: Set cookies and redirect
    response = redirect(f"{FRONTEND_URL}/dashboard")
    set_access_cookies(response, access)
    set_refresh_cookies(response, refresh)
    return response

@app.route('/auth/web/callback', methods=['POST'])
@limiter.limit("10 per minute")
def web_callback():
    # 1. Get code from the Next.js frontend request body
    data = request.json
    code = data.get('code')
    if not code:
        return jsonify({"status": "error", "message": "Code required"}), 400

    # 2. Exchange code for GitHub Access Token
    # FIX: Use the Vercel callback URI to match your new github_redirect logic
    web_redirect_uri = GITHUB_REDIRECT_URI
    
    token_resp = requests.post(
        'https://github.com/login/oauth/access_token',
        json={
            'client_id': GITHUB_CLIENT_ID,
            'client_secret': GITHUB_CLIENT_SECRET,
            'code': code,
            'redirect_uri': web_redirect_uri 
        },
        headers={'Accept': 'application/json'}
    ).json()

    if 'access_token' not in token_resp:
        return jsonify({"status": "error", "message": "Invalid code"}), 401

    # 3. Fetch User Data from GitHub API
    user_data = requests.get(
        'https://api.github.com/user',
        headers={'Authorization': f"token {token_resp['access_token']}"}
    ).json()

    # 4. Database Sync (Using logic from app_2.py)[cite: 1]
    user = User.query.filter_by(github_id=str(user_data['id'])).first()
    
    if not user:
        # Check if email matches a seeded user to link them automatically[cite: 1]
        user = User.query.filter_by(email=user_data.get('email')).first()
        if user:
            user.github_id = str(user_data['id'])
        else:
            # Create new user using the uuid6 logic
            user = User(
                id=str(uuid6.uuid7()),
                github_id=str(user_data['id']),
                username=user_data.get('login'),
                email=user_data.get('email'),
                avatar_url=user_data.get('avatar_url'),
                role='analyst'
            )
            db.session.add(user)
    
    user.last_login_at = datetime.now(timezone.utc)
    db.session.commit()

    # 5. Generate JWTs (Requirement 4: HTTP-only cookies)[cite: 1]
    access = create_access_token(identity=str(user.id), additional_claims={"role": user.role})
    refresh = create_refresh_token(identity=str(user.id))

    # 6. Build Response
    response = make_response(jsonify({
        "status": "success",
        "user": {
            "username": user.username,
            "role": user.role,
            "avatar_url": user.avatar_url
        }
    }))

    # Sets 'access_token_cookie' and 'refresh_token_cookie'[cite: 1]
    set_access_cookies(response, access)
    set_refresh_cookies(response, refresh)

    return response


@app.route('/auth/refresh', methods=['POST'])
@jwt_required(refresh=True)
def refresh():
    current_user = get_jwt_identity()
    user = User.query.get(current_user)
    
    # Invalidate old refresh token
    jti = get_jwt()["jti"]
    blacklist.add(jti)

    new_access = create_access_token(identity=current_user, additional_claims={"role": user.role})
    new_refresh = create_refresh_token(identity=current_user)

    return jsonify({
        "status": "success",
        "access_token": new_access,
        "refresh_token": new_refresh
    })

@app.route('/auth/logout', methods=['POST'])
@jwt_required()
def logout():
    jti = get_jwt()["jti"]
    blacklist.add(jti)
    return jsonify({"status": "success", "message": "Logged out"}), 200

# --- PROFILE API ---

@app.route('/api/profiles', methods=['POST'])
@admin_required
def create_profile():
    data = request.json
    if not data or 'name' not in data:
        return jsonify({"status": "error", "message": "Name required"}), 400
    
    name = str(data['name']).lower().strip()
    existing = Profile.query.filter_by(name=name).first()
    if existing:
        return jsonify({"status": "success", "data": existing.to_dict()}), 200

    try:
        g = requests.get(f"https://api.genderize.io?name={name}").json()
        a = requests.get(f"https://api.agify.io?name={name}").json()
        n = requests.get(f"https://api.nationalize.io?name={name}").json()

        top_country = max(n['country'], key=lambda x: x['probability'])
        
        new_p = Profile(
            id=str(uuid6.uuid7()), name=name, gender=g['gender'],
            gender_probability=g['probability'], sample_size=g['count'],
            age=a['age'], age_group=get_age_group(a['age']),
            country_id=top_country['country_id'], country_probability=top_country['probability']
        )
        db.session.add(new_p)
        db.session.commit()
        return jsonify({"status": "success", "data": new_p.to_dict()}), 201
    except:
        return jsonify({"status": "error", "message": "External API failure"}), 502

@app.route('/api/profiles', methods=['GET'])
@jwt_required()
def get_profiles():
    query = Profile.query
    query = apply_filters(query, request.args)

    sort_by = request.args.get('sort_by', 'created_at')
    order = request.args.get('order', 'asc')
    sort_map = {'age': Profile.age, 'created_at': Profile.created_at, 'gender_probability': Profile.gender_probability}
    sort_attr = sort_map.get(sort_by, Profile.created_at)
    query = query.order_by(sort_attr.desc() if order == 'desc' else sort_attr.asc())

    page = int(request.args.get('page', 1))
    limit = min(int(request.args.get('limit', 10)), 50)
    p = query.paginate(page=page, per_page=limit, error_out=False)

    return jsonify({
        "status": "success",
        "page": page,
        "limit": limit,
        "total": p.total,
        "total_pages": p.pages,
        "links": {
            "self": f"/api/profiles?page={page}&limit={limit}",
            "next": f"/api/profiles?page={page+1}&limit={limit}" if p.has_next else None,
            "prev": f"/api/profiles?page={page-1}&limit={limit}" if p.has_prev else None
        },
        "data": [item.to_dict() for item in p.items]
    })

@app.route('/api/profiles/search', methods=['GET'])
@jwt_required()
def search_profiles():
    q = request.args.get('q', '').lower()
    if not q:
        return jsonify({"status": "error", "message": "Query required"}), 400

    query = Profile.query
    interpreted = False

    if 'male' in q and 'female' not in q:
        query = query.filter(Profile.gender == 'male'); interpreted = True
    elif 'female' in q:
        query = query.filter(Profile.gender == 'female'); interpreted = True

    if 'young' in q:
        query = query.filter(Profile.age >= 16, Profile.age <= 24); interpreted = True
    if 'teenager' in q:
        query = query.filter(Profile.age_group == 'teenager'); interpreted = True
    if 'adult' in q:
        query = query.filter(Profile.age_group == 'adult'); interpreted = True

    above_match = re.search(r'above (\d+)', q)
    if above_match:
        query = query.filter(Profile.age > int(above_match.group(1))); interpreted = True

    for country_name, code in COUNTRIES_MAP.items():
        if country_name in q:
            query = query.filter(Profile.country_id == code); interpreted = True
            break

    if not interpreted:
        return jsonify({"status": "error", "message": "Unable to interpret query"}), 422

    page = int(request.args.get('page', 1))
    limit = min(int(request.args.get('limit', 10)), 50)
    p = query.paginate(page=page, per_page=limit, error_out=False)

    return jsonify({
        "status": "success",
        "page": page,
        "limit": limit,
        "total": p.total,
        "total_pages": p.pages,
        "links": {
            "self": f"/api/profiles/search?q={q}&page={page}&limit={limit}",
            "next": f"/api/profiles/search?q={q}&page={page+1}&limit={limit}" if p.has_next else None,
            "prev": f"/api/profiles/search?q={q}&page={page-1}&limit={limit}" if p.has_prev else None
        },
        "data": [item.to_dict() for item in p.items]
    })

@app.route('/api/profiles/<id>', methods=['GET'])
@jwt_required()
def get_single_profile(id):
    p = Profile.query.get(id)
    if not p: return jsonify({"status": "error", "message": "Not found"}), 404
    return jsonify({"status": "success", "data": p.to_dict()})

@app.route('/api/profiles/<id>', methods=['DELETE'])
@admin_required
def delete_profile(id):
    p = Profile.query.get(id)
    if not p: return jsonify({"status": "error", "message": "Not found"}), 404
    db.session.delete(p)
    db.session.commit()
    return '', 204

@app.route('/api/profiles/export', methods=['GET'])
@admin_required
def export_csv():
    query = Profile.query
    query = apply_filters(query, request.args)
    profiles = query.all()

    si = io.StringIO()
    cw = csv.writer(si)
    cw.writerow(['id', 'name', 'gender', 'gender_probability', 'age', 'age_group', 'country_id', 'country_name', 'country_probability', 'created_at'])
    
    for p in profiles:
        cw.writerow([
            p.id, p.name, p.gender, p.gender_probability, p.age, 
            p.age_group, p.country_id, p.country_name, p.country_probability, 
            p.created_at.isoformat() if p.created_at else ''
        ])
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output = make_response(si.getvalue())
    output.headers["Content-Disposition"] = f"attachment; filename=profiles_{timestamp}.csv"
    output.headers["Content-type"] = "text/csv"
    return output

@app.route('/api/stats', methods=['GET'])
@jwt_required()
def get_stats():
    """Provides dashboard metrics for the Web Portal."""
    total_profiles = Profile.query.count()
    total_users = User.query.count()
    
    # Distribution by Gender
    gender_stats = db.session.query(
        Profile.gender, db.func.count(Profile.id)
    ).group_by(Profile.gender).all()
    
    # Recent activity (last 5 added profiles)
    recent_profiles = Profile.query.order_by(Profile.created_at.desc()).limit(5).all()

    return jsonify({
        "status": "success",
        "data": {
            "counts": {
                "profiles": total_profiles,
                "users": total_users
            },
            "distribution": dict(gender_stats),
            "recent": [p.to_dict() for p in recent_profiles]
        }
    })


@app.route('/api/me', methods=['GET'])
@jwt_required()
def get_me():
    # get_jwt_identity() returns the 'sub' claim (user.id) from your token
    user_id = get_jwt_identity()
    user = User.query.get(user_id)

    if not user:
        return jsonify({"msg": "User not found"}), 404
    
    # Matching my frontend's expectation of avatar_url and a wrapped dictionary
    return jsonify({
        "user_dict": {
            "id": user.id,
            "username": user.username,
            "email": user.email,
            "role": user.role,
            "github_id": user.github_id,
            "avatar_url": user.avatar_url # Added this so your <img> tag works
        }
    }), 200

@app.route('/auth/cli/exchange', methods=['POST'])
@limiter.limit("10 per minute")
def cli_exchange():
    data = request.json
    code = data.get('code')
    
    if not code:
        return jsonify({"status": "error", "message": "Code required"}), 400

    # --- THE EXCHANGE LOGIC (Extracted from your callback) ---
    token_resp = requests.post(
        'https://github.com/login/oauth/access_token',
        json={
            'client_id': GITHUB_CLIENT_ID,
            'client_secret': GITHUB_CLIENT_SECRET,
            'code': code,
            'redirect_uri': GITHUB_REDIRECT_URI
        },
        headers={'Accept': 'application/json'}
    ).json()

    if 'access_token' not in token_resp:
        return jsonify({"status": "error", "message": "Invalid code from GitHub"}), 401

    user_data = requests.get(
        'https://api.github.com/user',
        headers={'Authorization': f"token {token_resp['access_token']}"}
    ).json()

    # --- USER SYNC LOGIC ---
    user = User.query.filter_by(github_id=str(user_data['id'])).first()
    if not user:
        user = User.query.filter_by(email=user_data.get('email')).first()
        if user:
            user.github_id = str(user_data['id'])
        else:
            user = User(
                id=str(uuid6.uuid7()),
                github_id=str(user_data['id']),
                username=user_data.get('login'),
                email=user_data.get('email'),
                avatar_url=user_data.get('avatar_url'),
                role='analyst'
            )
            db.session.add(user)
    
    user.last_login_at = datetime.now(timezone.utc)
    db.session.commit()

    # --- GENERATE TOKENS FOR CLI ---
    access = create_access_token(identity=str(user.id), additional_claims={"role": user.role})
    refresh = create_refresh_token(identity=str(user.id))

    return jsonify({
        "status": "success",
        "access_token": access,
        "refresh_token": refresh
    }), 200

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8000))
    app.run(host='0.0.0.0', port=port, debug=os.environ.get("DEBUG", "False").lower() == "true")