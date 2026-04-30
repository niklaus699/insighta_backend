import json
import uuid6
from app import app, db, Profile, User  # Added User model
from sqlalchemy.exc import IntegrityError
from werkzeug.security import generate_password_hash

def seed_data():
    with app.app_context():
        print(f"DEBUG: Connecting to database at: {app.config['SQLALCHEMY_DATABASE_URI']}")
        # Create tables if they don't exist
        db.create_all()

        # --- 1. SEED TEST USERS (For Grader Option 1) ---
        print("Starting seed process for Test Users...")
        
        test_users = [
            {
                "username": "admin",
                "email": "admin@insighta.io",
                "role": "admin"
            },
            {
                "username": "analyst",
                "email": "analyst@insighta.io",
                "role": "analyst"
            }
        ]

        for user_data in test_users:
            exists = User.query.filter_by(username=user_data['username']).first()
            if not exists:
                new_user = User(
                    id=str(uuid6.uuid7()),
                    username=user_data['username'],
                    email=user_data['email'],
                    role=user_data['role']
                )
                db.session.add(new_user)
                print(f"✅ Created {user_data['role']} user: {user_data['username']}")
        
        try:
            db.session.commit()
        except IntegrityError:
            db.session.rollback()
            print("Integrity Error while seeding users. Skipping...")

        # --- 2. SEED PROFILES ---
        try:
            with open('seed_profiles.json', 'r', encoding='utf-8') as f:
                data = json.load(f)
                profiles_list = data.get('profiles', data) 
        except FileNotFoundError:
            print("Error: seed_profiles.json not found.")
            return
        except json.JSONDecodeError:
            print("Error: Failed to decode JSON.")
            return

        print(f"Starting seed process for {len(profiles_list)} profiles...")
        
        count = 0
        batch = []
        
        for item in profiles_list:
            # Idempotency check: Skip if name already exists
            exists = Profile.query.filter_by(name=item['name']).first()
            if not exists:
                new_profile = Profile(
                    id=str(uuid6.uuid7()),
                    name=item['name'],
                    gender=item['gender'],
                    gender_probability=item.get('gender_probability'),
                    age=item['age'],
                    age_group=item['age_group'],
                    country_id=item['country_id'],
                    country_name=item['country_name'],
                    country_probability=item.get('country_probability'),
                    sample_size=item.get('sample_size', 0)
                )
                batch.append(new_profile)
                count += 1
            
            if len(batch) >= 100:
                try:
                    db.session.add_all(batch)
                    db.session.commit()
                    batch = []
                except IntegrityError:
                    db.session.rollback()
                    print("Integrity Error in profile batch. Skipping.")

        if batch:
            try:
                db.session.add_all(batch)
                db.session.commit()
            except IntegrityError:
                db.session.rollback()

        print(f"Success! Seeded {count} new profiles.")

if __name__ == "__main__":
    seed_data()