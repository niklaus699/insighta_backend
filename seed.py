import json
import uuid6
from app import app, db, Profile
from sqlalchemy.exc import IntegrityError

def seed_data():
    with app.app_context():
        # Create tables if they don't exist
        db.create_all()

        try:
            # Explicitly using utf-8 to handle special characters in names
            with open('seed_profiles.json', 'r', encoding='utf-8') as f:
                data = json.load(f)
                profiles_list = data.get('profiles', data) 
        except FileNotFoundError:
            print("Error: seed_profiles.json not found.")
            return
        except json.JSONDecodeError:
            print("Error: Failed to decode JSON. Check your file format.")
            return

        print(f"Starting seed process for {len(profiles_list)} records...")
        
        count = 0
        batch = []
        
        for item in profiles_list:
            # Idempotency check: Skip if the name already exists in the DB
            exists = Profile.query.filter_by(name=item['name']).first()
            if not exists:
                new_profile = Profile(
                    id=str(uuid6.uuid7()), # Requirement: UUID v7
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
            
            # Commit in batches of 100 for better performance
            if len(batch) >= 100:
                try:
                    db.session.add_all(batch)
                    db.session.commit()
                    batch = [] # Reset the batch
                except IntegrityError:
                    db.session.rollback()
                    print("Integrity Error encountered in batch. Rolling back and skipping.")

        # Final commit for the remaining items in the list
        if batch:
            try:
                db.session.add_all(batch)
                db.session.commit()
            except IntegrityError:
                db.session.rollback()
                print("Integrity Error encountered in final batch.")

        print(f"Success! Seeded {count} new profiles.")

if __name__ == "__main__":
    seed_data()