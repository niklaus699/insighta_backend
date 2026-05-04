from app import app, db, User
from flask_jwt_extended import create_access_token

if __name__ == '__main__':
    with app.app_context():
        user = User.query.filter_by(username='admin').first()
        if not user:
            print('NO_ADMIN')
        else:
            token = create_access_token(identity=user.id, additional_claims={'role': user.role}, expires_delta=None)
            print(token)
