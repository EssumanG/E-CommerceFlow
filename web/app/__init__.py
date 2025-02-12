from flask_sqlalchemy import SQLAlchemy
from flask import Flask
from flask_migrate import Migrate #Import Flask-Migrate
from app.flask_config import Config


db = SQLAlchemy()


def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)

    db.init_app(app)
    Migrate(app, db) 

    with app.app_context():
        from app.models import Ecommerce
        # db.Model.metadata.reflect(db.engine)

    #Register blueprints
    from app.routes import main
    app.register_blueprint(main)

    return app