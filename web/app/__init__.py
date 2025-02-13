from flask_sqlalchemy import SQLAlchemy
from flask import Flask
from flask_migrate import Migrate #Import Flask-Migrate
from app.flask_config import Config
from flask_caching import Cache
import redis


db = SQLAlchemy()
cache = Cache()
redis_client = None

def create_app():
    global redis_client
    app = Flask(__name__)
    app.config.from_object(Config)
   
    cache.init_app(app)

    redis_client = redis.StrictRedis.from_url(app.config['CACHE_REDIS_URL'], decode_responses=True)

    db.init_app(app)
    Migrate(app, db) 

    with app.app_context():
        from app.models import Ecommerce
        # db.Model.metadata.reflect(db.engine)

    #Register blueprints
    from app.routes import main
    app.register_blueprint(main)

    return app