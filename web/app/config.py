import os

class Config:
    DEBUG=True
    SQLALCHEMY_DATABASE_URI = os.getenv(
        "DATABASE_URL", "postgresql://test_user:test1234@localhost:5435/ecommerce"
    )
    SQLALCHEMY_TRACK_MODIFICATIONS = False