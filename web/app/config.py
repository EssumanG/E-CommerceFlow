import os

class Config:
    DEBUG=True
    SQLALCHEMY_DATABASE_URI = os.getenv(
        "DATABASE_URL", "postgresql://test_user:VbsthRufva4wgBpEXxuiaQxvRHSM6wEG@dpg-culq5iin91rc73egvqn0-a.oregon-postgres.render.com/ecommerce_hziq"
    )
    SQLALCHEMY_TRACK_MODIFICATIONS = False