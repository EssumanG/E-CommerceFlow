from flask import Blueprint, jsonify, render_template, current_app
from app.models import Ecommerce, Products
from sqlalchemy import func, distinct
from app import db, redis_client
import json, threading
import asyncio
main = Blueprint("main", __name__)


async def compute_summary():
    with current_app.app_context():
        columns = [col.name for col in Ecommerce.__table__.columns if not col.primary_key]
    unique_counts_list = [
        {
            "column_name": column,
            "unique_counts": db.session.query(func.count(distinct(getattr(Ecommerce, column)))).scalar()
        }
        for column in columns
    ]

    redis_client.set('summary', json.dumps(unique_counts_list))

@main.route("/")
async def hello():
    asyncio.create_task(compute_summary())
    return render_template("home.html")


@main.route("/overview")
async def hello_world():
    
    # Generate unique count results
    cache_response = redis_client.get('summary')
    if cache_response:
        print("Cached")
        unique_counts_list = json.loads(cache_response)
        
    else:
        await compute_summary()  # Compute and store it in Redis
        cache_response = redis_client.get('summary')
        unique_counts_list = json.loads(cache_response) if cache_response else []

    return render_template("index.html", columns=unique_counts_list)

@main.route("/get_products_10")
def get_product():
    records = Products.query.limit(10).all()
    return render_template("products.html", products = records)

@main.route("/get_records_10")
def get_records():
    records = Ecommerce.query.limit(10).all()
    return render_template("orders.html", orders = records)


@main.route("/in-progress")
def progress():
    return render_template("dev.html")