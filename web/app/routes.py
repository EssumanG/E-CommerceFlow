from flask import Blueprint, jsonify, render_template
from app.models import Ecommerce, Products
from sqlalchemy import func, distinct
main = Blueprint("main", __name__)
from app import db


@main.route("/")
def hello_world():
    executives = [
        {"name": "Essuman Godsaves", "company":"The Company", "title":"CEO"},
        {"name": "Essuman Godsaves", "company":"The Company", "title":"CEO"}
    ]


    columns = [col.name for col in Ecommerce.__table__.columns if not col.primary_key]

    # Generate unique count results
    unique_counts_list = [
        {
            "column_name": column,
            "unique_counts": db.session.query(func.count(distinct(getattr(Ecommerce, column)))).scalar()
        }
        for column in columns
    ]

    return render_template("index.html", columns=unique_counts_list)

@main.route("/get_products_10")
def get_product():
    records = Products.query.limit(10).all()
    return render_template("products.html", products = records)

@main.route("/get_records_10")
def get_records():
    records = Ecommerce.query.limit(10).all()
    return render_template("orders.html", orders = records)
