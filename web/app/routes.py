from flask import Blueprint, jsonify, render_template
from app.models import Ecommerce, Products
from spark.apps import spark_analysis

main = Blueprint("main", __name__)


@main.route("/")
def hello_world():
    executives = [
        {"name": "Essuman Godsaves", "company":"The Company", "title":"CEO"},
        {"name": "Essuman Godsaves", "company":"The Company", "title":"CEO"}
    ]
    return render_template("index.html", executives=executives)

@main.route("/get_products_10")
def get_product():
    res = spark_analysis.products_analysis.toPandas().to_dict(orien="records")
    records = Products.query.limit(10).all()
    return render_template("products.html", products = records)

@main.route("/get_orders_10")
def get_records():
    records = Ecommerce.query.limit(10).all()
    return render_template("orders.html", orders = records)
