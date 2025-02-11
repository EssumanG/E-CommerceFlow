from app  import db
import uuid

from sqlalchemy.dialects.postgresql import UUID

class Ecommerce(db.Model):
    __tablename__ = 'ecommerce'

    id = db.Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_id = db.Column(db.String(200), nullable=False)
    customer_first_name = db.Column(db.String(200), nullable=True)
    customer_last_name = db.Column(db.String(200), nullable=True)
    category_name = db.Column(db.String(200), nullable=True)
    product_name = db.Column(db.Text, nullable=True)
    customer_segment = db.Column(db.String(200), nullable=True)
    customer_city = db.Column(db.String(200), nullable=True)
    customer_state = db.Column(db.String(200), nullable=True)
    customer_country = db.Column(db.String(200), nullable=True)
    customer_region = db.Column(db.String(200), nullable=True)
    delivery_status = db.Column(db.String(100), nullable=True)
    order_date = db.Column(db.Date, nullable=True)
    order_id = db.Column(db.String(200), nullable=False)
    ship_date = db.Column(db.Date, nullable=True)
    shipping_type = db.Column(db.String(100), nullable=True)
    days_for_shipment_scheduled = db.Column(db.Integer, nullable=True)
    days_for_shipment_real = db.Column(db.Integer, nullable=True)
    order_item_discount = db.Column(db.Float, nullable=True)
    sales_per_order = db.Column(db.Integer, nullable=True)
    order_quantity = db.Column(db.Integer, nullable=True)
    profit_per_order = db.Column(db.Float, nullable=True)

    def __repr__(self):
        return f"<Ecommerce order_id={self.order_id}, product={self.product_name}, customer={self.customer_first_name} {self.customer_last_name}>"


# class Ecommerce(db.Model):
#     __tablename__ = 'ecommerce'
#     __table__ = db.Table(
#         'ecommerce', db.Model.metadata,
#         autoload_with=db.engine
#     )

#     def __repr__(self):
#         return f"<Ecommerce order_id={self.order_id}, product={self.product_name}, customer={self.customer_first_name} {self.customer_last_name}>"

class Products(db.Model):
    product_id = db.Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    product_name = db.Column(db.Text, nullable=True)
    category_id = db.Column(UUID(as_uuid=True), default=uuid.uuid4)