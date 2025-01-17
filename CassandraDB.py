from flask import Flask, request, jsonify
from cassandra.cluster import Cluster
import uuid

def create_app():
    app = Flask(__name__)
    cluster = Cluster(["localhost"], port=9042)
    session = cluster.connect()
    
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS my_keyspace
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
    """)
    session.set_keyspace("my_keyspace")

    session.execute("""
        CREATE TABLE IF NOT EXISTS warehouses (
            id text PRIMARY KEY,
            name text,
            location text
        )
    """)
    
    session.execute("""
        CREATE TABLE IF NOT EXISTS warehouse_inventory(
            id text,
            inventory_id text,
            category text,
            product_id text,
            amount int,
            description text,
            PRIMARY KEY ((id), inventory_id, product_id)
        )
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS warehouse_inventory_by_category (
            id text,
            category text,
            product_id text,
            amount int,
            description text,
            inventory_id text,
            PRIMARY KEY ((id, category), inventory_id, product_id)
        )
    """)

    @app.route('/warehouses', methods=['PUT'])
    def register_new_warehouse():
        data = request.json
        warehouse_id = data["id"]
        name = data["name"]
        location = data["location"]

        try:
            session.execute(
                "INSERT INTO warehouses (id, name, location) VALUES (%s, %s, %s) IF EXISTS",
                (warehouse_id, name, location)
            )
        
            return {"id": data["id"]}, 201
        except Exception as e:
            # Log the error if necessary
            return "Warehouse with this id exists", 400
    
    @app.route('/warehouses', methods=['GET'])
    def list_all_warehouses():
        rows = session.execute("SELECT * FROM warehouses")
        all_warehouses = [{"id": row.id, "name": row.name, "location": row.location} for row in rows]

        return jsonify(all_warehouses), 200

    @app.route('/warehouses/<warehouse_id>/inventory', methods=['PUT'])
    def add_product(warehouse_id):
        data = request.json
        product_id = data["id"]
        amount = data["amount"]
        description = data["description"]
        category = data["category"]

        # Generate unique inventory ID for each product entry
        inventory_id = str(uuid.uuid4())

        session.execute(
            """
            INSERT INTO warehouse_inventory (id, inventory_id, category, product_id, amount, description)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (warehouse_id, inventory_id, category, product_id, amount, description)
        )

        session.execute(
            """
            INSERT INTO warehouse_inventory_by_category (id, category, product_id, amount, description, inventory_id)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (warehouse_id, category, product_id, amount, description, inventory_id)
        )

        return '', 201

    @app.route('/warehouses/<warehouse_id>/inventory', methods=['GET'])
    def list_all_products_in_warehouse(warehouse_id):
        category = request.args.get("category")

        if category:
            rows = session.execute(
                """
                SELECT product_id, amount, description, category, inventory_id 
                FROM warehouse_inventory_by_category 
                WHERE id = %s AND category = %s
                """,
                (warehouse_id, category)
            )
        else:
            # Fetch all products in the warehouse inventory without category filter
            rows = session.execute(
                """
                SELECT product_id, amount, description, category, inventory_id
                FROM warehouse_inventory 
                WHERE id = %s
                """,
                (warehouse_id,)
            )

        # Format the fetched products as a list of dictionaries
        products = [{
            "id": row.product_id,
            "amount": row.amount,
            "description": row.description,
            "category": row.category,
            } for row in rows]

        return jsonify(products), 201

    @app.route('/warehouses/<warehouse_id>/inventory/<inventory_id>', methods=['GET'])
    def get_product_in_warehouse(warehouse_id, inventory_id):
        # Fetch the specific product in the warehouse's inventory by inventory_id
        row = session.execute(
            """
            SELECT product_id, amount, description, category, inventory_id
            FROM warehouse_inventory
            WHERE id = %s AND inventory_id = %s
            """,
            (warehouse_id, inventory_id)
        ).one()

        if row:
            product = {
                "id": row.product_id,
                "amount": row.amount,
                "description": row.description,
                "category": row.category
            }
            return (product), 201
        else:
            return "Product not found", 404
        
    @app.route('/warehouses/<warehouse_id>/inventory/<inventory_id>/amount', methods=['GET'])
    def get_amount(warehouse_id, inventory_id):
    
        row = session.execute(
            """
            SELECT amount
            FROM warehouse_inventory
            WHERE id = %s AND inventory_id = %s
            """,
            (warehouse_id, inventory_id)
        ).one()

        if row:
            return {"amount": row.amount}, 200
        else:
            return "Product not found", 404
        
    @app.route('/warehouses/<warehouse_id>/inventory/<inventory_id>/amount/change', methods=['POST'])
    def change_amount(warehouse_id, inventory_id):
         data = request.json
         change_amount = data.get("by")

         if change_amount is None or not isinstance(change_amount, int):
            return "Invalid amount change", 400
         
         row = session.execute(
            """
            SELECT  product_id, amount, category 
            FROM warehouse_inventory
            WHERE id = %s AND inventory_id = %s
            """,
            (warehouse_id, inventory_id)
        ).one()

         if not row:
            return "Product not found", 404

            
         prod_id = row.product_id
         cate = row.category
         current_amount = row.amount
         new_amount = current_amount + change_amount

         if new_amount < 0:
            return "Invalid amount change", 400
         
         try:
            # Update the amount in both tables
            session.execute(
                """
                UPDATE warehouse_inventory 
                SET amount = %s 
                WHERE id = %s AND inventory_id = %s AND product_id = %s
                """,
                (new_amount, warehouse_id, inventory_id, prod_id)
            )

            session.execute(
                """
                UPDATE warehouse_inventory_by_category 
                SET amount = %s 
                WHERE id = %s AND category = %s AND inventory_id = %s and product_id = %s
                """,
                (new_amount, warehouse_id, cate, inventory_id, prod_id)
            )

            return "Amount of product changed", 200
         
         except Exception as e:
            # Log the error if necessary
            print(f"Update failed: {e}")
            return "Update failed", 500



    return app

if __name__ == '__main__':
    app = create_app()
    app.run(debug=True)
