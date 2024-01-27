import sqlite3

def create_database():
    conn = sqlite3.connect("database.db")
    cursor = conn.cursor()

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS sales (
        sale_id TEXT PRIMARY KEY
    )
''')
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS plans (
        plan_name TEXT,
        datasource_id TEXT,
        PRIMARY KEY (plan_name, datasource_id),
        FOREIGN KEY (datasource_id) REFERENCES datasources(datasource_id)
    )
''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS datasources (
        datasource_name TEXT PRIMARY KEY,
        uuid TEXT
    )
''')
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS customers (
        customer_id TEXT,
        email TEXT,
        datasource_id TEXT,
        PRIMARY KEY (customer_id, datasource_id),
        FOREIGN KEY (datasource_id) REFERENCES datasources(dsID)
    )
    ''')
    conn.commit()
    return conn, cursor


def add_sale(cursor, sale_id):
    # Check if the sale_id already exists in sales
    cursor.execute('SELECT * FROM sales WHERE sale_id = ?', (sale_id,))
    existing_sale = cursor.fetchone()

    if not existing_sale:
        # Proceed with the insertion
        cursor.execute('INSERT INTO sales (sale_id) VALUES (?)', (sale_id,))
    cursor.connection.commit()
def add_datasource(cursor, datasource_name, datasource_uuid):
    # Check if the datasource_name already exists in datasources
    cursor.execute('SELECT * FROM datasources WHERE datasource_name = ?', (datasource_name,))
    existing_datasource = cursor.fetchone()

    if not existing_datasource:
        # Proceed with the insertion
        cursor.execute('INSERT INTO datasources (datasource_name, uuid) VALUES (?, ?)', (datasource_name, datasource_uuid))

    cursor.connection.commit()

def add_customer(cursor, email, datasource_id):
    # Check if the customer already exists in customers for the given datasource
    cursor.execute('SELECT * FROM customers WHERE email = ? AND datasource_id = ?', (email, datasource_id))
    existing_customer = cursor.fetchone()

    if not existing_customer:
        # Proceed with the insertion
        cursor.execute('INSERT INTO customers (email, datasource_id) VALUES (?, ?)', (email, datasource_id))
    cursor.connection.commit()
    
def add_plan(cursor, plan_name, datasource_id):
    # Check if the plan_name already exists in plans for the given datasource
    cursor.execute('SELECT * FROM plans WHERE plan_name = ? AND datasource_id = ?', (plan_name, datasource_id))
    existing_plan = cursor.fetchone()
    if not existing_plan:
        # Proceed with the insertion
        cursor.execute('INSERT INTO plans (plan_name, datasource_id) VALUES (?, ?)', (plan_name, datasource_id))
    cursor.connection.commit()

def datasource_exists(cursor, dsName):
    cursor.execute('SELECT uuid FROM datasources WHERE datasource_name = ?', (dsName,))
    result = cursor.fetchone()
    return result[0] if result else None
def sale_exists(cursor, sale_id):
    cursor.execute('SELECT COUNT(*) FROM sales WHERE sale_id = ?', (sale_id,))
    return cursor.fetchone()[0] > 0
def customer_exists(cursor, email,datasource_id):
    cursor.execute('SELECT COUNT(*) FROM customers WHERE email = ? AND datasource_id = ?', (email, datasource_id))
    return cursor.fetchone()[0] > 0
def plan_exists(cursor, plan_name, datasource_id):
    cursor.execute('SELECT COUNT(*) FROM plans WHERE plan_name = ? AND datasource_id = ?', (plan_name, datasource_id))
    return cursor.fetchone()[0] > 0



def close_database(conn):
    conn.close()
