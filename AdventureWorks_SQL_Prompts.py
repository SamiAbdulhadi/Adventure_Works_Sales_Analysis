import psycopg2 as pg2
import pandas as pd

filepath = 'C:/Users/Public/Documents/PostgreSQL/Adventure Works Data/'

conn = pg2.connect(database='postgres', user='postgres',password='python2021!')
cur = conn.cursor()

def CreateDataFrame(fetch = 10):
    colnames = [name[0] for name in cur.description]
    if type(fetch) == int:
        return pd.DataFrame(cur.fetchmany(fetch), columns = colnames)
    if fetch == 'all':
        return pd.DataFrame(cur.fetchall(), columns = colnames)

    
#%% Create tables and insert values from csv's
cur.execute("""
            CREATE TABLE customer(
            	customer_key INT PRIMARY KEY,	
            	prefix VARCHAR(15),	
            	firstname VARCHAR(20),
            	lastname VARCHAR(20),
            	birthdate DATE,
            	maritalstatus VARCHAR(3),
            	gender VARCHAR(3),
            	email VARCHAR(50),
            	income MONEY,
            	children INT,
            	education VARCHAR(20),
            	occupation VARCHAR(20),
            	homeowner VARCHAR(3)
            );

            CREATE TABLE territory(
            	territory_key INT PRIMARY KEY,	
            	region VARCHAR(25),	
            	country VARCHAR(25),
            	continent VARCHAR(25)
            );

            CREATE TABLE category(
            	category_key INT PRIMARY KEY,
            	category_name VARCHAR(30)
            );


            CREATE TABLE subcategory(
            	subcategory_key INT PRIMARY KEY,
            	subcategory_name VARCHAR(30),
            	category_key INT REFERENCES category(category_key)
            );

            CREATE TABLE product(
            	product_key INT PRIMARY KEY,
            	subcategory_key	INT REFERENCES subcategory(subcategory_key),
            	sku VARCHAR(30),
            	product_name VARCHAR(100),
            	model VARCHAR(50),
            	description	VARCHAR(300),
            	color VARCHAR(15),
            	size VARCHAR(10),
            	style VARCHAR(10),
            	cost MONEY,
            	price MONEY
            );

            CREATE TABLE sale(
            	orderdate DATE,	
            	stockdate DATE,
            	ordernumber VARCHAR(20),
            	product_key INT REFERENCES product(product_key),
            	customer_key INT REFERENCES customer(customer_key),
            	territory_key INT REFERENCES territory(territory_key),
            	lineitem INT,
            	quantity INT
            );

            CREATE TABLE return(
            	returndate DATE,	
            	territory_key INT REFERENCES territory(territory_key),
            	product_key INT REFERENCES product(product_key),
            	returnquantity INT
            );
            """)
conn.commit()

table_file_dict = {'customer': 'Customers', 
                   'territory': 'Territories', 
                   'category': 'Product_Categories', 
                   'subcategory': 'Product_Subcategories',
                   'product': 'Products',
                   'sale': 'Sales',
                   'return': 'Returns'
                   }


for table, file in table_file_dict.items():
    cur.execute(f"COPY {table} FROM '{filepath}AdventureWorks_{file}.csv' DELIMITER ',' CSV HEADER;")
    conn.commit()


#%% What are the top 10 products with the highest quantity sold?
cur.execute("""
            CREATE VIEW quantity_sold AS
                            SELECT product.product_name, SUM(sale.quantity) AS total_quantity
                            FROM sale
                            INNER JOIN product
                            ON sale.product_key = product.product_key
                            GROUP BY product.product_name
                            ORDER BY SUM(quantity) DESC;
            """)

cur.execute("""                
            SELECT * 
            FROM quantity_sold;
            """)

top_products_sold = CreateDataFrame()

#What are the top 10 products with the highest profit?
cur.execute("""
            CREATE VIEW unit_profit AS
                SELECT product.product_name, product.price-product.cost AS unit_profit
                FROM product
                ORDER BY product.price-product.cost DESC;
            """)
                
cur.execute("""
            SELECT quantity_sold.product_name, unit_profit.unit_profit * quantity_sold.total_quantity AS total_profit
            FROM quantity_sold
            INNER JOIN unit_profit
            ON quantity_sold.product_name = unit_profit.product_name
            ORDER BY unit_profit.unit_profit * quantity_sold.total_quantity DESC;
            """)

top_product_profits = CreateDataFrame()

#%% Who are the top 10 spending customers and what are their incomes?

cur.execute("""
    SELECT customer.firstname, customer.lastname, customer.income, SUM(product.price * sale.quantity) AS total_spent
    FROM sale
    JOIN customer
    ON sale.customer_key = customer.customer_key
    JOIN product
    ON sale.product_key = product.product_key
    GROUP BY customer.firstname, customer.lastname, customer.income
    ORDER BY total_spent DESC;
    """)

top_spending_customers = CreateDataFrame()

#Is there a correlation between customer spending and income?
spending_correlation = CreateDataFrame('all').loc[:, ['income', 'total_spent']]

currency_replacement = {'$':'', ',': ''}

def df_Currency_Conversion(df, column_list):
    for column in column_list:
        for key, value in currency_replacement.items():
            df[column] = df[column].str.replace(key, value)
    return df.astype(float)


spending_correlation = df_Currency_Conversion(spending_correlation, ['income', 'total_spent'])

spending_pearson_corr = spending_correlation.corr(method='pearson')

#With a Pearson coefficient of 0.16, there is a very weak correlation between customer income and spending.

#%% Close connection
conn.close()
