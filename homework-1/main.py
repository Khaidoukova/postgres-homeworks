import psycopg2
import csv

"""Скрипт для заполнения данными таблиц в БД Postgres."""
with open('./north_data/employees_data.csv') as f:
    emp_data = []
    data = csv.reader(f, delimiter=",")
    for line in data:
        n = tuple(line)
        emp_data.append(n)

with open('./north_data/customers_data.csv') as f:
    cust_data = []
    data = csv.reader(f, delimiter=",")
    for line in data:
        n = tuple(line)
        cust_data.append(n)

with open('./north_data/orders_data.csv') as f:
    ord_data = []
    data = csv.reader(f, delimiter=",")
    for line in data:
        n = tuple(line)
        ord_data.append(n)


conn = psycopg2.connect(host='localhost', database='north', user='postgres', password='320670')
cur = conn.cursor()
cur.executemany('INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)', emp_data)
cur.executemany('INSERT INTO customers VALUES (%s, %s, %s)', cust_data)
cur.executemany('INSERT INTO orders VALUES (%s, %s, %s, %s, %s)', ord_data)
conn.commit()

cur.close()
conn.close()




