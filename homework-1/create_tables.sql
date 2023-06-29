-- SQL-команды для создания таблиц
CREATE TABLE employees
(
	employee_id int PRIMARY KEY,
	first_name varchar(100) NOT NULL,
	last_name varchar(100) NOT NULL,
	title varchar(100) NOT NULL,
	birth_date varchar(100),
	notes text
);
CREATE TABLE customers
(
	customer_id varchar(100) PRIMARY KEY,
	company_name varchar(100),
	contact_name varchar(100)
);

CREATE TABLE orders
(
	order_id int PRIMARY KEY,
	customer_id varchar(100) REFERENCES customers(customer_id),
	employee_id int REFERENCES employees(employee_id),
	order_date varchar(100) NOT NULL,
	ship_city varchar(100) NOT NULL
);