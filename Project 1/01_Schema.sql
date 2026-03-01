show databases;

use Olist_Ecommerce;
## 1. create customer table and then load the data from csv into customer table
create table customers(
	customer_id varchar(50) primary key,
    customer_unique_id varchar(50) not null,
    customer_zip_code_prefix varchar(10) not null,
    customer_city varchar(100) not null,
    customer_state varchar(5) not null
    );
LOAD DATA LOCAL INFILE '/Users/chetan/Downloads/Data Engineering/tute dude assignment/assignment 1/dataset/olist_customers_dataset.csv'
INTO TABLE customers
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

select * from customers limit 5;
describe customers;
drop table customers;

## 2. create orders table and then load the data from csv into customer table
create table orders(
	order_id varchar(50) primary key,
    customer_id varchar(50) not null,
    order_status varchar(20) not null,
    order_purchase_timestamp timestamp not null,
    order_approved_at timestamp null,
    order_delivered_carrier_date timestamp null,
    order_delivered_customer_date timestamp null,
    order_estimated_delivery_date timestamp null,
    foreign key (customer_id) references customers(customer_id)
    );
    
LOAD DATA LOCAL INFILE '/Users/chetan/Downloads/Data Engineering/tute dude assignment/assignment 1/dataset/olist_orders_dataset.csv'
INTO TABLE orders
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

describe orders;
select * from orders limit 5;
drop table orders;

## 3. create products table and then load the data from csv into customer table
CREATE TABLE products (
    product_id VARCHAR(50) PRIMARY KEY,
    product_category_name VARCHAR(100),
    product_name_length INT CHECK (product_name_length >= 0),
    product_description_length INT CHECK (product_description_length >= 0),
    product_photos_qty INT CHECK (product_photos_qty >= 0),
    product_weight_g INT CHECK (product_weight_g >= 0),
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT,
    FOREIGN KEY (product_category_name) REFERENCES product_categories(product_category_name)
);

LOAD DATA LOCAL INFILE '/Users/chetan/Downloads/Data Engineering/tute dude assignment/assignment 1/dataset/olist_products_dataset.csv'
INTO TABLE products
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

describe products;
select * from products limit 5;
drop table products;

## 4. create order items table and then load the data from csv into customer table
create table order_items(
	order_id varchar(50),
    order_item_id int,
    product_id varchar(50) not null,
    seller_id varchar(50) not null,
    shipping_limit_date timestamp,
    price decimal(10,2) check(price>0),
    freight_value decimal(10,2) check (freight_value >0),
    primary key(order_id, order_item_id),
    foreign key(order_id) references orders(order_id),
    foreign key(product_id) references products(product_id),
    FOREIGN KEY (seller_id) REFERENCES sellers(seller_id)
    );

LOAD DATA LOCAL INFILE '/Users/chetan/Downloads/Data Engineering/tute dude assignment/assignment 1/dataset/olist_order_items_dataset.csv'
INTO TABLE order_items
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

describe order_items;
select * from order_items limit 5;
drop table order_items;


## 5. create Payments table and then load the data from csv into customer table
create table payments(
	order_id varchar(50),
    payment_sequential INT,
    payment_type varchar(20) not null,
    payment_installments int check (payment_installments >=0),
    payment_value decimal(10,2) check (payment_value >=0),
    primary key (order_id, payment_sequential),
    foreign key (order_id) references orders(order_id)
);

LOAD DATA LOCAL INFILE '/Users/chetan/Downloads/Data Engineering/tute dude assignment/assignment 1/dataset/olist_order_payments_dataset.csv'
INTO TABLE payments
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

describe payments;
select * from payments limit 5;

drop table payments;


## 6. create Payments table and then load the data from csv into customer table
create table order_reviews(
	review_id varchar(50) primary key,
    order_id varchar(50) not null,
    review_score int check(review_score between 1 and 5),
    review_comment_title text,
    review_comment_message text,
    review_creation_date timestamp,
    review_answer_timestamp timestamp,
    foreign key (order_id) references orders(order_id)
);
select * from order_reviews limit 10; 
describe order_reviews;

LOAD DATA LOCAL INFILE '/Users/chetan/Downloads/Data Engineering/tute dude assignment/assignment 1/dataset/olist_order_reviews_dataset.csv'
INTO TABLE order_reviews
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

drop table order_reviews;


## 7. create Seller table and then load the data from csv into customer table
create table sellers(
	seller_id varchar(50) primary key,
    seller_zip_code_prefix varchar(10) not null,
    seller_city varchar(100) not null,
    seller_state varchar(5) not null
);

LOAD DATA LOCAL INFILE '/Users/chetan/Downloads/Data Engineering/tute dude assignment/assignment 1/dataset/olist_sellers_dataset.csv'
INTO TABLE sellers
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

drop table sellers;

select * from sellers limit 10;

 ## 8. create product Catrgories table and then load the data from csv into customer table
create table product_categories(
	product_category_name varchar(200) primary key,
    product_category_name_english varchar(200) not null
);

LOAD DATA LOCAL INFILE '/Users/chetan/Downloads/Data Engineering/tute dude assignment/assignment 1/dataset/product_category_name_translation.csv'
INTO TABLE product_categories
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

drop table product_categories;

select * from product_categories limit 10;

## 9. create Geolocation table and then load the data from csv into customer table
CREATE TABLE geolocation (
    geolocation_zip_code_prefix VARCHAR(10) NOT NULL,
    geolocation_lat DECIMAL(10,6) NOT NULL,
    geolocation_lng DECIMAL(10,6) NOT NULL,
    geolocation_city VARCHAR(100) NOT NULL,
    geolocation_state VARCHAR(5) NOT NULL,
    
    CHECK (geolocation_lat BETWEEN -90 AND 90),
    CHECK (geolocation_lng BETWEEN -180 AND 180)
);
LOAD DATA LOCAL INFILE '/Users/chetan/Downloads/Data Engineering/tute dude assignment/assignment 1/dataset/olist_geolocation_dataset.csv'
INTO TABLE geolocation
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

drop table geolocation;
select * from geolocation limit 10;

