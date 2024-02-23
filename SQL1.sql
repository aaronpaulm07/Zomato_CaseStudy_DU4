CREATE DATABASE restaurant_data;
USE restaurant_data;
CREATE TABLE staging_restaurant (
    restaurant_name VARCHAR(255),
    location VARCHAR(255),
    cuisine VARCHAR(255),
    average_cost DECIMAL(10, 2),
    ratings DECIMAL(3, 2),
    votes INT,
    url VARCHAR(255)
);
CREATE TABLE reporting_restaurant (
    restaurant_id INT PRIMARY KEY AUTO_INCREMENT,
    restaurant_name VARCHAR(255),
    location VARCHAR(255),
    cuisine VARCHAR(255),
    average_cost DECIMAL(10, 2),
    ratings DECIMAL(3, 2),
    votes INT,
    url VARCHAR(255)
);
SELECT USER();

SELECT * FROM staging_restaurant LIMIT 5;

CREATE TABLE gettab (
    `NAME` VARCHAR(255),
    `PRICE` DECIMAL(10,2),
    `CUSINE_CATEGORY` VARCHAR(255),
    `CITY` VARCHAR(255),
    `REGION` VARCHAR(255),
    `URL` VARCHAR(255),
    `PAGE NO` INT,
    `CUSINE TYPE` VARCHAR(255),
    `TIMING` VARCHAR(255),
    `RATING_TYPE` VARCHAR(255),
    `RATING` DECIMAL(3,2),
    `VOTES` INT
);

