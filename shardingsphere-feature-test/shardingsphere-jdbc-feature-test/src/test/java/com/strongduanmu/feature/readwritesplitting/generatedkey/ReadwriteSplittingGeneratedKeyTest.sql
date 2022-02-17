CREATE DATABASE demo_write_ds;

USE demo_write_ds;
    
CREATE TABLE t_order
(
    order_id bigint(20) NOT NULL AUTO_INCREMENT,
    user_id  int(10) DEFAULT NULL,
    content  varchar(100) DEFAULT NULL,
    PRIMARY KEY (order_id)
);
