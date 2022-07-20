CREATE DATABASE demo_ds_0;

USE demo_ds_0;
    
CREATE TABLE t_order_0
(
    order_id bigint(20) NOT NULL,
    user_id  int(10) DEFAULT NULL,
    content  varchar(100) DEFAULT NULL,
    PRIMARY KEY (order_id)
);

CREATE TABLE t_order_1
(
    order_id bigint(20) NOT NULL,
    user_id  int(10) DEFAULT NULL,
    content  varchar(100) DEFAULT NULL,
    PRIMARY KEY (order_id)
);

CREATE DATABASE demo_ds_1;

USE demo_ds_1;

CREATE TABLE t_order_0
(
    order_id bigint(20) NOT NULL,
    user_id  int(10) DEFAULT NULL,
    content  varchar(100) DEFAULT NULL,
    PRIMARY KEY (order_id)
);

CREATE TABLE t_order_1
(
    order_id bigint(20) NOT NULL,
    user_id  int(10) DEFAULT NULL,
    content  varchar(100) DEFAULT NULL,
    PRIMARY KEY (order_id)
);
