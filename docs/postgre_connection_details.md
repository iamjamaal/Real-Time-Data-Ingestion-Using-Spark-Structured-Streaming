PostgreSQL Connection Details
=============================

Database Configuration:
-----------------------
Host: localhost
Port: 5432
Database Name: ecommerce_events
Username: spark_user
Password: spark_password

JDBC Connection String:
-----------------------
jdbc:postgresql://postgres:5432/ecommerce_events

Docker Network:
---------------
Network Name: ecommerce_network
Container Name: ecommerce-postgres

Connection Test Command:
------------------------
docker exec ecommerce-postgres psql -U spark_user -d ecommerce_events -c "\dt"

Table Information:
------------------
Table Name: ecommerce_events
Schema: See postgres_setup.sql for full table structure
