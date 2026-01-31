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

Security Notes:
---------------
- These credentials are for development/testing purposes only
- In production, use environment variables or secret management systems
- Never commit credentials to version control
- Consider using connection pooling for better performance

Table Information:
------------------
Table Name: ecommerce_events
Schema: See postgres_setup.sql for full table structure