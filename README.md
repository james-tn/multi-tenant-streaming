# Demo of multi-tenant streaming ingestion and processing 
Multi-Tenant Streaming from nested json files then performs mapping, validation and writes to CosmosDB/Postgres

Repo structure

- data: contain sample data to simulate client events. Data is copied from https://www.kaggle.com/kyanyoga/sample-sales-data
- src/simulator: contains python client file to generate events. run this program as python data_generator.py --duration 1000  
- src/databrics: contains databricks notebook for streaming processing logic and data write to CosmosDB/PostgresSQL



