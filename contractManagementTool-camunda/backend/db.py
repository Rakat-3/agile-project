import psycopg2
import os

def get_connection():
    return psycopg2.connect(
        database="contract_db",
        user="contract_user",
        password="contract_pass",
        host="localhost",
        port="5432"
    )
