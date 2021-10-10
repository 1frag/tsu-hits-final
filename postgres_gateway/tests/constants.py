import os

POSTGRES_DSN = os.getenv('POSTGRES_DSN', 'postgres://postgres:postgres@0.0.0.0:5432/postgres')
