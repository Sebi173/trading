import os
from sqlalchemy import create_engine

def setup_connection():
    engine = create_engine(f'postgresql://postgres:{os.getenv("postgresp")}@{os.getenv("postgresa")}:5432/dev')
    return engine