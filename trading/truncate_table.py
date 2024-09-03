from sqlalchemy import text

def truncate_table(engine, schema, table):
    with engine.connect() as connection:
        with connection.begin():
            query = f"truncate {schema}.{table};"
            print(query)
            try:
                connection.execute(text(query))
            except Exception as e:
                print(e)