from sqlalchemy           import create_engine, text, URL
from sqlalchemy.orm       import sessionmaker

# Configuração da URL de conexão
url_object = URL.create(
    "postgresql+pg8000",
    username="dcxhxgql",
    password="bPlk_dl7Xc4l0WEaPCJIYR4dnO9kGZbz",
    host="motty.db.elephantsql.com",
    database="dcxhxgql",
)

engine = create_engine(url_object)
Session = sessionmaker(bind=engine)
session = Session()
conn = engine.connect()

file_path = 'database\postgres\ddl_with_drop.sql'
with open(file_path, 'r') as file:
    sql_script = file.read()

sql_commands = sql_script.split(';')

try:
    with conn.begin():
        for command in sql_commands:
            if command.strip():
                conn.execute(text(command))
    print("CREATE executado com sucesso!")
except Exception as e:
    print(f"Ocorreu um erro ao executar a query: {e}")
finally:
    # conn.close()
    session.close()

file_path = 'database\postgres\small_relations_insert_file.sql'
with open(file_path, 'r') as file:
    sql_script = file.read()

sql_commands = sql_script.split(';')

try:
    with conn.begin():
        for command in sql_commands:
            if command.strip():
                conn.execute(text(command))
    print("INSERT executada com sucesso!")
except Exception as e:
    print(f"Ocorreu um erro ao executar a query: {e}")
finally:
    # conn.close()
    session.close()