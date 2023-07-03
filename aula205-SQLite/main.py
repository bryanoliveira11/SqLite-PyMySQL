import sqlite3
from pathlib import Path

ROOT_DIR = Path(__file__).parent
DB_NAME = 'db.sqlite3'
DB_FILE = ROOT_DIR / DB_NAME
TABLE_NAME = 'customers'

# conectando no banco de dados
connection = sqlite3.connect(DB_FILE)
cursor = connection.cursor()

# DELETE SEM WHERE
cursor.execute(f'DELETE FROM {TABLE_NAME}')
cursor.execute(f'DELETE FROM sqlite_sequence WHERE name="{TABLE_NAME}"')
connection.commit()

# criando uma tabela
cursor.execute(
    f'CREATE TABLE IF NOT EXISTS {TABLE_NAME}'
    '('
    'id INTEGER PRIMARY KEY AUTOINCREMENT,'
    'name TEXT,'
    'weight REAL'
    ')'
)
connection.commit()

# registrando um valor na tabela
cursor.execute(
    f'INSERT INTO {TABLE_NAME} (name, weight) VALUES ("bryan", 64.1)'
)

# registrando um valor usando bindings (evitando sql injection)
sql = f'INSERT INTO {TABLE_NAME} (name, weight) VALUES (?, ?)'

cursor.execute(sql, ['Joana', 4])

# registrando vários valores na tabela
cursor.executemany(
    sql,
    (
        ('Jose', 4), ('Luiz', 5), ('Username', 30)
    )
)

# registrando valores com dicionários
sql_dict = f'INSERT INTO {TABLE_NAME} (name, weight) VALUES (:name, :weight)'

# um valor
cursor.execute(sql_dict, {'name': 'User_Dict', 'weight': 55})

# vários valores
cursor.executemany(sql_dict, (
    {'name': 'Marco', 'weight': 60},
    {'name': 'Dict', 'weight': 88}
))

connection.commit()

if __name__ == '__main__':
    # fazendo um delete
    cursor.execute(f'DELETE FROM {TABLE_NAME} WHERE ID = "3"')
    connection.commit()

    # fazendo um update
    cursor.execute(
        f'UPDATE {TABLE_NAME} SET name = "bryan updated" WHERE ID = "1"')
    connection.commit()

    cursor.execute(f'SELECT * FROM {TABLE_NAME}')

    for row in cursor.fetchall():
        _id, name, weight = row
        print(_id, name, weight)

    # fechando o banco de dados
    cursor.close()
    connection.close()
