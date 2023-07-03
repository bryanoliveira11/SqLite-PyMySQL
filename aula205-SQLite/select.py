import sqlite3

from main import DB_FILE, TABLE_NAME

# conectando no banco de dados
connection = sqlite3.connect(DB_FILE)
cursor = connection.cursor()

# tudo da tabela
cursor.execute(f'SELECT * FROM {TABLE_NAME}')

for row in cursor.fetchall():
    _id, name, weight = row
    print(_id, name, weight)

# apenas um valor
cursor.execute(f'SELECT * FROM {TABLE_NAME} WHERE ID = "3"')

row = cursor.fetchone()
_id, name, weight = row
print(_id, name, weight)

# fechando o banco de dados
cursor.close()
connection.close()
