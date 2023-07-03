# PyMySQL - cliente mysql feito em python
import os
from typing import cast

import dotenv
import pymysql
import pymysql.cursors

TABLE_NAME = 'customers'
CURRENT_CURSOR = pymysql.cursors.DictCursor

# carregando arquivo .env
dotenv.load_dotenv()

# criando conexão
connection = pymysql.connect(
    host=os.environ['MYSQL_HOST'],
    user=os.environ['MYSQL_USER'],
    password=os.environ['MYSQL_PASSWORD'],
    database=os.environ['MYSQL_DATABASE'],
    cursorclass=pymysql.cursors.DictCursor,
)

# usando o context manager para abrir e fechar a connection
with connection:
    with connection.cursor() as cursor:
        # criando tabela customers no banco de dados
        cursor.execute(
            'CREATE TABLE IF NOT EXISTS customers ('
            'id INT NOT NULL AUTO_INCREMENT, '
            'nome VARCHAR(50) NOT NULL, '
            'idade INT NOT NULL, '
            'PRIMARY KEY (id) '
            ') '
        )
        # truncate limpa a tabela !!!
        cursor.execute(f'TRUNCATE TABLE {TABLE_NAME}')

    connection.commit()  # create não precisa necessariamente de commit.

    with connection.cursor() as cursor:
        # insert normal
        sql = (
            f'INSERT INTO {TABLE_NAME} (nome, idade) VALUES ("braia", 64)'
        )

        # insert com placeholder (evitar sql injection)
        sql_placeholder = (
            f'INSERT INTO {TABLE_NAME} (nome, idade) VALUES (%s, %s)'
        )

        cursor.execute(sql_placeholder, ('placeholder', 66))
        print(sql_placeholder)
    connection.commit()

    with connection.cursor() as cursor:
        # insert com dicionário
        sql_dict = f'INSERT INTO {TABLE_NAME} (nome, idade) VALUES' \
            '(%(nome)s, %(idade)s)'

        data = {
            'nome': 'dict_insert',
            'idade': 33,
        }

        cursor.execute(sql_dict, data)
        print(sql_dict)
    connection.commit()

    # insert com execute-many
    with connection.cursor() as cursor:
        # insert com dicionário
        sql_dict = f'INSERT INTO {TABLE_NAME} (nome, idade) VALUES' \
            '(%(nome)s, %(idade)s)'

        # execetemany pode usar tuplas de dicionários ou tuplas de tuplas
        data_tuple = (
            {'nome': 'data_tuple1', 'idade': 33, },
            {'nome': 'data_tuple2', 'idade': 34, },
            {'nome': 'data_tuple3', 'idade': 35, },
        )

        cursor.executemany(sql_dict, data_tuple)
        print(sql_dict)
    connection.commit()

    # Lendo valores com o SELECT

    with connection.cursor() as cursor:
        # menor_id = input('Digite o menor id : ')
        # maior_id = input('Digite o maior id : ')
        menor_id = 2
        maior_id = 4

        # comando com placeholder (%s) para evitar sql injection
        sql_select = f'SELECT * from {TABLE_NAME} WHERE id BETWEEN %s and %s'

        cursor.execute(sql_select, (menor_id, maior_id))

        print(cursor.mogrify(sql_select, (menor_id, maior_id),))

        # fetchall para trazer todos os dados ; fetchone traz apenas um valor
        data = cursor.fetchall()

        # for para mostrar os dados
        for row in data:
            print(row)

    # Deletando valores com o DELETE

    with connection.cursor() as cursor:
        deletar_id = 2

        # comando delete, com placeholder
        sql_delete = f'DELETE FROM {TABLE_NAME} WHERE id = %s'

        cursor.execute(sql_delete, (deletar_id,))  # delete

        cursor.execute(f'SELECT * FROM {TABLE_NAME}')  # select

        connection.commit()  # fazendo commit

        # for para mostrar os dados
        print(f'\nSELECT APÓS DELETE DO ID {deletar_id} \n')
        for row in cursor.fetchall():
            print(row)

    # Alterando valores com o UPDATE

    with connection.cursor() as cursor:
        cursor = cast(CURRENT_CURSOR, cursor)

        # comando delete, com placeholder
        sql_update = f'UPDATE {TABLE_NAME} SET nome = %s,' \
            'idade = %s WHERE id = %s'

        cursor.execute(sql_update, ('placeholder_updated', 666, 1))  # update

        cursor.execute(f'SELECT * FROM {TABLE_NAME}')  # select

        connection.commit()  # fazendo commit

        # data_up = cursor.fetchall()

        # for row in cursor.fetchall(): for com desempacotamento
        #    _id, nome, idade = row  # type:ignore
        #    print(row)  # type:ignore

        print('\nSELECT APÓS UPDATE \n')
        print('FOR 1')
        for row in cursor.fetchall():
            print(row)  # type:ignore

        print('\nFOR 2')
        # cursor.scroll(-1) scroll faz o cursor "andar" pelas linhas de dados
        # cursor é bom para mostrar dados sem necessariamente salvar todos em
        # uma variável, ou seja, ele é melhor em casos onde se tem muitos dados
        cursor.scroll(1, 'absolute')
        for row in cursor.fetchall():
            print(row)  # vem como dict por conta do DictCursor lá em cima

        # monstrando quantidade de linhas (rows) afetadas

        result_select = cursor.execute(f'SELECT * FROM {TABLE_NAME}')

        print('result_select', result_select)
        print('len', len(cursor.fetchall()))
        print('rowcount', cursor.rowcount)
        print('lastrowid', cursor.lastrowid)  # último id inserido
        print('lastrowid manual', cursor.execute(
            f'SELECT id FROM {TABLE_NAME} ORDER BY id DESC LIMIT 1'
        ), cursor.fetchone())  # último id inserido com select
        print('rownumber', cursor.rownumber)
