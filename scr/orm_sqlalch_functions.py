import os
# import pymysql
import psycopg2 as pg2
import pandas as pd

from dotenv import load_dotenv


load_dotenv()


# Load the environment variables from the .env file


usuario = os.getenv('USER_PGADMIN')
senha = os.getenv('PWS_PGADMIN')


def cria_tabela_municipios(usuario = usuario, senha = senha) -> bool:
    
    try:
        conn = pg2.connect(user= usuario, password= senha, host= 'localhost', database= 'tabelas_pessoais')

        cur = conn.cursor()

        cur.execute(
            '''
            DROP TABLE IF EXISTS tb_municipios_ibge CASCADE;
            CREATE TABLE IF NOT EXISTS tb_municipios_ibge(
                    cod_municipio SERIAL PRIMARY KEY,
                    cod_ibeg BIGINT NOT NULL,
                    nome_municipio VARCHAR(255) NOT NULL,
                    uf CHAR(2) NOT NULL,
                    regiao CHAR(12) NOT NULL
                )
            ''')

        conn.commit()
    
    except Exception as e:
        print(f'Erro ao criar tabela tb_municipios_ibge: {e}')
        return False
    
    return True


def popula_tabela_municipios(lista_valores: list):
        conn = pg2.connect(user= usuario, password= senha, host= 'localhost', database= 'tabelas_pessoais')

        cur = conn.cursor()

        
        cur.executemany(
            '''
            INSERT INTO tb_municipios_ibge (cod_ibeg, nome_municipio, uf, regiao)
            VALUES (%s, %s, %s, %s) 
            ''', lista_valores)
        
        conn.commit()

        conn.close()