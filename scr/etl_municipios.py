import os
import pandas as pd

from orm_sqlalch_functions import cria_tabela_municipios, popula_tabela_municipios


def transform_values_municipios(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop(columns= ['CÓDIGO DO MUNICÍPIO - TOM', 'MUNICÍPIO - TOM'])
    df = df.dropna()

    return df
    

df_raw = pd.read_csv(r"C:\Users\guilh\Downloads\municipios.csv", sep= ';', encoding='latin1')


df_municipios = transform_values_municipios(df_raw)

cria_tabela_municipios()

valores_str = df_municipios.values.tolist()

popula_tabela_municipios(valores_str)