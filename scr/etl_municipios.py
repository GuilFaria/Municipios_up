import os
import pandas as pd
import logging


from orm_sqlalch_functions import cria_tabela_municipios, popula_tabela_municipios


def transform_values_municipios(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop(columns= ['CÓDIGO DO MUNICÍPIO - TOM', 'MUNICÍPIO - TOM'])
    df = df.dropna()

    return df
    
def enrich_values_municipios(df: pd.DataFrame) -> pd.DataFrame:

    dict_regioes_estados = {
     'AC': 'Norte', 'AP': 'Norte', 'AM': 'Norte', 'PA': 'Norte', 'RO': 'Norte', 'RR': 'Norte', 'TO': 'Norte',
     'AL': 'Nordeste', 'BA': 'Nordeste', 'CE': 'Nordeste', 'MA': 'Nordeste', 'PB': 'Nordeste', 'PE': 'Nordeste', 'PI': 'Nordeste', 'RN': 'Nordeste', 'SE': 'Nordeste',
     'DF': 'Centro-Oeste', 'GO': 'Centro-Oeste', 'MT': 'Centro-Oeste', 'MS': 'Centro-Oeste',
     'ES': 'Sudeste', 'MG': 'Sudeste', 'RJ': 'Sudeste', 'SP': 'Sudeste',
     'PR': 'Sul', 'RS': 'Sul', 'SC': 'Sul'
     }
    
    print(df_municipios.columns)
    df['regiao'] = df['UF'].map(dict_regioes_estados)

    return df


if __name__ == "__main__":
    
    
    df_raw = pd.read_csv(r"C:\Users\guilh\Downloads\municipios.csv", sep= ';', encoding='latin1')

    df_municipios = transform_values_municipios(df_raw)

    df_municipios = enrich_values_municipios(df_municipios)

    criacao = cria_tabela_municipios()

    if criacao:
        
        valores_str = df_municipios.values.tolist()

        popula_tabela_municipios(valores_str)