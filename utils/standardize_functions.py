import numpy as np
import pandas as pd

def standard_saude_digital(df, fonte="SD"):
    sexo_dict = {
        "FEMININO": "F", "MASCULINO": "M", "Homem Cisgênero": "M", "Mulher Cisgênero": "F",
        "Prefiro não informar": "I", "Outro": "I", "Feminino": "F", "Masculino": "M",
        "Agênero": "I", "MULHER CISGÊNERO": "F", "HOMEM CISGÊNERO": "M", "OUTRO": "I",
        "Homem Transgênero": "M", "Mulher Transgênero": "F", "Gênero Flúido": "I",
        "PREFIRO NÃO INFORMAR": "I", "HOMEM TRANSGÊNERO": "M", "AGÊNERO": "I", "0": "I"
    }
    df["ID"] = df["ID"].apply(lambda x: f"{x:7.0f}".replace(" ", "0"))
    df["NOME_PACIENTE"] = df["NOME_PACIENTE"].copy()
    df["NOME_MAE"] = df["NOME_MAE"].copy()
    df["MUNICIPIO_RESIDENCIA"] = df["MUNICIPIO_RESIDENCIA"].copy()
    df["BAIRRO_RESIDENCIA"] = df["BAIRRO_RESIDENCIA"].apply(lambda x: x.upper().strip() if pd.notna(x) else x).copy()
    df["DATA_NASCIMENTO"] = df["DATA_NASCIMENTO"].copy()
    df["LOGRADOURO"] = df["LOGRADOURO"].apply(lambda x: x.upper().strip() if pd.notna(x) else x).copy()
    df["LOGRADOURO_NUMERO"] = df["LOGRADOURO_NUMERO"].copy()
    df["CEP"] = df["CEP"].copy()
    df["CNS"] = df["CNS"].copy()
    df["CPF"] = df["CPF"].copy()
    df["SEXO"] = df["SEXO"].map(sexo_dict)
    df["FONTE"] = [ fonte for n in range(df.shape[0]) ]
    return df

def standard_sim(df):
    df["ID"] = df["NUMERODO"].apply(lambda x: f'{x}')
    df["NOME_PACIENTE"] = df["NOME"].copy()
    df["NOME_MAE"] = df["NOMEMAE"].copy()
    df["MUNICIPIO_RESIDENCIA"] = df["CODMUNRES"].copy()
    df["BAIRRO_RESIDENCIA"] = df["BAIRES"].copy()
    df["DATA_NASCIMENTO"] = pd.to_datetime(df["DTNASC"], format="%d%m%Y", errors='coerce')
    df["LOGRADOURO"] = df["ENDRES"].copy()
    df["LOGRADOURO_NUMERO"] = df["NUMRES"].copy()
    df["CEP"] = df["CEPRES"].copy()
    df["CNS"] = df["NUMSUS"].copy()
    df["CPF"] = [ np.nan for elem in range(df.shape[0]) ]
    df["SEXO"] = df["SEXO"].copy()
    df["FONTE"] = [ 'SIM' for elem in range(df.shape[0]) ]
    return df

def standard_sivep_excel(df):
    df["ID"] = df["NU_NOTIFIC"].apply(lambda x: f'{x}')
    df["NOME_PACIENTE"] = df['NM_PACIENT'].copy()
    df["NOME_MAE"] = df['NM_MAE_PAC'].copy()
    df["MUNICIPIO_RESIDENCIA"] = df['CO_MUN_RES'].apply(lambda x: f'{int(x)}' if pd.notna(x) else np.nan)
    df["BAIRRO_RESIDENCIA"] = df['NM_BAIRRO'].copy()
    df["DATA_NASCIMENTO"] = pd.to_datetime(df['DT_NASC'], format="%d/%m/%Y", errors='coerce')
    df["LOGRADOURO"] = df['NM_LOGRADO'].copy()
    df["LOGRADOURO_NUMERO"] = df['NU_NUMERO'].copy()
    df["CEP"] = df['NU_CEP'].apply(lambda x: f"{x:8.0f}".replace(" ", "0") if pd.notna(x) else np.nan)
    df["CNS"] = [ np.nan for elem in range(df.shape[0]) ]
    df["CPF"] = df['NU_CPF'].apply(lambda x: f"{x:11.0f}".replace(" ", "0") if pd.notna(x) else np.nan)
    df["SEXO"] = df["CS_SEXO"].copy()
    df["FONTE"] = [ 'SIVEP' for elem in range(df.shape[0]) ]
    return df

def standard_sivep_dbf(df):
    df["ID"] = df["NU_NOTIFIC"].apply(lambda x: f'{x}')
    df["NOME_PACIENTE"] = df['NM_PACIENT'].copy()
    df["NOME_MAE"] = df['NM_MAE_PAC'].copy()
    df["MUNICIPIO_RESIDENCIA"] = df['CO_MUN_RES'].copy()
    df["BAIRRO_RESIDENCIA"] = df['NM_BAIRRO'].copy()
    df["DATA_NASCIMENTO"] = pd.to_datetime(df['DT_NASC'], format="%d/%m/%Y", errors='coerce')
    df["LOGRADOURO"] = df['NM_LOGRADO'].copy()
    df["LOGRADOURO_NUMERO"] = df['NU_NUMERO'].copy()
    df["CEP"] = df['NU_CEP'].copy()
    df["CNS"] = [ np.nan for elem in range(df.shape[0]) ]
    df["CPF"] = df['NU_CPF'].copy()
    df["SEXO"] = df["CS_SEXO"].copy()
    df["FONTE"] = [ 'SIVEP' for elem in range(df.shape[0]) ]
    return df