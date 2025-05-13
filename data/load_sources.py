import os
import pandas as pd
from pathlib import Path
from simpledbf import Dbf5
import datetime as dt
from utils.standardize_functions import standard_sivep_dbf, standard_sim

def load_sources(config):
    '''
    
    '''
    # -- load SIVEP-GRIPE
    # ---- where SIVEP-GRIPE files are located
    sivep_path = config['sivep']['path']
    # ---- list with the names of the files
    sivep_filenames = config['sivep']['filenames']
    # ---- consider only the records from this day forward 
    sivep_start_day= config['sivep']['start_day']
    # ---- which date field (name of the field) to consider when selecting records 
    type_of_date_start_day = config['sivep']['type_of_date_start_day']  

    sivep_df = []
    for current_sivep_fname in sivep_filenames:
        current_df = Dbf5(os.path.join(sivep_path, current_sivep_fname), codec='latin').to_dataframe()
        sivep_df.append( current_df.copy() )
    current_df = None
    sivep_df = pd.concat(sivep_df)

    sivep_df = sivep_df.copy()
    sivep_df = sivep_df.reset_index(drop=True)
    sivep_df["DT_NOTIFIC"] = pd.to_datetime(sivep_df["DT_NOTIFIC"], format="%d/%m/%Y")
    sivep_df["DT_SIN_PRI"] = pd.to_datetime(sivep_df["DT_SIN_PRI"], format="%d/%m/%Y")
    sivep_df["DT_EVOLUCA"] = pd.to_datetime(sivep_df["DT_EVOLUCA"], format="%d/%m/%Y")
    sivep_df = sivep_df[sivep_df[type_of_date_start_day]>=pd.Timestamp(sivep_start_day)]

    # ---- logging
    print("SIVEP-GRIPE:")
    print(f'\tNo. Registros: {sivep_df.shape[0]:,}')
    print(f'\tNotificação mais antiga: {sivep_df["DT_NOTIFIC"].min()}\n\tNotificação mais recente: {sivep_df["DT_NOTIFIC"].max()}\n')
    print(f'\tSintoma mais antigo: {sivep_df["DT_SIN_PRI"].min()}\n\tSintoma mais recente: {sivep_df["DT_SIN_PRI"].max()}\n')

    new_sivep_df = standard_sivep_dbf(sivep_df)

    # -- load SIM
    sim_path = config['sim']['path']
    sim_filenames = config['sim']['filenames']

    sim_df = []
    for current_sim_fname in sim_filenames:
        current_df = Dbf5(os.path.join(sim_path, current_sim_fname), codec='latin').to_dataframe()
        sim_df.append( current_df.copy() )
    current_df = None
    sim_df = pd.concat(sim_df)
    sim_df = sim_df.drop_duplicates(subset=["NUMERODO"])

    new_sim_df = standard_sim(sim_df)
    new_sim_df["DTOBITO"] = pd.to_datetime(new_sim_df["DTOBITO"], format="%d%m%Y", errors="coerce")
    new_sim_df["DTNASC"] = pd.to_datetime(new_sim_df["DTNASC"], format="%d%m%Y", errors="coerce")
    # ---- logging
    print("SIM:")
    print(f'\tNo. Registros: {new_sim_df.shape[0]:,}')
    print(f'\tÓbito mais antigo: {new_sim_df["DTOBITO"].min()}\n\tÓbito mais recente: {new_sim_df["DTOBITO"].max()}\n')
    return new_sivep_df, new_sim_df