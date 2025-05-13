from data.load_sources import load_sources
from data.schema import WarehouseSIM_SIVEP

import joblib
import numpy as np
import pandas as pd
from pathlib import Path
from tqdm import tqdm
from sqlalchemy import create_engine

from matching.matching_data import PLinkage
from data.process_sus import ProcessSus
from utils.sql_utils import perform_query

class LinkageSIM_SIVEP:
    def __init__(self, config):
        self.warehouse_location = Path(config['database']['path'])
        self.warehouse_name = config['database']['name']
        self.field_id = config['database']['field_id']
        self.path_to_models = Path(config['models']['path'])
        self.gbt_filename = config['models']['gbt_filename']
        self.rnf_filename = config['models']['rnf_filename']
        self.lgt_filename = config['models']['lgt_filename']
        self.blocking_window = config['linkage']['blocking_window']
        self.linkage_threshold = config['linkage']['threshold']
        self.string_method = config['linkage']['comparison_string_method']
        self.fonetica_column = config['linkage']['fonetica_column']
        self.simplify_fonetica = config['linkage']['simplify_fonetica']
        
        self.engine_url = f"sqlite:///{self.warehouse_location.joinpath(self.warehouse_name)}"
        self.warehouse = WarehouseSIM_SIVEP(self.engine_url)
        self.engine = self.warehouse.db_init()
        self.current_pairs = []
        self.linkage = None

        self.models = dict()
        self.models["GBT"] = joblib.load(self.path_to_models.joinpath(self.gbt_filename))
        self.models["RNF"] = joblib.load(self.path_to_models.joinpath(self.rnf_filename))
        self.models["LGT"] = joblib.load(self.path_to_models.joinpath(self.lgt_filename))

    def perform(self, target_ids, number_of_blocks, frac=1.0, chunksize=5000):
        ''' 
            'target_ids' -> sivep
        '''
        query_data = pd.DataFrame( self.warehouse.query_all(table_name='pessoa'))
        left_df = query_data[query_data[self.field_id].isin(target_ids)]
        right_df = query_data[query_data["FONTE"]=="SIM"]
        if frac<1.000:
            right_df = right_df.sample(frac=frac)
        processor_left = ProcessSus(left_df, self.field_id)
        processor_right = ProcessSus(right_df, self.field_id)
        processor_left.basic_standardize().specific_standardize(fonetica_column=self.fonetica_column, simplify_fonetica=self.simplify_fonetica)
        processor_right.basic_standardize().specific_standardize(fonetica_column=self.fonetica_column, simplify_fonetica=self.simplify_fonetica)
        processed_left = processor_left.data
        processed_right = processor_right.data
        processed_left = processed_left.rename({self.field_id: f"{self.field_id}_1"}, axis=1)
        processed_right = processed_right.rename({self.field_id: f"{self.field_id}_2"}, axis=1)

        # -- build the similarity matrix
        self.linkage = PLinkage(processed_left, processed_right, left_id=f"{self.field_id}_1", right_id=f"{self.field_id}_2", env_folder=None)
        linkage_vars = [
            ("cpf", "cpf", "exact", 'cpf'),
            ("cns", "cns", "exact", 'cns'),
            ("cep", "cep", "exact", 'cep'),
            ("sexo", "sexo", "exact", 'sexo'),
            ("bairro", "bairro", "string", 'bairro'),
            ("nascimento_dia", "nascimento_dia", "exact", 'nascimento_dia'),
            ("nascimento_mes", "nascimento_mes", "exact", 'nascimento_mes'),
            ("nascimento_ano", "nascimento_ano", "exact", 'nascimento_ano'),
            ("primeiro_nome", "primeiro_nome", "string", 'primeiro_nome'),
            ("primeiro_nome_mae", "primeiro_nome_mae", "string", 'primeiro_nome_mae'),
            ("complemento_nome", "complemento_nome", "string", 'complemento_nome'),
            ("complemento_nome_mae", "complemento_nome_mae", "string", 'complemento_nome_mae'),
        ]
        # -- 'FONETICA_N' was created in the 'ProcessSus' class.
        self.linkage.set_linkage_variables(linkage_vars, string_method=self.string_method).define_pairs("FONETICA_N", "FONETICA_N", window=self.blocking_window)
        #return (self.linkage.candidate_pairs, processed_left, processed_right)

        # -- remove the pairs already compared
        # -- left ID: SIVEP (string) - right ID: SIM (integer)
        ids_to_remove = []
        for j in tqdm(range(0, len(self.linkage.candidate_pairs), chunksize)):
            current_chunk = list(self.linkage.candidate_pairs[j:j + chunksize])
            chunk_tuple = tuple([ '"'+elem[0]+'-'+elem[1]+'"' for elem in current_chunk ])

            q = f'''
                SELECT * FROM likely_negative_pairs WHERE FMT_ID IN ({','.join(chunk_tuple)})
            '''
            found_ids = perform_query(q, self.engine)
            # ["FMT_ID"].tolist()
            if found_ids.shape[0]>0:
                found_ids = [ (elem.split("-")[0], elem.split("-")[1]) for elem in found_ids["FMT_ID"].tolist() ]                
                ids_to_remove += found_ids
        print(f"Pairs already compared before: {len(ids_to_remove)}")

        # ---- remove pairs that were already compared previously
        self.linkage.candidate_pairs = self.linkage.candidate_pairs.drop(ids_to_remove, errors='ignore')
        # -- free the space
        del ids_to_remove
        print(f"Pairs to be effectively compared: {self.linkage.candidate_pairs.shape[0]}")
        # -- compare and generate the similarity matrix
        self.linkage.perform_linkage(threshold=self.linkage_threshold, number_of_blocks=number_of_blocks)

        # -- classify pairs
        pair_ids, X_sel = self.linkage.comparison_matrix.reset_index().iloc[:,:2],  self.linkage.comparison_matrix.reset_index().iloc[:,2:].values

        batchsize = 6000
        Y_neg1, Y_neg2, Y_neg3 = [], [], []
        for batch in tqdm(np.array_split(X_sel, np.arange(batchsize, X_sel.shape[0]+1, batchsize))):
            Y_neg1 += [ res[0] for res in self.models["GBT"].predict_proba(batch) ]
            Y_neg2 += [ res[0] for res in self.models["RNF"].predict_proba(batch) ]
            Y_neg3 += [ res[0] for res in self.models["LGT"].predict_proba(batch) ]

        # -- create the subset of very likely positive pairs Yp and very likely negative pairs Yn
        border_thr = 0.70 # -- the smaller this value, the smaller will be the storage space used by the database.
        Yp = [ ( pair_ids[f"{self.field_id}_1"].iloc[index]+"-"+pair_ids[f"{self.field_id}_2"].iloc[index], yl[0], yl[1], yl[2]) for index, yl in enumerate(zip(Y_neg1, Y_neg2, Y_neg3)) if yl[0] <= border_thr and yl[1] <= border_thr and yl[2] <= border_thr]
        Yn = [ ( pair_ids[f"{self.field_id}_1"].iloc[index]+"-"+pair_ids[f"{self.field_id}_2"].iloc[index], yl[0], yl[1], yl[2]) for index, yl in enumerate(zip(Y_neg1, Y_neg2, Y_neg3)) if not (yl[0] <= border_thr and yl[1] <= border_thr and yl[2] <= border_thr)]

        # -- insert the likely positive pairs
        likely_positive = pd.DataFrame({
            "FMT_ID": [ y_elem[0] for y_elem in Yp ],
            "PROBA_NEGATIVO_MODELO_1": [ y_elem[1] for y_elem in Yp ],
            "PROBA_NEGATIVO_MODELO_2": [ y_elem[2] for y_elem in Yp ],
            "PROBA_NEGATIVO_MODELO_3": [ y_elem[3] for y_elem in Yp ], 
        })
        self.warehouse.insert('likely_positive_pairs', likely_positive, batchsize=500, verbose=True)

        # -- insert the likely negative pairs
        likely_negative = pd.DataFrame({
            "FMT_ID": [ y_elem[0] for y_elem in Yn ],
            "PROBA_NEGATIVO_MODELO_1": [ y_elem[1] for y_elem in Yn ],
            "PROBA_NEGATIVO_MODELO_2": [ y_elem[2] for y_elem in Yn ],
            "PROBA_NEGATIVO_MODELO_3": [ y_elem[3] for y_elem in Yn ], 
        })
        self.warehouse.insert('likely_negative_pairs', likely_negative, batchsize=500, verbose=True)


def linkage(config):
    '''
    
    '''
    # -- load sources
    sivep_df, sim_df = load_sources(config)
    
    # -- create database
    database_path = Path(config['database']['path'])
    database_name = config['database']['name']
    database_insert_batchsize = config['database']['insert_batchsize']

    engine_url = f"sqlite:///{database_path.joinpath(database_name)}"
    warehouse_obj = WarehouseSIM_SIVEP(engine_url)
    warehouse_obj.db_init()

    # -- insert SIM data
    warehouse_obj.insert('pessoa', sim_df, batchsize=database_insert_batchsize)
    # -- insert SIVEP data
    warehouse_obj.insert('pessoa', sivep_df, batchsize=database_insert_batchsize)

    # -- perform linkage
    linkage_agent = LinkageSIM_SIVEP(config)

    # -- Target IDS for RCBP
    engine = create_engine(engine_url)
    q = f'''
        SELECT * FROM pessoa WHERE FONTE = 'SIVEP'
    '''
    target_df = perform_query(q, engine)
    print(target_df.shape)

    number_of_blocks = config['linkage']['number_of_blocks']
    frac_list = config['linkage']['frac_list']
    target_ids = target_df["ID"].tolist()
    for frac_value in frac_list:
        print(frac_value)
        linkage_agent.perform(target_ids=target_ids, number_of_blocks=number_of_blocks, frac=frac_value, chunksize=5000)