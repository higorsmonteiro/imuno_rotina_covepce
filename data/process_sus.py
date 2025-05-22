import numpy as np
import pandas as pd
import data.utils as utils
from data.process_base import ProcessBase 

class ProcessSus(ProcessBase):
    db_type = "SINAN"

    def specific_standardize(self, fonetica_column="NOME_PACIENTE", simplify_fonetica=False):
        '''
            simplify_fonetica: if the size of the data allows, simplify fonetica (only first name) to increase number of compared pairs.
        '''
        sexo_arr = self._raw_data.set_index(self.field_id)["SEXO"].apply(lambda x: x.upper().strip() if pd.notna(x) else np.nan)
        cpf_arr = self._raw_data.set_index(self.field_id)["CPF"].apply(lambda x: f"{x:11.0f}".replace(" ", "0") if not isinstance(x, str) and pd.notna(x) else x)
        cns_arr = self._raw_data.set_index(self.field_id)["CNS"].apply(lambda x: x if isinstance(x, str) and utils.cns_is_valid(x) and pd.notna(x) else ( f"{x:13.0f}".replace(" ", "0") if not isinstance(x, str) and pd.notna(x) else np.nan))
        bairro_arr = self._raw_data.set_index(self.field_id)["BAIRRO_RESIDENCIA"].apply(lambda x: utils.uniformize_name(x.upper().strip(), sep=" ") if pd.notna(x) else np.nan)
        cep_arr = self._raw_data.set_index(self.field_id)["CEP"].apply(lambda x: x if isinstance(x, str) and pd.notna(x) else ( f"{x:8.0f}".replace(" ", "0") if not isinstance(x, str) and pd.notna(x) else np.nan))
        
        newcol, colnames = [sexo_arr, cns_arr, cpf_arr, bairro_arr, cep_arr], ["sexo", "cns", "cpf", "bairro", "cep"]
        for index, colname in enumerate(colnames):
            self._data = self._data.merge(newcol[index], left_on=self.field_id, right_index=True, how='left')

        if simplify_fonetica:
            self._data["FONETICA_N"] = self._data[fonetica_column].apply(lambda x: f"{x.split(' ')[0]}" if pd.notna(x) else np.nan)
            
        self._data = self._data.rename({"SEXO": "sexo", "CPF": "cpf", "CNS": "cns", "BAIRRO_RESIDENCIA": "bairro", "CEP": "cep"}, axis=1)

class ProcessDB(ProcessBase):
    db_type = "ANY"

    def specific_standardize(self):
        
        sexo_arr = self._raw_data.set_index(self.field_id)["SEXO"].apply(lambda x: x.upper().strip() if pd.notna(x) else np.nan)
        cpf_arr = self._raw_data.set_index(self.field_id)["CPF"].apply(lambda x: f"{x:11.0f}".replace(" ", "0") if not isinstance(x, str) and pd.notna(x) else x)
        cns_arr = self._raw_data.set_index(self.field_id)["CNS"].apply(lambda x: x if isinstance(x, str) and utils.cns_is_valid(x) and pd.notna(x) else ( f"{x:13.0f}".replace(" ", "0") if not isinstance(x, str) and pd.notna(x) else np.nan))
        bairro_arr = self._raw_data.set_index(self.field_id)["BAIRRO_RESIDENCIA"].apply(lambda x: utils.uniformize_name(x.upper().strip(), sep=" ") if pd.notna(x) else np.nan)
        cep_arr = self._raw_data.set_index(self.field_id)["CEP"].apply(lambda x: x if isinstance(x, str) and pd.notna(x) else ( f"{x:8.0f}".replace(" ", "0") if not isinstance(x, str) and pd.notna(x) else np.nan))
        
        newcol, colnames = [sexo_arr, cpf_arr, cns_arr, bairro_arr, cep_arr], ["sexo", "cpf", "cns", "bairro", "cep"]
        for index, colname in enumerate(colnames):
            self._data = self._data.merge(newcol[index], left_on=self.field_id, right_index=True, how='left')
            
        self._data = self._data.rename({"SEXO": "sexo", "CPF": "cpf", "CNS": "cns", "BAIRRO_RESIDENCIA": "bairro", "CEP": "cep"}, axis=1) 
        