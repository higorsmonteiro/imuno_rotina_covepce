'''
    ...
'''
import datetime as dt
from data.warehouse_base import WarehouseBase

from sqlalchemy import create_engine
from sqlalchemy import Column, Table, MetaData
from sqlalchemy import DateTime, Numeric, String

# ---------- datasus for SIM and RCBP ----------
class Pessoa:
    def __init__(self, metadata):
        self.metadata = metadata
        self.table_name = 'pessoa'

        # -- define schema for table.
        self.model = Table(
            self.table_name, self.metadata,
            Column("ID", String, primary_key=True),
            Column("NOME_PACIENTE", String, nullable=True),
            Column("DATA_NASCIMENTO", DateTime, nullable=True),
            Column("NOME_MAE", String, nullable=True),
            Column("SEXO", String, nullable=True),
            Column("LOGRADOURO", String, nullable=True),
            Column("LOGRADOURO_NUMERO", String, nullable=True),
            Column("BAIRRO_RESIDENCIA", String, nullable=True),
            Column("MUNICIPIO_RESIDENCIA", String, nullable=True),
            Column("CEP", String, nullable=True),
            Column("CNS", String, nullable=True),
            Column("CPF", String, nullable=True),
            Column("FONTE", String, nullable=False),
            Column("CRIADO_EM", DateTime, default=dt.datetime.now),
        )

        # -- define data mapping (could be import if too big)
        self.mapping = {
            "ID" : "ID", 
            "NOME_PACIENTE": "NOME_PACIENTE", 
            "SEXO": "SEXO",
            "DATA_NASCIMENTO": "DATA_NASCIMENTO",
            "NOME_MAE": "NOME_MAE",
            "MUNICIPIO_RESIDENCIA": "MUNICIPIO_RESIDENCIA",
            "BAIRRO_RESIDENCIA": "BAIRRO_RESIDENCIA", 
            "LOGRADOURO": "LOGRADOURO",
            "LOGRADOURO_NUMERO": "LOGRADOURO_NUMERO", 
            "CEP": "CEP", 
            "CNS": "CNS",
            "CPF": "CPF",
            "FONTE": "FONTE",
        }

    def define(self):
        '''
            Return dictionary elements containing the data model and 
            the data mapping, respectively.
        '''
        table_elem = { self.table_name : self.model }
        mapping_elem = { self.table_name : self.mapping }
        return table_elem, mapping_elem
    
# ---------- MATCHING DATA MODELS ----------
class PositivePairsLabel:
    def __init__(self, metadata):
        self.metadata = metadata
        self.table_name = 'likely_positive_pairs'

        # --> define schema for table.
        self.model = Table(
            self.table_name, self.metadata,
            Column("FMT_ID", String, primary_key=True),
            Column("PROBA_NEGATIVO_MODELO_1", Numeric(precision=4, scale=3), nullable=True),
            Column("PROBA_NEGATIVO_MODELO_2", Numeric(precision=4, scale=3), nullable=True),
            Column("PROBA_NEGATIVO_MODELO_3", Numeric(precision=4, scale=3), nullable=True),
        )

        # -- define data mapping (could be imported if too big) - include all columns!
        self.mapping = {
            "FMT_ID": "FMT_ID",
            "PROBA_NEGATIVO_MODELO_1" : "PROBA_NEGATIVO_MODELO_1",
            "PROBA_NEGATIVO_MODELO_2": "PROBA_NEGATIVO_MODELO_2",
            "PROBA_NEGATIVO_MODELO_3": "PROBA_NEGATIVO_MODELO_3",  
        }

    def define(self):
        '''
            Return dictionary elements containing the data model and 
            the data mapping, respectively.
        '''
        table_elem = { self.table_name : self.model }
        mapping_elem = { self.table_name : self.mapping }
        return table_elem, mapping_elem
    
# ---------- MATCHING DATA MODELS ----------
class NegativePairsLabel:
    def __init__(self, metadata):
        self.metadata = metadata
        self.table_name = 'likely_negative_pairs'

        # --> define schema for table.
        self.model = Table(
            self.table_name, self.metadata,
            Column("FMT_ID", String, primary_key=True),
        )

        # -- define data mapping (could be imported if too big) - include all columns!
        self.mapping = {
            "FMT_ID": "FMT_ID",
        }

    def define(self):
        '''
            Return dictionary elements containing the data model and 
            the data mapping, respectively.
        '''
        table_elem = { self.table_name : self.model }
        mapping_elem = { self.table_name : self.mapping }
        return table_elem, mapping_elem

# ---------- WAREHOUSE MODEL ----------
class WarehouseSIM_SIVEP(WarehouseBase):
    def __init__(self, engine_url):
        self._engine = create_engine(engine_url, future=True)
        self._metadata = MetaData()
        self._tables = {}
        self._mappings = {}

        # -- include the data models
        self._imported_data_models = [ Pessoa(self._metadata).define(),
                                       PositivePairsLabel(self._metadata).define(),
                                       NegativePairsLabel(self._metadata).define() ]

        for elem in self._imported_data_models:
            self._tables.update(elem[0])
            self._mappings.update(elem[1])  