import os
import json
from bplustree import BPlusTree
from table_schema import Column, TableSchema
from datetime import datetime

class DatabaseManager:
    def __init__(self, db_path='my_db'):
        self.db_path = db_path
        self.tables = {}
        self.data = {}
        if not os.path.exists(db_path):
            os.makedirs(db_path)
        self.load_from_disk()

    def create_table(self, schema: TableSchema):
        if schema.name in self.tables:
            raise ValueError(f"Tabela '{schema.name}' já existe.")
        self.tables[schema.name] = schema
        self.data[schema.name] = BPlusTree(order=50)
    
    def insert(self, table_name, record: dict):
        if table_name not in self.tables:
            raise ValueError(f"Tabela '{table_name}' não encontrada.")
        
        schema = self.tables[table_name]

        # --- NOVA VALIDAÇÃO DE ESQUEMA E TIPOS ---
        # 1. Verificar se o registro não contém colunas que não existem no esquema
        for col_name in record:
            if col_name not in schema.columns:
                raise ValueError(f"Coluna '{col_name}' não existe no esquema da tabela '{table_name}'.")

        # 2. Validar cada coluna definida no esquema contra o registro
        for col_name, col_schema in schema.columns.items():
            value = record.get(col_name)
            expected_type = col_schema.data_type.lower()

            # A. Validação de Nulidade
            if value is None:
                if not col_schema.nullable:
                    raise ValueError(f"Erro de integridade: Coluna '{col_name}' não pode ser nula.")
                continue # O valor é nulo e a coluna permite, então está OK.

            # B. Validação de Tipos de Dados
            if expected_type == 'int' and not isinstance(value, int):
                raise TypeError(f"Tipo de dado inválido para '{col_name}'. Esperado: int, recebido: {type(value).__name__}.")
            elif expected_type == 'float' and not isinstance(value, (int, float)):
                raise TypeError(f"Tipo de dado inválido para '{col_name}'. Esperado: float, recebido: {type(value).__name__}.")
            elif expected_type == 'string' and not isinstance(value, str):
                raise TypeError(f"Tipo de dado inválido para '{col_name}'. Esperado: string, recebido: {type(value).__name__}.")
            elif expected_type == 'boolean' and not isinstance(value, bool):
                raise TypeError(f"Tipo de dado inválido para '{col_name}'. Esperado: boolean, recebido: {type(value).__name__}.")
            elif expected_type == 'date':
                if not isinstance(value, str):
                    raise TypeError(f"Tipo de dado inválido para '{col_name}'. Esperado: string no formato AAAA-MM-DD, recebido: {type(value).__name__}.")
                try:   datetime.strptime(value, '%Y-%m-%d')
                except ValueError:
                    raise ValueError(f"Formato de data inválido para '{col_name}'. Use o formato AAAA-MM-DD.")
        # --- FIM DA NOVA VALIDAÇÃO ---

        pk_name = schema.get_pk_name()
        pk_value = record.get(pk_name)

        # Validações de Chaves (PK e FK)
        if pk_value is None:
            raise ValueError(f"Erro de integridade: Chave primária '{pk_name}' não pode ser nula.")
        if self.data[table_name].search(pk_value) is not None:
            raise ValueError(f"Erro de integridade: Chave primária duplicada '{pk_value}'.")
        for fk in schema.foreign_keys:
            fk_value = record.get(fk['fk_col'])
            if fk_value is not None:
                ref_table = fk['ref_table']
                if ref_table not in self.data or self.data[ref_table].search(fk_value) is None:
                    raise ValueError(f"Erro de integridade: FK '{fk_value}' não existe na tabela '{ref_table}'.")

        self.data[table_name].insert(pk_value, record)

    def select(self, table_name, where_clause=None):
        if table_name not in self.data:
            raise ValueError(f"Tabela '{table_name}' não encontrada.")
        tree = self.data[table_name]
        pk_name = self.tables[table_name].get_pk_name()

        if not where_clause:
            return tree.get_all()
        if len(where_clause) == 1 and pk_name in where_clause:
            result = tree.search(where_clause[pk_name])
            return [result] if result else []
        
        all_records = tree.get_all()
        filtered = [r for r in all_records if all(r.get(k) == v for k, v in where_clause.items())]
        return filtered

    def delete(self, table_name, pk_value):
        if table_name not in self.tables:
            raise ValueError(f"Tabela '{table_name}' não encontrada.")
        for other_table, other_schema in self.tables.items():
            if other_table == table_name: continue
            for fk in other_schema.foreign_keys:
                if fk['ref_table'] == table_name:
                    if self.select(other_table, where_clause={fk['fk_col']: pk_value}):
                        raise ValueError(f"Erro: Não é possível deletar, registro é referenciado em '{other_table}'.")
        
        leaf = self.data[table_name]._find_leaf(pk_value)
        try:
            idx = leaf.keys.index(pk_value)
            leaf.keys.pop(idx)
            leaf.children_or_values.pop(idx)
        except ValueError:
            raise ValueError(f"Registro com PK '{pk_value}' não encontrado para deleção.")

    def save_to_disk(self):
        metadata_path = os.path.join(self.db_path, 'metadata.json')
        metadata = { name: schema.__dict__ for name, schema in self.tables.items() }
        for name in metadata:
            metadata[name]['columns'] = [c.__dict__ for c in self.tables[name].columns.values()]
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=4)
        
        for table_name, tree in self.data.items():
            data_path = os.path.join(self.db_path, f"{table_name}.json")
            with open(data_path, 'w') as f:
                json.dump(tree.get_all(), f, indent=4)
        print("INFO: Banco de dados salvo em disco.")

    def load_from_disk(self):
        metadata_path = os.path.join(self.db_path, 'metadata.json')
        if not os.path.exists(metadata_path):
            print("INFO: Nenhum arquivo de metadados encontrado. Iniciando novo banco de dados.")
            return

        with open(metadata_path, 'r') as f:
            metadata = json.load(f)
        
        for name, schema_data in metadata.items():
            columns = [Column(**c) for c in schema_data['columns']]
            schema = TableSchema(name, columns, schema_data['pk_name'], schema_data.get('foreign_keys'))
            self.tables[schema.name] = schema
            self.data[schema.name] = BPlusTree(order=50)

            data_path = os.path.join(self.db_path, f"{name}.json")
            if os.path.exists(data_path):
                with open(data_path, 'r') as f:
                    records = json.load(f)
                for record in records:
                    pk_value = record[schema.get_pk_name()]
                    self.data[name].insert(pk_value, record)
        print(f"INFO: Banco de dados '{self.db_path}' carregado com sucesso.")