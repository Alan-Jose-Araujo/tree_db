import os
import json
from bplustree import BPlusTree
from table_schema import Column, TableSchema

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
        self.data[schema.name] = BPlusTree(order=50) # Ordem maior para menos acessos a disco
    
    def insert(self, table_name, record: dict):
        if table_name not in self.tables:
            raise ValueError(f"Tabela '{table_name}' não encontrada.")
        
        schema = self.tables[table_name]
        pk_name = schema.get_pk_name()
        pk_value = record.get(pk_name)

        # Validações...
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
        # Salva metadados
        metadata_path = os.path.join(self.db_path, 'metadata.json')
        metadata = { name: schema.__dict__ for name, schema in self.tables.items() }
        for name in metadata:
            metadata[name]['columns'] = [c.__dict__ for c in self.tables[name].columns.values()]
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=4)
        
        # Salva dados
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