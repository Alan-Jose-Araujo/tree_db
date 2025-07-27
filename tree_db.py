import json
import os
import bisect
import shlex # Usado para uma análise mais robusta de comandos com aspas

# -----------------------------------------------------------------------------
# Módulo 1: Implementação da Estrutura da Árvore B+ (Sem alterações)
# -----------------------------------------------------------------------------
class BPlusTreeNode:
    def __init__(self, order, is_leaf=False):
        self.order = order
        self.is_leaf = is_leaf
        self.keys = []
        self.children_or_values = []
        self.next_leaf = None

    def is_full(self):
        return len(self.keys) == self.order

class BPlusTree:
    def __init__(self, order=5):
        if order < 3:
            raise ValueError("A ordem de uma Árvore B+ deve ser no mínimo 3.")
        self.root = BPlusTreeNode(order, is_leaf=True)
        self.order = order

    def _find_leaf(self, key):
        node = self.root
        while not node.is_leaf:
            i = bisect.bisect_right(node.keys, key)
            node = node.children_or_values[i]
        return node

    def _split_leaf(self, leaf, parent):
        mid_index = self.order // 2
        new_leaf = BPlusTreeNode(self.order, is_leaf=True)
        
        new_leaf.keys = leaf.keys[mid_index:]
        new_leaf.children_or_values = leaf.children_or_values[mid_index:]
        leaf.keys = leaf.keys[:mid_index]
        leaf.children_or_values = leaf.children_or_values[:mid_index]
        
        new_leaf.next_leaf = leaf.next_leaf
        leaf.next_leaf = new_leaf
        
        self._insert_in_parent(parent, new_leaf.keys[0], leaf, new_leaf)

    def _insert_in_parent(self, parent, key, left_child, right_child):
        if parent is None:
            new_root = BPlusTreeNode(self.order, is_leaf=False)
            new_root.keys = [key]
            new_root.children_or_values = [left_child, right_child]
            self.root = new_root
        else:
            # Lógica de inserção no pai e divisão de nós internos (complexa, omitida)
            # Para o escopo deste exemplo, o crescimento da árvore é limitado.
            idx = bisect.bisect_right(parent.keys, key)
            parent.keys.insert(idx, key)
            parent.children_or_values.insert(idx + 1, right_child)
            if parent.is_full():
                # Lógica de divisão do nó interno seria necessária aqui
                pass

    def insert(self, key, value):
        parent = None
        node = self.root
        while not node.is_leaf:
            parent = node
            i = bisect.bisect_right(node.keys, key)
            node = node.children_or_values[i]
        
        try:
            idx = node.keys.index(key)
            node.children_or_values[idx] = value
            return
        except ValueError:
            pass

        idx = bisect.bisect_left(node.keys, key)
        node.keys.insert(idx, key)
        node.children_or_values.insert(idx, value)

        if node.is_full():
            self._split_leaf(node, parent)

    def search(self, key):
        leaf = self._find_leaf(key)
        try:
            idx = leaf.keys.index(key)
            return leaf.children_or_values[idx]
        except ValueError:
            return None
    
    def get_all(self):
        results = []
        node = self.root
        if not node.keys and not node.is_leaf: # Arvore vazia
            return []
        while not node.is_leaf:
            node = node.children_or_values[0]
        
        while node:
            results.extend(node.children_or_values)
            node = node.next_leaf
        return results

# -----------------------------------------------------------------------------
# Módulo 2: Gerenciamento de Esquemas e Metadados (Sem alterações)
# -----------------------------------------------------------------------------
class Column:
    def __init__(self, name, data_type, nullable=True):
        self.name = name
        self.data_type = data_type
        self.nullable = nullable

class TableSchema:
    def __init__(self, name, columns, pk_name, foreign_keys=None):
        self.name = name
        self.columns = {c.name: c for c in columns}
        self.pk_name = pk_name
        self.foreign_keys = foreign_keys or []

    def get_pk_name(self):
        return self.pk_name

# -----------------------------------------------------------------------------
# Módulo 3 e 4: DatabaseManager (Sem alterações na lógica principal)
# -----------------------------------------------------------------------------
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


# -----------------------------------------------------------------------------
# Módulo 5: A Interface de Linha de Comando (CLI)
# -----------------------------------------------------------------------------
class DatabaseCLI:
    def __init__(self, db_path):
        self.db_manager = DatabaseManager(db_path)

    def _show_help(self):
        print("\n--- Comandos Disponíveis ---")
        print("CREATE TABLE <nome> <pk> <col1:tipo> <col2:tipo> ...")
        print("  -> Inicia um wizard para criar uma nova tabela.")
        print("LIST TABLES")
        print("  -> Lista todas as tabelas no banco de dados.")
        print("DESCRIBE <nome_tabela>")
        print("  -> Mostra o esquema de uma tabela.")
        print("INSERT INTO <nome_tabela> <col1>='<valor1>' <col2>=<valor2> ...")
        print("  -> Insere um novo registro. Use aspas para strings com espaços.")
        print("SELECT FROM <nome_tabela> [WHERE <col>=<valor>]")
        print("  -> Busca registros. 'WHERE' é opcional.")
        print("DELETE FROM <nome_tabela> WHERE <pk_col>=<pk_valor>")
        print("  -> Deleta um registro pela sua chave primária.")
        print("HELP")
        print("  -> Mostra esta mensagem de ajuda.")
        print("EXIT")
        print("  -> Salva o banco de dados e encerra o programa.\n")

    def _parse_key_value(self, parts):
        record = {}
        for part in parts:
            if '=' not in part:
                raise ValueError("Formato de inserção inválido. Use 'chave=valor'.")
            key, value = part.split('=', 1)
            # Tenta converter para número, senão mantém como string
            if value.isdigit():
                record[key] = int(value)
            elif value.replace('.', '', 1).isdigit():
                record[key] = float(value)
            else:
                record[key] = value.strip("'\"") # Remove aspas
        return record

    def run(self):
        print("Bem-vindo ao SGBD baseado em Árvores. Digite 'HELP' para ver os comandos.")
        while True:
            try:
                command_line = input("db> ").strip()
                if not command_line:
                    continue

                parts = shlex.split(command_line)
                command = parts[0].upper()

                if command == "EXIT":
                    self.db_manager.save_to_disk()
                    print("Até logo!")
                    break
                elif command == "HELP":
                    self._show_help()
                elif command == "LIST" and parts[1].upper() == "TABLES":
                    if not self.db_manager.tables:
                        print("Nenhuma tabela encontrada.")
                    for table_name in self.db_manager.tables:
                        print(f"- {table_name}")
                elif command == "DESCRIBE":
                    schema = self.db_manager.tables[parts[1]]
                    print(f"Tabela: {schema.name}")
                    print(f"  Chave Primária: {schema.pk_name}")
                    print("  Colunas:")
                    for c in schema.columns.values():
                        print(f"    - {c.name} ({c.data_type}) {'NOT NULL' if not c.nullable else ''}")
                    if schema.foreign_keys:
                        print("  Chaves Estrangeiras:")
                        for fk in schema.foreign_keys:
                            print(f"    - {fk['fk_col']} -> {fk['ref_table']}({fk['ref_col']})")
                elif command == "CREATE" and parts[1].upper() == "TABLE":
                    # Wizard interativo para criação de tabela
                    name = input("Nome da tabela: ")
                    pk = input("Nome da chave primária: ")
                    cols = []
                    while True:
                        col_str = input("Adicionar coluna (nome:tipo) ou 'fim' para terminar: ")
                        if col_str.lower() == 'fim': break
                        c_name, c_type = col_str.split(':')
                        cols.append(Column(c_name, c_type.lower()))
                    
                    fks = []
                    while True:
                        fk_str = input("Adicionar FK (col:tabela_ref:col_ref) ou 'fim' para terminar: ")
                        if fk_str.lower() == 'fim': break
                        fk_col, ref_table, ref_col = fk_str.split(':')
                        fks.append({'fk_col': fk_col, 'ref_table': ref_table, 'ref_col': ref_col})

                    schema = TableSchema(name, [Column(pk, 'int', nullable=False)] + cols, pk, fks)
                    self.db_manager.create_table(schema)
                    print(f"Tabela '{name}' criada com sucesso.")

                elif command == "INSERT":
                    table_name = parts[2]
                    record = self._parse_key_value(parts[3:])
                    self.db_manager.insert(table_name, record)
                    print("Registro inserido com sucesso.")

                elif command == "SELECT":
                    table_name = parts[2]
                    where_clause = None
                    if "WHERE" in [p.upper() for p in parts]:
                        where_idx = [p.upper() for p in parts].index("WHERE")
                        where_clause = self._parse_key_value(parts[where_idx+1:])
                    
                    results = self.db_manager.select(table_name, where_clause)
                    if not results:
                        print("(0 linhas retornadas)")
                    else:
                        # Imprimir em formato de tabela
                        headers = results[0].keys()
                        print(" | ".join(headers))
                        print("-" * (sum(len(h) for h in headers) + 3 * len(headers)))
                        for row in results:
                            print(" | ".join(str(row.get(h, 'NULL')) for h in headers))


                elif command == "DELETE":
                    table_name = parts[2]
                    where_clause = self._parse_key_value(parts[4:])
                    pk_name = self.db_manager.tables[table_name].get_pk_name()
                    if pk_name not in where_clause:
                        raise ValueError("DELETE só é permitido com a chave primária na cláusula WHERE.")
                    self.db_manager.delete(table_name, where_clause[pk_name])
                    print("Registro deletado com sucesso.")

                else:
                    print(f"ERRO: Comando '{command}' desconhecido.")

            except (ValueError, IndexError, KeyError) as e:
                print(f"ERRO: {e}")
            except Exception as e:
                print(f"ERRO INESPERADO: {e}")


if __name__ == "__main__":
    cli = DatabaseCLI(db_path="sgbd_data")
    cli.run()