import shlex
from table_schema import Column, TableSchema
from database_manager import DatabaseManager

class DatabaseCLI:
    VALID_TYPES = {'int', 'float', 'string', 'boolean', 'date'}
 
    def __init__(self, db_path):
        self.db_manager = DatabaseManager(db_path)

    def _show_help(self):
        print("\n--- Comandos Disponíveis ---")
        print("CREATE TABLE")
        print("  -> Inicia um wizard para criar uma nova tabela.")
        print(f"  -> Tipos de dados permitidos: {', '.join(self.VALID_TYPES)}.")
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
            
            key, value_str = part.split('=', 1)
            value_str_lower = value_str.lower()
            
            value = None
            if value_str_lower == 'true':
                value = True
            elif value_str_lower == 'false':
                value = False
            elif value_str.isdigit():
                value = int(value_str)
            elif value_str.replace('.', '', 1).isdigit():
                value = float(value_str)
            else:
                value = value_str.strip("'\"")

            record[key] = value
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
                elif command == "LIST" and len(parts) > 1 and parts[1].upper() == "TABLES":
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
                
                elif command == "CREATE" and len(parts) > 1 and parts[1].upper() == "TABLE":
                    name = input("Nome da tabela: ")
                    pk = input("Nome da chave primária (ex: id): ")
                    
                    pk_type = ""
                    while pk_type not in self.VALID_TYPES:
                        pk_type = input(f"Tipo da chave primária '{pk}' (ex: int): ").lower()
                        if pk_type not in self.VALID_TYPES:
                            print(f"ERRO: Tipo inválido. Tipos permitidos: {', '.join(self.VALID_TYPES)}")
                    
                    cols = [Column(pk, pk_type, nullable=False)]
                    
                    while True:
                        col_str = input("Adicionar coluna (nome:tipo) ou 'fim' para terminar: ")
                        if col_str.lower() == 'fim': break
                        
                        if ':' not in col_str:
                            print("ERRO: Formato inválido. Use 'nome:tipo'. Tente novamente.")
                            continue

                        c_name, c_type = col_str.split(':', 1)
                        c_type = c_type.lower()
                        
                        if c_type not in self.VALID_TYPES:
                            print(f"ERRO: Tipo de dado '{c_type}' é inválido.")
                            print(f"Tipos permitidos são: {', '.join(self.VALID_TYPES)}. Tente novamente.")
                            continue

                        nullable_str = input(f"A coluna '{c_name}' pode ser nula? (s/N): ").lower()
                        cols.append(Column(c_name, c_type, nullable_str == 's'))
                        print(f" -> Coluna '{c_name}' adicionada com sucesso.")
                    
                    # --- VALIDAÇÃO DE CHAVE ESTRANGEIRA (FK) ---
                    fks = []
                    col_names_in_new_table = {c.name for c in cols} # Conjunto para busca rápida
                    
                    while True:
                        fk_str = input("Adicionar FK (coluna:tabela_ref:coluna_ref) ou 'fim' para terminar: ")
                        if fk_str.lower() == 'fim': break
                        
                        # 1. Validação do formato do comando
                        try:
                            fk_col, ref_table, ref_col = fk_str.split(':')
                        except ValueError:
                            print("ERRO: Formato inválido. Use o formato 'coluna:tabela_ref:coluna_ref'.")
                            continue

                        # 2. Validação da coluna local
                        if fk_col not in col_names_in_new_table:
                            print(f"ERRO: A coluna '{fk_col}' não foi definida nesta tabela. Defina a coluna primeiro.")
                            continue
                        
                        # 3. Validação da tabela de referência
                        if ref_table not in self.db_manager.tables:
                            print(f"ERRO: A tabela de referência '{ref_table}' não existe.")
                            continue
                            
                        # 4. Validação da coluna de referência (deve ser a PK da outra tabela)
                        referenced_schema = self.db_manager.tables[ref_table]
                        if ref_col != referenced_schema.pk_name:
                            print(f"ERRO: A coluna de referência '{ref_col}' não é a chave primária da tabela '{ref_table}'.")
                            print(f"       Chaves estrangeiras devem apontar para a chave primária (que é '{referenced_schema.pk_name}').")
                            continue

                        # Se todas as validações passaram, adiciona a FK
                        fks.append({'fk_col': fk_col, 'ref_table': ref_table, 'ref_col': ref_col})
                        print(f" -> FK em '{fk_col}' referenciando '{ref_table}({ref_col})' adicionada com sucesso.")

                    schema = TableSchema(name, cols, pk, fks)
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
                        headers = results[0].keys()
                        print(" | ".join(headers))
                        print("-" * (sum(len(str(h)) for h in headers) + 3 * len(headers)))
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
                    if command_line:
                        print(f"ERRO: Comando '{command_line}' desconhecido ou incompleto.")

            except (ValueError, IndexError, KeyError, TypeError) as e:
                print(f"ERRO: {e}")
            except Exception as e:
                print(f"ERRO INESPERADO: {e}")