import shlex
from table_schema import Column, TableSchema
from database_manager import DatabaseManager

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

