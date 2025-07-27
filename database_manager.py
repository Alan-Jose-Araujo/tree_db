import tkinter as tk
from tkinter import messagebox
import os
import json
from datetime import datetime

# --- MOCK BPlusTree, Column, and TableSchema for self-contained example ---
# Essas classes são versões simplificadas para permitir que o DatabaseManager
# funcione sem depender de implementações complexas de bplustree ou table_schema.

class BPlusTreeNode:
    """
    Um nó mock de BPlusTree para representação interna simplificada.
    Usado pela BPlusTree mock e adaptado para o método delete do DatabaseManager.
    """
    def __init__(self, order):
        self.order = order
        self.keys = []
        self.children_or_values = []
        self.is_leaf = True
        self.next_leaf = None # Não usado neste mock, mas parte de uma B+ Tree real

class BPlusTree:
    """
    Uma B-Plus Tree mock altamente simplificada usando um dicionário para armazenamento.
    Esta classe destina-se apenas a permitir que o DatabaseManager seja executado para fins de demonstração,
    e não para fornecer a funcionalidade completa de uma B-Plus Tree.
    """
    def __init__(self, order=50):
        # Usamos um dicionário interno para simular o armazenamento de chave-valor.
        # Uma B-Plus Tree real gerenciaria seus nós e se balancearia.
        self._data = {} 
        self.order = order # Mantém a ordem para consistência com a inicialização do DatabaseManager

    def insert(self, key, value):
        """Insere um par chave-valor na árvore mock."""
        self._data[key] = value

    def search(self, key):
        """Procura uma chave e retorna seu valor associado."""
        return self._data.get(key)

    def get_all(self):
        """Retorna todos os valores (registros) atualmente armazenados na árvore mock."""
        return list(self._data.values())

    def _find_leaf(self, pk_value):
        """
        Simula a localização de um nó folha. Para fins de exclusão, retorna um nó folha mock
        se a chave primária existir, permitindo que o método delete do DatabaseManager
        interaja com ele (embora delete seja modificado para ignorar este mock).
        """
        if pk_value in self._data:
            # Cria um nó folha mock que imita a estrutura esperada pelo método delete original
            mock_leaf = BPlusTreeNode(self.order)
            mock_leaf.is_leaf = True
            mock_leaf.keys = [pk_value]
            mock_leaf.children_or_values = [self._data[pk_value]]
            return mock_leaf
        return None # Chave não encontrada

class Column:
    """Representa uma coluna em um esquema de tabela de banco de dados."""
    def __init__(self, name, data_type, primary_key=False, nullable=True):
        self.name = name
        self.data_type = data_type
        self.primary_key = primary_key
        self.nullable = nullable

class TableSchema:
    """Representa o esquema de uma tabela de banco de dados."""
    def __init__(self, name, columns, pk_name=None, foreign_keys=None):
        self.name = name
        # Armazena as colunas como um dicionário para fácil consulta pelo nome
        self.columns = {c.name: c for c in columns}
        # Se pk_name não for fornecido, tenta inferi-lo a partir das colunas
        self.pk_name = pk_name if pk_name else self._infer_pk_name(columns)
        self.foreign_keys = foreign_keys if foreign_keys is not None else []

    def _infer_pk_name(self, columns):
        """Infer o nome da chave primária a partir da lista de colunas."""
        for col in columns:
            if col.primary_key:
                return col.name
        return None

    def get_pk_name(self):
        """Retorna o nome da coluna da chave primária."""
        if not self.pk_name:
            raise ValueError(f"Tabela '{self.name}' não tem chave primária definida.")
        return self.pk_name

# --- Classe DatabaseManager (Fornecida pelo usuário) ---
class DatabaseManager:
    """
    Gerencia as operações do banco de dados, incluindo criação de tabelas, inserção de dados,
    seleção, exclusão e persistência em disco.
    """
    def __init__(self, db_path='my_db'):
        self.db_path = db_path
        self.tables = {}  # Armazena objetos TableSchema
        self.data = {}    # Armazena instâncias BPlusTree para cada tabela
        
        # Cria o diretório do banco de dados se não existir
        if not os.path.exists(db_path):
            os.makedirs(db_path)
        
        self.load_from_disk()

    def create_table(self, schema: TableSchema):
        """
        Cria uma nova tabela com o esquema fornecido.
        Levanta ValueError se uma tabela com o mesmo nome já existir.
        """
        if schema.name in self.tables:
            raise ValueError(f"Tabela '{schema.name}' já existe.")
        self.tables[schema.name] = schema
        self.data[schema.name] = BPlusTree(order=50) # Inicializa BPlusTree para nova tabela
    
    def insert(self, table_name, record: dict):
        """
        Insere um registro na tabela especificada.
        Realiza validação de esquema, verificação de tipo e verificações de integridade (PK/FK).
        """
        if table_name not in self.tables:
            raise ValueError(f"Tabela '{table_name}' não encontrada.")
        
        schema = self.tables[table_name]

        # 1. Valida se o registro contém apenas colunas definidas no esquema
        for col_name in record:
            if col_name not in schema.columns:
                raise ValueError(f"Coluna '{col_name}' não existe no esquema da tabela '{table_name}'.")

        # 2. Valida cada coluna em relação à sua definição de esquema
        for col_name, col_schema in schema.columns.items():
            value = record.get(col_name)
            expected_type = col_schema.data_type.lower()

            # A. Validação de nulidade
            if value is None:
                if not col_schema.nullable:
                    raise ValueError(f"Erro de integridade: Coluna '{col_name}' não pode ser nula.")
                continue # O valor é nulo e permitido, então passa para a próxima coluna

            # B. Validação do tipo de dados
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
                try: 
                    datetime.strptime(value, '%Y-%m-%d')
                except ValueError:
                    raise ValueError(f"Formato de data inválido para '{col_name}'. Use o formato AAAA-MM-DD.")
        
        # Validação da Chave Primária (PK)
        pk_name = schema.get_pk_name()
        pk_value = record.get(pk_name)

        if pk_value is None:
            raise ValueError(f"Erro de integridade: Chave primária '{pk_name}' não pode ser nula.")
        if self.data[table_name].search(pk_value) is not None:
            raise ValueError(f"Erro de integridade: Chave primária duplicada '{pk_value}'.")
        
        # Validação da Chave Estrangeira (FK)
        for fk in schema.foreign_keys:
            fk_value = record.get(fk['fk_col'])
            if fk_value is not None:
                ref_table = fk['ref_table']
                if ref_table not in self.data or self.data[ref_table].search(fk_value) is None:
                    raise ValueError(f"Erro de integridade: FK '{fk_value}' não existe na tabela '{ref_table}'.")

        self.data[table_name].insert(pk_value, record)

    def select(self, table_name, where_clause=None):
        """
        Seleciona registros de uma tabela.
        Pode filtrar por uma cláusula where (apenas correspondências exatas).
        """
        if table_name not in self.data:
            raise ValueError(f"Tabela '{table_name}' não encontrada.")
        tree = self.data[table_name]
        pk_name = self.tables[table_name].get_pk_name()

        if not where_clause:
            return tree.get_all()
        
        # Otimizado para pesquisa de chave primária se apenas PK estiver na cláusula where
        if len(where_clause) == 1 and pk_name in where_clause:
            result = tree.search(where_clause[pk_name])
            return [result] if result else []
        
        # Filtragem geral para outras colunas ou múltiplas condições
        all_records = tree.get_all()
        filtered = [r for r in all_records if all(r.get(k) == v for k, v in where_clause.items())]
        return filtered

    def delete(self, table_name, pk_value):
        """
        Exclui um registro da tabela especificada por sua chave primária.
        Realiza verificações de integridade de chave estrangeira antes da exclusão.
        """
        if table_name not in self.tables:
            raise ValueError(f"Tabela '{table_name}' não encontrada.")
        
        # Verifica se há chaves estrangeiras dependentes em outras tabelas
        for other_table, other_schema in self.tables.items():
            if other_table == table_name: 
                continue # Pula a tabela da qual estamos excluindo
            for fk in other_schema.foreign_keys:
                if fk['ref_table'] == table_name:
                    # Se qualquer outra tabela fizer referência a este registro, impede a exclusão
                    if self.select(other_table, where_clause={fk['fk_col']: pk_value}):
                        raise ValueError(f"Erro: Não é possível deletar, registro é referenciado em '{other_table}'.")
        
        # Exclui o registro diretamente do dicionário interno da BPlusTree mock
        # O código original interagia com nós folha, mas nosso mock simplifica isso.
        if pk_value in self.data[table_name]._data:
            del self.data[table_name]._data[pk_value]
        else:
            raise ValueError(f"Registro com PK '{pk_value}' não encontrado para deleção.")

    def save_to_disk(self):
        """
        Salva o estado atual do banco de dados (esquemas e dados) em arquivos JSON no disco.
        Os metadados (esquemas de tabela) são salvos em 'metadata.json'.
        Os dados de cada tabela são salvos em um arquivo JSON separado (ex: 'tablename.json').
        """
        metadata_path = os.path.join(self.db_path, 'metadata.json')
        
        # Prepara metadados para serialização JSON: converte objetos Column para dicionários
        metadata = {}
        for name, schema in self.tables.items():
            # Cria uma cópia do dicionário do esquema para evitar modificar o objeto original
            schema_dict = schema.__dict__.copy() 
            # Converte o dicionário de objetos Column para uma lista de suas representações de dicionário
            schema_dict['columns'] = [c.__dict__ for c in schema.columns.values()]
            metadata[name] = schema_dict

        # Salva esquemas de tabela (metadados)
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=4)
        
        # Salva dados para cada tabela
        for table_name, tree in self.data.items():
            data_path = os.path.join(self.db_path, f"{table_name}.json")
            with open(data_path, 'w') as f:
                # O método get_all() da BPlusTree mock retorna uma lista de registros, que é serializável em JSON
                json.dump(tree.get_all(), f, indent=4)
        
        print("INFO: Banco de dados salvo em disco.")

    def load_from_disk(self):
        """
        Carrega o estado do banco de dados (esquemas e dados) de arquivos JSON no disco.
        Se 'metadata.json' não for encontrado, um novo banco de dados vazio é inicializado.
        """
        metadata_path = os.path.join(self.db_path, 'metadata.json')
        if not os.path.exists(metadata_path):
            print("INFO: Nenhum arquivo de metadados encontrado. Iniciando novo banco de dados.")
            return

        # Carrega esquemas de tabela (metadados)
        with open(metadata_path, 'r') as f:
            metadata = json.load(f)
        
        for name, schema_data in metadata.items():
            # Reconstrói objetos Column a partir dos dados do dicionário carregado
            columns = [Column(name=c['name'], data_type=c['data_type'], 
                              primary_key=c.get('primary_key', False), # Usa .get() para chaves opcionais
                              nullable=c.get('nullable', True)) 
                       for c in schema_data['columns']]
            
            # Reconstrói o objeto TableSchema, incluindo chaves estrangeiras se presentes
            schema = TableSchema(name, columns, schema_data['pk_name'], schema_data.get('foreign_keys'))
            self.tables[schema.name] = schema
            self.data[schema.name] = BPlusTree(order=50) # Inicializa uma nova BPlusTree para esta tabela

            # Carrega dados para a tabela atual
            data_path = os.path.join(self.db_path, f"{name}.json")
            if os.path.exists(data_path):
                with open(data_path, 'r') as f:
                    records = json.load(f)
                for record in records:
                    pk_value = record[schema.get_pk_name()]
                    self.data[name].insert(pk_value, record) # Insere registros na BPlusTree
        
        print(f"INFO: Banco de dados '{self.db_path}' carregado com sucesso.")


# --- Aplicativo GUI usando Tkinter ---
class DatabaseGUI:
    """
    Uma interface gráfica Tkinter simples para interagir com o DatabaseManager.
    Fornece botões para salvar, recarregar e inserir dados de exemplo.
    Exibe mensagens e dados da tabela atual em uma área de texto.
    """
    def __init__(self, master):
        self.master = master
        master.title("Simulador de SGBD (com Salvar/Recarregar)")
        master.geometry("600x450") # Altura aumentada
        master.option_add('*Font', 'Inter 10') # Define a fonte padrão para todos os widgets

        # Inicializa o DatabaseManager
        self.db_manager = DatabaseManager()
        
        # Cria os widgets da GUI
        self.create_widgets()
        
        # Configura alguns dados iniciais ou carrega dados existentes se for a primeira execução
        self.setup_initial_data() 

    def create_widgets(self):
        """
        Cria os principais widgets Tkinter para a GUI: botões e uma área de exibição.
        """
        # Frame para botões de controle
        control_frame = tk.Frame(self.master, padx=10, pady=10, bg="#e0e0e0")
        control_frame.pack(side=tk.TOP, fill=tk.X, pady=(10, 5))

        # Botão Salvar
        self.save_button = tk.Button(control_frame, text="Salvar Dados", command=self.save_database, 
                                     bg="#4CAF50", fg="white", activebackground="#45a049", 
                                     relief=tk.RAISED, bd=3, cursor="hand2", width=15)
        self.save_button.pack(side=tk.LEFT, padx=5, pady=5)

        # Botão Recarregar (útil para demonstrar que o salvamento funciona)
        self.reload_button = tk.Button(control_frame, text="Recarregar Dados", command=self.reload_database, 
                                       bg="#2196F3", fg="white", activebackground="#1e88e5",
                                       relief=tk.RAISED, bd=3, cursor="hand2", width=15)
        self.reload_button.pack(side=tk.LEFT, padx=5, pady=5)
        
        # Botão Inserir Dados (para demonstração de adição de novos dados para salvar)
        self.insert_button = tk.Button(control_frame, text="Inserir Dados Exemplo", command=self.insert_example_data,
                                       bg="#FFC107", fg="#333", activebackground="#fbc02d",
                                       relief=tk.RAISED, bd=3, cursor="hand2", width=20)
        self.insert_button.pack(side=tk.RIGHT, padx=5, pady=5)

        # Frame da Área de Exibição
        self.display_frame = tk.Frame(self.master, padx=10, pady=10, bg="#f5f5f5")
        self.display_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=10, pady=(5, 10))

        # Widget de texto para exibir mensagens e dados da tabela
        self.display_text = tk.Text(self.display_frame, wrap=tk.WORD, state=tk.DISABLED, 
                                    bg="#ffffff", fg="#333", relief=tk.SUNKEN, bd=2, 
                                    font=('Inter', 9), padx=5, pady=5)
        self.display_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # Barra de rolagem para a área de texto
        self.scrollbar = tk.Scrollbar(self.display_frame, command=self.display_text.yview)
        self.scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.display_text.config(yscrollcommand=self.scrollbar.set)
        
        # Adiciona um placeholder para uma barra de status ou mensagem simples
        self.status_label = tk.Label(self.master, text="Pronto.", bd=1, relief=tk.SUNKEN, anchor=tk.W, 
                                     font=('Inter', 8), bg="#e0e0e0", fg="#555")
        self.status_label.pack(side=tk.BOTTOM, fill=tk.X, ipady=2)


    def log_message(self, message):
        """
        Adiciona uma mensagem à área de texto de exibição.
        Garante que a área de texto possa ser rolada até o final.
        """
        self.display_text.config(state=tk.NORMAL) # Habilita a edição temporariamente
        self.display_text.insert(tk.END, message + "\n")
        self.display_text.see(tk.END) # Rola até o final
        self.display_text.config(state=tk.DISABLED) # Desabilita a edição
        self.status_label.config(text=message) # Atualiza a barra de status

    def save_database(self):
        """
        Chama o método save_to_disk do DatabaseManager e fornece feedback.
        """
        try:
            self.db_manager.save_to_disk()
            self.log_message("Banco de dados salvo com sucesso!")
            messagebox.showinfo("Salvar", "Dados salvos com sucesso!")
        except Exception as e:
            self.log_message(f"Erro ao salvar: {e}")
            messagebox.showerror("Erro", f"Falha ao salvar dados: {e}")

    def reload_database(self):
        """
        Reinicializa o DatabaseManager para forçar um recarregamento dos dados do disco.
        Útil para verificar se os dados salvos persistem.
        """
        try:
            # Reinicializa o DatabaseManager que chamará load_from_disk
            self.db_manager = DatabaseManager() 
            self.log_message("Banco de dados recarregado do disco.")
            self.display_current_data() # Exibe os dados recarregados
            messagebox.showinfo("Recarregar", "Dados recarregados do disco.")
        except Exception as e:
            self.log_message(f"Erro ao recarregar: {e}")
            messagebox.showerror("Erro", f"Falha ao recarregar dados: {e}")

    def setup_initial_data(self):
        """
        Configura uma tabela de exemplo ('usuarios') se ela ainda não existir.
        Isso é executado quando o aplicativo é iniciado.
        """
        try:
            # Verifica se a tabela 'usuarios' existe a partir do load_from_disk anterior
            if "usuarios" not in self.db_manager.tables:
                self.log_message("Configurando dados iniciais...")
                user_schema = TableSchema(
                    name="usuarios",
                    columns=[
                        Column("id", "int", primary_key=True),
                        Column("nome", "string"),
                        Column("email", "string", nullable=False),
                        Column("idade", "int", nullable=True)
                    ],
                    pk_name="id" # Define explicitamente o nome da PK
                )
                self.db_manager.create_table(user_schema)
                self.log_message("Tabela 'usuarios' criada.")
            else:
                self.log_message("Tabela 'usuarios' já existe. Carregado do disco.")
            
            self.display_current_data() # Mostra o estado atual dos dados

        except ValueError as e:
            self.log_message(f"Erro ao configurar tabela: {e}")
        except Exception as e:
            self.log_message(f"Erro inesperado ao configurar dados iniciais: {e}")

    def insert_example_data(self):
        """
        Insere um novo registro de exemplo na tabela 'usuarios'.
        Gera um ID único para novos usuários.
        """
        try:
            self.log_message("\n--- Tentando inserir novos dados ---")
            current_users = self.db_manager.select("usuarios")
            # Determina o próximo ID disponível
            next_id = 1
            if current_users:
                # Obtém o ID máximo dos usuários atuais para garantir a exclusividade
                max_id = max([u['id'] for u in current_users if 'id' in u])
                next_id = max_id + 1

            new_user = {
                "id": next_id,
                "nome": f"Usuário {next_id}",
                "email": f"usuario{next_id}@example.com",
                "idade": 20 + next_id # Apenas uma maneira simples de variar a idade
            }
            self.db_manager.insert("usuarios", new_user)
            self.log_message(f"Inserido: {new_user}")
            self.display_current_data() # Atualiza a exibição após a inserção
            self.log_message("--- Inserção concluída ---")
        except Exception as e:
            self.log_message(f"Erro ao inserir dados de exemplo: {e}")
            messagebox.showerror("Erro de Inserção", f"Falha ao inserir dados: {e}")

    def display_current_data(self):
        """
        Limpa a área de exibição e mostra todos os registros de todas as tabelas
        atualmente gerenciadas pelo DatabaseManager.
        """
        self.display_text.config(state=tk.NORMAL)
        self.display_text.delete(1.0, tk.END) # Limpa o texto existente
        self.display_text.config(state=tk.DISABLED)

        self.log_message("\n--- Conteúdo Atual do Banco de Dados ---")
        if not self.db_manager.tables:
            self.log_message("Nenhuma tabela no banco de dados.")
            return

        for table_name in self.db_manager.tables:
            self.log_message(f"\n>>>> Tabela: '{table_name}' <<<<")
            try:
                records = self.db_manager.select(table_name)
                if records:
                    for record in records:
                        self.log_message(f"  Registro: {record}")
                else:
                    self.log_message("  Nenhum registro nesta tabela.")
            except Exception as e:
                self.log_message(f"  Erro ao buscar dados da tabela '{table_name}': {e}")
        self.log_message("\n--- Fim do Conteúdo do Banco de Dados ---\n")


if __name__ == "__main__":
    # Garante que o diretório 'my_db' exista antes de iniciar o aplicativo,
    # pois o DatabaseManager espera por ele. Se não existir, o DatabaseManager o cria.
    
    root = tk.Tk()
    app = DatabaseGUI(root)
    root.mainloop()
