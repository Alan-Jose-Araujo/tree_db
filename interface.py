import tkinter as tk
from tkinter import ttk, messagebox, simpledialog, scrolledtext
import shlex
from database_manager import DatabaseManager
from table_schema import Column, TableSchema

class SGBD_GUI:
    def __init__(self, db_path='my_db_gui'):
        self.db_manager = DatabaseManager(db_path)
        self.root = tk.Tk()
        self.root.title("Mini SGBD - Interface Gráfica")
        self.root.geometry("1000x700")

        # --- Layout Principal ---
        self.paned_window = ttk.PanedWindow(self.root, orient=tk.HORIZONTAL)
        self.paned_window.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # --- Frame da Esquerda (Sidebar com Tabelas e Esquema) ---
        self.left_frame = ttk.Frame(self.paned_window, width=300)
        self.paned_window.add(self.left_frame, weight=1)
        self._create_sidebar()

        # --- Frame da Direita (Comandos e Resultados) ---
        self.right_frame = ttk.Frame(self.paned_window)
        self.paned_window.add(self.right_frame, weight=3)
        self._create_main_panel()
        
        self.refresh_table_list()
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)

    def _create_sidebar(self):
        sidebar_frame = ttk.LabelFrame(self.left_frame, text="Banco de Dados")
        sidebar_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        # Botão para criar tabela
        self.new_table_button = ttk.Button(sidebar_frame, text="Nova Tabela", command=self.create_table_wizard)
        self.new_table_button.pack(fill=tk.X, padx=5, pady=5)
        
        # Treeview para listar tabelas
        self.table_list_tree = ttk.Treeview(sidebar_frame, show="tree")
        self.table_list_tree.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        self.table_list_tree.bind("<<TreeviewSelect>>", self.show_schema_for_selected_table)

        # Frame para exibir o esquema da tabela
        schema_frame = ttk.LabelFrame(self.left_frame, text="Esquema da Tabela")
        schema_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=(0,5))
        self.schema_display = scrolledtext.ScrolledText(schema_frame, height=10, state='disabled', wrap=tk.WORD)
        self.schema_display.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

    def _create_main_panel(self):
        main_panel_frame = ttk.Frame(self.right_frame)
        main_panel_frame.pack(fill=tk.BOTH, expand=True)

        # Input de Comando
        command_frame = ttk.LabelFrame(main_panel_frame, text="Comando SQL (Use: SELECT, INSERT, DELETE)")
        command_frame.pack(fill=tk.X, padx=5, pady=5)
        
        self.sql_input = scrolledtext.ScrolledText(command_frame, height=8, wrap=tk.WORD)
        self.sql_input.pack(fill=tk.X, expand=True, padx=5, pady=5)
        self.sql_input.insert(tk.END, "SELECT FROM nome_tabela WHERE coluna='valor'\n")
        self.sql_input.insert(tk.END, "INSERT INTO nome_tabela coluna1='valor1' coluna2=123\n")
        self.sql_input.insert(tk.END, "DELETE FROM nome_tabela WHERE pk_coluna='pk_valor'")

        self.execute_button = ttk.Button(command_frame, text="Executar", command=self.execute_sql)
        self.execute_button.pack(pady=5, padx=5)

        # Output de Resultados
        result_frame = ttk.LabelFrame(main_panel_frame, text="Resultado")
        result_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Usar Treeview para resultados de SELECT
        self.result_tree = ttk.Treeview(result_frame, show='headings')
        self.result_tree.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

    def refresh_table_list(self):
        # Limpa a lista de tabelas
        for item in self.table_list_tree.get_children():
            self.table_list_tree.delete(item)
        # Preenche com as tabelas atuais
        for table_name in sorted(self.db_manager.tables.keys()):
            self.table_list_tree.insert("", "end", text=table_name, iid=table_name)
    
    def show_schema_for_selected_table(self, event=None):
        selected_item = self.table_list_tree.selection()
        if not selected_item:
            return
        
        table_name = selected_item[0]
        schema = self.db_manager.tables.get(table_name)
        
        if not schema:
            return

        self.schema_display.config(state='normal')
        self.schema_display.delete('1.0', tk.END)
        
        text = f"Tabela: {schema.name}\n"
        text += f"Chave Primária: {schema.pk_name}\n\n"
        text += "Colunas:\n"
        for c in schema.columns.values():
            null_status = 'NOT NULL' if not c.nullable else 'NULL'
            text += f"  - {c.name} ({c.data_type}) {null_status}\n"
        
        if schema.foreign_keys:
            text += "\nChaves Estrangeiras:\n"
            for fk in schema.foreign_keys:
                text += f"  - {fk['fk_col']} -> {fk['ref_table']}({fk['ref_col']})\n"

        self.schema_display.insert('1.0', text)
        self.schema_display.config(state='disabled')
    
    def _parse_key_value(self, parts):
        record = {}
        for part in parts:
            if '=' not in part: raise ValueError(f"Formato de atribuição inválido: '{part}'. Use 'chave=valor'.")
            key, value_str = part.split('=', 1)
            
            value_str_lower = value_str.lower()
            if value_str_lower == 'true': value = True
            elif value_str_lower == 'false': value = False
            elif value_str_lower == 'null': value = None
            elif value_str.isdigit(): value = int(value_str)
            elif value_str.replace('.', '', 1).isdigit(): value = float(value_str)
            else: value = value_str.strip("'\"")
            record[key] = value
        return record

    def execute_sql(self):
        query = self.sql_input.get("1.0", tk.END).strip()
        if not query:
            return

        try:
            parts = shlex.split(query)
            command = parts[0].upper()

            # Limpar resultados anteriores
            self.result_tree.delete(*self.result_tree.get_children())
            if self.result_tree["columns"]:
                self.result_tree["columns"] = ()

            if command == "SELECT" and len(parts) > 2 and parts[1].upper() == "FROM":
                table_name = parts[2]
                where_clause = None
                if "WHERE" in [p.upper() for p in parts]:
                    where_idx = [p.upper() for p in parts].index("WHERE")
                    where_clause = self._parse_key_value(parts[where_idx + 1:])
                
                results = self.db_manager.select(table_name, where_clause)
                self.display_select_results(results)

            elif command == "INSERT" and len(parts) > 2 and parts[1].upper() == "INTO":
                table_name = parts[2]
                record = self._parse_key_value(parts[3:])
                self.db_manager.insert(table_name, record)
                messagebox.showinfo("Sucesso", "Registro inserido com sucesso.")

            elif command == "DELETE" and len(parts) > 4 and parts[1].upper() == "FROM":
                table_name = parts[2]
                pk_name = self.db_manager.tables[table_name].get_pk_name()
                if parts[3].upper() != "WHERE": raise ValueError("DELETE requer cláusula WHERE.")
                
                where_clause = self._parse_key_value(parts[4:])
                if pk_name not in where_clause:
                    raise ValueError(f"DELETE só pode ser feito pela chave primária ('{pk_name}').")
                
                self.db_manager.delete(table_name, where_clause[pk_name])
                messagebox.showinfo("Sucesso", "Registro deletado com sucesso.")

            else:
                raise ValueError(f"Comando '{command}' desconhecido ou mal formatado.")

        except Exception as e:
            messagebox.showerror("Erro de Execução", str(e))

    def display_select_results(self, results):
        if not results:
            messagebox.showinfo("Resultado", "Nenhum registro encontrado.")
            return

        headers = list(results[0].keys())
        self.result_tree["columns"] = headers
        
        for header in headers:
            self.result_tree.heading(header, text=header)
            self.result_tree.column(header, anchor=tk.W, width=120)

        for row in results:
            self.result_tree.insert("", "end", values=[row.get(h, "NULL") for h in headers])
            
    def create_table_wizard(self):
        wizard = CreateTableWizard(self.root, self.db_manager.tables.keys())
        self.root.wait_window(wizard.top)

        if wizard.schema_data:
            try:
                schema = TableSchema(**wizard.schema_data)
                self.db_manager.create_table(schema)
                self.refresh_table_list()
                messagebox.showinfo("Sucesso", f"Tabela '{schema.name}' criada com sucesso.")
            except Exception as e:
                messagebox.showerror("Erro ao Criar Tabela", str(e))
    
    def on_closing(self):
        if messagebox.askokcancel("Sair", "Deseja salvar as alterações e sair?"):
            self.db_manager.save_to_disk()
            self.root.destroy()

    def run(self):
        self.root.mainloop()

class CreateTableWizard:
    VALID_TYPES = ['int', 'float', 'string', 'boolean', 'date']

    def __init__(self, parent, existing_tables):
        self.top = tk.Toplevel(parent)
        self.top.title("Assistente de Criação de Tabela")
        self.top.geometry("550x500")
        self.top.transient(parent)
        self.top.grab_set()

        self.existing_tables = existing_tables
        self.columns = []
        self.schema_data = None

        # Nome da tabela
        ttk.Label(self.top, text="Nome da Tabela:").grid(row=0, column=0, padx=10, pady=5, sticky="w")
        self.table_name_entry = ttk.Entry(self.top, width=40)
        self.table_name_entry.grid(row=0, column=1, columnspan=2, padx=10, pady=5, sticky="we")

        # Colunas
        col_frame = ttk.LabelFrame(self.top, text="Definição de Colunas")
        col_frame.grid(row=1, column=0, columnspan=3, padx=10, pady=10, sticky="ew")
        
        self.col_tree = ttk.Treeview(col_frame, columns=("name", "type", "nullable"), show="headings")
        self.col_tree.heading("name", text="Nome")
        self.col_tree.column("name", width=150)
        self.col_tree.heading("type", text="Tipo")
        self.col_tree.column("type", width=100)
        self.col_tree.heading("nullable", text="Permite Nulo?")
        self.col_tree.column("nullable", width=100, anchor="center")

        self.col_tree.grid(row=0, column=0, columnspan=3, padx=5, pady=5)
        
        # Entradas para nova coluna
        ttk.Label(col_frame, text="Nome:").grid(row=1, column=0, padx=5)
        self.col_name_entry = ttk.Entry(col_frame)
        self.col_name_entry.grid(row=1, column=1, padx=5, pady=5)

        ttk.Label(col_frame, text="Tipo:").grid(row=2, column=0, padx=5)
        self.col_type_combo = ttk.Combobox(col_frame, values=self.VALID_TYPES, state="readonly")
        self.col_type_combo.grid(row=2, column=1, padx=5)
        self.col_type_combo.set('string')
        
        self.col_nullable_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(col_frame, text="Permite Nulo", variable=self.col_nullable_var).grid(row=3, column=1, sticky="w", padx=5)
        
        ttk.Button(col_frame, text="Adicionar Coluna", command=self.add_column).grid(row=4, column=1, pady=10)

        # Chave Primária
        ttk.Label(self.top, text="Chave Primária:").grid(row=2, column=0, sticky="w", padx=10)
        self.pk_combo = ttk.Combobox(self.top, state="readonly")
        self.pk_combo.grid(row=2, column=1, columnspan=2, padx=10, pady=5, sticky="we")
        
        # Botões Finais
        ttk.Button(self.top, text="Criar Tabela", command=self.create_schema).grid(row=3, column=1, pady=20)
        ttk.Button(self.top, text="Cancelar", command=self.top.destroy).grid(row=3, column=2, pady=20)

    def add_column(self):
        name = self.col_name_entry.get().strip()
        dtype = self.col_type_combo.get()
        if not name or not dtype:
            messagebox.showwarning("Aviso", "Nome e tipo da coluna são obrigatórios.", parent=self.top)
            return
        
        for item in self.col_tree.get_children():
            if self.col_tree.item(item, 'values')[0] == name:
                messagebox.showerror("Erro", f"A coluna '{name}' já existe.", parent=self.top)
                return
        
        nullable_str = 'Sim' if self.col_nullable_var.get() else 'Não'

        self.col_tree.insert("", "end", values=(name, dtype, nullable_str))

        self.col_name_entry.delete(0, tk.END)
        self.update_pk_options()

    def update_pk_options(self):
        # Acessa o nome e a nulidade pelos índices corretos em 'values'
        pk_candidates = [
            self.col_tree.item(item, 'values')[0]
            for item in self.col_tree.get_children()
            if self.col_tree.item(item, 'values')[2] == 'Não'
        ]
        self.pk_combo['values'] = pk_candidates
        if pk_candidates:
            self.pk_combo.set(pk_candidates[0])

    def create_schema(self):
        table_name = self.table_name_entry.get().strip()
        if not table_name:
            messagebox.showerror("Erro", "O nome da tabela é obrigatório.", parent=self.top)
            return

        pk_name = self.pk_combo.get()
        if not pk_name:
            messagebox.showerror("Erro", "A chave primária é obrigatória.", parent=self.top)
            return

        cols = []
        for item in self.col_tree.get_children():
            values = self.col_tree.item(item, 'values')
            name = values[0]
            dtype = values[1]
            nullable = values[2] == 'Sim'
            cols.append(Column(name, dtype, nullable))

        if not cols:
            messagebox.showerror("Erro", "A tabela deve ter pelo menos uma coluna (a chave primária).", parent=self.top)
            return

        self.schema_data = {
            'name': table_name,
            'columns': cols,
            'pk_name': pk_name,
            'foreign_keys': []
        }
        self.top.destroy()