from cli import DatabaseCLI
from interface import SGBD_GUI

def main():
    # Adiciona uma janela de seleção de modo
    try:
        import tkinter as tk
        from tkinter import simpledialog

        root = tk.Tk()
        root.withdraw() # Esconde a janela principal
        modo = simpledialog.askstring("Modo de Operação", "Deseja iniciar em modo 'cli' ou 'gui'?", parent=root)
        root.destroy()

        if modo is None: # Se o usuário fechar a caixa de diálogo
            print("Nenhum modo selecionado. Encerrando.")
            return
        
        modo = modo.strip().lower()

    except ImportError:
        # Fallback para input de console se o tkinter não estiver disponível
        modo = input("Deseja iniciar em modo 'cli' ou 'gui'? ").strip().lower()

    if modo == "cli":
        cli = DatabaseCLI(db_path="sgbd_data")
        cli.run()
    elif modo == "gui":
        # Modificação: Passa o caminho do banco de dados para a GUI
        gui = SGBD_GUI(db_path="sgbd_data")
        gui.run()
    else:
        print("Modo inválido. Digite 'cli' ou 'gui'.")

if __name__ == "__main__":
    main()