from cli import DatabaseCLI

if __name__ == "__main__":
    cli = DatabaseCLI(db_path="sgbd_data")
    cli.run()
