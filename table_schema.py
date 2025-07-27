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
