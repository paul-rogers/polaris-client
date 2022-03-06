class Table:

    def __init__(self, client, info):
        self._client = client
        self._name = info['name']
        self._id = info['id']
        self._schema = None

    def client(self):
        return self._client

    def _display(self):
        return self._client.show()._display

    def name(self):
        return self._name

    def id(self):
        return self._id

    def desription(self):
        return self._client.resolve_table_name(self._name)['description']

    def summary(self):
        return self._client.table_summary(self._id)

    def details(self):
        return self._client.table_details(self._id)

    def input_schema(self):
        return self.details().get('inputSchema')

    def schema(self):
        if self._schema is None:
            schemas = self._client.schemas()
            schema = schemas.get(self._name)
            if schema is None:
                raise Exception("Schema not found")
            self._schema = schema['columns']
        return self._schema

    def show_summary(self):
        self._display().show_object(self.summary())

    def show_details(self):
        self._display().show_object(self.details())

    def show_input_schema(self):
        schema = self.input_schema()
        if schema == None or len(schema) == 0:
            return
        self._display().show_object_list(schema, {'name': 'Name', 'type': 'Type'})

    def show_schema(self):
        schema = self.schema()
        if schema is not None:
            self._display().show_object_list(schema, {'name': 'Name', 'type': 'Type'})

    def insert(self, rows):
        self._client.push_events(self._id, rows)
