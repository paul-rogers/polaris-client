class Table:

    def __init__(self, client, id=None, details=None):
        self._client = client
        if id is None:
            self._details = details
        else:
            self._details = client.table_details(id)
        self._schema = None

    def client(self):
        return self._client

    def _display(self):
        return self._client.show()._display

    def name(self):
        return self._details['name']

    def id(self):
        return self._details['id']

    def details(self):
        self._details = self._client.table_details(self.id())
        return self._details

    def input_schema(self):
        return self.details().get('inputSchema')

    def schema(self):
        if self._schema is None:
            schemas = self._client.schemas()
            schema = schemas.get(self.name())
            if schema is None:
                raise Exception("Schema not found")
            self._schema = schema['columns']
        return self._schema

    def show_details(self):
        self._display().show_object(self.id())

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
        self._client.push_events(self.id(), rows)
