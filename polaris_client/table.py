# Copyright 2022 Paul Rogers
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import requests
from xmlrpc.client import Boolean

SUMMARY_LABELS = {
    'name': "Name",
    'id': "ID",
    'version': 'Version',
    'lastUpdateDateTime': 'Last Update',
    'lastModifiedByUsername': 'Updated By',
    'createdByUsername': 'Created By',
    'timePartitioning': 'Time Partitioning',
    'pushEndpointUrl': 'Push Endpoint'
}

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
        self._display().show_object(self.summary(), SUMMARY_LABELS)

    def show_details(self):
        labels = SUMMARY_LABELS.copy()
        labels.update({
            'status': 'Status',
            'totalDataSize': 'Data Size (bytes)',
            'totalRows': 'Row Count',
        })
        self._display().show_object(self.details(), labels)

    def show_input_schema(self):
        """
        Displays the table input schema.

        Note that the input schema does not include the mandatory
        `__time` column in the current Polaris version.
        """
        schema = self.input_schema()
        if schema == None or len(schema) == 0:
            return
        self._display().show_object_list(schema, {'name': 'Name', 'type': 'Type'})

    def show_schema(self):
        """
        Displays the table schema.
        """
        schema = self.schema()
        if schema is not None:
            self._display().show_object_list(schema, {'name': 'Name', 'type': 'Type'})

    def insert(self, rows):
        """
        Insert one or more rows to the table using the Push API.

        The rows must match the table's "input schema." All dates must be within
        the last week, else they will be silently ignored by Polaris.
        """
        self._client.push_events(self._id, rows)

    def drop(self):
        """
        Drop this table and all its data.

        Note that it may take Polaris a while to actually delete the table. If you want to
        immediately create a new one, poll the exists() method until the method returns False.

        After this call, this object remains valid, but the only legal operation is a call
        to exists(). 
        """
        self._client.drop_table(self._id)

    def exists(self) -> Boolean:
        """
        Check if the table exists.

        Use this after dropping a table to detect when Polaris has completed the
        deletion process, if you then want to create a new table with the same
        name.
        """
        try:
            self.details()
            return True
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == requests.codes.not_found:
                return False 
            raise e

    def enable_push(self):
        return self._client.enable_push_for_table(self._id)

    def disable_push(self):
        self._client.disable_push_for_table(self._id)

    def is_push_enabled(self):
        details = self.details()
        return details.get('pushEndpointUrl') is not None