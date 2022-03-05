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

from .display import Display

def toMb(bytes):
    return round(bytes / 1000) / 1000

class Show:

    def __init__(self, client):
        self._client = client
        self._display = Display()
    
    def client(self):
        return self._client

    def as_text(self):
        self._display.text()

    def as_html(self):
        self._display.html()

    def object(self, obj):
        self._display.show_object(obj)

    def tables(self):
        tables = [[t['name']] for t in self._client.tables()]
        self._display.show_table(tables, ['Table'])

    def table_details(self):
        self._display.show_object_list(self._client.tables())

    def projects(self):
        projs = self._client.projects()
        if projs is None or len(projs) == 0:
            self._display.message("No projects available.")
            return
        proj_list = []
        for p in projs:
            proj = [
                p['metadata']['name'],
                p['metadata']['uid'],
                p['spec']['plan'],
                toMb(p['status']['currentBytes']),
                p['status']['state']
            ]
            proj_list.append(proj)
        heads = ['Name', 'ID', 'Plan', 'Size (MB)', 'State']
        self._display.show_table(proj_list, heads)

    def project(self, name='default'):
        proj = self._client.project(name)
        if proj is None:
            self._display.alert("Project {} is undefined".format(name))
            return
        details = proj['metadata'].copy()
        details.update(proj['spec'])
        details.update(proj['status'])
        details['maxMb'] = toMb(details['maxBytes'])
        details['currentMb'] = toMb(details['currentBytes'])
        labels = {
            'name': 'Name',
            'uid': 'ID',
            'plan': 'Plan',
            'maxMb': 'Size Limit (MB)',
            'currentMb': 'Current Size (MB)',
            'desiredState': 'Desired State',
            'state': 'Actual State'
        }
        self._display.show_object(details, labels)

    def sql(self, stmt):
        results = self._client.sql(stmt)
        if results is None or len(results) == 0:
            self._display.message("Query returned no results.")
            return
        self._display.show_object_list(results)

