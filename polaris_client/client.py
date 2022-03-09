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

from urllib.parse import quote
import requests, json
from .util import is_blank, dict_get
from .show import Show
from .table import Table
from . import consts

POST_TOKEN = "https://id.{}imply.io/auth/realms/{}/protocol/openid-connect/token"
BASE_URL = "https://api.{}imply.io/v1"
REQ_TABLES = "/tables"
REQ_TABLE = REQ_TABLES + "/{}"
REQ_SCHEMAS = "/schemas"
REQ_PROJECTS = "/projects"
REQ_QUERY = REQ_PROJECTS + "/{}/query/sql"
REQ_EVENTS = "/events/{}"

# Internal Imply API to enable push streaming for a table
REQ_ENABLE_PUSH = REQ_TABLE + '/ingestion/streaming'

class NotFoundException(Exception):

    def __init__(self, msg):
        Exception.__init__(self, msg)

class Client:

    def __init__(self, org, client_id, secret, domain=None):
        self.org = org
        self.client_id = client_id
        self.secret = secret
        self.domain = "" if is_blank(domain) else domain + "."
        self._trace = False
        self.token = None
        self.session = requests.Session()
        self._show = None
        self._project_id = None
        self.renew_token()

    #-------- REST --------

    def trace(self, flag):
        self._trace = flag == True
    
    def _get_error_msg(self, json):
        # { "code": 400, "message": "Unable to process JSON" }
        msg = dict_get(json, 'message')
        if not is_blank(msg):
            return msg

        # { "error": { "code": "AlreadyExists",
        # "message": "A table with name [Example Table] already exists", ...
        error_obj = json.get('error')
        if error_obj is None:
            return None
        msg = error_obj.get('message')
        if not is_blank(msg):
            return msg
        msg = error_obj.get('code')
        if not is_blank(msg):
            return msg
        return None
    
    def check_error(self, response):
        """
        Raises a requests HttpError if the response code is not OK or Accepted.

        If the response inclded a JSON payload, then the message is extracted
        from that payload, else the message is from requests. The JSON
        payload, if any, is returned in the json field of the error.
        """
        code = response.status_code
        if code == requests.codes.ok or code == requests.codes.accepted:
            return
       
        if self._trace and response.text is not None:
            print("Error: ", response.text)
        error = None
        json = None
        try:
            json = response.json()
            error = self._get_error_msg(json)
        except Exception:
            pass
        if code == requests.codes.not_found and error is None:
            error = "Not found"
        if error is not None:
            response.reason = error
        try:
            response.raise_for_status()
        except Exception as e:
            e.json = json
            raise e

    def build_url(self, req, args=None) -> str:
        """
        Returns the full URL for a REST call given the relative request API and
        optional parameters to fill placeholders within the request URL.
        
        Parameters
        ----------
        req : str
            relative URL, with optional {} placeholders

        args : list
            optional list of values to match {} placeholders
            in the URL.
        """
        url = BASE_URL + req
        quoted = [self.domain]
        if args is not None:
            quoted += [quote(arg) for arg in args]
        url = url.format(*quoted)
        return url

    def add_token(self, headers):
        if headers is None:
            headers = {}
        headers["Authorization"] = "Bearer " + self.token['access_token']
        return headers

    def submit(self, req, headers=None):
        '''
        Internal method to submit a REST call with authentication.

        If the first all fails with a 401 (unauthorized) response,
        obtains a new temporary ticket and tries again.

        See https://docs.imply.io/polaris/oauth/
        '''
        r = req(self.session, self.add_token(headers))
        if r.status_code == requests.codes.unauthorized:
            self.renew_token()
            r = req(self.session, self.add_token(headers))
        return r
    
    def get(self, req, args=None, params=None, require_ok=True) -> requests.Request:
        '''
        Generic GET request to this service.

        Parameters
        ----------
        req: str
            The request URL without host, port or query string.
            Example: `/status`

        args: [str], default = None
            Optional parameters to fill in to the URL.
            Example: `/customer/{}`

        params: dict, default = None
            Optional map of query variables to send in
            the URL. Query parameters are the name/values pairs
            that appear after the `?` marker.

        require_ok: bool, default = True
            Whether to require an OK (200) response. If `True`, and
            the request returns a different response code, then raises
            a `RestError` exception.

        Returns
        -------
        The `requests` `Request` object.
        '''
        url = self.build_url(req, args)
        if self._trace:
            print("GET:", url)
        r = self.submit(lambda session, h: session.get(url, params=params, headers=h))
        if require_ok:
            self.check_error(r)
        return r

    def get_json(self, url_tail, args=None, params=None):
        '''
        Generic GET request which expects a JSON response.
        '''
        r = self.get(url_tail, args, params)
        return r.json()

    def post(self, req, body, args=None, headers=None, require_ok=True) -> requests.Request:
        """
        Issues a POST request for the given URL on this
        node, with the given payload and optional URL query 
        parameters.
        """
        url = self.build_url(req, args)
        if self._trace:
            print("POST:", url)
            print("body:", body)
        r = self.submit(lambda session, h: session.post(url, data=body, headers=h), headers)
        if self._trace:
            print("Response code:", r.status_code)
        if require_ok:
            self.check_error(r)
        return r

    def post_json(self, req, body, args=None, headers=None):
        """
        Issues a POST request for the given URL on this
        node, with the given payload and optional URL query 
        parameters. The payload is serialized to JSON.
        """
        r = self.post_only_json(req, body, args, headers)
        self.check_error(r)
        return r.json()

    def post_only_json(self, req, body, args=None, headers=None) -> requests.Request:
        """
        Issues a POST request for the given URL on this
        node, with the given payload and optional URL query 
        parameters. The payload is serialized to JSON.

        Does not parse error messages: that is up to the caller.
        """
        url = self.build_url(req, args)
        if self._trace:
            print("POST:", url)
            print("body:", body)
        return self.submit(lambda session, h: session.post(url, json=body, headers=h), headers)

    def delete_req(self, req, args=None, headers=None):
        url = self.build_url(req, args)
        if self._trace:
            print("DELETE:", url)
        return self.submit(lambda session, h: session.delete(url, headers=h), headers)

    def delete_json(self, req, args=None, headers=None):
        return self.delete_req(req, args, headers).json()

    #-------- Misc --------

    def show(self):
        """
        Returns an object which displays Polaris information as either a plain-text
        or HTML table.

        For use in ineractive sections. Use the HTML mode in Jupyter:

        show = client.show()
        show.as_html()
        show.tables()
        """
        if self._show is None:
            self._show = Show(self)
        return self._show

    def renew_token(self):
        """
        Renews the temporary OAuth ticket using the client ID
        and secret for this client.

        Normally done automatically internally, most clients never need
        to call this method.

        See https://docs.imply.io/polaris/oauth/
        """
        params = {
            "client_id": self.client_id,
            "client_secret": self.secret,
            "grant_type": "client_credentials",
        }

        r = self.session.post(POST_TOKEN.format(self.domain, self.org), data=params)
        r.raise_for_status()
        self.token = r.json()

    def create_table(self, table):
        """
        Create a table given a name or table object.

        Calls POST /v1/tables

        Parameters
        ----------
        table: string or dict
            If a string, the name of the (empty) table to create.
            If a dict, the values of a TableRequest object.

        See https://docs.imply.io/polaris/api-create-table/
        See https://docs.imply.io/polaris/TablesApi/#create-a-table
        """
        if type(table) is str:
            table = {'name': table}
        details = self.post_json(REQ_TABLES, table)
        return Table(self, details)

    def drop_table(self, table_id):
        """
        Drops a table given its table ID.

        Polaris returns OK even if the table does not exist. Check for
        existence using table_summary() before droping if your app needs
        to distinguish these two cases.

        Parameters
        ----------
        table_id: str
            The ID for the table.
        """
        return self.delete_req(REQ_TABLE, args=[table_id])

    def all_table_summaries(self):
        """
        Returns the summary informaton for all tables.

        Calls GET /v1/tables?detail=summary

        Returns
        -------
        A list of the table summaries. This is the list under the `value`
        key in the REST response.

        See https://docs.imply.io/polaris/TablesApi/#list-available-tables
        """
        return self.get_json(REQ_TABLES, params={'detail': 'summary'})['values']

    def all_table_details(self):
        """
        Returns the detail informaton for all tables.

        Calls GET /v1/tables?detail=detailed

        Returns
        -------
        A list of the table details. This is the list under the `value`
        key in the REST response.

        See https://docs.imply.io/polaris/TablesApi/#list-available-tables
        """
        return self.get_json(REQ_TABLES, params={'detail': 'detailed'})['values']

    def resolve_table_name(self, table_name):
        """
        Returns table summary information given a table name.

        Calls `GET /v1/tables?name={table_name}`

        Parameters
        ----------
        table_name: str
            The name of the table.
        
        Returns
        -------
        A dict with the table name, id, description and last update time,
        or None if the table is not defined.

        See https://docs.imply.io/polaris/api-table-id/
        """
        values = self.get_json(REQ_TABLES, params={'name': table_name})['values']
        if len(values) == 0:
            return None
        return values[0]

    def table_id(self, table_name):
        """
        Returns the ID for a table given the table name.

        Calls `GET /v1/tables?name={table_name}`

        Parameters
        ----------
        table_name: str
            The name of the table.
        
        Returns
        -------
        The Table ID, or None if the table is not defined.

        See https://docs.imply.io/polaris/api-table-id/
        """
        info = self.resolve_table_name(table_name)
        if info is None:
            return None
        return info['id']

    def table_summary(self, table_id):
        """
        Returns the summary metadata for a table.

        Calls `GET /tables/{tableId}?detail=summary`

        Parameters
        ----------
        table_id: str
            The ID for the table.
        
        Returns
        -------
        The summary metadata for the table. See link below for details.

        See https://docs.imply.io/polaris/TablesApi/#get-a-tables-metadata
        See table_id(table_name) to get the table ID.
        """
        return self.get_json(REQ_TABLE, args=[table_id], params={'detail': 'summary'})

    def table_details(self, table_id):
        """
        Returns the detail metadata for a table.

        Calls `GET /tables/{tableId}?detail=detailed`

        Parameters
        ----------
        table_id: str
            The ID for the table.
        
        Returns
        -------
        The detail metadata for the table. See link below for details.

        See https://docs.imply.io/polaris/TablesApi/#get-a-tables-metadata
        See table_id(table_name) to get the table ID.
        """
        return self.get_json(REQ_TABLE, args=[table_id], params={'detail': 'detailed'})

    def table_for_name(self, name):
        """
        Return a Table object for a table given its name.

        Raises an exception if the name is undefined.

        Parameters
        ----------
        table_name: str
            The name of the table.
        """
        info = self.resolve_table_name(name)
        if info is None:
            raise NotFoundException("Table '{}' is not defined".format(name))
        return Table(self, info)

    def table_for_id(self, id):
        """
        Return a Table object for a table given its ID.

        Raises an exception if the name is undefined.

        Parameters
        ----------
        table_id: str
            The ID for the table.
        """
        details = self.table_summary(id)
        if details is None:
            raise NotFoundException("Table ID '{}' is not defined".format(id))
        return Table(self, details)

    def schemas(self):
        """
        Returns the schemas for all tables.

        Calls `GET /schemas`

        Returns
        -------
        The set of schemas as a dict with the table name as a key.

        See https://docs.imply.io/polaris/SchemasApi/#get-table-schemas
        """
        return self.get_json(REQ_SCHEMAS)

    def push_events(self, table_id, events):
        """
        Push (insert) events into a table using its input schema.

        Parameters
        ----------
        table_id: str
            The table ID.
        events: dict or array
            The event(s) to push. Can be a single event as a dict, or an arrary
            of such objects. Each object must include the `__time` column, along
            with the other columns defined in the input schema.

        See https://docs.imply.io/polaris/api-stream/
        See https://docs.imply.io/polaris/EventsApi/
        """
        if events is None:
            return
        if type(events) is not list:
            events = [events]
        # PITA: format must be line-delimited JSON
        # (JSON Lines: https://jsonlines.org/)
        lines = [json.dumps(event) for event in events]
        return self.post(REQ_EVENTS, '\n'.join(lines), args=[table_id])

    def projects(self):
        """
        Returns the list of projects.

        Returns the project list under the `values` key in the REST response.

        Calls `/v1/projects`

        See https://docs.imply.io/polaris/api-query/#get-project-id
        """
        return self.get_json(REQ_PROJECTS)

    def set_project(self, proj_name):
        proj = self.project(proj_name)
        if proj is None:
            raise Exception('Project "{}" is undefined'.format(proj_name))
        self._project_id = proj['metadata']['uid']

    def project(self, proj_name):
        """
        Returns the project object for a project.

        Parameters
        ----------
        proj_name: str
            The name of the project

        Returns
        -------
        The single project object for the named project, or None if the
        project is not defined.

        See projects()
        """
        projects = self.projects()
        target = proj_name.casefold()
        for p in projects:
            if p['metadata']['name'].casefold() == target:
                return p
        return None

    def default_project(self):
        """
        Return the project object for the default project.

        See project(proj_name)
        """
        projects = self.projects()
        for p in projects:
            if p['metadata']['name'] == consts.DEFAULT_PROJECT:
                return p
        return None

    def infer_project(self):
        projects = self.projects()
        if len(projects) == 0:
            raise NotFoundException("No projects found")
        if len(projects) == 1:
            self._project_id = projects[0]['metadata']['uid']
            return
        for p in projects:
            if p['metadata']['name'] == consts.DEFAULT_PROJECT:
                self._project_id = p['metadata']['uid']
                return
        raise Exception("More than one project defined: please call set_project()")

    def sql(self, stmt):
        """
        Executes a SQL query and returns the results.

        Polaris executes queries within the context of a project. This method
        uses the project specified with set_project(), else the default project.

        Calls `/v1/projects/{project_id}/query/sql`

        Parameters
        ----------
        sql: str
            A Druid SQL statement.

        Returns
        -------
        The query result as an array of objects, where each object has key/value
        pairs for each column.

        Note that to figure out the schema for the results, you must scan the first
        object. If your query returns no rows, there is no way to learn the schema.
        That is, you cannot play the "LIMIT 0" trick to learn the result schema using
        this API.

        See https://docs.imply.io/polaris/api-query/
        See https://docs.imply.io/latest/druid/querying/sql/
        """
        if self._project_id is None:
            self.infer_project()
        return self.post_json(REQ_QUERY, {'query': stmt}, args=[self._project_id])

    def enable_push_for_table(self, table_id):
        return self.post(REQ_ENABLE_PUSH, '', args=[table_id])

    def disable_push_for_table(self, table_id):
        return self.delete_req(REQ_ENABLE_PUSH, args=[table_id])
