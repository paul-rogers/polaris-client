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

class Client:

    def __init__(self, org, client_id, secret, domain=None):
        self.org = org
        self.client_id = client_id
        self.secret = secret
        self.domain = "" if is_blank(domain) else domain + "."
        self.trace = False
        self.token = None
        self.session = requests.Session()
        self._show = None
        self._project_id = None
        self.get_token()

    #-------- REST --------
    
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
        r = req(self.session, self.add_token(headers))
        if r.status_code == requests.codes.unauthorized:
            self.get_token()
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
        if self.trace:
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
        if self.trace:
            print("POST:", url)
            print("body:", body)
        r = self.submit(lambda session, h: session.post(url, data=body, headers=h), headers)
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
        if self.trace:
            print("POST:", url)
            print("body:", body)
        return self.submit(lambda session, h: session.post(url, json=body, headers=h), headers)

    def delete_json(self, req, args=None, headers=None):
        url = self.build_url(req, args)
        if self.trace:
            print("DELETE:", url)
        r = self.submit(lambda session, h: session.delete(url, headers=h), headers)
        return r.json()

    #-------- Misc --------

    def show(self):
        if self._show is None:
            self._show = Show(self)
        return self._show

    def get_token(self):
        params = {
            "client_id": self.client_id,
            "client_secret": self.secret,
            "grant_type": "client_credentials",
        }

        r = self.session.post(POST_TOKEN.format(self.domain, self.org), data=params)
        r.raise_for_status()
        self.token = r.json()

    def create_table(self, table):
        if type(table) is str:
            table = {'name': table}
        details = self.post_json(REQ_TABLES, table)
        return Table(self, details=details)

    def tables(self):
        return self.get_json(REQ_TABLES)['values']

    def table_id(self, table_name):
        tables = self.tables()
        target = table_name.casefold()
        for t in tables:
            if t['name'].casefold() == target:
                return t['id']
        return None

    def table_details(self, table_id):
        return self.get_json(REQ_TABLE, [table_id])

    def table_for_name(self, name):
        id = self.table_id(name)
        if id is None:
            raise Exception("Table '{}' is not defined".format(name))
        return Table(self, id)

    def table_for_id(self, id):
        return Table(self, id)

    def schemas(self):
        return self.get_json(REQ_SCHEMAS)

    def push_events(self, table_id, events):
        if events is None:
            return
        if type(events) is not list:
            events = [events]
        # PITA: format must be line-delimited JSON
        lines = [json.dumps(event) for event in events]
        return self.post(REQ_EVENTS, '\n'.join(lines), args=[table_id])

    def projects(self):
        return self.get_json(REQ_PROJECTS)

    def set_project(self, proj_name):
        proj = self.project(proj_name)
        if proj is None:
            raise Exception('Project "{}" is undefined'.format(proj_name))
        self._project_id = proj['metadata']['uid']

    def project(self, proj_name):
        projects = self.projects()
        target = proj_name.casefold()
        for p in projects:
            if p['metadata']['name'].casefold() == target:
                return p
        return None

    def default_project(self):
        projects = self.projects()
        for p in projects:
            if p['metadata']['name'] == consts.DEFAULT_PROJECT:
                return p
        return None

    def sql(self, stmt):
        if self._project_id is None:
            self.set_project(consts.DEFAULT_PROJECT)
        return self.post_json(REQ_QUERY, {'query': stmt}, args=[self._project_id])