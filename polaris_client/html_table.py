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

from IPython.display import display, HTML
from .base_table import BaseTable

STYLES = '''
<style>
  .druid table {
    border: 1px solid black;
    border-collapse: collapse;
  }

  .druid th, .druid td {
    padding: 4px 1em ;
    text-align: left;
  }

  td.druid-right, th.druid-right {
    text-align: right;
  }

  td.druid-center, th.druid-center {
    text-align: center;
  }

  .druid .druid-left {
    text-align: left;
  }

  .druid-alert {
    color: red;
  }
</style>
'''

def html(s):
    s =  '<div class="druid">' + s + '</div>'
    display(HTML(s))

def styles():
    display(HTML(STYLES))

def alert(s):
    html('<span class="druid-alert">' + s + '</span>')

def message(s):
    html(s)

alignments = ['druid-left', 'druid-center', 'druid-right']

def start_tag(tag, align):
    s = '<' + tag
    if align is not None:
        s += ' class="{}"'.format(alignments[align])
    return s + '>'

class HtmlTable(BaseTable):

    def __init__(self):
        self._headers = None
        self._align = None
        self._col_fmt = None

    def widths(self, widths):
        self._widths = widths
    
    def format(self, rows):
        _, width = self.row_width(rows)
        headers = self.pad_headers(width)
        rows = self.pad_rows(rows, width)
        s = '<table>\n'
        s += self.gen_header(headers)
        s += self.gen_rows(rows)
        return s + '\n</table>'

    def show(self, rows):
        html(self.format(rows))

    def gen_header(self, headers):
        if headers is None or len(headers) == 0:
            return ''
        s = '<tr>'
        for i in range(len(headers)):
            s += start_tag('th', self.col_align(i)) + headers[i] + '</th>'
        return s + '</tr>\n'

    def gen_rows(self, rows):
        html_rows = []
        for row in rows:
            r = "<tr>"
            for i in range(len(row)):
                cell = row[i]
                r += start_tag('td', self.col_align(i))
                value = '' if cell is None else str(cell)
                r += value + '</td>'
            html_rows.append(r + "</tr>")
        return "\n".join(html_rows)

    def col_align(self, col):
        if self._align is None:
            return None
        if col >= len(self._align):
            return None
        return self._align[col]