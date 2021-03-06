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

from .util import padded, pad

ALIGN_LEFT = 0
ALIGN_CENTER = 1
ALIGN_RIGHT = 2

class BaseTable:

    def __init__(self):
        self._headers = None
        self._align = None
        self._col_fmt = None
        self.sample_size = 10

    def headers(self, headers):
        self._headers = headers

    def alignments(self, align):
        self._align = align

    def col_format(self, col_fmt):
        self._col_fmt = col_fmt

    def row_width(self, rows):
        max_width = 0
        min_width = None
        if self._headers is not None:
            max_width = len(self._headers)
            min_width = max_width
        for row in rows:
            max_width = max(max_width, len(row))
            min_width = max_width if min_width is None else min(min_width, max_width)
        min_width = max_width if min_width is None else min_width
        return (min_width, max_width)

    def find_alignments(self, rows, width):
        align = padded(self._align, width, None)
        unknown_count = 0
        for v in align:
            if v is None:
                unknown_count += 1
        if unknown_count == 0:
            return align
        for row in rows:
            for i in range(len(row)):
                if align[i] is not None:
                    continue
                v = row[i]
                if v is None:
                    continue
                if type(v) is str:
                    align[i] = ALIGN_LEFT
                else:
                    align[i] = ALIGN_RIGHT
                unknown_count -= 1
                if unknown_count == 0:
                    return align
        for i in range(width):
            if align[i] is None:
                align[i] = ALIGN_LEFT
        return align

    def pad_rows(self, rows, width):
        new_rows = []
        for row in rows:
            new_rows.append(padded(row, width, None))
        return new_rows

    def pad_headers(self, width):
        if self._headers is None:
            return None
        if len(self._headers) == 0:
            return None
        has_none = False
        for i in range(len(self._headers)):
            if self._headers[i] is None:
                has_none = True
                break
        if len(self._headers) >= width and not has_none:
            return self._headers
        headers = self._headers.copy()
        if has_none:
            for i in range(len(headers)):
                if headers[i] is None:
                    headers[i] = ''
        return pad(headers, width, '')
