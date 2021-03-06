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

def is_blank(s):
    """
    Returns True if the given string is None or blank (after stripping spaces),
    False otherwise.
    """
    return s is None or len(s.strip()) == 0

def dict_get(dict, key, default=None):
    """
    Returns the value of key in the given dict, or the default value if
    the key is not found. 
    """
    if dict is None:
        return default
    return dict.get(key, default)

def padded(array, width, fill):
    if array is not None and len(array) >= width:
        return array
    if array is None:
        result = []
    else:
        result = array.copy()
    return pad(result, width, fill)
    
def pad(array, width, fill):
    for _ in range(len(array), width):
        array.append(fill)
    return array

def druid_timestamp(dt):
    dt.isoformat() + 'Z'