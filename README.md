# polaris-client

Python client for the Imply Polaris hosted Druid offering.

This library provides a Python client to work with the REST API of Imply's Polaris
product, which is a hosted version of Apache Druid. See the Polaris
[developer guide](https://docs.imply.io/polaris/api-overview/) for an overiew
of the REST API.

The Poliaris API is at an early stage and is incomplete in some areas. This
library works around those flaws where possible. Expect changes to the
library as Polaris evolves.

## Getting Started

First connect to your account:

```python
import polaris_client

org = 'your-org'
client_id = 'your-client-id'
secret = 'your-secret'
client = polaris_client.connect(org, client_id, client)
```

See [Authenticate API requests](https://docs.imply.io/polaris/oauth/) for
details:

* `org`: the name displayed in the top-left of the home page.
* `client_id`: the client ID you create as described in the doc referenced abvove.
* `secret`: The scret associated with the client ID.

The client automatically renews the security ticket as needed.

If you are on a non-standard domain, add the domain
as a named argument:

```python
client = polaris_client.connect(org, client_id, client, domain='test')
```

Once connected you can use the Polaris APIs. There are two forms. The
client itself is a light wrapper over the REST API calls and returns
Python objects for use in your application.

```python
client.tables()

>>> [{'name': 'my-table',
  'id': '9b4va3ea-dbdf-4d40-af5c-965450755556',
  'version': 0,
  'lastUpdateDateTime': '2022-02-11T19:07:30Z',
  'createdByUsername': 'bob@example.com',
  'timePartitioning': 'day'}]
```

If you want to work with your account manually, we suggest using Jupyter.
Then, use the the `show()` function to display the results as a table:

```python
show = client.show()
show.tables()
```

Displays:

```text
Table
my-table
```

Switch to HTML format if you use Jupyter:

```python
show.as_html()
show.tables()
```

Displays the same information, but formatted as an HTML table.

## Working with Tables

The client provides a convenience class to work with tables:

```python
table = client.table_for_name('my-table')
table.name()

>>> my-table

table.id()

>>> 9b4va3ea-dbdf-4d40-af5c-965450755556

table.show_schema()

>>> Name   | Type
>>> __time | LONG
>>> col1   | STRING
>>> col2   | LONG

table.show_input_schema()

>>> Name   | Type
>>> col1   | STRING
>>> col2   | LONG

table.insert({'__time': '2022-03-03T22:37:13Z', 'col1': 'value1', 'col2': 10})
```

## Queries

To run a query:

``` python
show.sql('SELECT * FROM "my-table"')

>>> __time               | col1   | col2
>>> 2022-03-03T22:37:13Z | value1 | 10
```

`client.sql(stmt)` runs the query and returns the results as a list of
objects for programmatic use.