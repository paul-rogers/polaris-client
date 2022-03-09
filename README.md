# polaris-client

Python client for the Imply Polaris hosted Druid offering.

This library provides a Python client to work with the REST API of Imply's Polaris
product, which is a hosted version of Apache Druid.

See the Polaris
[Developer Guide](https://docs.imply.io/polaris/api-overview/) for an overiew
of the REST API. The class documentation assumes you are familiar with
Polaris concepts and the Polaris API. Each method refers to the Polaris
documentation for the details.

The documentation describes JSON objects, which mostly map directly into
Python `dict` objects in this client. In some cases (identified in method
comments), this client "unwraps" some Polaris boilerplate.

The Polaris API is at an early stage and is incomplete in some areas. This
library works around those flaws where possible. Expect changes to the
library as Polaris evolves.

See [druid-client](https://github.com/paul-rogers/druid-client) for a similar 
client for Apache (and Imply) Druid.

## Getting Started

You should already have a [Polaris account](https://imply.io/polaris-signup)
(you can create a free trial.) You must also create a
[custom API client](https://docs.imply.io/polaris/oauth/#create-a-custom-api-client)
to use below. Remember to give your client the service account roles needed
for those APIs you wish to use.

Then connect to your account:

```python
import polaris_client

org = 'your-org'
client_id = 'your-client-id'
secret = 'your-secret'
client = polaris_client.connect(org, client_id, secret)
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
client = polaris_client.connect(org, client_id, secret, domain='test')
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

If you want to explore the API interactively, we suggest using Jupyter.
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
```

Results:

```text
Name   | Type
__time | LONG
col1   | STRING
col2   | LONG
```

To see the "input schema" for pushing events:

```python
table.show_input_schema()
```

Results:

```text
Name   | Type
col1   | STRING
col2   | LONG
```

The actual pushed event must include the `__time` column as well. To add data:

```python
table.insert({'__time': '2022-03-03T22:37:13Z', 'col1': 'value1', 'col2': 10})
```

Note that the python `datetime.isoformat()` method will produce the correct format
for the `__time` field.

In Polaris, you create a table with a name, but then reference it in the
RST API via the ID. Use `client.table_id(name)` to get the ID for a table given its
name. Or, just use the `Table` class which handles the ID for you.

## Queries

To run a query:

``` python
show.sql('SELECT * FROM "my-table"')

>>> __time               | col1   | col2
>>> 2022-03-03T22:37:13Z | value1 | 10
```

`client.sql(stmt)` runs the query and returns the results as a list of
objects for programmatic use.

## Method Documentation

Each method references the API it uses, with a link to the Imply documentation.
In Jupyter, use `help()` to see the available methods given an instance of
that object:

```python
help(client)
help(show)
help(table)
```

To see the detailed documentation, with links:

```python
help(client.projects)
```

Displays:

```text
Help on method projects in module polaris_client.client:

projects() method of polaris_client.client.Client instance
    Returns the list of projects.
    
    Returns the project list under the `values` key in the REST response.
    
    Calls `/v1/projects`
    
    See https://docs.imply.io/polaris/api-query/#get-project-id
``` 

## Tracing

You can see the APIs for each call with:

```python
client.trace(True)
client.projects()

>>> GET: https://api.eng.imply.io/v1/projects
>>> [{'metadata': {'uid': '1b460394-3645-404d-a241-7ddbb7845ea6', ...
```

Tracing also shows the payload for `POST` calls and the raw details
of error messages.
