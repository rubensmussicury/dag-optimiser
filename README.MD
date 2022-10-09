# DAG Optimisation

This code uses [sql-metadata](https://pypi.org/project/sql-metadata) as a SQL parse to identify if a query of a task does not reference any other query from another task, and vice-versa, suggesting the best flow.
