# Setup

## Deployment

Run the following command to deploy the application:

```bash
make build && make deploy
```

## Postgres SQL

* Please run the following commands to set up the `pg_stat_statements` extension in your PostgreSQL database. This extension is useful for tracking execution statistics of SQL statements.

```sql
CREATE EXTENSION pg_stat_statements;
select pg_stat_statements_reset();
```

* Add postgres database as the datasource in grafana.
* Import the dashboard `dashboard.json` in grafana.
