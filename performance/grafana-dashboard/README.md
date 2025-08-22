# Configuring Dashboards

* Log in to [Grafana](http://127.0.0.1:3000).

## Add New PostgreSQL Connection

* In the left sidebar, open the Grafana **Menu** and click **Connections**.
* Click **Add new connection**.
* Search for and select **PostgreSQL**.
* Fill in the connection details:
  * Host, Port, Database, User, Password.
* Click **Save & Test** to verify the connection.

> **Enable Query Performance Tracking**
> 
> To get detailed SQL query performance metrics, ensure the `pg_stat_statements` extension is enabled in your PostgreSQL database:
> 
> ```sql
> CREATE EXTENSION pg_stat_statements;
> SELECT pg_stat_statements_reset();
> ```

## PostgreSQL Dashboard

* In the left sidebar, open the Grafana **Menu** and click **Dashboards**.
* Click **New** -> **Import**.
* Enter `9628` in the Dashboard ID and click **Load**.
* Select the `DS_PROMETHEUS` data source as `Prometheus`.
* Click **Import** to finish.

> Dashboard 9628 provides general PostgreSQL monitoring (connections, transactions, cache ratios). 
> For detailed query performance analysis, consider creating custom panels using `pg_stat_statements` data.

## Go Metrics Dashboard

* In the left sidebar, open the Grafana **Menu** and click the **Dashboards** icon.
* Upload your dashboard JSON file (`./performance/grafana-dashboard/go-metrics.json`)
  or paste its contents.
* Select the appropriate data source (e.g., `Prometheus`).
* Click **Import** to finish.

## Orbital Status Dashboard

* In the left sidebar, open the Grafana **Menu** and click **Dashboards**.
* Upload your dashboard JSON file (`./performance/grafana-dashboard/orbital-status.json`)
  or paste its contents.

> [!NOTE]
> Please make sure to replace the datasource `uid` value.
>
> ```json
>      "datasource": {
>       "type": "grafana-postgresql-datasource",
>      "uid": "changeme"
>     },
>```
>
> This `uid` should match the one you set up in the PostgreSQL connection.

* Click **Import** to finish.
