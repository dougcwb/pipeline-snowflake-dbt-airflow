![Snowflake Free Virtual Hands-on Lab](https://img.shields.io/badge/Snowflake-blue)
![DBT](https://img.shields.io/badge/DBT-orange)
![Airflow](https://img.shields.io/badge/Airflow-red)
![UV](https://img.shields.io/badge/UV-black)
![Docker](https://img.shields.io/badge/Docker-blue)

# Pipeline using Snowflake, dbt core and Airflow
![Airflow>DBT>Snowflake](/images/Airflow-DBT-Snowflake.png)

This project will cover these topics:
- Manage our packages with `UV`
- Snowflake 
  * RBAC (Role-Based Access Control)
  * Warehouse
  * Roles
  * Schemas
  * Views
  * Database
  * Users
  * Account connection
  * Worksheets
- DBT project setup
  * dbt_project.yml
  * profiles.yml (Connections)
  * packages.yml (dbt_utils)
  * Macros
  * Data Mart
  * Staging
  * Tests (Generic and Singular)
- Airflow
  * Install Astronomer
  * Deploy DBT project in Airflow
  * Build DAGS
  * Configure Connections


## Prerequisites
- A Snowflake account (you can use a [Free-Trial](https://signup.snowflake.com/))
- Docker Windows

## Step 1: Install and start UV
We will use `UV` to manage our environment. Use PIPX like this `pipx install uv`

Then initialize UV with `uv init`

## Step 2: Install DBT-Core
`uv add dbt-core dbt-snowflake`

## Step 3: Setup snowflake environment
```sql
-- Create accounts
use role accountadmin;

create warehouse COMPUTE_WH with warehouse_size='x-small';
create database if not exists dbt_db;
create role if not exists dbt_role;

-- Check permissions
show grants on warehouse COMPUTE_WH;

-- Grant permissions to user and dbt
grant role dbt_role to user DOUGCWB;
grant usage on warehouse COMPUTE_WH to role dbt_role;
grant all on database dbt_db to role dbt_role;
use role dbt_role;

create schema if not exists dbt_db.dbt_schema;
```
## Step 4: Initialize and configure dbt
### Initialize DBT
```bash
uv run dbt init
```
### Configure the snowflake environment:
1. Name the project: dbt_sf_pipeline
2. Choose the snowflake option
3. Account: In snowflake UI, go to `Admin > Accounts` and copy LOCATOR and ACCOUNT in this format: `<account_locator>-<account_name>`
4. Your user when login to snowflake: dougcwb
5. Password or whatever you choose to login
6. role: dbt_role
7. Warehouse: COMPUTE_WH
8. Database: dbt_db
9. Schema: dbt_schema
10. Threads: 10

## Step 5: configure dbt profile
Go to dbt folder and edit the `dbt_profile.yaml`
```yml
models:
  snowflake_workshop:
    staging:
      materialized: view
      snowflake_warehouse: dbt_wh
    marts:
      materialized: table
      snowflake_warehouse: dbt_wh
```
Now create `profile.yml` to keep manage profiles
```yml
dbt_sf_pipeline:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: otpdfdd-ycb02066

      # User/password auth
      user: DOUGCWB
      password: <password>

      role: dbt_role
      database: dbt_db
      warehouse: COMPUTE_WH
      schema: dbt_schema
      threads: 10
      client_session_keep_alive: False
```

## Step 6: Add dbt-labs
This dbt package contains macros that can be (re)used across dbt projects.

Go to dbt folder `cd dbt_sf_pipeline` and create a file `packages.yml`:
```yml
packages:
  - package: dbt-labs/dbt_utils
    version: ">=1.3.0"
```
Now run `uv run dbt deps` to download utils.

## Step 7: Create source and staging files
Create `models/staging/tpch_sources.yml`
```yml
version: 2

sources:
  - name: tpch
    database: snowflake_sample_data
    schema: tpch_sf1
    tables:
      - name: orders
        columns:
          - name: o_orderkey
            tests:
              - unique
              - not_null
      - name: lineitem
        columns:
          - name: l_orderkey
            tests:
              - relationships:
                  to: source('tpch', 'orders')
                  field: o_orderkey
```
​Create staging models `models/staging/stg_tpch_orders.sql`
```sql
select
    o_orderkey as order_key,
    o_custkey as customer_key,
    o_orderstatus as status_code,
    o_totalprice as total_price,
    o_orderdate as order_date
from
    {{ source('tpch', 'orders') }}
```
​Create `models/staging/stg_tpch_line_items.sql`
```sql
select
    {{
        dbt_utils.generate_surrogate_key([
            'l_orderkey',
            'l_linenumber'
        ])
    }} as order_item_key,
	l_orderkey as order_key,
	l_partkey as part_key,
	l_linenumber as line_number,
	l_quantity as quantity,
	l_extendedprice as extended_price,
	l_discount as discount_percentage,
	l_tax as tax_rate
from
    {{ source('tpch', 'lineitem') }}
```

## Step 8: Macros (Don’t repeat yourself or D.R.Y.)
Create `macros/pricing.sql`. This will calculate the value for the order discount.

```sql
{% macro discounted_amount(extended_price, discount_percentage, scale=2) %}
    (-1 * {{extended_price}} * {{discount_percentage}})::decimal(16, {{ scale }})
{% endmacro %}
```

## Step 9: Transform models (fact tables, data marts)
Create Intermediate table `models/marts/int_order_items.sql` 
```sql
select
    line_item.order_item_key,
    line_item.part_key,
    line_item.line_number,
    line_item.extended_price,
    orders.order_key,
    orders.customer_key,
    orders.order_date,
    {{ discounted_amount('line_item.extended_price', 'line_item.discount_percentage') }} as item_discount_amount
from
    {{ ref('stg_tpch_orders') }} as orders
join
    {{ ref('stg_tpch_line_items') }} as line_item
        on orders.order_key = line_item.order_key
order by
    orders.order_date
```
​Create `marts/int_order_items_summary.sql` to aggregate info
```sql
select 
    order_key,
    sum(extended_price) as gross_item_sales_amount,
    sum(item_discount_amount) as item_discount_amount
from
    {{ ref('int_order_items') }}
group by
    order_key
```
​create fact model `models/marts/fct_orders.sql`
```sql
select
    orders.*,
    order_item_summary.gross_item_sales_amount,
    order_item_summary.item_discount_amount
from
    {{ref('stg_tpch_orders')}} as orders
join
    {{ref('int_order_items_summary')}} as order_item_summary
        on orders.order_key = order_item_summary.order_key
order by order_date
```
Try to run with command `uv run dbt run`

## ​Step 10: Generic and Singular tests
Create `models/marts/generic_tests.yml`
```yml
models:
  - name: fct_orders
    columns:
      - name: order_key
        tests:
          - unique
          - not_null
          - relationships:
              to: ref('stg_tpch_orders')
              field: order_key
              severity: warn
      - name: status_code
        tests:
          - accepted_values:
              values: ['P', 'O', 'F']
```
Build Singular Tests `tests/fct_orders_discount.sql`
```sql
select
    *
from
    {{ref('fct_orders')}}
where
    item_discount_amount > 0
```
Create `tests/fct_orders_date_valid.sql`
```sql
select
    *
from
    {{ref('fct_orders')}}
where
    date(order_date) > CURRENT_DATE()
    or date(order_date) < date('1990-01-01')
```
Run with `uv run dbt test`
## Step 11: Deploy on Airflow
- Install astronomer-cosmos in Windows with `winget install -e --id Astronomer.Astro`
- Go to root folder (outside dbt project) and create a new folder called `dbt_dag`
- Enter the folder `cd dbt_dag`
- Initialize with `astro dev init`
- Update Dockerfil with this:
```bash
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-snowflake && deactivate
```
Update `requirements.txt`
```bash
astronomer-cosmos
apache-airflow-providers-snowflake
```
Check if Airflow is ok by running `astro dev start`. Note that Docker must be up.

Airflow will be available at `http://127.0.0.1:8080/home`. To login use:
- Username: admin
- Password: admin

In order to run DBT in airflow, move `dbt_sf_pipeline` folder to airflow folder `dbt_dag/dags/dbt/`.

Add snowflake_conn in UI
```json
{
  "account": "otpdfdd-ycb02066",
  "warehouse": "COMPUTE_WH",
  "database": "dbt_db",
  "role": "dbt_role",
  "insecure_mode": false
}
```
Create `dbt_dag.py`
```python
import os
from datetime import datetime

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping


profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_conn", 
        profile_args={"database": "dbt_db", "schema": "dbt_schema"},
    )
)

dbt_snowflake_dag = DbtDag(
    project_config=ProjectConfig("/usr/local/airflow/dags/dbt/dbt_sf_pipeline",),
    operator_args={"install_deps": True},
    profile_config=profile_config,
    execution_config=ExecutionConfig(dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",),
    schedule_interval="@daily",
    start_date=datetime(2023, 9, 10),
    catchup=False,
    dag_id="dbt_dag",
)
```
![Dag Diagram overview](/images/dag-diagram.png)

This is just the beginig. We could do a lot more in this project, such as:
- Create documentation from DBT
- Ingest some AWS S3 csv files
- Use medalion architecture
- etc

But let's try another time.