--!jinja

/*-----------------------------------------------------------------------------
Hands-On Lab: Deploying pipelines with Snowflake and dbt labs
Script:       deploy_environment.sql
Author:       Dmytro Yaroshenko
Last Updated: 4/10/2025
-----------------------------------------------------------------------------*/


CREATE OR ALTER DATABASE dbt_hol_2025_{{env}};
CREATE OR ALTER WAREHOUSE vwh_dbt_hol_{{env}} warehouse_size = 'XSMALL' initially_suspended = true auto_resume=true auto_suspend=60;
CREATE OR ALTER WAREHOUSE vwh_dbt_hol warehouse_size = 'XSMALL' initially_suspended = true  auto_resume=true auto_suspend=60;
CREATE OR ALTER ROLE dbt_hol_role_{{env}};

GRANT ALL ON DATABASE  dbt_hol_2025_{{env}} TO ROLE dbt_hol_role_{{env}};
GRANT ALL ON SCHEMA    PUBLIC               TO ROLE dbt_hol_role_{{env}};
GRANT ALL ON WAREHOUSE vwh_dbt_hol_{{env}}  TO ROLE dbt_hol_role_{{env}};
GRANT ALL ON WAREHOUSE VWH_DBT_HOL          TO ROLE dbt_hol_role_{{env}};
GRANT ALL ON FUTURE TABLES IN DATABASE dbt_hol_2025_{{env}} TO ROLE dbt_hol_role_{{env}};
GRANT IMPORTED PRIVILEGES ON DATABASE STOCK_TRACKING_US_STOCK_PRICES_BY_DAY TO ROLE dbt_hol_role_{{env}};
GRANT IMPORTED PRIVILEGES ON DATABASE FOREX_TRACKING_CURRENCY_EXCHANGE_RATES_BY_DAY TO ROLE dbt_hol_role_{{env}};

GRANT ROLE dbt_hol_role_{{env}} TO USER dbt_hol_user;
GRANT ROLE dbt_hol_role_{{env}} TO ROLE ACCOUNTADMIN;

