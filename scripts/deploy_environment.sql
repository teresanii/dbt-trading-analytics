--!jinja

/*-----------------------------------------------------------------------------
Hands-On Lab: Deploying pipelines with Snowflake and dbt labs
Script:       deploy_environment.sql
Author:       Dmytro Yaroshenko
Last Updated: 4/10/2025
-----------------------------------------------------------------------------*/


CREATE OR ALTER DATABASE dbt_hol_2025_dev;
CREATE OR ALTER WAREHOUSE vwh_dbt_hol_dev warehouse_size = 'XSMALL' initially_suspended = true auto_resume=true auto_suspend=60;
CREATE OR ALTER WAREHOUSE vwh_dbt_hol warehouse_size = 'XSMALL' initially_suspended = true  auto_resume=true auto_suspend=60;
CREATE OR ALTER ROLE dbt_hol_role_dev;

GRANT ALL ON DATABASE  dbt_hol_2025_dev TO ROLE dbt_hol_role_dev;
GRANT ALL ON SCHEMA    PUBLIC               TO ROLE dbt_hol_role_dev;
GRANT ALL ON WAREHOUSE vwh_dbt_hol_dev  TO ROLE dbt_hol_role_dev;
GRANT ALL ON WAREHOUSE VWH_DBT_HOL          TO ROLE dbt_hol_role_dev;
GRANT ALL ON FUTURE TABLES IN DATABASE dbt_hol_2025_dev TO ROLE dbt_hol_role_dev;
GRANT IMPORTED PRIVILEGES ON DATABASE STOCK_TRACKING_US_STOCK_PRICES_BY_DAY TO ROLE dbt_hol_role_dev;
GRANT IMPORTED PRIVILEGES ON DATABASE FOREX_TRACKING_CURRENCY_EXCHANGE_RATES_BY_DAY TO ROLE dbt_hol_role_dev;

GRANT ROLE dbt_hol_role_dev TO USER TNASCIMENTO;
GRANT ROLE dbt_hol_role_dev TO ROLE ACCOUNTADMIN;

