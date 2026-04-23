"""
Databricks Job wrapper — sets up sys.path and runs the refresh pipeline.
This file is uploaded to DBFS and executed by the scheduled Databricks Job.
"""
import sys
import os

SCRIPTS_DIR = "/dbfs/FileStore/ebp_dashboard/scripts"
sys.path.insert(0, SCRIPTS_DIR)

os.environ.setdefault("SNOWFLAKE_AUTH_METHOD", "password")
os.environ.setdefault("SNOWFLAKE_ACCOUNT", "nike-nike_aws_us_west_2")
os.environ.setdefault("SNOWFLAKE_WAREHOUSE", "EDA_MPO_ZOOMANALYTICS_PROD")
os.environ.setdefault("SNOWFLAKE_USER", "CSPRU1")
os.environ.setdefault("SNOWFLAKE_ROLE", "DF_CSPRU1")
os.environ.setdefault("SNOWFLAKE_DATABASE", "DA_DSM_SCANALYTICS_PROD")
os.environ.setdefault("SNOWFLAKE_SCHEMA", "INTEGRATED")

OP_SUBMIT_PATH = "/dbfs/FileStore/ebp_dashboard/OP_Submit.xlsx"

sys.argv = [
    "refresh_from_snowflake.py",
    "--op-submit-xlsx", OP_SUBMIT_PATH,
    "--dbfs-path", "dbfs:/FileStore/ebp_dashboard/demand_plan_blob.json",
]

from refresh_from_snowflake import main
main()
