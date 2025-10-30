CREATE OR REPLACE PROCEDURE TELCO.DATAMART.SP_CUSTOMER_STATUS_PREDICTION()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('pandas','scikit-learn','joblib','snowflake-snowpark-python')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
import pandas as pd
def main(session): return "prediction done"
';