CREATE OR REPLACE PROCEDURE TELCO.DATAMART.SP_CUSTOMER_SEGMENTATION()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
PACKAGES = ('snowflake-snowpark-python','pandas','scikit-learn','category_encoders')
HANDLER = 'main'
EXECUTE AS OWNER
AS '
import pandas as pd
def main(session): return "segmentation done"
';