CREATE OR REPLACE PROCEDURE TELCO.DATAMART.SP_ENRICH_REVIEWS("MODE" VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
IMPORTS = ('@TELCO.APPS.ST_CODE/enrich_reviews.py')
EXECUTE AS OWNER
AS '
import enrich_reviews
def run(mode: str): return enrich_reviews.main(mode)
';