CREATE OR REPLACE STAGE telco.datamart.fomc
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');
    

CREATE OR REPLACE TABLE TELCO.DATAMART.TB_R_PARSED_FOMC_CONTENT AS SELECT 
      relative_path,
      TO_VARCHAR(
        SNOWFLAKE.CORTEX.PARSE_DOCUMENT(
          @TELCO.DATAMART.fomc, 
          relative_path, 
          {'mode': 'LAYOUT'}
        ) :content
      ) AS parsed_text
    FROM directory(@TELCO.DATAMART.fomc)
    WHERE relative_path LIKE '%.pdf';


CREATE OR REPLACE TABLE telco.datamart.TB_F_CHUNKED_FOMC_CONTENT (
    file_name VARCHAR,
    CHUNK VARCHAR
);

INSERT INTO telco.datamart.TB_F_CHUNKED_FOMC_CONTENT (file_name, CHUNK)
SELECT
    relative_path,
    c.value AS CHUNK
FROM
    telco.datamart.TB_R_PARSED_FOMC_CONTENT,
    LATERAL FLATTEN( input => SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER (
        parsed_text,
        'markdown',
        100,
        25
    )) c;

CREATE OR REPLACE CORTEX SEARCH SERVICE TELCO.DATAMART.FOMC_SEARCH_SERVICE
    ON chunk
    WAREHOUSE = snowflake_learning_wh
    TARGET_LAG = '1 hour'
    EMBEDDING_MODEL = 'snowflake-arctic-embed-l-v2.0'
    AS (
    SELECT
        file_name,
        chunk
    FROM TELCO.DATAMART.TB_F_CHUNKED_FOMC_CONTENT
    );