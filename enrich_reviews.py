# enrich_reviews.py (FIX: pisah multi-statement jadi single statements)
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, sql_expr, when, lit

def ensure_table_exists(session):
    session.sql("""
      CREATE SCHEMA IF NOT EXISTS TELCO.DATAMART;
    """).collect()
    session.sql("""
      CREATE TABLE IF NOT EXISTS TELCO.DATAMART.TB_F_REVIEWS_ENRICHED AS
      SELECT
        _FIVETRAN_ID, REVIEW_ID, APP_ID, COUNTRY, LANG, APP_VERSION, USER_NAME,
        CONTENT_CLEAN, THUMBS_UP_COUNT, CAST(SCORE AS NUMBER(10,2)) AS STAR_SCORE,
        TRY_TO_TIMESTAMP_TZ(REVIEWED_AT::STRING)          AS REVIEWED_TS_TZ,
        TRY_TO_TIMESTAMP_TZ(NULLIF(REPLIED_AT::STRING,'')) AS REPLIED_TS_TZ,
        IFF(NULLIF(REPLIED_AT::STRING,'') IS NOT NULL, 1, 0) AS IS_REPLIED,
        CAST(NULL AS TIMESTAMP_NTZ) AS REVIEWED_TS,
        CAST(NULL AS TIMESTAMP_NTZ) AS REPLIED_TS,
        CAST(NULL AS STRING)        AS TOPIC_CLASS,
        CAST(NULL AS FLOAT)         AS SENTIMENT_SCORE,
        CAST(NULL AS STRING)        AS SATISFACTION_TEXT,
        CAST(NULL AS STRING)        AS SENTIMENT_BUCKET,
        CAST(NULL AS STRING)        AS SATISFACTION_LABEL,
        CAST(NULL AS NUMBER)        AS REPLY_LATENCY_MIN
      FROM TELCO.RAW.TB_R_REVIEW WHERE 1=2
    """).collect()

    pk_exists = session.sql("""
      SELECT COUNT(*)
      FROM TELCO.INFORMATION_SCHEMA.TABLE_CONSTRAINTS
      WHERE TABLE_SCHEMA = 'DATAMART'
        AND TABLE_NAME   = 'TB_F_REVIEWS_ENRICHED'
        AND CONSTRAINT_TYPE = 'PRIMARY KEY'
    """).collect()[0][0]

    if pk_exists == 0:
        session.sql("""
          ALTER TABLE TELCO.DATAMART.TB_F_REVIEWS_ENRICHED
          ADD PRIMARY KEY (_FIVETRAN_ID) RELY
        """).collect()

def build_frame_from_raw(session, source_df):
    df = source_df.select(
        col("_FIVETRAN_ID"),
        col("REVIEW_ID"), col("APP_ID"), col("COUNTRY"), col("LANG"),
        col("APP_VERSION"), col("USER_NAME"),
        col("CONTENT_CLEAN"), col("THUMBS_UP_COUNT"),
        sql_expr("SCORE::NUMBER(10,2) AS STAR_SCORE"),
        sql_expr("TRY_TO_TIMESTAMP_TZ(REVIEWED_AT::STRING)  AS REVIEWED_TS_TZ"),
        sql_expr("TRY_TO_TIMESTAMP_TZ(NULLIF(REPLIED_AT::STRING,'')) AS REPLIED_TS_TZ"),
        sql_expr("IFF(NULLIF(REPLIED_AT::STRING,'') IS NOT NULL, 1, 0) AS IS_REPLIED"),
    )

    df_en = df.select(
        "*",
        sql_expr("CAST(REVIEWED_TS_TZ AS TIMESTAMP_NTZ) AS REVIEWED_TS"),
        sql_expr("CAST(REPLIED_TS_TZ  AS TIMESTAMP_NTZ) AS REPLIED_TS"),
        sql_expr("""(SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
                       CONTENT_CLEAN,
                       ARRAY_CONSTRUCT('Network','App','Payment/Billing','Recharge/Sales','Customer Service','Other')
                    ):"label")::STRING""").alias("TOPIC_CLASS"),
        sql_expr("SNOWFLAKE.CORTEX.SENTIMENT(CONTENT_CLEAN)").alias("SENTIMENT_SCORE"),
        sql_expr("""TO_VARCHAR(SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
                      CONTENT_CLEAN,
                      'Apakah pelanggan puas? Jawab dengan salah satu: Puas / Tidak Puas / Netral.'
                    ))""").alias("SATISFACTION_TEXT"),
    ).select(
        "*",
        when(col("SENTIMENT_SCORE") > lit(0.2), lit("POSITIVE"))
          .when(col("SENTIMENT_SCORE") < lit(-0.2), lit("NEGATIVE"))
          .otherwise(lit("NEUTRAL")).alias("SENTIMENT_BUCKET"),
        when(sql_expr("SATISFACTION_TEXT ILIKE '%puas%' AND SATISFACTION_TEXT NOT ILIKE '%tidak%'"), lit("PUAS"))
          .when(sql_expr("SATISFACTION_TEXT ILIKE '%tidak puas%' OR SATISFACTION_TEXT ILIKE '%gak puas%'"), lit("TIDAK_PUAS"))
          .otherwise(lit("NETRAL")).alias("SATISFACTION_LABEL"),
        sql_expr("""CASE WHEN REPLIED_TS_TZ IS NOT NULL
                          THEN DATEDIFF('minute', REVIEWED_TS_TZ, REPLIED_TS_TZ)
                          ELSE NULL END""").alias("REPLY_LATENCY_MIN")
    )
    return df_en

def full_refresh(session):
    src = session.table("TELCO.RAW.TB_R_REVIEW")
    df_en = build_frame_from_raw(session, src)

    # Tulis ke staging
    df_en.write.save_as_table("TELCO.DATAMART.TB_F_REVIEWS_ENRICHED__STG", mode="overwrite")

    # ⬇️ Pisahkan jadi 3 statement terpisah (bukan 1 blok)
    session.sql("""
      CREATE TABLE IF NOT EXISTS TELCO.DATAMART.TB_F_REVIEWS_ENRICHED
      LIKE TELCO.DATAMART.TB_F_REVIEWS_ENRICHED__STG
    """).collect()

    session.sql("""
      ALTER TABLE TELCO.DATAMART.TB_F_REVIEWS_ENRICHED
      SWAP WITH TELCO.DATAMART.TB_F_REVIEWS_ENRICHED__STG
    """).collect()

    session.sql("""
      DROP TABLE IF EXISTS TELCO.DATAMART.TB_F_REVIEWS_ENRICHED__STG
    """).collect()

    return "FULL_REFRESH_DONE"

def incremental_merge(session):
    src = session.table("TELCO.RAW.TB_R_REVIEW_STM_DM")
    if src.count() == 0:
        return "NO_DELTA"

    df_en = build_frame_from_raw(session, src.select("*", sql_expr("METADATA$ACTION AS ACTION")))
    df_en.create_or_replace_temp_view("ENRICH_DELTA")

    session.sql("""
      MERGE INTO TELCO.DATAMART.TB_F_REVIEWS_ENRICHED t
      USING ENRICH_DELTA d
      ON t._FIVETRAN_ID = d._FIVETRAN_ID

      WHEN MATCHED AND d.ACTION = 'DELETE' THEN
        DELETE

      WHEN MATCHED AND d.ACTION <> 'DELETE' THEN
        UPDATE SET
          REVIEW_ID=d.REVIEW_ID, APP_ID=d.APP_ID, COUNTRY=d.COUNTRY, LANG=d.LANG,
          APP_VERSION=d.APP_VERSION, USER_NAME=d.USER_NAME, CONTENT_CLEAN=d.CONTENT_CLEAN,
          THUMBS_UP_COUNT=d.THUMBS_UP_COUNT, STAR_SCORE=d.STAR_SCORE,
          REVIEWED_TS=d.REVIEWED_TS, REPLIED_TS=d.REPLIED_TS,
          TOPIC_CLASS=d.TOPIC_CLASS, SENTIMENT_SCORE=d.SENTIMENT_SCORE,
          SATISFACTION_TEXT=d.SATISFACTION_TEXT, REVIEWED_TS_TZ=d.REVIEWED_TS_TZ,
          REPLIED_TS_TZ=d.REPLIED_TS_TZ, IS_REPLIED=d.IS_REPLIED,
          SENTIMENT_BUCKET=d.SENTIMENT_BUCKET, SATISFACTION_LABEL=d.SATISFACTION_LABEL,
          REPLY_LATENCY_MIN=d.REPLY_LATENCY_MIN

      WHEN NOT MATCHED AND d.ACTION <> 'DELETE' THEN
        INSERT (
          _FIVETRAN_ID,REVIEW_ID,APP_ID,COUNTRY,LANG,APP_VERSION,USER_NAME,
          CONTENT_CLEAN,THUMBS_UP_COUNT,STAR_SCORE,REVIEWED_TS,REPLIED_TS,
          TOPIC_CLASS,SENTIMENT_SCORE,SATISFACTION_TEXT,REVIEWED_TS_TZ,REPLIED_TS_TZ,IS_REPLIED,
          SENTIMENT_BUCKET,SATISFACTION_LABEL,REPLY_LATENCY_MIN
        )
        VALUES (
          d._FIVETRAN_ID,d.REVIEW_ID,d.APP_ID,d.COUNTRY,d.LANG,d.APP_VERSION,d.USER_NAME,
          d.CONTENT_CLEAN,d.THUMBS_UP_COUNT,d.STAR_SCORE,d.REVIEWED_TS,d.REPLIED_TS,
          d.TOPIC_CLASS,d.SENTIMENT_SCORE,d.SATISFACTION_TEXT,d.REVIEWED_TS_TZ,d.REPLIED_TS_TZ,d.IS_REPLIED,
          d.SENTIMENT_BUCKET,d.SATISFACTION_LABEL,d.REPLY_LATENCY_MIN
        )
    """).collect()

    return "INCREMENTAL_MERGE_DONE"

def main(mode: str = "incremental"):
    session = get_active_session()
    ensure_table_exists(session)
    if mode and mode.lower().startswith("full"):
        return full_refresh(session)
    return incremental_merge(session)
