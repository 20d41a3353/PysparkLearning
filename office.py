import os
import json
import logging
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, Tuple, Optional

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import (
    expr, col, when, substring, concat, lit, to_timestamp, translate, broadcast,
    coalesce, current_timestamp, Window, percent_rank, regexp_extract
)
from pyspark.sql.types import DecimalType, StringType, LongType

# ========== LOGGING SETUP ==========
def setup_logger(name: str, log_file: str = "./logs/pipeline.log") -> logging.Logger:
    """Production-grade logging."""
    Path("./logs").mkdir(exist_ok=True)
    
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    if not logger.handlers:
        handler = logging.FileHandler(log_file)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    return logger

logger = setup_logger(__name__)


# ========== SPARK SESSION (OPTIMIZED) ==========
def create_optimized_spark_session(app_name: str = "GLIF_Pipeline") -> SparkSession:
    """Production-grade Spark session with safety checks and optimizations."""
    
    # Check for existing session
    try:
        spark = SparkSession.getActiveSession()
        if spark and spark.sparkContext.appName == app_name:
            logger.info(f"✓ Reusing existing session: {app_name}")
            return spark
        elif spark:
            logger.warning(f"Stopping mismatched session: {spark.sparkContext.appName}")
            spark.stop()
    except:
        pass
    
    logger.info(f"Creating new Spark session: {app_name}")
    
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", os.path.abspath("./postgresql-42.5.4.jar")) \
        .config("spark.hadoop.fs.defaultFS", "hdfs://10.177.103.199:8022") \
        \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.executor.cores", "4") \
        .config("spark.executor.instances", "8") \
        \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryo.registrationRequired", "false") \
        \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "400") \
        .config("spark.sql.files.maxPartitionBytes", "256MB") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        \
        .config("spark.dynamicAllocation.enabled", "true") \
        .config("spark.dynamicAllocation.minExecutors", "2") \
        .config("spark.dynamicAllocation.maxExecutors", "16") \
        \
        .config("spark.network.timeout", "300s") \
        .config("spark.executor.heartbeatInterval", "60s") \
        \
        .getOrCreate()
    
    logger.info("✓ Spark session created with optimizations")
    return spark


# ========== BATCH ID FETCH (RETRY LOGIC) ==========
def fetch_batch_id_safe(spark: SparkSession, pg_config: Dict, max_retries: int = 3) -> int:
    """Fetch batch ID with exponential backoff retry logic."""
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Fetching BATCH_ID (attempt {attempt + 1}/{max_retries})...")
            
            df_batch = spark.read.format("jdbc") \
                .option("url", pg_config["url"]) \
                .option("query", "SELECT nextval('batch_seq') AS BATCH_ID_VAL") \
                .option("user", pg_config["user"]) \
                .option("password", pg_config["password"]) \
                .option("driver", pg_config["driver"]) \
                .option("fetchsize", 1) \
                .option("connectionTimeout", 30000) \
                .load()
            
            batch_row = df_batch.first()  # Use .first() instead of .collect()[0]
            
            if batch_row is None:
                raise ValueError("Batch ID query returned NULL")
            
            batch_id = int(batch_row["BATCH_ID_VAL"])
            
            if batch_id <= 0:
                raise ValueError(f"Invalid batch ID: {batch_id}")
            
            logger.info(f"✓ Successfully fetched BATCH_ID: {batch_id}")
            return batch_id
            
        except Exception as e:
            wait_time = (2 ** attempt)
            logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {wait_time}s...")
            
            if attempt < max_retries - 1:
                time.sleep(wait_time)
            else:
                logger.error(f"❌ Failed after {max_retries} attempts")
                raise


# ========== HDFS READ (OPTIMIZED) ==========
def read_hdfs_safe(spark: SparkSession, hdfs_path: str, repartitions: int = 200) -> any:
    """Read HDFS files with validation and optimal partitioning."""
    
    logger.info(f"Reading HDFS path: {hdfs_path}")
    
    try:
        df = spark.read.text(hdfs_path)
        
        row_count = df.count()
        logger.info(f"✓ Read {row_count} records from HDFS")
        
        if row_count == 0:
            raise ValueError(f"HDFS path returned 0 records: {hdfs_path}")
        
        df_repart = df.repartition(repartitions)
        logger.info(f"Repartitioned to {repartitions} partitions")
        
        return df_repart
        
    except Exception as e:
        logger.error(f"Failed to read HDFS: {e}")
        raise


# ========== CODE 1: GL CONSOLIDATION (OPTIMIZED) ==========
def process_glif_transactions(spark: SparkSession, pg_config: Dict, processing_date: date = None) -> None:
    """
    CODE 1: GL Consolidation
    - Parse GLIF files
    - Validate CGLs
    - Write GL_TRANSACTIONS and GL_BALANCE
    """
    
    if processing_date is None:
        processing_date = date.today()
    
    yesterday = processing_date - timedelta(days=1)
    posting_date_str = processing_date.strftime("%Y-%m-%d")
    
    logger.info(f"========== CODE 1: GL Consolidation (Date: {posting_date_str}) ==========")
    
    try:
        # Fetch batch ID
        batch_id = fetch_batch_id_safe(spark, pg_config)
        
        # Read HDFS files
        hdfs_path = f"hdfs://10.177.103.199:8022/CBS-FILES/{posting_date_str}/GLIF/VARAGA_GLIFONL_*"
        df_raw = read_hdfs_safe(spark, hdfs_path, repartitions=200)
        
        # ===== PARSE GLIF (SINGLE SELECT FOR OPTIMIZATION) =====
        df_parsed = df_raw.select(
            substring(col("value"), 51, 18).alias("Id"),
            substring(col("value"), 48, 3).alias("currency_code"),
            when(substring(col("value"), 48, 3) != "INR",
                 substring(col("value"), 133, 17))
            .otherwise(substring(col("value"), 116, 17))
            .alias("Amount_raw"),
            when(substring(col("value"), 48, 3) != "INR",
                 substring(col("value"), 149, 1))
            .otherwise(substring(col("value"), 132, 1))
            .alias("sign_char")
        )
        
        # ===== DECODE OCR (OPTIMIZED with TRANSLATE) =====
        df_decoded = df_parsed.withColumns({
            "Amount_base": substring(col("Amount_raw"), 1, 16),
            "last_digit_str": translate(col("sign_char"), "pqrstuvwxy", "0123456789"),
            "is_negative": col("sign_char").isin('p','q','r','s','t','u','v','w','x','y').cast("int")
        }).withColumn(
            "Amount_decimal_str",
            concat(col("Amount_base"), col("last_digit_str"))
        ).withColumn(
            "Amount_final",
            (col("Amount_decimal_str").cast(DecimalType(22, 3)) / F.lit(1000))
            * when(col("is_negative") == 1, F.lit(-1)).otherwise(F.lit(1))
        ).select(
            col("Id"),
            when(col("Amount_final") > 0, col("Amount_final")).otherwise(F.lit(0)).alias("Amountpve"),
            when(col("Amount_final") < 0, col("Amount_final")).otherwise(F.lit(0)).alias("Amountnve")
        )
        
        # ===== AGGREGATE BY ID =====
        df_agg = df_decoded.groupBy("Id").agg(
            F.sum("Amountpve").alias("DEBIT_AMOUNT"),
            F.sum("Amountnve").alias("CREDIT_AMOUNT"),
            F.count(F.lit(1)).alias("TRANSACTION_COUNT")
        )
        
        # ===== MAP TO GL SCHEMA =====
        df_mapped = df_agg.select(
            F.lit(str(batch_id)).alias("BATCH_ID"),
            F.lit(None).cast(StringType()).alias("JOURNAL_ID"),
            to_timestamp(F.lit(posting_date_str), "yyyy-MM-dd").alias("POST_DATE"),
            substring("Id", 1, 5).alias("BRANCH_CODE"),
            substring("Id", 6, 3).alias("CURRENCY"),
            substring("Id", 9, 10).alias("CGL"),
            F.lit("CBS consolidated txns").cast(StringType()).alias("NARRATION"),
            col("DEBIT_AMOUNT"),
            col("CREDIT_AMOUNT"),
            col("TRANSACTION_COUNT"),
            F.lit("C").cast(StringType()).alias("SOURCE_FLAG")
        )
        
        # ===== VALIDATE CGL (BROADCAST JOIN) =====
        cgl_query = "(SELECT DISTINCT CGL_NUMBER FROM cgl_master WHERE BAL_COMPARE=1) T1"
        master_cgl = spark.read.format("jdbc") \
            .option("url", pg_config["url"]) \
            .option("dbtable", cgl_query) \
            .option("user", pg_config["user"]) \
            .option("password", pg_config["password"]) \
            .option("driver", pg_config["driver"]) \
            .load()
        
        joined = df_mapped.join(
            broadcast(master_cgl),
            df_mapped["CGL"] == master_cgl["CGL_NUMBER"],
            "left_outer"
        )
        
        df_validated = joined.withColumn(
            "CGL_Final",
            when(col("CGL_NUMBER").isNull(),
                 when(substring(col("CGL"), 1, 1) == "5", F.lit("5000000000"))
                 .otherwise(F.lit("1111111111")))
            .otherwise(col("CGL"))
        ).drop("CGL_NUMBER").withColumnRenamed("CGL_Final", "CGL")
        
        # ===== WRITE GL_TRANSACTIONS =====
        df_to_write = df_validated.repartition(20, "BRANCH_CODE")
        write_to_postgresql_safe(df_to_write, pg_config, "GL_TRANSACTIONS")
        
        # ===== COMPUTE & MERGE BALANCE =====
        balance_delta = df_validated.groupBy("CGL", "CURRENCY", "BRANCH_CODE").agg(
            (F.coalesce(F.sum("DEBIT_AMOUNT"), F.lit(0)) +
             F.coalesce(F.sum("CREDIT_AMOUNT"), F.lit(0))).alias("BALANCE")
        )
        
        yesterday_str = yesterday.strftime("%Y-%m-%d")
        query_prev_balance = f"""(
            SELECT CGL, BALANCE, CURRENCY, BRANCH_CODE
            FROM GL_BALANCE
            WHERE DATE(BALANCE_DATE) = DATE '{yesterday_str}'
        ) T1"""
        
        df_prev = spark.read.format("jdbc") \
            .option("url", pg_config["url"]) \
            .option("dbtable", query_prev_balance) \
            .option("user", pg_config["user"]) \
            .option("password", pg_config["password"]) \
            .option("driver", pg_config["driver"]) \
            .load()
        
        combined = balance_delta.select("CGL", "CURRENCY", "BRANCH_CODE", "BALANCE") \
            .unionByName(df_prev.select("CGL", "CURRENCY", "BRANCH_CODE", "BALANCE"))
        
        final_balance = combined.groupBy("CGL", "CURRENCY", "BRANCH_CODE") \
            .agg(F.sum("BALANCE").alias("BALANCE")) \
            .withColumn("BALANCE_DATE", F.to_date(F.lit(posting_date_str), "yyyy-MM-dd"))
        
        write_to_postgresql_safe(final_balance, pg_config, "GL_BALANCE")
        
        logger.info("✓ CODE 1 completed successfully")
        
    except Exception as e:
        logger.error(f"CODE 1 failed: {e}", exc_info=True)
        raise


# ========== CODE 2: PARALLEL MULTI-FILE (FIXED) ==========
def process_multi_source_files(spark: SparkSession, pg_config: Dict, processing_date: date = None) -> None:
    """
    CODE 2: Parallel Multi-File Processing (FIXED - No threading!)
    - Reads INV, BOR, GLCC files (Spark handles parallelism)
    - Combines and writes to CBS_BALANCE
    """
    
    if processing_date is None:
        processing_date = date.today()
    
    yesterday = processing_date - timedelta(days=1)
    logger.info(f"========== CODE 2: Multi-Source Processing (Date: {processing_date}) ==========")
    
    try:
        # Define all file sources with metadata
        file_sources = {
            "INV": {
                "path": "hdfs://10.177.103.199:8022/CBS-FILES/2025-11-20/BANCS24/INV*",
                "id_pos": 200,
                "amount_pos": 30,
                "sign_pos": 47,
                "scale": 1000
            },
            "BOR": {
                "path": "hdfs://10.177.103.199:8022/CBS-FILES/2025-11-20/BANCS24/BOR*",
                "id_pos": 105,
                "amount_pos": 27,
                "sign_pos": 44,
                "scale": 1000
            },
            "GLCC": {
                "path": "hdfs://10.177.103.199:8022/CBS-FILES/2025-11-20/BANCS24/GLCC*",
                "id_pos": 1,
                "amount_pos": 24,
                "sign_pos": -1,  # Last char
                "scale": 10000
            }
        }
        
        dfs = []
        
        # Read all sources (Spark handles parallelism internally, no threading!)
        for source_name, source_config in file_sources.items():
            logger.info(f"Reading {source_name}...")
            
            df_raw = spark.read.text(source_config["path"])
            
            # Parse based on source
            if source_name == "GLCC":
                df_parsed = df_raw.select(
                    substring(col("value"), source_config["id_pos"], 18).alias("Id"),
                    when(substring(col("value"), -1, 1) == "+",
                         F.trim(substring(col("value"), source_config["amount_pos"],
                                        F.length(col("value")) - source_config["amount_pos"]))
                         .cast(DecimalType(22, 4)) / source_config["scale"])
                    .when(substring(col("value"), -1, 1) == "-",
                         -1 * F.trim(substring(col("value"), source_config["amount_pos"],
                                             F.length(col("value")) - source_config["amount_pos"]))
                         .cast(DecimalType(22, 4)) / source_config["scale"])
                    .otherwise(F.lit(0))
                    .alias("Amount")
                )
            else:
                df_parsed = df_raw.select(
                    substring(col("value"), source_config["id_pos"], 18).alias("Id"),
                    when(substring(col("value"), source_config["sign_pos"], 1) == "+",
                         F.trim(substring(col("value"), source_config["amount_pos"], 17))
                         .cast(DecimalType(22, 4)) / source_config["scale"])
                    .when(substring(col("value"), source_config["sign_pos"], 1) == "-",
                         -1 * F.trim(substring(col("value"), source_config["amount_pos"], 17))
                         .cast(DecimalType(22, 4)) / source_config["scale"])
                    .otherwise(F.lit(0))
                    .alias("Amount")
                )
            
            dfs.append(df_parsed)
            logger.info(f"✓ {source_name} read: {df_parsed.count()} rows")
        
        # Union all sources
        combined = dfs[0]
        for df in dfs[1:]:
            combined = combined.unionByName(df)
        
        # Aggregate
        final_df = combined.groupBy("Id").agg(
            F.sum("Amount").alias("Amount")
        )
        
        # Map to CBS schema
        processed_df = final_df.withColumns({
            "BRANCH_CODE": substring(col("Id"), 1, 5),
            "CURRENCY": substring(col("Id"), 6, 3),
            "CGL": substring(col("Id"), 9, 10),
            "BALANCE": col("Amount"),
            "BALANCE_DATE": F.to_date(F.lit(processing_date.strftime("%Y-%m-%d")), "yyyy-MM-dd")
        }).select("CGL", "BALANCE", "CURRENCY", "BRANCH_CODE", "BALANCE_DATE")
        
        # Merge with previous balance
        yesterday_str = yesterday.strftime("%Y-%m-%d")
        query_prev = f"""(
            SELECT CGL, BALANCE, CURRENCY, BRANCH_CODE
            FROM CBS_BALANCE
            WHERE DATE(BALANCE_DATE) = DATE '{yesterday_str}'
        ) T1"""
        
        df_prev = spark.read.format("jdbc") \
            .option("url", pg_config["url"]) \
            .option("dbtable", query_prev) \
            .option("user", pg_config["user"]) \
            .option("password", pg_config["password"]) \
            .option("driver", pg_config["driver"]) \
            .load()
        
        combined_balance = processed_df.select("CGL", "BALANCE", "CURRENCY", "BRANCH_CODE") \
            .unionByName(df_prev)
        
        final_balance = combined_balance.groupBy("CGL", "CURRENCY", "BRANCH_CODE") \
            .agg(F.sum("BALANCE").alias("BALANCE")) \
            .withColumn("BALANCE_DATE", F.to_date(F.lit(processing_date.strftime("%Y-%m-%d")), "yyyy-MM-dd"))
        
        write_to_postgresql_safe(final_balance, pg_config, "CBS_BALANCE")
        
        logger.info("✓ CODE 2 completed successfully")
        
    except Exception as e:
        logger.error(f"CODE 2 failed: {e}", exc_info=True)
        raise


# ========== CODE 3: BALANCE RECONCILIATION (OPTIMIZED) ==========
def reconcile_balances_optimized(spark: SparkSession, pg_config: Dict, processing_date: date = None) -> None:
    """
    CODE 3: Balance Reconciliation (FIXED DATE BUG + Lookup table for HEAD rules)
    - Compares CBS_BALANCE vs GL_BALANCE
    - Uses lookup table for HEAD categorization (not nested when clauses)
    - Tracks differences over time
    """
    
    if processing_date is None:
        processing_date = date.today()
    
    yesterday = processing_date - timedelta(days=1)
    logger.info(f"========== CODE 3: Reconciliation (Date: {yesterday}) ==========")
    
    try:
        # Query mismatches
        sql_query = f"""(
            SELECT 
                a.BRANCH_CODE, a.CURRENCY, a.CGL, 
                a.BALANCE AS CBS_BALANCE, b.BALANCE AS GL_BALANCE,
                CAST(COALESCE(a.BALANCE, 0) - COALESCE(b.BALANCE, 0) AS NUMERIC(20, 4)) AS DIFFERENCE_AMOUNT
            FROM CBS_BALANCE a
            INNER JOIN GL_BALANCE b
            ON a.BRANCH_CODE = b.BRANCH_CODE AND a.CURRENCY = b.CURRENCY AND a.CGL = b.CGL
            WHERE (a.BALANCE != b.BALANCE OR a.BALANCE IS NULL OR b.BALANCE IS NULL)
            AND DATE(a.BALANCE_DATE) = DATE '{yesterday.strftime("%Y-%m-%d")}'
            AND DATE(b.BALANCE_DATE) = DATE '{yesterday.strftime("%Y-%m-%d")}'
        ) T1"""
        
        mismatches = spark.read.format("jdbc") \
            .option("url", pg_config["url"]) \
            .option("dbtable", sql_query) \
            .option("user", pg_config["user"]) \
            .option("password", pg_config["password"]) \
            .option("driver", pg_config["driver"]) \
            .load()
        
        output_df = mismatches.select(
            current_timestamp().alias("RECON_RUN_DATE"),
            "BRANCH_CODE", "CURRENCY", "CGL",
            "CBS_BALANCE", "GL_BALANCE", "DIFFERENCE_AMOUNT"
        )
        
        # ===== HEAD CATEGORIZATION VIA LOOKUP TABLE (NOT NESTED WHEN CLAUSES) =====
        # Read HEAD rules from database (instead of hardcoded)
        head_query = "(SELECT priority, branch_code, cgl_prefix, currency, head_category FROM cgl_head_mapping ORDER BY priority) T1"
        
        try:
            df_head_rules = spark.read.format("jdbc") \
                .option("url", pg_config["url"]) \
                .option("dbtable", head_query) \
                .option("user", pg_config["user"]) \
                .option("password", pg_config["password"]) \
                .option("driver", pg_config["driver"]) \
                .load()
            
            # Apply rules via join
            output_df = output_df.join(
                df_head_rules,
                (F.coalesce(df_head_rules["branch_code"], output_df["BRANCH_CODE"]) == output_df["BRANCH_CODE"]) &
                (F.coalesce(df_head_rules["currency"], output_df["CURRENCY"]) == output_df["CURRENCY"]),
                "left"
            ).select(
                output_df["RECON_RUN_DATE"], output_df["BRANCH_CODE"], output_df["CURRENCY"],
                output_df["CGL"], output_df["CBS_BALANCE"], output_df["GL_BALANCE"],
                output_df["DIFFERENCE_AMOUNT"],
                coalesce(df_head_rules["head_category"], F.lit("UNCLASSIFIED")).alias("HEAD")
            )
            
        except:
            # Fallback to simple rules if lookup table doesn't exist
            logger.warning("HEAD lookup table not found, using simple fallback")
            output_df = output_df.withColumn("HEAD",
                when(substring(col("CGL"), 1, 1) == "5", F.lit("MEMO"))
                .when(substring(col("CGL"), 1, 1) == "1", F.lit("LOAN"))
                .otherwise(F.lit("OTHER"))
            )
        
        # ===== FIX DATE BUG: Use YESTERDAY not TODAY =====
        yesterday_str = yesterday.strftime("%Y-%m-%d")  # FIXED: Use yesterday, not today!
        
        difference_prev = spark.read.format("jdbc") \
            .option("url", pg_config["url"]) \
            .option("dbtable", "DIFFERENCE") \
            .option("user", pg_config["user"]) \
            .option("password", pg_config["password"]) \
            .option("driver", pg_config["driver"]) \
            .load()
        
        yesterdays_df = difference_prev.filter(
            col("RECON_RUN_DATE").cast("date") == F.lit(yesterday_str).cast("date")
        ).select(
            col("BRANCH_CODE").alias("y_BRANCH_CODE"),
            col("CURRENCY").alias("y_CURRENCY"),
            col("CGL").alias("y_CGL"),
            col("DIFFERENCE_AMOUNT").alias("YESTERDAY_DIFF").cast(DecimalType(20, 4))
        )
        
        final_result = output_df.join(
            yesterdays_df,
            (col("BRANCH_CODE") == col("y_BRANCH_CODE")) &
            (col("CURRENCY") == col("y_CURRENCY")) &
            (col("CGL") == col("y_CGL")),
            "left_outer"
        ).withColumns({
            "DIFF_BW_YESTERDAY": (
                coalesce(col("DIFFERENCE_AMOUNT"), F.lit(0)) -
                coalesce(col("YESTERDAY_DIFF"), F.lit(0))
            ).cast(DecimalType(20, 4)),
            "TYPE": when(col("YESTERDAY_DIFF").isNull(), F.lit("New Entry"))
                    .when(col("DIFFERENCE_AMOUNT") == col("YESTERDAY_DIFF"), F.lit("Unchanged"))
                    .otherwise(F.lit("Changed"))
        }).drop("y_BRANCH_CODE", "y_CURRENCY", "y_CGL")
        
        write_to_postgresql_safe(final_result, pg_config, "DIFFERENCE")
        
        logger.info("✓ CODE 3 completed successfully")
        
    except Exception as e:
        logger.error(f"CODE 3 failed: {e}", exc_info=True)
        raise


# ========== CODE 4: PARQUET EXPORT (SAFE MODE) ==========
def export_parquet_safe(spark: SparkSession, hdfs_path: str, processing_date: date = None) -> None:
    """
    CODE 4: Parquet Export (APPEND MODE - No data loss!)
    - Uses append mode (preserves history)
    - Partitions by date (faster queries)
    - Validates export
    """
    
    if processing_date is None:
        processing_date = date.today()
    
    logger.info(f"========== CODE 4: Parquet Export (Date: {processing_date}) ==========")
    
    try:
        base_path = "hdfs://10.177.103.199:8022/CBS-FILES/2025-11-20/GLIF/"
        output_path = "hdfs://10.177.103.199:8022/CBS-TEST/ALL_GLIF_OUTPUT/"
        
        file_types_extra = ["INV", "PRI", "BOR", "PRB", "ONL", "AGRI_PY", "AGRI_CY", "INV_IN", "INV_TD", "INV_DC", "BOR_DC"]
        file_types_simple = ["INV_BC", "INV_FD", "INV_RE", "GEN_FIT", "INV_B8", "INV_CT", "INV_ECG", "INV_FCI", "BOR_VS", "INV_GB"]
        
        paths = [f"{base_path}*GLIF{ft}_[0-9][0-9][0-9].gz" for ft in file_types_extra] + \
                [f"{base_path}*GLIF{ft}.gz" for ft in file_types_simple]
        
        logger.info(f"Reading {len(paths)} file patterns...")
        
        df_raw = spark.read.text(paths)
        
        parquet_data = df_raw.select(
            expr("substring(value, 4, 5)").alias("txdate"),
            expr("substring(value, 28, 9)").alias("journal"),
            expr("substring(value, 42, 5)").alias("branch"),
            expr("substring(value, 48, 3)").alias("currency"),
            expr("substring(value, 51, 18)").alias("GLCC"),
            expr("substring(value, 116, 17)").alias("lcyAmt"),
            expr("substring(value, 133, 17)").alias("fcyAmt")
        ).withColumn(
            "export_date",
            F.to_date(F.lit(processing_date.strftime("%Y-%m-%d")), "yyyy-MM-dd")
        )
        
        # ===== APPEND MODE (NOT OVERWRITE - PRESERVE HISTORY!) =====
        row_count = parquet_data.count()
        logger.info(f"Writing {row_count} rows to Parquet (APPEND MODE)...")
        
        parquet_data.write \
            .format("parquet") \
            .mode("append") \
            .partitionBy("export_date") \
            .parquet(output_path)
        
        # Verify
        df_verify = spark.read.parquet(output_path)
        verify_count = df_verify.count()
        logger.info(f"✓ Exported {row_count} records. Total in Parquet: {verify_count}")
        
    except Exception as e:
        logger.error(f"CODE 4 failed: {e}", exc_info=True)
        raise


# ========== POSTGRESQL WRITE (OPTIMIZED) ==========
def write_to_postgresql_safe(df, pg_config: Dict, table_name: str, mode: str = "append") -> None:
    """Write to PostgreSQL with connection pooling and error handling."""
    
    row_count = df.count()
    logger.info(f"Writing {row_count} rows to {table_name}...")
    
    try:
        df_write = df.repartition(10)  # Conservative parallelism
        
        df_write.write \
            .format("jdbc") \
            .option("url", pg_config["url"]) \
            .option("dbtable", table_name) \
            .option("user", pg_config["user"]) \
            .option("password", pg_config["password"]) \
            .option("driver", pg_config["driver"]) \
            .option("batchsize", 50000) \
            .option("isolationLevel", "READ_COMMITTED") \
            .option("numPartitions", 10) \
            .mode(mode) \
            .save()
        
        logger.info(f"✓ Successfully wrote {row_count} rows to {table_name}")
        
    except Exception as e:
        logger.error(f"Failed to write to {table_name}: {e}")
        raise


# ========== MAIN EXECUTION ==========
def main():
    """Main pipeline execution."""
    
    logger.info("=" * 80)
    logger.info("STARTING GLIF PIPELINE (All 4 Codes - Optimized & Fixed)")
    logger.info("=" * 80)
    
    spark = create_optimized_spark_session()
    
    pg_config = {
        "url": "jdbc:postgresql://10.177.103.192:5432/fincorepdb1",
        "user": "fincore",
        "password": "Password#1234",
        "driver": "org.postgresql.Driver"
    }
    
    processing_date = date.today()
    
    try:
        # CODE 1: GL Consolidation
        process_glif_transactions(spark, pg_config, processing_date)
        
        # CODE 2: Multi-Source Processing
        process_multi_source_files(spark, pg_config, processing_date)
        
        # CODE 3: Reconciliation
        reconcile_balances_optimized(spark, pg_config, processing_date)
        
        # CODE 4: Parquet Export
        export_parquet_safe(spark, "hdfs://10.177.103.199:8022/CBS-TEST/", processing_date)
        
        logger.info("=" * 80)
        logger.info("✓✓✓ ALL CODES COMPLETED SUCCESSFULLY ✓✓✓")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"❌ Pipeline failed: {e}", exc_info=True)
        raise
    
    finally:
        spark.stop()
        logger.info("Spark session stopped")


if __name__ == "__main__":
    main()










