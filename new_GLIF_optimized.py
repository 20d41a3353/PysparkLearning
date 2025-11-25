import json
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import (
    expr, col, when, substring, concat, lit, to_timestamp, translate, broadcast
)
from pyspark.sql.types import DecimalType, StringType

# ========== LOGGER SETUP ==========
def setup_logger(log_file: str, level: str = "INFO"):
    """Configure logging to file and console."""
    logger = logging.getLogger("GLIFPipeline")
    logger.setLevel(getattr(logging, level.upper()))
    
    # File handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    ))
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s"
    ))
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger


# ========== CONFIG LOADER ==========
def load_config(config_path: str) -> dict:
    """Load JSON configuration file."""
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        raise ValueError(f"Config file not found: {config_path}")
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in config: {e}")


# ========== SPARK SESSION BUILDER ==========
def create_spark_session(config: dict, logger) -> SparkSession:
    """Create optimized Spark session."""
    spark_conf = config.get("spark", {})
    
    logger.info(f"Creating Spark session: {spark_conf.get('app_name')}")
    
    spark = SparkSession.builder \
        .appName(spark_conf.get("app_name", "GLIF_Pipeline")) \
        .config("spark.jars", spark_conf.get("jar_path", "./ojdbc8.jar")) \
        .config("spark.hadoop.fs.defaultFS", spark_conf.get("hdfs_uri", "hdfs://localhost:8020")) \
        .getOrCreate()
    
    # Apply tuning configs
    spark.conf.set("spark.sql.adaptive.enabled", spark_conf.get("adaptive_enabled", True))
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", True)
    spark.conf.set("spark.sql.shuffle.partitions", spark_conf.get("shuffle_partitions", 400))
    spark.conf.set("spark.sql.files.maxPartitionBytes", spark_conf.get("files_max_partition_bytes", "256MB"))
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", spark_conf.get("arrow_enabled", True))
    
    logger.info("Spark session configured successfully")
    return spark


# ========== ORACLE HELPERS ==========
def fetch_batch_id(spark, oracle_config: dict, logger) -> int:
    """Fetch next batch ID from Oracle sequence."""
    logger.info("Fetching BATCH_ID from Oracle...")
    
    try:
        df_batch = spark.read.format("jdbc") \
            .option("url", oracle_config["url"]) \
            .option("query", "SELECT get_next_batch_id AS BATCH_ID_VAL FROM DUAL") \
            .option("user", oracle_config["user"]) \
            .option("password", oracle_config["password"]) \
            .option("driver", oracle_config["driver"]) \
            .option("fetchsize", 1) \
            .load()
        
        batch_id = int(df_batch.first()["BATCH_ID_VAL"])
        logger.info(f"Fetched BATCH_ID: {batch_id}")
        return batch_id
    except Exception as e:
        logger.error(f"Failed to fetch BATCH_ID: {e}")
        raise


def read_from_oracle(spark, oracle_config: dict, query: str, logger) -> any:
    """Generic Oracle read with error handling."""
    logger.debug(f"Reading from Oracle: {query[:100]}...")
    try:
        return spark.read.format("jdbc") \
            .option("url", oracle_config["url"]) \
            .option("dbtable", query) \
            .option("user", oracle_config["user"]) \
            .option("password", oracle_config["password"]) \
            .option("driver", oracle_config["driver"]) \
            .option("fetchsize", oracle_config.get("batchsize", 110000)) \
            .load()
    except Exception as e:
        logger.error(f"Failed to read from Oracle: {e}")
        raise


def write_to_oracle(df, oracle_config: dict, table_name: str, logger, mode: str = "append"):
    """Generic Oracle write with error handling and row count logging."""
    row_count = df.count()
    logger.info(f"Writing {row_count} rows to {table_name}...")
    
    try:
        df.write \
            .format("jdbc") \
            .option("url", oracle_config["url"]) \
            .option("dbtable", table_name) \
            .option("user", oracle_config["user"]) \
            .option("password", oracle_config["password"]) \
            .option("driver", oracle_config["driver"]) \
            .option("batchsize", oracle_config.get("batchsize", 110000)) \
            .mode(mode) \
            .save()
        logger.info(f"Successfully wrote {row_count} rows to {table_name}")
    except Exception as e:
        logger.error(f"Failed to write to {table_name}: {e}")
        raise


# ========== GLIF PROCESSING FUNCTIONS ==========
def build_hdfs_path(config: dict, processing_date: date, logger) -> str:
    """Dynamically build HDFS path based on date."""
    hdfs_conf = config.get("hdfs", {})
    base = hdfs_conf.get("base_path", "hdfs://localhost:8020/CBS-FILES")
    date_str = processing_date.strftime("%Y-%m-%d")
    subpath = hdfs_conf.get("glif_subpath", "GLIF")
    pattern = hdfs_conf.get("file_pattern", "VARAGA_GLIFONL_*")
    
    path = f"{base}/{date_str}/{subpath}/{pattern}"
    logger.info(f"Built HDFS path: {path}")
    return path


def read_glif_files(spark, path: str, read_partitions: int, logger):
    """Read GLIF files from HDFS."""
    logger.info(f"Reading GLIF files from {path}...")
    try:
        df = spark.read.text(path).repartition(read_partitions)
        row_count = df.count()
        logger.info(f"Read {row_count} rows from GLIF files")
        return df
    except Exception as e:
        logger.error(f"Failed to read GLIF files: {e}")
        raise


def parse_glif_records(df, config: dict, posting_date_str: str, logger):
    """Parse fixed-width GLIF records into structured columns."""
    logger.info("Parsing GLIF records...")
    
    proc_conf = config.get("processing", {})
    
    # Extract base fields
    df_parsed = df.select(
        substring("value", proc_conf.get("id_start", 51), proc_conf.get("id_length", 18)).alias("Id"),
        substring("value", proc_conf.get("currency_start", 48), proc_conf.get("currency_length", 3)).alias("currency_code"),
        when(substring("value", proc_conf.get("currency_start", 48), 3) != "INR",
             substring("value", proc_conf.get("non_inr_amount_start", 133), proc_conf.get("non_inr_amount_length", 17)))
        .otherwise(substring("value", proc_conf.get("inr_amount_start", 116), proc_conf.get("inr_amount_length", 17)))
        .alias("Amount_raw"),
        when(substring("value", proc_conf.get("currency_start", 48), 3) != "INR",
             substring("value", proc_conf.get("non_inr_sign_pos", 149), 1))
        .otherwise(substring("value", proc_conf.get("inr_sign_pos", 132), 1))
        .alias("sign_char")
    )
    
    # Extract amount base and sign
    amount_scale = proc_conf.get("amount_scale_factor", 1000)
    
    df_parsed = df_parsed \
        .withColumn("Amount_base", substring("Amount_raw", 1, 16)) \
        .withColumn("last_digit",
                    when(col("sign_char").rlike("^[p-y]$"),
                         translate(col("sign_char"), "pqrstuvwxy", "0123456789"))
                    .otherwise(col("sign_char"))) \
        .withColumn("sign", when(col("sign_char").isin('p','q','r','s','t','u','v','w','x','y'), -1).otherwise(1)) \
        .withColumn("Amount_decimal_str", concat(col("Amount_base"), col("last_digit"))) \
        .withColumn("Amount_final",
                    (col("Amount_decimal_str").cast(DecimalType(22, 3)) / F.lit(amount_scale)) * col("sign")) \
        .withColumn("Amountpve", when(col("Amount_final") > 0, col("Amount_final")).otherwise(F.lit(0))) \
        .withColumn("Amountnve", when(col("Amount_final") < 0, col("Amount_final")).otherwise(F.lit(0)))
    
    logger.info("GLIF records parsed successfully")
    return df_parsed.select("Id", "Amountpve", "Amountnve")


def aggregate_by_id(df, logger):
    """Aggregate amounts per transaction ID."""
    logger.info("Aggregating amounts by transaction ID...")
    
    df_agg = df.groupBy("Id").agg(
        F.sum("Amountpve").alias("DEBIT_AMOUNT"),
        F.sum("Amountnve").alias("CREDIT_AMOUNT"),
        F.count(lit(1)).alias("TRANSACTION_COUNT")
    )
    
    logger.info(f"Aggregated to {df_agg.count()} unique transaction IDs")
    return df_agg


def map_to_gl_schema(df_agg, batch_id: int, posting_date_str: str, config: dict, logger):
    """Map aggregated data to GL_TRANSACTIONS schema."""
    logger.info("Mapping to GL_TRANSACTIONS schema...")
    
    proc_conf = config.get("processing", {})
    
    df_mapped = df_agg.select(
        F.lit(str(batch_id)).alias("BATCH_ID").cast(StringType()),
        F.lit(None).cast(StringType()).alias("JOURNAL_ID"),
        to_timestamp(F.lit(posting_date_str), "yyyy-MM-dd").alias("POST_DATE"),
        substring("Id", proc_conf.get("branch_start", 1), proc_conf.get("branch_length", 5)).alias("BRANCH_CODE"),
        substring("Id", proc_conf.get("currency_offset", 6), 3).alias("CURRENCY"),
        substring("Id", proc_conf.get("cgl_start", 9), proc_conf.get("cgl_length", 10)).alias("CGL"),
        F.lit("CBS consolidated txns").cast(StringType()).alias("NARRATION"),
        col("DEBIT_AMOUNT"),
        col("CREDIT_AMOUNT"),
        col("TRANSACTION_COUNT"),
        F.lit("C").cast(StringType()).alias("SOURCE_FLAG")
    )
    
    return df_mapped


def validate_cgl_codes(df_mapped, spark, oracle_config: dict, cgl_table: str, logger):
    """Validate CGL codes against master list using broadcast join."""
    logger.info("Validating CGL codes...")
    
    # Read CGL master
    cgl_query = f"(SELECT DISTINCT CGL_NUMBER FROM {cgl_table} WHERE BAL_COMPARE=1) T1"
    master_cgl = read_from_oracle(spark, oracle_config, cgl_query, logger) \
        .select("CGL_NUMBER")
    
    logger.info(f"Loaded {master_cgl.count()} valid CGL codes")
    
    # Broadcast join
    joined = df_mapped.join(broadcast(master_cgl),
                           df_mapped["CGL"] == master_cgl["CGL_NUMBER"],
                           "left_outer")
    
    # Apply validation rules
    df_valid = joined.withColumn(
        "CGL_validated",
        when(col("CGL_NUMBER").isNull(),
             when(substring(col("CGL"), 1, 1) == lit("5"), lit("5000000000"))
             .otherwise(lit("1111111111")))
        .otherwise(col("CGL"))
    ).select(
        "BATCH_ID", "JOURNAL_ID", "POST_DATE", "BRANCH_CODE", "CURRENCY",
        F.col("CGL_validated").alias("CGL"),
        "NARRATION", "DEBIT_AMOUNT", "CREDIT_AMOUNT", "TRANSACTION_COUNT", "SOURCE_FLAG"
    )
    
    logger.info("CGL validation completed")
    return df_valid


# ========== BALANCE AGGREGATION ==========
def compute_balance_deltas(df_gl_txns, logger):
    """Compute net balance change from GL transactions."""
    logger.info("Computing balance deltas...")
    
    balances = df_gl_txns.groupBy("CGL", "CURRENCY", "BRANCH_CODE").agg(
        (F.coalesce(F.sum("DEBIT_AMOUNT"), F.lit(0)) + 
         F.coalesce(F.sum("CREDIT_AMOUNT"), F.lit(0))).alias("BALANCE")
    )
    
    logger.info(f"Computed {balances.count()} balance records")
    return balances


def merge_with_previous_balance(df_new_balance, spark, oracle_config: dict, balance_table: str,
                                filter_date: date, logger):
    """Union new balances with previous day's balances and re-aggregate."""
    logger.info(f"Merging with previous balance (date: {filter_date})...")
    
    # Read previous balance
    filter_str = filter_date.strftime("%Y-%m-%d")
    query = f"""(
        SELECT CGL, BALANCE, CURRENCY, BRANCH_CODE
        FROM {balance_table}
        WHERE TRUNC(BALANCE_DATE) = TO_DATE('{filter_str}', 'YYYY-MM-DD')
    ) T1"""
    
    try:
        df_prev = read_from_oracle(spark, oracle_config, query, logger) \
            .select("CGL", "CURRENCY", "BRANCH_CODE", "BALANCE")
        prev_count = df_prev.count()
        logger.info(f"Loaded {prev_count} previous balance records")
    except Exception as e:
        logger.warning(f"Could not load previous balance: {e}. Proceeding with new data only.")
        df_prev = None
    
    # Union and aggregate
    df_new = df_new_balance.select("CGL", "CURRENCY", "BRANCH_CODE", "BALANCE")
    
    if df_prev is not None:
        combined = df_new.unionByName(df_prev)
    else:
        combined = df_new
    
    final_balance = combined.groupBy("CGL", "CURRENCY", "BRANCH_CODE") \
        .agg(F.sum("BALANCE").alias("BALANCE")) \
        .orderBy("CGL", "BRANCH_CODE")
    
    logger.info(f"Final aggregated balance: {final_balance.count()} records")
    return final_balance


# ========== MAIN PIPELINE ==========
def run_glif_pipeline(config_path: str, processing_date: date = None, logger = None):
    """
    Main GLIF processing pipeline.
    
    Args:
        config_path: Path to JSON config file
        processing_date: Date to process (defaults to today)
        logger: Logger instance (creates if None)
    """
    if processing_date is None:
        processing_date = date.today()
    
    # Setup
    Path("./logs").mkdir(exist_ok=True)
    if logger is None:
        log_config = json.load(open(config_path)).get("logging", {})
        logger = setup_logger(
            log_config.get("log_file", "./logs/pipeline.log"),
            log_config.get("level", "INFO")
        )
    
    config = load_config(config_path)
    spark = create_spark_session(config, logger)
    
    spark_conf = config.get("spark", {})
    oracle_conf = config.get("oracle", {})
    table_conf = config.get("tables", {})
    
    posting_date_str = processing_date.strftime("%Y-%m-%d")
    yesterday = processing_date - timedelta(days=config.get("processing", {}).get("days_lookback", 1))
    
    try:
        logger.info(f"========== Starting GLIF Pipeline (date: {posting_date_str}) ==========")
        
        # 1. Fetch batch ID
        batch_id = fetch_batch_id(spark, oracle_conf, logger)
        
        # 2. Build and read HDFS path
        hdfs_path = build_hdfs_path(config, processing_date, logger)
        df_raw = read_glif_files(spark, hdfs_path, spark_conf.get("read_repartitions", 200), logger)
        
        # 3. Parse GLIF records
        df_parsed = parse_glif_records(df_raw, config, posting_date_str, logger)
        
        # 4. Aggregate by ID
        df_agg = aggregate_by_id(df_parsed, logger)
        
        # 5. Map to GL schema
        df_mapped = map_to_gl_schema(df_agg, batch_id, posting_date_str, config, logger)
        
        # 6. Validate CGL codes
        df_valid = validate_cgl_codes(df_mapped, spark, oracle_conf,
                                     table_conf.get("cgl_master", "cgl_master"), logger)
        
        # 7. Write GL_TRANSACTIONS
        df_to_write = df_valid.repartition(spark_conf.get("write_partitions", 20), "BRANCH_CODE")
        write_to_oracle(df_to_write, oracle_conf, table_conf.get("gl_transactions", "GL_TRANSACTIONS"),
                       logger, mode="append")
        
        # 8. Compute balance deltas
        balance_delta = compute_balance_deltas(df_valid, logger)
        
        # 9. Merge with previous balance and write
        final_balance = merge_with_previous_balance(balance_delta, spark, oracle_conf,
                                                   table_conf.get("gl_balance", "GL_BALANCE"),
                                                   yesterday, logger)
        
        final_balance = final_balance.withColumn("BALANCE_DATE",
                                                F.to_date(F.lit(posting_date_str), "yyyy-MM-dd"))
        write_to_oracle(final_balance, oracle_conf, table_conf.get("gl_balance", "GL_BALANCE"),
                       logger, mode="append")
        
        logger.info(f"========== GLIF Pipeline completed successfully ==========")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        raise
    finally:
        spark.stop()
        logger.info("Spark session stopped")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="GLIF Processing Pipeline")
    parser.add_argument("--config", type=str, default="./config.json", help="Path to config.json")
    parser.add_argument("--date", type=str, default=None, help="Processing date (YYYY-MM-DD)")
    
    args = parser.parse_args()
    
    processing_date = None
    if args.date:
        processing_date = date.fromisoformat(args.date)
    
    run_glif_pipeline(args.config, processing_date)
