# import datetime 
# from pyspark.sql import SparkSession , functions as F
# from pyspark.sql.functions import expr, col,when, length, substring,trim, concat, lit, create_map, to_timestamp, sum
# from pyspark.sql.types import DecimalType, StringType, StructType, StructField, LongType 
# from datetime import date, timedelta

# # Get today's date
# today = date.today()

# # Calculate yesterday's date
# # timedelta(days=1) represents a difference of one day
# yesterday = today - timedelta(days=1)

# spark = SparkSession.builder \
#     .appName("HDFS_Text_to_Postgres") \
#     .config("spark.jars", "./postgresql-42.5.4.jar") \  # download Postgres JDBC jar and place here
#     .config("spark.hadoop.fs.defaultFS", "hdfs://10.177.103.199:8022") \
#     .getOrCreate()

# # oracle_url = "jdbc:oracle:thin:@//10.191.216.58:1523/crsprod"
# # oracle_user = "ftwoahm"
# # oracle_password = "Password@123"
# # oracle_driver = "oracle.jdbc.driver.OracleDriver"

# pg_url = "jdbc:postgresql://10.177.103.192:5432/fincorepdb1"   # adjust host:port:dbname
# pg_user = "fincore"
# pg_password = "Password#1234"
# pg_driver = "org.postgresql.Driver"

# DriverManager = spark._jvm.java.sql.DriverManager


# batch_id_schema = StructType([
#     StructField("BATCH_ID_VAL", LongType(), False) 
# ])

# # Fetch batch id — replace 'batch_seq' with your actual sequence or change the query to a function call
# BATCH_ID_LITERAL = None
# try:
#     df_batch_id = spark.read.format("jdbc") \
#         .option("url", pg_url) \
#         .option("query", "SELECT nextval('batch_seq') AS BATCH_ID_VAL") \  # <-- adjust sequence/function as needed
#         .option("user", pg_user) \
#         .option("password", pg_password) \
#         .option("driver", pg_driver) \
#         .option("fetchsize",1).load()

#     batch_id_value = df_batch_id.collect()[0]["BATCH_ID_VAL"]
#     BATCH_ID_LITERAL = int(batch_id_value)
#     print(f"=== Fetched BATCH_ID: {BATCH_ID_LITERAL} ===")
# except Exception as e:
#     print(f"Error fetching BATCH_SEQ via JDBC: {e}")
#     spark.stop()
#     exit()

# posting_date_str = datetime.date.today().strftime("%Y-%m-%d")
# print(f"=== Using current date for POST_DATE: {posting_date_str} ===")


# base_path = "hdfs://10.177.103.199:8022/CBS-FILES/2025-11-20/GLIF/"

# # file_types_with_extra = ["INV", "PRI", "BOR", "PRB", "ONL", "AGRI_PY", "AGRI_CY", "INV_IN","INV_TD","INV_DC","BOR_DC"]
# # file_types_without_extra = ["INV_BC", "INV_FD", "INV_RE", "GEN_FIT", "INV_B8", "INV_CT", "INV_ECG", "INV_FCI","BOR_VS","INV_GB"]
# # paths_to_read = [f"{base_path}*GLIF{ft}_[0-9][0-9][0-9].gz" for ft in file_types_with_extra]
# # paths_to_read.extend([f"{base_path}*GLIF{ft}.gz" for ft in file_types_without_extra])

# # df_raw = spark.read.text(paths_to_read)


# df_raw = spark.read.text("hdfs://10.177.103.199:8022/CBS-FILES/2025-11-20/GLIF/VARAGA_GLIFONL_*")

# digit_map = create_map(
#     lit('1'), lit('1'), lit('2'), lit('2'), lit('3'), lit('3'), lit('4'), lit('4'), lit('5'), lit('5'),
#     lit('6'), lit('6'), lit('7'), lit('7'), lit('8'), lit('8'), lit('9'), lit('9'), lit('0'), lit('0'),
#     lit('p'), lit('0'), lit('q'), lit('1'), lit('r'), lit('2'), lit('s'), lit('3'), lit('t'), lit('4'),
#     lit('u'), lit('5'), lit('v'), lit('6'), lit('w'), lit('7'), lit('x'), lit('8'), lit('y'), lit('9')
# )
# df_processed = df_raw.withColumns({
#     "Id": expr("substring(value, 51, 18)"),
#     "currency_code": expr("substring(value, 48, 3)")
# })
# df_clean = df_processed.withColumns({
#     "Amount_raw": when(col("currency_code") != "INR", expr("substring(value, 133, 17)"))
#                  .otherwise(expr("substring(value, 116, 17)")),
#     "last_char": when(col("currency_code") != "INR", expr("substring(value, 149, 1)"))
#                  .otherwise(expr("substring(value, 132, 1)"))
# }).drop("value", "currency_code")
# df_clean = df_clean.withColumns({
#     "Amount_base": substring(col("Amount_raw"), 1, 16),
#     "sign_char": substring(col("Amount_raw"), 17, 1)
# })
# df_with_signed_amount = df_clean.withColumns({
#     "last_digit": digit_map.getItem(col("sign_char")),
#     "sign": when(col("sign_char").isin(['p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y']), -1).otherwise(1)
# })
# df_with_signed_amount = df_with_signed_amount.withColumn(
#     "Amount_decimal_str",
#     concat(col("Amount_base"), col("last_digit"))
# )
# df_final = df_with_signed_amount.withColumns({
#     "Amount_final": (col("Amount_decimal_str").cast(DecimalType(22, 3)) / 1000) * col("sign")
# })
# df_with_signed_amount = df_final.withColumns({
#     "Amountpve": when(df_final["Amount_final"] > 0, df_final["Amount_final"]).otherwise(0),
#     "Amountnve": when(df_final["Amount_final"] < 0, df_final["Amount_final"]).otherwise(0)
# })
# df_agg = df_with_signed_amount.groupBy("Id").agg(
#     F.sum(F.col("Amountpve")).alias("DEBIT_AMOUNT"),
#     F.sum(F.col("Amountnve")).alias("CREDIT_AMOUNT"),
#     F.count(F.col("Id")).alias("TRANSACTION_COUNT")
# )


# df_mapped = df_agg.withColumns({ 
#     "BATCH_ID": lit(BATCH_ID_LITERAL).cast(StringType()), 
#     "JOURNAL_ID": lit(None).cast(StringType()),
#     "POST_DATE": to_timestamp(lit(posting_date_str), "yyyy-MM-dd").cast("timestamp"),
#     "BRANCH_CODE": substring(col("Id"), 1, 5),
#     "CURRENCY": substring(col("Id"), 6, 3),
#     "CGL": substring(col("Id"), 9, 10),
#     "NARRATION": lit("CBS consolidated txns").cast(StringType()),
#     "SOURCE_FLAG": lit("C").cast(StringType())
# })

# df_final_schema = df_mapped.select(
#     "BATCH_ID",
#     "JOURNAL_ID",
#     "POST_DATE",
#     "BRANCH_CODE",
#     "CURRENCY",
#     "CGL",
#     "NARRATION",
#     "DEBIT_AMOUNT",
#     "CREDIT_AMOUNT",
#     "TRANSACTION_COUNT",
#     "SOURCE_FLAG"
# )


# cgl_query = f"""
# (
#    SELECT CGL_NUMBER FROM cgl_master
# ) T1
# """
# master_cgl_list = spark.read.format("jdbc") \
#     .option("url", pg_url) \
#     .option("dbtable", cgl_query) \
#     .option("user", pg_user) \
#     .option("password", pg_password) \
#     .option("driver", pg_driver) \
#     .option("batchsize", 110000) \
#     .option("numPartitions", 50) \
#     .load()
  

# cgl_codes = [row.CGL_NUMBER for row in master_cgl_list.select("CGL_NUMBER").collect()]

# print(cgl_codes)
# print("=== Final DataFrame Schema and Sample Data for Oracle ===")

# df_result = df_final_schema.withColumn(
#         "CGL",
#         when(col("CGL").isin(cgl_codes), col("CGL")) 
#         .otherwise(
#             when(substring(col("CGL"), 1, 1) == lit("5"), lit("5000000000")) 
#             .otherwise(lit("1111111111")) 
#         )
#     )

# processed_df = df_result.withColumn(
#     "BALANCE", 
#     col("DEBIT_AMOUNT") + col("CREDIT_AMOUNT")
# ).select("CGL", "BALANCE","CURRENCY","BRANCH_CODE")




# print("==============result Dataframe======================")
# df_result.show()
# print("==============processed Dataframe======================")
# processed_df.show(truncate=False)

# try:
#     df_result.write \
#             .format("jdbc") \
#             .option("url", pg_url) \
#             .option("dbtable", "GL_TRANSACTIONS") \
#             .option("user", pg_user) \
#             .option("password", pg_password) \
#             .option("driver", pg_driver) \
#             .option("batchsize", 110000) \
#             .option("numPartitions", 50) \
#             .mode("append") \
#             .save()

#     print("=== Data successfully written to Postgres DB ===") 
# except Exception as e:
#     print(f"Error writing to Postgres DB: {e}")

    

# # ====================================================================================================================================================================================================================================================================

# # ====================================================================================================================================================================================================================================================================


# # filter_date_str = '2025-11-06'
# filter_date_str = yesterday

# sql_query = f"""
# (
#     SELECT CGL,BALANCE,CURRENCY,BRANCH_CODE 
#     FROM GL_BALANCE 
#     WHERE DATE(BALANCE_DATE) = DATE '{filter_date_str}'
# ) T1
# """




# df_date_filtered = spark.read.format("jdbc") \
#     .option("url", pg_url) \
#     .option("dbtable", sql_query) \
#     .option("user", pg_user) \
#     .option("password", pg_password) \
#     .option("driver", pg_driver) \
#     .option("batchsize", 110000) \
#     .option("numPartitions", 50) \
#     .load()

# # print("==============Fetched Dataframe======================")
# # # df_date_filtered.show(100,False)

# combined_df = df_result.unionAll(df_date_filtered)

# final_aggregated_df = combined_df.groupBy("CGL", "CURRENCY", "BRANCH_CODE").agg(sum("BALANCE").alias("BALANCE")).orderBy("CGL", "BRANCH_CODE")

# print("==========================Final Dataframe=================")
# # final_aggregated_df.show(50,False)

# final_aggregated_df.write \
#     .format("jdbc") \
#     .option("url", pg_url) \
#     .option("dbtable", "GL_BALANCE") \
#     .option("user", pg_user) \
#     .option("password", pg_password) \
#     .option("driver", pg_driver) \
#     .option("batchsize", 110000) \
#     .option("numPartitions", 50) \
#     .mode("append") \
#     .save()

# spark.stop()




# import concurrent.futures

# from pyspark.sql.functions import expr, col,when, length, substring,trim, concat, lit, create_map, to_timestamp, sum
# from pyspark.sql.types import DecimalType, StringType, StructType, StructField, LongType, FloatType
# from datetime import date, timedelta
# from pyspark.sql import SparkSession , functions as F
# from pyspark.storagelevel import StorageLevel



# today = date.today()
# yesterday = today - timedelta(days=9)


# def create_spark_session(app_name, jars_path, hdfs_uri):
#     """
#     Creates and returns a SparkSession with the specified configurations.
#     """
#     return (
#         SparkSession.builder.appName(app_name)
#         .config("spark.jars", jars_path)
#         .config("spark.hadoop.fs.defaultFS", hdfs_uri)
#         .getOrCreate()
#     )

# def invtogl(spark):
#     """
#     Reads inv data from HDFS, processes it, and returns a standardized DataFrame.
#     """
#     print("Starting processing of invtogl files.")
#     try:
#         df_raw = spark.read.text(
#             "hdfs://10.177.103.199:8022/CBS-FILES/2025-11-20/BANCS24/INV*"
#         )
#         id_col = expr("substring(value, 200, 18)")
#         amount_str = expr("substring(value, 30, 17)")
#         value_type = expr("substring(value, 47, 1)")
#         df_final = df_raw.select(
#             id_col.alias("Id"),
#             when(value_type == "+", trim(amount_str).cast(DecimalType(22,4)) / 1000)
#             .when(value_type == "-", -1 * trim(amount_str).cast(DecimalType(22,4)) / 1000)
#             .otherwise(0)
#             .alias("Amount"),
#         ).select("Id", "Amount")
#         print("Successfully processed invtogl data.")
#         return df_final
#     except Exception as e:
#         print(f"Error processing invtogl files: {e}")
#         raise

# def bortogl(spark):
#     """
#     Reads bor data from HDFS, processes it, and returns a standardized DataFrame.
#     """
#     print("Starting processing of bortogl files.")
#     try:
#         df_raw = spark.read.text(
#             "hdfs://10.177.103.199:8022/CBS-FILES/2025-11-20/BANCS24/BOR*"
#         )
        
#         id_col = expr("substring(value, 105, 18)")
#         amount_str = expr("substring(value, 27, 17)")
#         value_type = expr("substring(value, 44, 1)")

#         df_final = df_raw.select(
#             id_col.alias("Id"),
#             when(value_type == "+", trim(amount_str).cast(DecimalType(22,4)) / 1000)
#             .when(value_type == "-", -1 * trim(amount_str).cast(DecimalType(22,4)) / 1000)
#             .otherwise(0)
#             .alias("Amount"),
#         ).select("Id", "Amount")
        
#         print("Successfully processed bortogl data.")
#         return df_final
#     except Exception as e:
#         print(f"Error processing bortogl files: {e}")
#         raise

# def glccgen(spark):
#     """
#     Reads glccgen data from HDFS, processes it, and returns a standardized DataFrame.
#     """
#     print("Starting processing of glccgen files.")
#     try:
#         df_raw = spark.read.text(
#             "hdfs://10.177.103.199:8022/CBS-FILES/2025-11-20/BANCS24/GLCC*"
#         )
#         id_col = expr("substring(value, 1, 18)")
#         amount_str = expr("substring(value, 24, length(value)-24)")
#         value_type = expr("substring(value, length(value), 1)")
#         df_final = df_raw.select(
#             id_col.alias("Id"),
#             when(value_type == "+", trim(amount_str).cast(DecimalType(22,4)) / 10000)
#             .when(value_type == "-", -1 * trim(amount_str).cast(DecimalType(22,4)) / 10000)
#             .otherwise(0)
#             .alias("Amount"),
#         ).select("Id", "Amount")
#         print("Successfully processed glccgen data.")
#         return df_final
#     except Exception as e:
#         print(f"Error processing glccgen files: {e}")
#         raise

# if __name__ == "__main__":
#     spark = create_spark_session(
#         "HDFS_Text_to_Postgres",
#         "./postgresql-42.5.4.jar",  # updated jar
#         "hdfs://10.177.103.199:8022"
#     )
    

#     try:
#         with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
#             df_invtogl = executor.submit(invtogl, spark)
#             df_bortogl = executor.submit(bortogl, spark)
#             df_glccgen = executor.submit(glccgen, spark)
#             concurrent.futures.wait([df_invtogl, df_bortogl,df_glccgen])
#             if df_invtogl.exception():
#                 raise df_invtogl.exception()
#             if df_bortogl.exception():
#                 raise df_bortogl.exception()
#             if df_glccgen.exception():
#                 raise df_glccgen.exception()    
              
#             df_invtogl_result = df_invtogl.result()
#             df_bortogl_result = df_bortogl.result()
#             df_glccgen_result = df_glccgen.result()

#             final_df = df_invtogl_result.unionByName(df_bortogl_result).unionByName(df_glccgen_result)
#             final_result_df = final_df.groupBy("Id").agg(F.sum("Amount").alias("Amount")).select("Id", "Amount")

#             print("===================== final df ============= ")
#             # final_result_df.show(20, False)

            
#             df_mapped = final_result_df.withColumn("BRANCH_CODE", substring(col("Id"), 1, 5)) \
#                  .withColumn("CURRENCY", substring(col("Id"), 6, 3)) \
#                  .withColumn("CGL", substring(col("Id"), 9, 10))
            

#             df_final_schema = df_mapped.select("BRANCH_CODE", "CURRENCY", "CGL", "Amount")

#             print("=== Final DataFrame Schema and Sample Data for Oracle ===")
#             # df_final_schema.show(20, False)

            
#             processed_df = df_final_schema.withColumnRenamed("Amount", "BALANCE").select("CGL", "BALANCE", "CURRENCY", "BRANCH_CODE")

#             print("========================processed data============")
#             # processed_df.show(20, False)


# # =======================================================================================================

#         print("Aggregated data:")
#         # JDBC details for later writes (reuse pg variables)
#         pg_url = "jdbc:postgresql://10.177.103.192:5432/fincorepdb1"
#         pg_user = "fincore"
#         pg_password = "Password#1234"

#         print("Data successfully written to Postgres DB.")

#     except Exception as e:
#        print(f"An error occurred during the overall process: {e}")
    


#     try:
#         # filter_date_str = '2025-11-06'
#         # filter_date_str = today
#         filter_date_str = yesterday
#         pg_url = "jdbc:postgresql://10.177.103.192:5432/fincorepdb1"
#         pg_user = "fincore"
#         pg_password = "Password#1234"
#         pg_driver = "org.postgresql.Driver"

#         # oracle_url = "jdbc:oracle:thin:@//10.191.216.58:1522/crsprod"
#         # oracle_user = "ftwoahm"
#         # oracle_password = "Password@123"
#         # oracle_driver = "oracle.jdbc.driver.OracleDriver"

#         sql_query = f"""(
#              SELECT CGL,BALANCE,CURRENCY,BRANCH_CODE 
#              FROM CBS_BALANCE 
#              WHERE DATE(BALANCE_DATE) = DATE '{filter_date_str}'
#          ) T1
#          """
#          #  
    
#         df_date_filtered = spark.read.format("jdbc") \
#             .option("url", pg_url) \
#             .option("dbtable", sql_query) \
#             .option("user", pg_user) \
#             .option("password", pg_password) \
#             .option("driver", pg_driver) \
#             .option("batchsize", 110000) \
#             .option("numPartitions", 50) \
#             .load()
#         print("==============Fetched Dataframe======================")
#         # df_date_filtered.show(100,False)
#         combined_df = processed_df.unionAll(df_date_filtered)
        
#         final_aggregated_df = combined_df.groupBy("CGL", "CURRENCY", "BRANCH_CODE").agg(
#              sum("BALANCE").alias("BALANCE") # Use `sum` as imported
#          ).orderBy("CGL", "BRANCH_CODE")
#         print("==========================Final Dataframe=================")
#         # final_aggregated_df.show(50,False)
#         final_aggregated_df.write \
#             .format("jdbc") \
#             .option("url", pg_url) \
#             .option("dbtable", "CBS_BALANCE") \
#             .option("user", pg_user) \
#             .option("password", pg_password) \
#             .option("driver", pg_driver) \
#             .option("batchsize", 110000) \
#             .option("numPartitions", 50) \
#             .mode("append") \
#             .save()
#     except Exception as e:
#         print(f"An error occurred during the overall process: {e}")
#     finally:
#         spark.stop()





# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, lit, current_timestamp, coalesce, when, substring, length
# from datetime import date, timedelta
# from pyspark.sql.types import DecimalType, StructType, StructField, StringType, TimestampType

# def reconcile_balances():
#     today = date.today()
#     yesterday = today - timedelta(days=8)
#     print()
#     print()
#     print()
#     print()
#     print(yesterday)
#     print()
#     print()
#     print()
#     print()
#     print()
    
#     spark = SparkSession.builder \
#     .appName("Balance Sheet Comparison") \
#     .config("spark.driver.extraClassPath", "/app/ojdbc8.jar") \
#     .getOrCreate()

#     JDBC_URL = "jdbc:postgresql://10.177.103.192:5432/fincorepdb1"
#     DB_USER = "fincore"
#     DB_PASSWORD = "Password#1234"
#     JDBC_DRIVER = "org.postgresql.Driver"

#     connection_properties = {
#         "user": DB_USER,
#         "password": DB_PASSWORD,
#         "driver": JDBC_DRIVER
#     }

#     # Define the single SQL query that joins the tables and filters mismatches
#     sql_query = f"""
#     (SELECT 
#         a.BRANCH_CODE, 
#         a.CURRENCY, 
#         a.CGL, 
#         a.BALANCE AS CBS_BALANCE, 
#         b.BALANCE AS GL_BALANCE,
#         CAST(
#             COALESCE(a.BALANCE, 0) - COALESCE(b.BALANCE, 0) 
#             AS NUMERIC(20, 4)
#         ) AS DIFFERENCE_AMOUNT
#     FROM 
#         CBS_BALANCE a
#     INNER JOIN 
#         GL_BALANCE b
#     ON 
#         a.BRANCH_CODE = b.BRANCH_CODE AND
#         a.CURRENCY = b.CURRENCY AND
#         a.CGL = b.CGL
#     WHERE 
#         (a.BALANCE != b.BALANCE OR a.BALANCE IS NULL OR b.BALANCE IS NULL)
#         AND DATE(a.BALANCE_DATE) = DATE '{yesterday}'
#         AND DATE(b.BALANCE_DATE) = DATE '{yesterday}'
#     ) T1"""


#     # Read the result of the SQL query directly into a single DataFrame
#     mismatches_df_single = spark.read.format("jdbc") \
#     .option("url", JDBC_URL) \
#     .option("dbtable", sql_query) \
#     .options(**connection_properties) \
#     .load()
    
#     mismatches_df_single.show()

#     output_df_base = mismatches_df_single.select(
#         current_timestamp().alias("RECON_RUN_DATE"),
#         "BRANCH_CODE",
#         "CURRENCY",
#         "CGL",
#         "CBS_BALANCE",
#         "GL_BALANCE",
#         "DIFFERENCE_AMOUNT"
#     )
    
#     head_rules = [
#         (col("BRANCH_CODE") == "01999", lit("CAO")),
#         (substring(col("CGL"), 9, 1) == "9", lit("PROVISION")),
#         (substring(col("CGL"), 1, 4) == "1204", lit("CASH")),
#         (substring(col("CGL"), 1, 2) == "62", lit("AUCA")),
#         (substring(col("CGL"), 1, 4) == "6363", lit("CTA 6363")),
#         (substring(col("CGL"), 1, 1) == "4", when(substring(col("CGL"), 1, 4) == "4363", lit("CTA 4363")).otherwise(lit("CTA"))),
#         (substring(col("CGL"), 1, 1) == "6", lit("CTA")),
#         (substring(col("CGL"), 1, 1) == "7", lit("I/E")),
#         (substring(col("CGL"), 1, 1) == "8", lit("I/E")),
#         (substring(col("CGL"), 1, 1) == "5", when(col("CURRENCY") == "INR", lit("MEMO")).otherwise(lit("FC MEMO"))),
#         (substring(col("CGL"), 1, 4) == "2148", when(col("CURRENCY") == "INR", lit("BGL")).otherwise(lit("FC"))),
#         (substring(col("CGL"), 1, 4) == "2249", lit("INCA")),
#         (col("CGL").isin("2111", "2115", "2060"), lit("PPF")), # Combined PPF checks
#         (col("CGL") == "1106505002", when(col("BRANCH_CODE").isin("03973", "03974", "03975", "03976", "04030", "04425", "04454", "06939"), lit("LHO")).otherwise(lit("BCGA"))),
#         (col("CGL") == "1000505003", lit("MIGRATION")),
#         (col("CGL") == "1100505001", lit("LHO")),
#         (substring(col("CGL"), 1, 1) == "1", when(col("CURRENCY") == "INR", lit("LOAN")).otherwise(lit("FC")))
#     ]
    
#     head_column = None
#     for condition, result in head_rules:
#         if head_column is None:
#             head_column = when(condition, result)
#         else:
#             head_column = head_column.when(condition, result)
            
    
#     # head_column = head_column.otherwise(when(col("CURRENCY") == "INR", lit("DEPOSITS")).otherwise(lit("FC")))

    
#     output_df = output_df_base.withColumn("HEAD", head_column)
    
#     yesterday_filter_date_str = today.strftime('%Y-%m-%d')
        
#     difference_df = spark.read.format("jdbc") \
#         .option("url", JDBC_URL) \
#         .option("dbtable", "DIFFERENCE") \
#         .options(**connection_properties) \
#         .load()

    
#     yesterdays_df = difference_df.filter(
#         col("RECON_RUN_DATE").cast("date") == lit(yesterday_filter_date_str).cast("date")
#     ).select(
#         col("BRANCH_CODE").alias("y_BRANCH_CODE"), 
#         col("CURRENCY").alias("y_CURRENCY"), 
#         col("CGL").alias("y_CGL"), 
#         col("DIFFERENCE_AMOUNT").alias("YESTERDAY_DIFF").cast(DecimalType(20, 4))
#     )
    
#     join_con = [
#         col("BRANCH_CODE") == col("y_BRANCH_CODE"),
#         col("CURRENCY") == col("y_CURRENCY"),
#         col("CGL") == col("y_CGL")
#     ]
    
    
#     final_result_df = output_df.join(yesterdays_df, join_con, "left_outer")

#     today_diff_coalesced = coalesce(col("DIFFERENCE_AMOUNT"), lit(0))
#     yesterday_diff_coalesced = coalesce(col("YESTERDAY_DIFF"), lit(0))
    
#     diff_bw_yesterday = (today_diff_coalesced - yesterday_diff_coalesced).cast(DecimalType(20, 4))
    
    
#     final_result_df = final_result_df.withColumn("TODAY_DIFF", col("DIFFERENCE_AMOUNT")) \
#                                      .withColumn("DIFF_BW_YESTERDAY", diff_bw_yesterday) \
#         .withColumn(
#         "Type",
#         when(col("YESTERDAY_DIFF").isNull(), "New Entry")
#         .when(col("DIFF_BW_YESTERDAY") == lit(0), "Zero Difference") # Simplified logic for zero diff
#         .otherwise("Diff change") 
#     ).drop("y_BRANCH_CODE", "y_CURRENCY", "y_CGL") # Drop the duplicated join keys

    
#     final_result_df = final_result_df.select(
#         "RECON_RUN_DATE","BRANCH_CODE","CURRENCY","CGL","CBS_BALANCE","GL_BALANCE",
#         "DIFFERENCE_AMOUNT","DIFF_BW_YESTERDAY","TYPE","HEAD"
#     )
    
#     final_result_df.show(truncate=False)
    
    
    
#     final_result_df.write \
#             .format("jdbc") \
#             .option("dbtable", "DIFFERENCE") \
#             .option("url", JDBC_URL) \
#             .options(**connection_properties) \
#             .option("batchsize", 110000) \
#             .option("numPartitions", 50) \
#             .mode("append") \
#             .save()
    
    
#     spark.stop()

# if __name__ == "__main__":
#     reconcile_balances()
 








# from pyspark.sql import SparkSession, functions as F
# from pyspark.sql.functions import expr, col, when, substring, concat, lit, create_map
# from pyspark.sql.types import DecimalType

# import logging


# def create_spark_session(name):
#     """Create a new Spark session for the single job."""
#     print(f"Creating SparkSession for {name}")
    
#     spark_config = { 
#         # "spark.sql.adaptive.enabled": "true",
#         # "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
#         # "spark.sql.shuffle.partitions": "200", # Reduced from 500 for better write behavior
#         # "spark.executor.memory": "4g",         # INCREASED MEMORY
#         # "spark.memory.fraction": "0.8",        # More aggressive memory management
        
#         "spark.sql.adaptive.enabled": "true",
#         "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
#         "spark.sql.shuffle.partitions": "200",         # Reduced from 500
#         "spark.executor.memory": "4g",                
#         # Optional: Enable dynamic allocation
#         "spark.dynamicAllocation.enabled": "true",
#         "spark.dynamicAllocation.minExecutors": "2",
#         "spark.dynamicAllocation.maxExecutors": "10",
#         "spark.sql.files.maxRecordsPerFile": "500000" # Force smaller output files if needed
#     }

#     spark = (
#         SparkSession.builder.appName(name)
#         .config("spark.hadoop.fs.defaultFS", "hdfs://10.177.103.199:8022")
#         .config(map=spark_config)
#         .config("spark.jars", "./ojdbc8.jar")
#         .getOrCreate()
#     )
#     return spark




# def process_all_files(job_name, file_types_extra, file_types_simple):
#     """
#     Single job that reads all files using a unified path list, 
#     processes them, and writes to a single output directory.
#     """
#     spark = create_spark_session(job_name)
#     base_path = "hdfs://10.177.103.199:8022/CBS-FILES/2025-11-20/GLIF/"
    
#     output_path = "hdfs://10.177.103.199:8022/CBS-TEST/ALL_GLIF_OUTPUT.parquet" 

#     try:
#         print(f"======================== {job_name} started ========================")

        
#         paths_extra = [f"{base_path}*GLIF{ft}_[0-9][0-9][0-9].gz" for ft in file_types_extra]
        
#         paths_simple = [f"{base_path}*GLIF{ft}.gz" for ft in file_types_simple]
        
        
#         all_paths = paths_extra + paths_simple
        
#         print(f"Reading files from {len(all_paths)} distinct path patterns.")

        
#         df_raw = spark.read.text(all_paths)
#         print(f"{job_name} — Files fetched successfully.")
        
        
#         parquetData = df_raw.select(
            
#             expr("substring(value, 4, 5)").alias("txdate"),
#             expr("substring(value, 28, 9)").alias("journal"), 
#             expr("substring(value, 28, 11)").alias("jrnl"),
#             expr("substring(value, 42, 5)").alias("branch"),
#             expr("substring(value, 48, 3)").alias("currency"),
#             expr("substring(value, 51, 18)").alias("GLCC"),
#             expr("substring(value, 76, 6)").alias("txnCode"),
#             expr("substring(value, 82, 24)").alias("description"),
#             expr("substring(value, 111, 5)").alias("txdatetwo"),
#             expr("substring(value, 116, 17)").alias("lcyAmt"),
#             expr("substring(value, 133, 17)").alias("fcyAmt"),
#             expr("substring(value, 160, 16)").alias("acct"),
#             expr("substring(value, 176, 3)").alias("sourceApp")
#         )

#         # parquetData.show(25, False)

        
#         parquetData.write.mode("overwrite").parquet(output_path)
        
#         print(f"{job_name} - Data written Successfully at {output_path}")

        
        

#     except Exception as e:
#         logging.exception(f" Error in {job_name}: {e}")
#         print(f"An error occurred while processing {job_name}: {e}")
#         raise
#     finally: 
#         spark.stop()
#         print(f"======================== {job_name} completed ========================")


# if __name__ == "__main__":
#     try:
        
#         file_types_with_extra = ["INV", "PRI", "BOR", "PRB", "ONL", "AGRI_PY", "AGRI_CY", "INV_IN", "INV_TD", "INV_DC", "BOR_DC"]

        
#         file_types_without_extra = ["INV_BC", "INV_FD", "INV_RE", "GEN_FIT", "INV_B8", "INV_CT", "INV_ECG", "INV_FCI", "BOR_VS", "INV_GB"]

#         print("Starting single consolidated Spark job for all file groups")

        
#         process_all_files("JOB_ALL_GLIF_FILES", file_types_with_extra, file_types_without_extra)

#         print("All jobs completed successfully!")

#     except Exception as e:
#         logging.exception("Fatal error during execution")
#         print(f"An error occurred during execution: {e}")










