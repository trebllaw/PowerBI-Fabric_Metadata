# fabric_utils.py

import pandas as pd
from pyspark.sql import SparkSession
import logging
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def _serialize_object(obj):
    """A simple helper to serialize complex Python objects to a string."""
    if obj is None:
        return None
    if isinstance(obj, (list, dict)):
        try:
            # Use a compact JSON representation
            return json.dumps(obj, separators=(',', ':'))
        except TypeError:
            return str(obj) # Fallback for non-serializable content
    return str(obj)

def cast_dataframe_to_fabric_compatible_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Efficiently casts Pandas DataFrame columns to types compatible with Fabric Warehouse.
    """
    if df.empty:
        return df

    logging.info("Casting DataFrame to Fabric compatible types...")
    df_copy = df.copy()

    # --- Process Datetime Columns ---
    dt_cols = df_copy.select_dtypes(include=['datetime64[ns]', 'datetimetz']).columns
    if not dt_cols.empty:
        logging.info(f"Processing datetime columns: {list(dt_cols)}")
        for col in dt_cols:
            # Ensure datetime is timezone-naive, which is safer for many data warehouses
            if df_copy[col].dt.tz is not None:
                df_copy[col] = df_copy[col].dt.tz_localize(None)
            df_copy[col] = pd.to_datetime(df_copy[col], errors='coerce')

    # --- Process Boolean Columns ---
    # Spark handles nullable booleans well. Convert to pandas' dedicated nullable type.
    bool_cols = df_copy.select_dtypes(include=['bool']).columns
    if not bool_cols.empty:
        logging.info(f"Processing boolean columns: {list(bool_cols)}")
        for col in bool_cols:
            df_copy[col] = df_copy[col].astype('boolean') # Use pandas nullable boolean

    # --- Process Object Columns (most complex) ---
    # These may contain lists, dicts, or mixed types. We serialize them to JSON strings.
    obj_cols = df_copy.select_dtypes(include=['object']).columns
    if not obj_cols.empty:
        logging.info(f"Serializing object columns: {list(obj_cols)}")
        for col in obj_cols:
            # .apply is necessary here due to the heterogeneous nature of object columns
            df_copy[col] = df_copy[col].apply(_serialize_object)

    # All columns are now either a primitive type (int, float, bool, datetime) or a string.
    # Spark's `createDataFrame` can handle this schema efficiently.
    return df_copy

def save_to_fabric_warehouse(
    pandas_dfs: dict[str, pd.DataFrame],
    warehouse_schema: str,
    spark: SparkSession
):
    """Saves a dictionary of Pandas DataFrames to specified tables in a Fabric Warehouse."""
    for table_name, df_pandas in pandas_dfs.items():
        logging.info(f"Processing table '{table_name}'...")
        
        # if df_pandas.empty:
            # logging.warning(f"DataFrame for table '{table_name}' is empty. Skipping.")
            # continue

        try:
            # 1. Cast Pandas DataFrame to compatible types
            df_casted = cast_dataframe_to_fabric_compatible_types(df_pandas)
            if df_casted.shape[0] == 0:
                logging.warning(f"DataFrame for table '{table_name}' has 0 rows after processing. Skipping save operation.")
                continue
            
            # 2. Convert to Spark DataFrame (Spark will infer the schema)
            # Enable Arrow for faster conversion
            spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
            spark_df = spark.createDataFrame(df_casted)
            logging.info(f"'{table_name}' converted to Spark DataFrame with {spark_df.count()} rows.")

            # 3. Save to Fabric Warehouse
            full_table_name = f"{warehouse_schema}.{table_name}"
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {warehouse_schema}")
            
            spark_df.write.format("delta").mode("overwrite").saveAsTable(full_table_name)
            logging.info(f"✅ Successfully saved data to Fabric table: {full_table_name}")

        except Exception as e:
            logging.error(f"❌ Error processing table '{table_name}': {e}", exc_info=True)
            raise