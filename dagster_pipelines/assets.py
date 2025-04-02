import dagster as dg
import pandas as pd
from datetime import datetime
from dagster_pipelines.etl.extract import read_excel, read_csv
from dagster_pipelines.etl.transform import pivot_data
from dagster_pipelines.etl.load import load_to_duckdb

# 2.3.1.1 Load pivoted KPI_FY.xlsm into KPI_FY
@dg.asset(compute_kind="duckdb", group_name="plan")
def kpi_fy(context: dg.AssetExecutionContext):
    """Loads and transforms KPI_FY data into DuckDB."""
    filepath = "dagster_pipelines/data/KPI_FY.xlsm"
    sheet_name = "Data to DB"

    df = pivot_data(filepath, sheet_name)
    load_to_duckdb(df, "KPI_FY")
    
    context.log.info("KPI_FY data loaded successfully.")
    return df

# 2.3.1.2 Load M_Center.csv into M_Center
@dg.asset(compute_kind="duckdb", group_name="plan")
def m_center(context: dg.AssetExecutionContext):
    """Loads M_Center data into DuckDB."""
    filepath = "dagster_pipelines/data/M_Center.csv"
    
    df = read_csv(filepath)
    load_to_duckdb(df, "M_Center")
    
    context.log.info("M_Center data loaded successfully.")
    return df

# 2.3.2 Create asset kpi_fy_final_asset() to join KPI_FY and M_Center
@dg.asset(compute_kind="duckdb", group_name="plan", deps=["kpi_fy", "m_center"])
def kpi_fy_final_asset(context: dg.AssetExecutionContext):
    """Joins KPI_FY and M_Center data and loads into KPI_FY_Final."""
    query = """
    INSERT INTO KPI_FY_Final
    SELECT k.*, m.Center_Name, NOW() AS updated_at
    FROM KPI_FY k
    LEFT JOIN M_Center m
    ON k.Center_ID = m.Center_ID
    """
