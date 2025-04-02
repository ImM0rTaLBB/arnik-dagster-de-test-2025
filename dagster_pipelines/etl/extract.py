import pandas as pd

# 2.1.1 Read KPI evaluation data from the "Data to DB" sheet in the "KPI_FY.xlsm" Excel file
def read_excel(filepath="dagster_pipelines/data/KPI_FY.xlsm") -> pd.DataFrame:
    df = pd.read_excel(filepath, sheet_name='Data to DB')
    return df

# 2.1.2 Read center master data from the "M_Center.csv" CSV file
def read_csv(filepath="dagster_pipelines/data/M_Center.csv") -> pd.DataFrame:
    df = pd.read_csv(filepath)
    return df