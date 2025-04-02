import pandas as pd

def pivot_data(filepath: str = "dagster_pipelines/data/KPI_FY.xlsm", sheet_name: str = "Data to DB") -> pd.DataFrame:
    df = pd.read_excel(filepath, sheet_name=sheet_name)

    value_cols = [col for col in df.columns if "Plan_" in col or "Actual_" in col]
    id_cols = [col for col in df.columns if col not in value_cols]

    df_melted = pd.melt(df, id_vars=id_cols, value_vars=value_cols, var_name="Amount Name", value_name="Amount")
    df_melted["Amount Type"] = df_melted["Amount Name"].apply(lambda x: "Plan" if "Plan" in x else "Actual")
    df_melted["Amount Name"] = df_melted["Amount Name"].apply(lambda x: x.split("_")[-1])

    df_melted = df_melted[id_cols + ["Amount Type", "Amount Name", "Amount"]]

    return df_melted