from dagster import Definitions
from dagster_pipelines import assets
from dagster_pipelines import schedules

defs = Definitions(
    assets=[assets.kpi_fy, assets.m_center, assets.kpi_fy_final_asset],
    schedules=[schedules.kpi_fy_monthly_job_schedule],
)