from dagster import Definitions

from .assets import *
from .schedules import *

# all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=mysql_to_clickhouse_assets,
    schedules=[mysql_to_clickhouse_schedule],
    jobs=[mysql_to_clickhouse_job]
)
