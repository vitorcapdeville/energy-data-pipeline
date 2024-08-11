import warnings
from datetime import date

import sqlalchemy as sa
from extralo import ETL, SQLAppendDestination

from src.source import EIASource
from src.transformer import SeriesAggregator

warnings.simplefilter(action="ignore", category=FutureWarning)


engine = sa.create_engine("sqlite:///data.db")

ano, mes = 2024, 5

etl = ETL(
    sources={
        "ciso_pgae": EIASource(
            route="electricity/rto/region-sub-ba-data/data/",
            series=["CISO", "PGAE"],
            facet=["parent", "subba"],
            frequency="hourly",
            ano=ano,
            mes=mes,
        ),
        "ciso_sce": EIASource(
            route="electricity/rto/region-sub-ba-data/data/",
            series=["CISO", "SCE"],
            facet=["parent", "subba"],
            frequency="hourly",
            ano=ano,
            mes=mes,
        ),
        "ciso_sdge": EIASource(
            route="electricity/rto/region-sub-ba-data/data/",
            series=["CISO", "SDGE"],
            facet=["parent", "subba"],
            frequency="hourly",
            ano=ano,
            mes=mes,
        ),
        "ciso_vea": EIASource(
            route="electricity/rto/region-sub-ba-data/data/",
            series=["CISO", "VEA"],
            facet=["parent", "subba"],
            frequency="hourly",
            ano=ano,
            mes=mes,
        ),
    },
    transformer=SeriesAggregator(),
    destinations={
        "data": [
            SQLAppendDestination(
                engine,
                "eletricity",
                schema=None,
                group_column="ref_date",
                group_value=date(ano, mes, 1).strftime("%Y-%m-%d"),
            )
        ],
    },
)

etl.execute()
