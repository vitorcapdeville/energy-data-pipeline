from datetime import date

from dateutil.relativedelta import relativedelta
from extralo.source import Source
from extralo.typing import DataFrame
from myeia.api import API


class EIASource(Source):
    def __init__(self, ano, mes, route, frequency, series, facet) -> None:
        self._series = series
        self._facet = facet
        self._route = route
        self._frequency = frequency
        self._start = date(ano, mes, 1).strftime("%Y-%m-%d")
        self._end = (date(ano, mes, 1) + relativedelta(day=31)).strftime("%Y-%m-%d")
        super().__init__()

    def extract(self) -> DataFrame:
        df = (
            API()
            .get_series_via_route(
                route=self._route,
                frequency=self._frequency,
                series=self._series,
                facet=self._facet,
                start_date=self._start,
                end_date=self._end,
            )
            .reset_index()
        )
        df = df.rename(columns={df.columns[-1]: "value"})
        df["ref_date"] = self._start
        return df

    def __repr__(self) -> str:
        return f"EIASource(ref_date={self._start}, route={self._route}, series={self._series})"
