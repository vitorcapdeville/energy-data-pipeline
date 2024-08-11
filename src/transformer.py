
from extralo.transformer import Transformer
from extralo.typing import DataFrame
import pandas as pd

class SeriesAggregator(Transformer):
    def transform(self, ciso_pgae, ciso_sce, ciso_sdge, ciso_vea) -> dict[str, DataFrame]:
        return {"data": pd.concat([ciso_pgae, ciso_sce, ciso_sdge, ciso_vea], ignore_index=True)}
