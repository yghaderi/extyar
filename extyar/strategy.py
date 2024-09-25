import polars as pl

from extyar.models import Fee
from extyar.strategy import covered_call_stg

class Strategy:
    def __init__(self, data:pl.DataFrame, fee:Fee):
        self.data = data
        self.fee = fee
    def covered_call(self):
        df = covered_call_stg(self.data, self.fee)
        return df