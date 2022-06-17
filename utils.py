from typing import Union

from IPython.core.display import clear_output
from IPython.display import display
from pandas import option_context
from pandas.core.frame import DataFrame
from pandas.core.generic import NDFrame
from pandas.core.series import Series


@option_context("display.max_rows", None, "display.max_columns", None)  # type: ignore
def display_df(df: Union[DataFrame, NDFrame, Series], clear: bool = False):
    not clear or clear_output()
    _ = display(df)
