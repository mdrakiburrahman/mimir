from pydantic import BaseModel, ConfigDict, Field
from mimir.api.types import GRANULARITY

import typing as t


class InquiryRequest(BaseModel):
    """
    A server request for an inquiry.

    Attributes:
        dimensions: A list of dimensions to group by.
        metrics: A list of metrics to include in the inquiry.
        start_date: The start date for the inquiry.
        end_date: The end date for the inquiry.
        granularity: The granularity of the inquiry.
        global_filter: A global filter to apply to the inquiry.
        order_by: The column to order the results by.
        client_sql: A SQL query to use to combine the results of the atomic queries.
    """

    model_config = ConfigDict()
    dimensions: list[str] = Field(default_factory=list)
    metrics: list[str]
    start_date: t.Optional[str] = None
    end_date: t.Optional[str] = None
    granularity: t.Optional[t.Union[str, GRANULARITY]] = None
    global_filter: t.Optional[str] = None
    order_by: t.Optional[str] = None
    client_sql: t.Optional[str] = None

    def parse_granularity(self) -> t.Self:
        """Parses the granularity string into a GRANULARITY enum."""
        if isinstance(self.granularity, str):
            self.granularity = GRANULARITY[self.granularity]

        return self
