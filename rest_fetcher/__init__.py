__version__ = '0.5.7'
from .client import APIClient
from .events import PaginationEvent
from .exceptions import (
    AuthError,
    CallbackError,
    DeadlineExceeded,
    FixtureFormatError,
    PaginationError,
    PlaybackError,
    RateLimitError,
    RateLimitExceeded,
    RequestError,
    ResponseError,
    RestFetcherError,
    SchemaError,
    StateViewMutationError,
    raise_,
)
from .metrics import MetricsSession, MetricsSummary
from .pagination import (
    cursor_pagination,
    link_header_pagination,
    offset_pagination,
    page_number_pagination,
    url_header_pagination,
)
from .schema import validate
from .types import (
    AuthConfig,
    ClientSchema,
    EndpointSchema,
    OnErrorFn,
    OnEventFn,
    OnPageCompleteFn,
    OnRequestFn,
    PageCycleOutcome,
    PaginationConfig,
    RateLimitConfig,
    RetryConfig,
    SchemaBuilder,
    StopSignal,
    StreamRun,
    StreamSummary,
)

__all__ = [
    'APIClient',
    'PaginationEvent',
    'MetricsSession',
    'MetricsSummary',
    # exceptions
    'RestFetcherError',
    'SchemaError',
    'AuthError',
    'RequestError',
    'RateLimitError',
    'RateLimitExceeded',
    'ResponseError',
    'PaginationError',
    'CallbackError',
    'StateViewMutationError',
    'DeadlineExceeded',
    'raise_',
    'PlaybackError',
    'FixtureFormatError',
    # schema helpers
    'validate',
    # typing / builder (optional, zero runtime cost)
    'SchemaBuilder',
    'ClientSchema',
    'EndpointSchema',
    'PaginationConfig',
    'RateLimitConfig',
    'RetryConfig',
    'AuthConfig',
    'OnErrorFn',
    'OnEventFn',
    'OnPageCompleteFn',
    'OnRequestFn',
    'StreamRun',
    'StreamSummary',
    'StopSignal',
    'PageCycleOutcome',
    # built-in pagination strategies
    'cursor_pagination',
    'link_header_pagination',
    'url_header_pagination',
    'offset_pagination',
    'page_number_pagination',
]
