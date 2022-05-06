"""Tool to monitor _fetch_range requests made by fsspec file objects.

Author: Geert Barentsen
"""
import functools
import time

from fsspec.implementations.local import LocalFileOpener
from fsspec.implementations.http import HTTPFile
from s3fs import S3File


# Which fsspec file classes are monitored by default?
# Targets must implement the `_fetch_range(start, end)` method.
DEFAULT_TARGETS = [LocalFileOpener, HTTPFile, S3File]


class FsspecMonitor:
    """Tool to monitor byte-range requests made by fsspec.

    This class monkey-patches the `_fetch_range()` method of fsspec-provided
    file objects (e.g., ``HTTPFile``, ``S3File``) to collect and print
    statistics on the byte-range data fetch requests executed by these
    objects.  This enables the network I/O behavior of fsspec to be explored.

    Example
    -------
    >>> with FsspecMonitor() as monitor:
    >>>     with s3fs.open(s3_uri, mode="rb", block_size=50, cache_type="block") as fh:
    >>>         fh.seek(100)
    >>>         fh.read(80)
    >>>     monitor.summary()
    """

    def __init__(self, targets=DEFAULT_TARGETS, verbose=True):
        """
        Parameters
        ----------
        targets : list
            Which `fsspec` file objects do we want to monitor?
        verbose : bool
            Print stats to stdout while requests are being made?
        """
        self.targets = targets
        self.verbose = verbose
        # `_requests`` will hold a log of all calls to _fetch_range,
        # which we need to print a summary at the end
        self._requests = []
        # `_original_methods`` will hold pointers to the original `_fetch_range`
        # methods, enabling us to undo the monkey-patching upon `__exit__`
        self._original_methods = {}

    def reset(self):
        self._requests = []

    def __enter__(self):
        self.reset()  # reset the summary stats

        for target in self.targets:
            if target not in self._original_methods:
                self._original_methods[target] = target._fetch_range
            target._fetch_range = self.get_wrapper(target._fetch_range)

        return self

    def __exit__(self, exc_type=None, exc_value=None, exc_tb=None):
        for target in self.targets:
            target._fetch_range = self._original_methods[target]

    def print(self, msg, end=None, color="\u001b[1m\u001b[31;1m"):
        reset = "\u001b[0m"
        print(f"{color}{msg}{reset}", end=end)

    def summary(self):
        self.print(
            f"Summary: fetched {self.bytes_transferred()} "
            f"bytes ({self.bytes_transferred()/(1024*1024):.2f} MB) "
            f"in {self.time_elapsed():.2f} s ({self.throughput():.2f} MB/s) "
            f"using {self.requests()} requests."
        )

    def log(self, byte_start, byte_end, time_elapsed):
        """Register a byte-range requests."""
        self._requests.append((byte_start, byte_end, time_elapsed))

    def bytes_transferred(self) -> int:
        """Total number of bytes fetched."""
        return sum([req[1] - req[0] for req in self._requests])

    def time_elapsed(self) -> float:
        """Total time elapsed during network I/O in seconds."""
        return sum([req[2] for req in self._requests])

    def throughput(self) -> float:
        """Returns the throughput in MB/s."""
        return _compute_throughput(self.bytes_transferred(), self.time_elapsed())

    def requests(self) -> int:
        """Total number of GET byte-range requests made."""
        return len(self._requests)

    def get_wrapper(self, func):
        """Wrapper intended to monkey-patch the `_fetch_range` methods in fsspec."""

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            obj, start, end = args
            time_start = time.perf_counter()
            result = func(*args, **kwargs)
            time_elapsed = time.perf_counter() - time_start
            if self.verbose:
                if not self._requests:
                    self.print(f"Reading {obj.path} ({obj.size/(1024*1024):.2f} MB)")
                throughput = _compute_throughput(end - start, time_elapsed)
                self.print(f"FETCH bytes {start}-{end} " f"({throughput:.2f} MB/s)")
            self.log(start, end, time_elapsed)
            return result

        return wrapper


def _compute_throughput(bytes_transferred, time_elapsed) -> float:
    if time_elapsed <= 0:
        return float("inf")
    return bytes_transferred / (time_elapsed * 1024 * 1024)
