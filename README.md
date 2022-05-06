# fsspec-monitor

**Monitor network traffic when reading remote files using `fsspec`.**

Filesystem Spec ([fsspec](https://filesystem-spec.readthedocs.io)) is a fabulous Python package which provides a unified pythonic interface to local, remote and embedded file systems and bytes storage.

This repository contains a lightweight monitoring tool which enables the network I/O behavior to be explored when using `fsspec` to read remote files.
It works by monkey-patching the ``_fetch_range()`` methods of fsspec-provided file objects (e.g., ``HTTPFile``, ``S3File``) to collect and print statistics on the exact byte-range data fetch requests executed by these objects.


## Example use

```python
import fsspec
from fsspecmonitor import FsspecMonitor

with FsspecMonitor() as monitor:
    with fsspec.open(url) as fh:
        fh.seek(100)
        fh.read(80)
    monitor.summary()
```
