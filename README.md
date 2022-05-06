# fsspec-monitor

**Monitor network traffic when reading remote files using `fsspec`.**

`fsspec-monitor` is a lightweight tool which enables network I/O to be monitored when using [fsspec](https://filesystem-spec.readthedocs.io) to read remote files.

This tool monkey-patches the ``_fetch_range()`` methods which are used by `fsspec`-based file objects (e.g., ``HTTPFile``, ``S3File``) to download byte ranges of remote files.  This enables us to print diagnostic information on the exact byte-range download requests that are being issued by these objects.

## Purpose

The purpose of this tool is to verify whether existing Python packages can use `fsspec`-based file objects in an efficient way.

## Example use

The following example was used to demonstrate that the `astropy` package can use `fsspec` to read astronomical data stored in the FITS file format in an efficient way.

```python
>>> import fsspec
... from fsspecmonitor import FsspecMonitor
... from astropy.io import fits
...
... # URL of a large file stored in the FITS format (213 MB)
... url = "https://mast.stsci.edu/api/v0.1/Download/file/?uri=mast:HST/product/j8pu0y010_drc.fits"
...
... with FsspecMonitor() as monitor:
...     with fsspec.open(url) as fh:
...         with fits.open(fh) as hdul:
...             cutout = hdul[2].section[10:20, 30:40]
...     monitor.summary()
Reading https://mast.stsci.edu/api/v0.1/Download/file/?uri=mast:HST/product/j8pu0y010_drc.fits (213.82 MB)
FETCH bytes 0-5242960 (5.47 MB/s)
FETCH bytes 74759040-80004800 (11.29 MB/s)
Summary: fetched 10488720 bytes (10.00 MB) in 1.36 s (7.37 MB/s) using 2 requests.
```

Success!  We were able to verify that `astropy` is able to extract data from a 213 MB FITS file without downloading the entire object.
