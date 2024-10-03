import re
import numpy as np
import pandas as pd
import logging
import s3fs
import fsspec
import time
import h5py
from datetime import datetime
from uuid import uuid4


def parse_fsspec_log(log_path):
    """
    This method only parses fsspec logs that have a FileSize: attached to them.
    """
    head_line  = re.compile('<File-like object S3FileSystem, .*?>\s*(read: 0 - \d+)')
    fsize_line = re.compile('FileSize: (\d+)')
    # range_line = re.compile('<File-like object S3FileSystem, .*?>\s* read: (?P<start>[0-9]+) - (?P<end>[0-9]+)')
    range_line = re.compile('<File-like object S3FileSystem, .*?>\s* read: (?P<start>[0-9]+) - (?P<end>[0-9]+)(?:\s*,\s*.*?:\s*(?P<hits>[0-9]+)\s*hits,\s*(?P<misses>[0-9]+)\s*misses)?')
  


    ranges = list()
    with open(log_path) as logtxt:
        for line in logtxt:
            if head_line.match(line):
                break
        else:
            raise RuntimeError('HEAD line not found in the log file')

        for line in logtxt:
            match = fsize_line.match(line)
            if match:
                fsize = int(match.group(1))
                break
        else:
            raise RuntimeError('FILESIZE line not found in the log file')
            
        logtxt.seek(0)
        for line in logtxt:
            match = range_line.match(line)
            if match:
                start=int(match.group(1))
                end=int(match.group(2))
                hits=match.group(3)
                missed=match.group(4)
                rsize=end-start+1
                
                ranges.append({"start": start, "end": end, "size": rsize, "hits": hits, "missed": missed})

    df = pd.DataFrame(ranges, columns=['start', 'end', 'size', 'hits', 'missed'])
    return df

def read_file(info):
    h5py_fsspec_benchmarks = {}
    ranges = None
    file_size = None
    block_size = None
    iteration, dataset, variables, flavor, url, optimized_read, driver, default_io_params, optimized_io_params = info
    if url.endswith(".json"):
        return {}   
    io_params = default_io_params
    if optimized_read:
        if "rechunked" in url or "page" in url:
            optimized = "yes"
            print(f"Reading: {url} with optimized I/O parameters")
            io_params = optimized_io_params
            block_size = io_params["fsspec_params"]["block_size"]
        else:
            # we cannot read the original file with optimized parameters
            optimized = "no"
            print(f"Reading: {url} with default parameters")
    else:
        optimized = "no"
        print(f"Reading: {url} with default parameters")
    cache_type = io_params["fsspec_params"]["cache_type"]
    
    # this is mostly IO so no perf_counter is needed
    start = time.time()
    if driver == "fsspec":
        fs = s3fs.S3FileSystem(anon=True)
        logger = logging.getLogger('fsspec')
        logger.setLevel(logging.DEBUG)
        file_info = fs.info(url)
        file_size = file_info['size']
        file_name = url.split("/")[-1]
        current_time = datetime.now()
        formatted_time = current_time.strftime(f"%Y-%m-%d_%H-%M-%S-{uuid4()}")
        log_filename = f"logs/fsspec-{file_name}-{driver}-{optimized}-{formatted_time}.log"
        # Create a new FileHandler for each iteration
        file_handler = logging.FileHandler(log_filename)
        file_handler.setLevel(logging.DEBUG)
        # Add the handler to the root logger
        logging.getLogger().addHandler(file_handler)
        with fs.open(url, mode="rb", **io_params["fsspec_params"]) as fo:
            with h5py.File(fo, **io_params["h5py_params"]) as f:
                for variable in variables:
                    data = f[variable][:]
                    data_mean = data.mean()
                    req_bytes = fo.cache.total_requested_bytes
        logger.debug(f"FileSize: {file_size}")
        logging.getLogger().removeHandler(file_handler)
        file_handler.close()
        ranges = parse_fsspec_log(log_filename)
    else:
        cloud_params = {
            "mode": "r",
            "driver": "ros3",
            "aws_region": "us-west-2".encode("utf-8")
        }
        with h5py.File(url, **io_params["h5py_params"], **cloud_params) as f:
            for variable in variables:
                data = f[variable][:]
                data_mean = data.mean()  
                req_bytes = None # not available
    elapsed = time.time() - start
    return {
        "benchmark": {
            "iteration": iteration,
            "library": "h5py",
            "driver": driver,
            "dataset": dataset,
            "optimized-read": optimized,
            "format": flavor,
            "file": url,
            "time": elapsed,
            "shape": data.shape,
            "bytes_requested": req_bytes,
            "mean": data_mean}, 
        "ranges": { 
             "file": url,
             "optimized-read": optimized,
             "cache_type": cache_type,
             "block_size": block_size,
             "time": time,
             "bytes_requested": req_bytes,
             "file_size": file_size,
             "ranges": ranges}
    }
