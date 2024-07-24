# Benchmarking Cloud Optimized HDF5 files for the ICESat-2 mission.

The notebook on this repository benchmarks access times to different versions of ATL03 files stored in S3.
These ATL03 files were optimized using the [optimize.py](optimize.py) script that allow adjust the chunk size of the ground tracks. 

## Installation


Ensure that you have Conda/Mamba installed on your system. You can download and install Conda from the [official website](https://docs.conda.io/en/latest/miniconda.html).

### Steps

1. **Clone the repository**:
   ```bash
   git clone https://github.com/nsidc/cloud-optimized-icesat2.git
   cd cloud-optimized-icesat2/hdf5-benckmarks
   mamba env update -f environment.yml
   mamba activate h5benchmarks
   jupyter lab
   ```

