# The project for downscaling any GCM from CMIP6 using the CHELSA data set to 10km
# Climate Data Downscaling Project

This repository contains Python code for downscaling climate model projections. It utilizes libraries such as `xarray`, `pandas`, `dask`, `numpy`, and `gcsfs`, along with climate indices from `xclim`. The core of this project is to process high-resolution climate data, focusing on temperature projections across different models and scenarios.

## Prerequisites

Before you run this project, ensure you have the following installed:
- Python 3.6 or later
- [Dask](https://dask.org/)
- [Xarray](http://xarray.pydata.org/en/stable/)
- [Pandas](https://pandas.pydata.org/)
- [Numpy](https://numpy.org/)
- [GCSFS](https://gcsfs.readthedocs.io/en/latest/)
- [xclim](https://xclim.readthedocs.io/en/stable/)

You'll also need access to climate model data, which this code expects to be accessible either locally or through a cloud storage service.

## Installation

Clone this repository to your local machine:

```bash
git clone https://github.com/yourusername/climate-data-downscaling.git
cd climate-data-downscaling
```

## Usage 

the main code to run is 

```bash

```


- The method is based on analogues metho: https://utcdw.physics.utoronto.ca/UTCDW_Guidebook/README.html
- github repo : https://github.com/mikemorris12/UTCDW_Guidebook
## TODO:
- find the best settings for other variables than tas
- implement a code for looping over variables
- 
