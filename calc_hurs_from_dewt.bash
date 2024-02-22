#!/bin/bash


if [ $# -ne 3 ];then
  echo "ERROR: not enough command line arguments"
  echo "  arg 1 - tas file"
  echo "  arg 2 - dewt file"
  echo "  arg 3 - file out"
  exit
fi

FILE_TAS=${1}
FILE_DEWT=${2}
FILE_OUT=${3}

conda activate bias

RANDOMA=`tr -cd '[:alnum:]' < /dev/urandom | fold -w30 | head -n1`


cat > script_${RANDOMA}.py <<EOI
import xarray as xr
import dask
##from my_modules.auxiliary_tools import physics
import calc_relative_humidity_from_dewpoint
with dask.config.set(**{'array.slicing.split_large_chunks': False}):
    data = xr.open_mfdataset(["${FILE_TAS}","${FILE_DEWT}"], chunks={'time': 24})
    dims = data['t2m'].dims
    coords = data['t2m'].coords
    hurs = xr.apply_ufunc(calc_relative_humidity_from_dewpoint,
                          data['d2m'], data['t2m'],
                          dask='parallelized', output_dtypes=[float])
    data_out = xr.DataArray(hurs, dims=dims, coords=coords).to_dataset(name='hurs')
    data_out['hurs'] = data_out['hurs'].assign_attrs({'standard_name':'relative_humidity', 'long_name':'surface relative humidity', 'units':'%'})
    data_out.to_netcdf("${FILE_OUT}")
    data.close()
EOI

##source activate my_python
##export PYTHONPATH="${PYTHONPATH}:/home/menz/tools/python/python_modules:/home/menz/tools/python/model-comparison-toolbox"
##export PYTHONPATH="${PYTHONPATH}:/home/menz/tools/python"
python -Bu script_${RANDOMA}.py
rm -f script_${RANDOMA}.py
sleep 5
