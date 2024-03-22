import sys
import os
import gc
import numpy as np
import xarray as xr
import dask
#dask.config.set({'array.slicing.split_large_chunks': True})

from dask.distributed import Client, get_client
import pandas as pd
import gcsfs
import xclim.indices as xci
from xclim.core.calendar import convert_calendar

# Add the downscaling_code directory to the PATH
sys.path.append("/home/bijan/Documents/scripts/UTCDW_Guidebook/downscaling_code")
from DBCCA_bijan import DBCCA

# Define models, member IDs, and scenarios
models_scenarios = [
	("GFDL-ESM4", "r1i1p1f1", ["ssp126", "ssp370", "ssp585"]),
	("UKESM1-0-LL", "r1i1p1f2", ["ssp126", "ssp370", "ssp585"]),
	("MPI-ESM1-2-HR", "r1i1p1f1", ["ssp126", "ssp370", "ssp585"]),
	("IPSL-CM6A-LR", "r1i1p1f1", ["ssp126", "ssp370", "ssp585"]),
	("MRI-ESM2-0", "r1i1p1f1", ["ssp126", "ssp370", "ssp585"])
]

def manage_dask_client():
	try:
		client = get_client()
		print("Found an existing Dask client. Closing it.")
		client.close()
	except ValueError:
		print("No existing Dask client found. Proceeding to create a new one.")
	return Client(n_workers=8, threads_per_worker=2, timeout="60s", memory_limit='4GB')

import os
import pandas as pd
import xarray as xr
import gcsfs
from xclim.core.calendar import convert_calendar
#from DBCCA import DBCCA  # Assuming this is your downscaling function

#lat_bnds = [35.25, 37.75]
#lon_bnds = [45.25, 47.75]

#lat_bnds = [33.25, 56.75]
#lon_bnds = [45.25, 90.75]

# Define the whole domain
##whole_lat_bnds = [33.25, 56.75]
##whole_lon_bnds = [45.25, 90.75]
whole_lat_bnds = [33.25, 35.75]
whole_lon_bnds = [45.25, 47.75]
# Generate 4x4 degree boxes within the domain
def generate_boxes(lat_range, lon_range, step=4):
    lat_boxes = np.arange(lat_range[0], lat_range[1], step)
    lon_boxes = np.arange(lon_range[0], lon_range[1], step)
    boxes = []
    for lat in lat_boxes:
        for lon in lon_boxes:
            boxes.append((max(lat, lat_range[0]), min(lat+step, lat_range[1]),
                          max(lon, lon_range[0]), min(lon+step, lon_range[1])))
    return boxes


boxes = generate_boxes(whole_lat_bnds, whole_lon_bnds, step=46)



#years_hist = range(1979, 2011)
#years_future = range(2071, 2101)
years_obs = range(1979, 2015)
years_hist = range(1979, 1981)
years_future = range(2071, 2073)


url = "chelsa-w5e5v1.0_obsclim_tas_300arcsec_global_daily_1979_2014.nc"
era5_ds = xr.open_dataset(url, engine='netcdf4')

#tas_obs.to_netcdf("obs.nc")

def reorder_netcdf_dimensions(input_file_path, output_file_path):
	"""
	Reorders the dimensions of a NetCDF file to (time, lat, lon) and saves the reordered dataset.

	Parameters:
	- input_file_path: str, path to the original NetCDF file.
	- output_file_path: str, path where the reordered NetCDF file will be saved.
	"""
	# Load the original NetCDF file
	ds = xr.open_dataset(input_file_path)

	# Attempt to reorder dimensions to (time, lat, lon), if those dimensions exist
	try:
		# Transpose dimensions. This assumes 'time', 'lat', and 'lon' are the names of the dimensions.
		# If your dataset uses different names, you might need to adjust them here.
		ds_transposed = ds.transpose('time', 'lat', 'lon')

		# Save the transposed dataset to a new NetCDF file
		ds_transposed.to_netcdf(output_file_path)

		print(f"Reformatted file saved to: {output_file_path}")
	except ValueError as e:
		# In case of an error (e.g., one of the dimensions does not exist), print the error message.
		print(f"Error reordering dimensions: {e}")

def scatter_data(client, *args):
    # Scatter multiple datasets at once and return their futures
    futures = client.scatter(args, broadcast=True)
    return futures  # Returns a tuple of futures


def load_and_process_data(source_id, member_id, scenarios, box):

	lat_bnds = [box[0], box[1]]
	lon_bnds = [box[2], box[3]]
	lat_lon_ext = f"{lat_bnds[0]}-{lat_bnds[1]}_{lon_bnds[0]}-{lon_bnds[1]}"
	tas_obs = era5_ds.tas.sel(lat=slice(*lat_bnds), lon=slice(*lon_bnds),
						  time=era5_ds.time.dt.year.isin(years_obs)) - 273.15
	#tas_obs = tas_obs.chunk({'time': -1})

	print(f"Processing box: {lat_lon_ext} for {source_id}, {member_id}, scenarios: {', '.join(scenarios)}")
	scenario = scenarios[0]
	if True:
		tas_hist_raw = xr.open_dataset( 'dbcca_data/GFDL-ESM4_historical_r1i1p1f1_RAW.nc' , engine='netcdf4')
		tas_hist_raw = tas_hist_raw.tas.sel(lat=slice(*lat_bnds), lon=slice(*lon_bnds),
						  time=tas_hist_raw.time.dt.year.isin(years_hist))
		tas_ssp3_raw = xr.open_dataset( 'dbcca_data/GFDL-ESM4_ssp585_r1i1p1f1_RAW.nc' , engine='netcdf4')
		tas_ssp3_raw = tas_ssp3_raw.tas.sel(lat=slice(*lat_bnds), lon=slice(*lon_bnds),
						  time=tas_ssp3_raw.time.dt.year.isin(years_future))
		#print(tas_hist_raw.tas.isel(time = 0).values)
		#tas_hist_raw = tas_hist_raw.chunk({'time': -1})
		#tas_ssp3_raw = tas_ssp3_raw.chunk({'time': -1})
		file_hist_dbcca = f"dbcca_data/{source_id}_historical_{member_id}_DBCCA_{lat_lon_ext}.nc"
		file_ssp3_dbcca = f"dbcca_data/{source_id}_{scenario}_{member_id}_DBCCA_{lat_lon_ext}.nc"
		file_hist_bcca = f"dbcca_data/{source_id}_historical_{member_id}_BCCA_{lat_lon_ext}.nc"
		file_ssp3_bcca = f"dbcca_data/{source_id}_{scenario}_{member_id}_BCCA_{lat_lon_ext}.nc"
		print("size of the datasets used are ", 		tas_hist_raw.shape	, tas_ssp3_raw.shape, tas_obs.shape)
		if not os.path.exists(file_hist_dbcca) or not os.path.exists(file_ssp3_dbcca):
				print("DBCCA outputs not found. Starting downscaling process...")
				# Assuming DBCCA function is properly defined and imported
				tas_hist_dbcca, tas_ssp3_dbcca = DBCCA(
				tas_hist_raw,
				tas_ssp3_raw,
				tas_obs,
				"tas",
				units="degC",
				bc_grouper = 'time.dayofyear',
				fout_hist_bcca=file_hist_bcca,
				fout_future_bcca=file_ssp3_bcca,
				fout_hist_dbcca=file_hist_dbcca,
				fout_future_dbcca=file_ssp3_dbcca,
				write_output=True, box_length= 2
				)
				print("Downscaling process completed.")
		else:
				print("DBCCA outputs found. Skipping downscaling.")
	else:
			print(f"No data found for {source_id}, {scenario}, {member_id}")
	del tas_obs


def main():
	for box in boxes:
		#for model, member_id, scenarios in models_scenarios:
		#	load_and_process_data(model, member_id, scenarios, box)
		model = "GFDL-ESM4"
		member_id = "r1i1p1f1"
		scenarios = ["ssp585"]
		load_and_process_data(model, member_id, scenarios, box)
	print("Process completed successfully.")

if __name__ == '__main__':
	main()
