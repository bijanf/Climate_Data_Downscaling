"""
test_code_for_all_test.py
This script performs downscaling of coarse CMIP6 model outputs to a finer resolution using the Double
Bias-Corrected Constructed Analogues (DBCCA) statistical downscaling method, as described in Werner & Cannon (2016).
The target dataset for downscaling is based on the CHELSA (Climatologies at high resolution for the earth’s
land surface areas) dataset observations, providing users with high-resolution climate projections.


The script includes functionality for:
1. Loading CMIP6 global climate model (GCM) data.
2. Selecting specific models, scenarios, and time slices.
3. Downloading the selected CMIP6 data and preparing it for downscaling.
4. Applying the DBCCA method to downscale the GCM data to the resolution of the CHELSA observational dataset.
5. Saving the downscaled data for further analysis or visualization.

This script is intended for researchers and practitioners in climate science and related fields who require high-resolution
climate projections for impact assessments, climate change research, and adaptation planning.

Prerequisites:
- Installation of necessary Python packages including xarray, dask, numpy, pandas, gcsfs, and netCDF4.
- Access to CMIP6 data either through a local repository or remote data services.
- The CHELSA dataset or a similar high-resolution observational dataset for the target variable (e.g., surface temperature).

Usage:
- Update the script with the specific CMIP6 models, scenarios, and time slices of interest.
- Ensure the CHELSA dataset (or similar) is accessible by the script for the downscaling process.

"""
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
		#return Client(n_workers=8, threads_per_worker=2, timeout="60s", memory_limit='4GB')
		# Example configuration: 4 workers, one for each day, with a large amount of memory per worker
		#return Client(n_workers=8, threads_per_worker=2, timeout="60s", memory_limit='4GB')
		client = Client(n_workers=4, threads_per_worker=1)
		return client


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
offset = -273.15 # Convert from K to C
# Define the whole domain
whole_lat_bnds = [33.25, 56.75]
whole_lon_bnds = [45.25, 90.75]
#whole_lat_bnds = [33.25, 35.75]
#whole_lon_bnds = [45.25, 47.75]
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


start_future = 2091
end_future = 2096
start_hist = 1991
end_hist = 1996

# Define the years for historical and future periods
years_hist = range(start_hist, end_hist + 1)
years_future = range(start_future, end_future + 1)
years_obs = range(1979, 2015)
#years_hist = range(1979, 1981)
#years_future = range(2071, 2073)


url = "chelsa-w5e5v1.0_obsclim_tas_300arcsec_global_daily_1979_2014.nc"
era5_ds = xr.open_dataset(url, engine='netcdf4')


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
						  time=era5_ds.time.dt.year.isin(years_obs)) + offset

	print(f"Processing box: {lat_lon_ext} for {source_id}, {member_id}, scenarios: {', '.join(scenarios)}")
	# Load CMIP6 data catalog
	df_catalog = pd.read_csv('https://storage.googleapis.com/cmip6/cmip6-zarr-consolidated-stores.csv')
	df_search_hist = df_catalog[(df_catalog.table_id == 'day') & (df_catalog.source_id == source_id) &
					   (df_catalog.variable_id == 'tas') & (df_catalog.experiment_id == 'historical') &
					   (df_catalog.member_id == member_id)]
	# Example for loading dataset (simplified)
	# Actual implementation may vary based on your data source and structure
	cs = gcsfs.GCSFileSystem(token='anon')
	url = df_search_hist[df_search_hist.experiment_id == 'historical'].zstore.values[0]
	mapper = cs.get_mapper(url)
	ds = xr.open_zarr(mapper, consolidated=True)
	tas = ds.tas.sel(lat=slice(*lat_bnds), lon=slice(*lon_bnds))
	tas_hist_raw = tas.sel(time=tas.time.dt.year.isin(years_hist)) + offset
	tas_hist_raw = convert_calendar(tas_hist_raw, 'noleap', align_on='date')#.chunk({'time': -1})
	for scenario in scenarios:  # Include historical scenario

		print(f"Processing {source_id}, {member_id}, {scenario}...")
		df_search_scenario = df_catalog[(df_catalog.table_id == 'day') & (df_catalog.source_id == source_id) &
								   (df_catalog.variable_id == 'tas') & (df_catalog.experiment_id == scenario) &
								   (df_catalog.member_id == member_id)]

		if not df_search_scenario.empty:
			# Example for loading dataset (simplified)
			# Actual implementation may vary based on your data source and structure
			cs = gcsfs.GCSFileSystem(token='anon')
			url = df_search_scenario[df_search_scenario.experiment_id == scenario].zstore.values[0]
			mapper = cs.get_mapper(url)
			ds = xr.open_zarr(mapper, consolidated=True)
			tas = ds.tas.sel(lat=slice(*lat_bnds), lon=slice(*lon_bnds))
			tas_ssp3_raw = tas.sel(time=tas.time.dt.year.isin(years_future)) + offset


			print(df_search_hist)
			# Convert calendar if necessary
			tas_ssp3_raw = convert_calendar(tas_ssp3_raw, 'noleap', align_on='date')#.chunk({'time': -1})
			print("Saving datasets to NetCDF...")
			output_path_hist = f"dbcca_data/{source_id}_historical_{member_id}_RAW.nc"
			output_path_ssp3 = f"dbcca_data/{source_id}_{scenario}_{member_id}_RAW.nc"

			# Check if the historical dataset already exists
			if not os.path.exists(output_path_hist):
				print(f"Saving historical dataset to: {output_path_hist}")
				tas_hist_raw.to_netcdf(output_path_hist)
			else:
				print(f"Historical dataset already exists at: {output_path_hist}")

			# Check if the future scenario dataset already exists
			if not os.path.exists(output_path_ssp3):
				print(f"Saving future scenario dataset to: {output_path_ssp3}")
				tas_ssp3_raw.to_netcdf(output_path_ssp3)
			else:
				print(f"Future scenario dataset already exists at: {output_path_ssp3}")

			print(f"Historical data saved to: {output_path_hist}")
			print(f"Future SSP scenario data saved to: {output_path_ssp3}")

			tas_hist_raw = xr.open_dataset( output_path_hist , engine='netcdf4')
			tas_hist_raw = tas_hist_raw.tas.sel(lat=slice(*lat_bnds), lon=slice(*lon_bnds),
							  time=tas_hist_raw.time.dt.year.isin(years_hist))
			tas_ssp3_raw = xr.open_dataset( output_path_ssp3 , engine='netcdf4')
			tas_ssp3_raw = tas_ssp3_raw.tas.sel(lat=slice(*lat_bnds), lon=slice(*lon_bnds),
							  time=tas_ssp3_raw.time.dt.year.isin(years_future))
			file_hist_dbcca = f"dbcca_data/{source_id}_historical_{member_id}_DBCCA_{lat_lon_ext}_{start_hist}_{end_hist}.nc"
			file_ssp3_dbcca = f"dbcca_data/{source_id}_{scenario}_{member_id}_DBCCA_{lat_lon_ext}_{start_future}_{end_future}.nc"
			file_hist_bcca = f"dbcca_data/{source_id}_historical_{member_id}_BCCA_{lat_lon_ext}_{start_hist}_{end_hist}.nc"
			file_ssp3_bcca = f"dbcca_data/{source_id}_{scenario}_{member_id}_BCCA_{lat_lon_ext}_{start_future}_{end_future}.nc"
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
					write_output=True, box_length= 7
					)
					print("Downscaling process completed.")
			else:
					print("DBCCA outputs found. Skipping downscaling.")
		else:
			print(f"No data found for {source_id}, {scenario}, {member_id}")
	del tas_obs
	gc.collect()


def main():
	# Manage Dask client
	#
	#manage_dask_client()
	for box in boxes:
		for model, member_id, scenarios in models_scenarios:
			load_and_process_data(model, member_id, scenarios, box)

		#load_and_process_data(model, member_id, scenarios, box)
	print("Process completed successfully.")

if __name__ == '__main__':
	main()
