import sys
import os
import gc
import numpy as np
import xarray as xr
import dask
from dask.distributed import Client, get_client
import pandas as pd
import gcsfs
import xclim.indices as xci
from xclim.core.calendar import convert_calendar

# Add the downscaling_code directory to the PATH
sys.path.append("/home/bijan/Documents/scripts/UTCDW_Guidebook/downscaling_code")
from DBCCA import DBCCA

# Define models, member IDs, and scenarios
models_scenarios = [
	("GFDL-ESM4", "r1i1p1f1", ["ssp126", "ssp370", "ssp585"]),
	("UKESM1-0-LL", "r1i1p1f2", ["ssp126", "ssp370", "ssp585"]),
	("MPI-ESM1-2-hr", "r1i1p1f1", ["ssp126", "ssp370", "ssp585"]),
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
	return Client(n_workers=8, threads_per_worker=2, timeout="60s")

import os
import pandas as pd
import xarray as xr
import gcsfs
from xclim.core.calendar import convert_calendar
from DBCCA import DBCCA  # Assuming this is your downscaling function

lat_bnds = [35.25, 37.75]
lon_bnds = [45.25, 47.75]
years_hist = range(1979, 2011)
years_future = range(2071, 2101)


url = "chelsa-w5e5v1.0_obsclim_tas_300arcsec_global_daily_1979_2014.nc"
era5_ds = xr.open_dataset(url, engine='netcdf4')
tas_obs = era5_ds.tas.sel(lat=slice(*lat_bnds), lon=slice(*lon_bnds),
						  time=era5_ds.time.dt.year.isin(years_hist)) - 273.15

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



def load_and_process_data(source_id, member_id, scenarios):
	# Assuming manage_dask_client and other initial setup is done elsewhere

	# Define spatial and temporal boundaries


	# Load CMIP6 data catalog
	df_catalog = pd.read_csv('https://storage.googleapis.com/cmip6/cmip6-zarr-consolidated-stores.csv')

	for scenario in scenarios:  # Include historical scenario
		print(f"Processing {source_id}, {member_id}, {scenario}...")

		df_search_scenario = df_catalog[(df_catalog.table_id == 'day') & (df_catalog.source_id == source_id) &
							   (df_catalog.variable_id == 'tas') & (df_catalog.experiment_id == scenario) &
							   (df_catalog.member_id == member_id)]
		print(df_search_scenario)
		if not df_search_scenario.empty:
			# Example for loading dataset (simplified)
			# Actual implementation may vary based on your data source and structure
			cs = gcsfs.GCSFileSystem(token='anon')
			url = df_search_scenario[df_search_scenario.experiment_id == scenario].zstore.values[0]
			mapper = cs.get_mapper(url)
			ds = xr.open_zarr(mapper, consolidated=True)
			tas = ds.tas.sel(lat=slice(*lat_bnds), lon=slice(*lon_bnds))
			tas_ssp3_raw = tas.sel(time=tas.time.dt.year.isin(years_future)) - 273.15

		df_search_hist = df_catalog[(df_catalog.table_id == 'day') & (df_catalog.source_id == source_id) &
							   (df_catalog.variable_id == 'tas') & (df_catalog.experiment_id == 'historical') &
							   (df_catalog.member_id == member_id)]
		print(df_search_hist)
		if not df_search_scenario.empty:
			# Example for loading dataset (simplified)
			# Actual implementation may vary based on your data source and structure
			cs = gcsfs.GCSFileSystem(token='anon')
			url = df_search_hist[df_search_hist.experiment_id == 'historical'].zstore.values[0]
			mapper = cs.get_mapper(url)
			ds = xr.open_zarr(mapper, consolidated=True)
			tas = ds.tas.sel(lat=slice(*lat_bnds), lon=slice(*lon_bnds))
			tas_hist_raw = tas.sel(time=tas.time.dt.year.isin(years_hist)) - 273.15



			# Convert calendar if necessary
			tas_hist_raw = convert_calendar(tas_hist_raw, 'noleap', align_on='date').chunk({'time': -1})
			tas_ssp3_raw = convert_calendar(tas_ssp3_raw, 'noleap', align_on='date').chunk({'time': -1})
			print("Saving datasets to NetCDF...")
			output_path_hist = f"dbcca_data/{source_id}_historical_{member_id}_RAW.nc"
			output_path_ssp3 = f"dbcca_data/{source_id}_{scenario}_{member_id}_RAW.nc"
			#tas_hist_raw.to_netcdf(output_path_hist)
			#tas_ssp3_raw.to_netcdf(output_path_ssp3)
			print(f"Historical data saved to: {output_path_hist}")
			print(f"Future SSP3-7.0 scenario data saved to: {output_path_ssp3}")

			# Define output file paths for DBCCA
			file_hist_dbcca = f"dbcca_data/{source_id}_historical_{member_id}_DBCCA.nc"
			file_ssp3_dbcca = f"dbcca_data/{source_id}_{scenario}_{member_id}_DBCCA.nc"
			file_hist_bcca = f"dbcca_data/{source_id}_historical_{member_id}_BCCA.nc"
			file_ssp3_bcca = f"dbcca_data/{source_id}_{scenario}_{member_id}_BCCA.nc"

			# Conditional downscaling logic
			if not os.path.exists(file_hist_dbcca) or not os.path.exists(file_ssp3_dbcca):
					print("DBCCA outputs not found. Starting downscaling process...")
					# Assuming DBCCA function is properly defined and imported
					tas_hist_dbcca, tas_ssp3_dbcca = DBCCA(
					tas_hist_raw,
					tas_ssp3_raw,
					tas_obs,
					"tas",
					units="degC",
					fout_hist_bcca=file_hist_bcca,
					fout_future_bcca=file_ssp3_bcca,
					fout_hist_dbcca=file_hist_dbcca,
					fout_future_dbcca=file_ssp3_dbcca,
					write_output=True
					)
					# Ensure consistent indentation for the following lines
					# Reorder dimensions of the output NetCDF files
					#reorder_netcdf_dimensions(file_hist_dbcca, file_hist_dbcca + '_reordered.nc')
					#reorder_netcdf_dimensions(file_ssp3_dbcca, file_ssp3_dbcca + '_reordered.nc')
					#reorder_netcdf_dimensions(file_hist_bcca,  file_hist_bcca  + '_reordered.nc')
					#reorder_netcdf_dimensions(file_ssp3_bcca,  file_ssp3_bcca  + '_reordered.nc')
					print("Downscaling process completed.")
			else:
					print("DBCCA outputs found. Skipping downscaling.")

		else:
			print(f"No data found for {source_id}, {scenario}, {member_id}")


def main():
	for model, member_id, scenarios in models_scenarios:
		load_and_process_data(model, member_id, scenarios)

	print("Process completed successfully.")

if __name__ == '__main__':
	main()
