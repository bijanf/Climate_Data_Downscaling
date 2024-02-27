import xarray as xr
import numpy as np
from scipy.ndimage import gaussian_filter

# List of NetCDF files to merge - ideally sorted by their longitude start points
files = [
    "MPI-ESM1-2-HR_ssp370_r1i1p1f1_DBCCA_33.25-37.25_45.25-49.25.nc",
    "MPI-ESM1-2-HR_ssp370_r1i1p1f1_DBCCA_33.25-37.25_49.25-53.25.nc",
    "MPI-ESM1-2-HR_ssp370_r1i1p1f1_DBCCA_33.25-37.25_53.25-57.25.nc",
    "MPI-ESM1-2-HR_ssp370_r1i1p1f1_DBCCA_33.25-37.25_57.25-61.25.nc",
    "MPI-ESM1-2-HR_ssp370_r1i1p1f1_DBCCA_33.25-37.25_61.25-65.25.nc",
    "MPI-ESM1-2-HR_ssp370_r1i1p1f1_DBCCA_33.25-37.25_65.25-69.25.nc",
    "MPI-ESM1-2-HR_ssp370_r1i1p1f1_DBCCA_33.25-37.25_69.25-73.25.nc",
    "MPI-ESM1-2-HR_ssp370_r1i1p1f1_DBCCA_33.25-37.25_73.25-77.25.nc"
]

# Open the datasets and store them in a list
datasets = [xr.open_dataset(f) for f in files]

# Concatenate the datasets along the longitude dimension
merged_dataset = xr.concat(datasets, dim='lon')

# Function to apply smoothing at the boundaries of concatenated datasets
def smooth_transition_zones(merged_data, variable_name, transition_indices, boundary_width=20, sigma=.31):
    for index in transition_indices:
        # Define the slices for the transition zone
        left_boundary = max(0, index - boundary_width)
        right_boundary = min(merged_data.dims['lon'], index + boundary_width)

        # Apply smoothing to the transition zones
        print(merged_data[variable_name].shape)
        merged_data[variable_name][left_boundary:right_boundary, :,:] = gaussian_filter(
            merged_data[variable_name][left_boundary:right_boundary,:,:], sigma=[0, sigma, sigma])

# Identify transition zones based on the longitude values of the original datasets
# Assuming that each file increments the longitude by a fixed amount
# This is a simplification and should be adjusted based on actual longitude values in your datasets
transition_indices = [merged_dataset['lon'].size // len(files) * i for i in range(1, len(files))]

# Smooth only the transition zones for the 'tas' variable
smooth_transition_zones(merged_dataset, 'tas', transition_indices)
ds_transposed = merged_dataset['tas'].transpose('time', 'lat', 'lon')

# To modify the dataset in-place, you might want to assign the transposed data back
merged_dataset['tas'] = ds_transposed
# Save the smoothed dataset to a new file
merged_dataset.to_netcdf('boundary_smoothed_merged_dataset.nc')
