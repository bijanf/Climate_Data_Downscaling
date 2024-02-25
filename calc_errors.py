import xarray as xr

def calculate_monthly_climatology_difference(observation_file, simulation_file, variable_name='temp'):
    # Load the observation and simulation data
    obs = xr.open_dataset(observation_file)
    sim = xr.open_dataset(simulation_file)

    # Ensure dimensions order match, assuming observation is in the correct order
    correct_dims_order = obs[variable_name].dims
    sim = sim.transpose(*correct_dims_order)

    # Calculate monthly climatology for each dataset
    obs_climatology = obs.groupby('time.month').mean(dim='time')
    sim_climatology = sim.groupby('time.month').mean(dim='time')

    # Calculate the difference in monthly climatology
    climatology_difference = sim_climatology[variable_name] - obs_climatology[variable_name]

    return climatology_difference

# Replace 'temp' with your variable name if different
variable_name = 'tas'
observation_file = 'obs.nc'
simulation_file = 'dbcca_data/MPI-ESM1-2-HR_historical_r1i1p1f1_DBCCA.nc'

climatology_difference = calculate_monthly_climatology_difference(observation_file, simulation_file, variable_name)

# Save the climatology difference to a new NetCDF file
climatology_difference.to_netcdf('climatology_difference.nc')

print("Monthly climatology differences saved in 'climatology_difference.nc'")
