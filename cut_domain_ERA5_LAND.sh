#!/bin/bash 
#=============================================
#code to prepare the obs_hist,sim_hist,sim_fut
# and cut the domain for the Target
#=============================================
#SBATCH --qos=short
#SBATCH --partition=standard
#SBATCH --account=gvca
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=6
#SBATCH --job-name=gvcabasd
#SBATCH --output=slogs/out.%j
#SBATCH --error=slogs/out.%j
module load nco 
module load cdo 

set -ex 

# header of the chelsa
#header="chelsa-w5e5v1.0_obsclim_"
header="2m_dewpoint_temperature_ERA5-Land_"
# suffix: 
#suffix="_30arcsec_global_daily_"
#suffix="_300arcsec_global_daily_"
#suffix="_90arcsec_global_daily_"
suffix=""
res_obs="300" # in arcsecond
#chelsa_dir="/p/projects/proclias/1km/data/chelsa_w5e5/nc/"
#chelsa_dir="/p/projects/proclias/1km/data/chelsa_w5e5/aggregate/300arcsec/"
chelsa_dir="/p/projects/climate_data_central/reanalysis/ERA5-Land/2m_dewpoint_temperature/"
out_dir_intermediate="./data/"

lat0=33.25 #first lat
lat1=56.75 #second lat
lon0=45.25 #first lon
lon1=90.75 #second lon
var="2m_dewpoint_temperature_"
for file in ${chelsa_dir}${var}*.nc; do
   echo "The file is "$file
   filename=$(basename -- "$file")
   ncks -O -d lat,${lat0},${lat1} -d lon,${lon0},${lon1} ${file} ${out_dir_intermediate}${filename}_lat${lat0}_${lat1}_lon${lon0}_${lon1}_cut.nc


done #file




