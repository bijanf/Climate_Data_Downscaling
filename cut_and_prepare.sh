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
#SBATCH --job-name=preprocess
#SBATCH --output=slogs/out.%j
#SBATCH --error=slogs/out.%j


module load nco 
module load cdo 

set -e
#source 0_export.sh
source namelist.txt
latlon="lat${lat0}_${lat1}_lon${lon0}_${lon1}"
echo "variables are "${variables[@]}
variables=("pr" "rsds" "tas" "tasmax" "tasmin")
models=(GFDL-ESM4  IPSL-CM6A-LR  MPI-ESM1-2-HR  MRI-ESM2-0  UKESM1-0-LL) 
###################
cutoff_do="yes"
mkdir -p ${out_dir_intermediate}
if [ "yes"  == "yes" ]
then 

for var in "${variables[@]}"
do 
    echo "The variable is "$var
    # cutoff the region for chelsa
    for year in {1979..2014}
    do 
        echo "The year is "$year
        for mon in {01..12}
        do 
            echo "The month is "$mon
            ncks -O -d lat,${lat0},${lat1} -d lon,${lon0},${lon1} ${chelsa_dir}${header}${var}${suffix}${year}${mon}.nc ${out_dir_intermediate}${header}${var}${suffix}${year}${mon}_lat${lat0}_${lat1}_lon${lon0}_${lon1}_cut.nc

        done
    done

    # cutoff the region for scenarios
    for scen in "${scenarios[@]}"
    do 
    for mod in "${models[@]}"
        do  
            if [ "$scen" == "historical" ]
            then 

                for yy in 1951_1960 1961_1970 1971_1980 1981_1990 1991_2000 2001_2010 2011_2014
                do 

                    echo 
                    echo "----------cuttiung the scenarios---------------"
                    echo "scenario is"$scen "and model i "$mod  "for year " $yy
                    echo 
                    mod_lower=$(echo "$mod" | tr '[:upper:]' '[:lower:]')
                    if [ "$mod_lower" == "ukesm1-0-ll" ] || [ "$mod_lower" == "cnrm-cm6-1" ] || [ "$mod_lower" == "cnrm-esm2-1" ] 
                    then 
                       realization="r1i1p1f2"
                    else
                       realization="r1i1p1f1"
                    fi  
                    ncks -O -d lat,${lat0},${lat1} -d lon,${lon0},${lon1} ${isimip3b_dir}${scen}/${mod}/${mod_lower}_${realization}_w5e5_${scen}_${var}_global_daily_${yy}.nc ${out_dir_intermediate}${mod_lower}_${realization}_w5e5_${scen}_${var}_global_daily_${yy}_lat${lat0}_${lat1}_lon${lon0}_${lon1}_cut.nc 

                done
            else
                for yy in 2015_2020 2021_2030 2031_2040 2041_2050 2051_2060 2061_2070 2071_2080 2081_2090 2091_2100
                do 

                    echo 
                    echo "----------cuttiung the scenarios---------------"
                    echo "scenario is "$scen "and model is "$mod  "for year " $yy
                    echo 
                    mod_lower=$(echo "$mod" | tr '[:upper:]' '[:lower:]')
                    if [ "$mod_lower" == "ukesm1-0-ll" ]
                    then 
                        realization="r1i1p1f2"
                    else
                        realization="r1i1p1f1"
                    fi  

                    ncks -O -d lat,${lat0},${lat1} -d lon,${lon0},${lon1} ${isimip3b_dir}${scen}/${mod}/${mod_lower}_${realization}_w5e5_${scen}_${var}_global_daily_${yy}.nc ${out_dir_intermediate}${mod_lower}_${realization}_w5e5_${scen}_${var}_global_daily_${yy}_lat${lat0}_${lat1}_lon${lon0}_${lon1}_cut.nc 


                
                done
            fi
        done
    done
done
fi

echo "merging..................."
merging_do="yes"
if [ "${merging_do}"  == "yes" ]
then 
## merge single data to a complete data: 
## observations
for var in "${variables[@]}"
do 
   echo "------------__"
#    if [ ! -f ${out_dir_intermediate}/merged/${header}${var}${suffix}_cutoff_lat${lat0}${lat1}_lon${lon0}_${lon1}mergetime.nc ]
#    then 

        cdo -O -mergetime ${out_dir_intermediate}${header}${var}${suffix}*_lat${lat0}_${lat1}_lon${lon0}_${lon1}_cut.nc ${out_dir_intermediate}${header}${var}${suffix}_lat${lat0}_${lat1}_lon${lon0}_${lon1}_cut_mergetime.nc 
#        $(chunk_time_series ${out_dir_intermediate}${header}${var}${suffix}_lat${lat0}_${lat1}_lon${lon0}_${lon1}_cut_mergetime.nc .rechunked "-C -v lon,lat,time,$var")

#    fi

done 
#
## models

for var in "${variables[@]}"
do 
    for scen in "${scenarios[@]}"
    do 
        for mod in "${models[@]}"
        do    
            mod_lower=$(echo "$mod" | tr '[:upper:]' '[:lower:]')
            if [ "$mod_lower" == "ukesm1-0-ll" ] || [ "$mod_lower" == "cnrm-cm6-1" ] || [ "$mod_lower" == "cnrm-esm2-1" ]
            then 
                realization="r1i1p1f2"
            else
                realization="r1i1p1f1"
            fi  
#            if [ ! -f ${out_dir_intermediate}/merged/${mod_lower}_${realization}_w5e5_${scen}_${var}_global_daily_cutoff_lat${lat0}${lat1}_lon${lon0}_${lon1}mergetime.nc ]    
#            then 

               cdo -O -mergetime   ${out_dir_intermediate}${mod_lower}_${realization}_w5e5_${scen}_${var}_global_daily_*_lat${lat0}_${lat1}_lon${lon0}_${lon1}_cut.nc ${out_dir_intermediate}${mod_lower}_${realization}_w5e5_${scen}_${var}_global_daily_lat${lat0}_${lat1}_lon${lon0}_${lon1}_cut_mergetime.nc
#                $(chunk_time_series ${out_dir_intermediate}${mod_lower}_${realization}_w5e5_${scen}_${var}_global_daily_lat${lat0}_${lat1}_lon${lon0}_${lon1}_cut_mergetime.nc .rechunked "-C -v lon,lat,time,$var")
#            fi

        done
    done
done 
fi

mkdir -p ${out_dir_intermediate}/merged
mv ${out_dir_intermediate}/*mergetime.nc ${out_dir_intermediate}/merged/
rm ${out_dir_intermediate}/*.nc
echo "FINISHED-------------------------"