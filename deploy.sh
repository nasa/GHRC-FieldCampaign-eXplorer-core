#!/bin/bash

source ~/.bashrc

if ! conda &> /dev/null
then
    echo "conda not found, installing miniconda..."
    mkdir $HOME/tmpconda
    cd $HOME
    wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
    TMPDIR=$HOME/tmpconda bash ~/Miniconda3-latest-Linux-x86_64.sh -b -p $HOME/miniconda
    eval "$($HOME/miniconda/bin/conda shell.bash hook)"
    conda update -n base -c defaults conda -y
    conda init bash
    conda config --set channel_priority strict
    conda create --name fcx python=3.6 -y
    conda activate fcx
    conda install -c conda-forge xarray dask netCDF4 bottleneck terracotta gdal rasterio pandas boto3 s3fs numpy -y

    conda install -c conda-forge xarray -y
    conda install -c conda-forge dask -y
    conda install -c conda-forge s3fs -y
    conda install -c conda-forge zarr -y
    conda install -c conda-forge scipy -y
    conda install -c conda-forge h5netcdf -y
    conda install -c conda-forge numpy -y
    conda install -c conda-forge netCDF4 -y
    conda install -c conda-forge gdal -y
    conda install -c conda-forge boto3 -y
    conda install -c conda-forge tqdm -y
    conda install -c conda-forge terracotta -y

    rm Miniconda3-latest-Linux-x86_64.sh
    source ~/.bashrc
fi


cd $HOME/fcx-backend/

source ~/.bashrc

conda init bash

source ./env.sh

conda activate fcx


export LOCAL_OUTPUT_PATH=/home/ec2-user/raw-output-data
export LOCAL_INPUT_PATH=/home/ec2-user/raw-input-data

export CURRENT_DIR=$(pwd)
export S3_OUTPUT_PATH='https://'${OUTPUT_DATA_BUCKET}'.s3-'${AWS_REGION}'.amazonaws.com/fieldcampaign/goesrplt/'

mkdir $LOCAL_OUTPUT_PATH
mkdir $LOCAL_INPUT_PATH

aws s3 sync $CURRENT_DIR/logo s3://$OUTPUT_DATA_BUCKET/$OUTPUT_DATA_BUCKET_KEY/fieldcampaign/goesrplt/logo/
aws s3 sync $CURRENT_DIR/legend s3://$OUTPUT_DATA_BUCKET/$OUTPUT_DATA_BUCKET_KEY/fieldcampaign/goesrplt/legend/

function ABI {
    export ABI_INPUT_PATH=$LOCAL_INPUT_PATH/ABI
    export ABI_OUTPUT_PATH=$LOCAL_OUTPUT_PATH/ABI
    mkdir -p $ABI_INPUT_PATH
    mkdir -p $ABI_OUTPUT_PATH

    #python abi.py

    python abi_zappa.py

    pip install virtualenv --user
    virtualenv ~/envs/tc-deploy --python=python3.7
    source ~/envs/tc-deploy/bin/activate
    export GDAL_DATA=$HOME/envs/tc-deploy/lib64/python3.7/site-packages/rasterio/gdal_data/
    echo GDAL_DATA is $GDAL_DATA
    rm -rf terracotta
    git clone https://github.com/DHI-GRAS/terracotta
    cd terracotta
    pip install -r zappa_requirements.txt
    pip install -e .
    pip install awscli
    sed -i 's/ALLOWED_ORIGINS_TILES: List\[str\] = \[\]/ALLOWED_ORIGINS_TILES: List\[str\] = \["*"\]/g' terracotta/config.py
    mv ../zappa_settings.json .

    #use following command if you need to delete zappa stack and do clean deployment
    #zappa undeploy abi -y

    zappa deploy abi

    zappa update abi

    rm -rf $ABI_INPUT_PATH
    rm -rf $ABI_OUTPUT_PATH
}

function CRS {
   export CRS_OUTPUT_PATH=$LOCAL_OUTPUT_PATH/CRS/
   mkdir -p $CRS_OUTPUT_PATH
   python crs.py
   rm -rf $CRS_OUTPUT_PATH
}

function FEGS {
   export FEGS_OUTPUT_PATH=$LOCAL_OUTPUT_PATH/FEGS/
   python fegs_czml.py
   rm -rf $FEGS_OUTPUT_PATH
}

function LIP {
   export LIP_INPUT_PATH=$LOCAL_INPUT_PATH/LIP/
   export LIP_OUTPUT_PATH=$LOCAL_OUTPUT_PATH/LIP/
   mkdir -p $LIP_OUTPUT_PATH
   mkdir -p $LIP_INPUT_PATH
   python lip_czml.py
   rm -rf $LIP_OUTPUT_PATH
   rm -rf $LIP_INPUT_PATH
}

function GLM {
   export GLM_OUTPUT_PATH=$LOCAL_OUTPUT_PATH/GLM/
   mkdir -p $GLM_OUTPUT_PATH
   python glm_pcloud_czml.py
   rm -rf $GLM_OUTPUT_PATH
}

function LIS {
   export LIS_OUTPUT_PATH=$LOCAL_OUTPUT_PATH/LIS/
   mkdir -p $LIS_OUTPUT_PATH
   python lis_pcloud_czml.py
   rm -rf $LIS_OUTPUT_PATH
}

function LMA {
   export LMA_OUTPUT_PATH=$LOCAL_OUTPUT_PATH/LMA/
   mkdir -p $LMA_OUTPUT_PATH
   python lma_pcloud_czml.py
}

function STATIONS {
   export LMA_STATIONS_OUTPUT_PATH=$LOCAL_OUTPUT_PATH/LMAStations/
   mkdir -p $LMA_STATIONS_OUTPUT_PATH
   python lma_stations.py
   rm -rf $LMA_STATIONS_OUTPUT_PATH
}

function FLIGHT_TRACK {
   export FLIGHT_TRACK_OUTPUT_PATH=$LOCAL_OUTPUT_PATH/FlightTrack/
   mkdir -p $FLIGHT_TRACK_OUTPUT_PATH
   python flight_track.py

}


#call above functions to start processing and uploading data to AWS

FLIGHT_TRACK
CRS
FEGS
LIP
GLM
LIS
LMA
STATIONS
ABI