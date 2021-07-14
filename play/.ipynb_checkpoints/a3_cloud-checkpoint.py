# import libraries
import re
import zipfile
import getpass
from osgeo import gdal 
import os  # for chdir, getcwd, path.basename, path.exists
import pandas as pd # for DatetimeIndex
import codecs # for text parsing code
import netrc
import rasterio as rio
import glob
import io
import shutil
from subprocess import PIPE, Popen
import subprocess
import fcntl ##may need to pip install this one
import select
import sys

def downloading(file):
    """
    Downloads and unzips UAVSAR images from ASF Vertex. Only tested on .GRD Interferometric Pairs. 
    Ideally for this application only pass 1 url at a time.
    :param zip_url: url pointing at a UAVSAR .zip file
    """
    
    
    # Get NASA EARTHDATA Credentials from ~/.netrc or manual input
    try:
        os.chmod('/home/jovyan/.netrc', 0o600) #only necessary on jupyterhub
        (ASF_USER, account, ASF_PASS) = netrc.netrc().authenticators("urs.earthdata.nasa.gov")
    except:
        ASF_USER = input("Enter Username: ")
        ASF_PASS = getpass.getpass("Enter Password: ")
        
        
    data_dir = '/tmp/'
   
    # directory for data downloads

    os.makedirs(data_dir, exist_ok=True)
    os.chdir(data_dir)

    print(f'downloading {file}...')
    filename = os.path.basename(file)

    if not os.path.exists(os.path.join(data_dir,filename)):
        
        cmd = "wget -1 {0} --user={1} --password={2} -P {3} --progress=bar:force".format(file, ASF_USER, ASF_PASS, data_dir)
        #os.system(cmd) 
        #subprocess.call(cmd)
        process = Popen(['wget',file,'--user={}'.format(ASF_USER),'--password={}'.format(ASF_PASS),'-P',data_dir,'--progress=bar'], stderr=subprocess.PIPE)
        started = False
        for line in process.stderr:
            line = line.decode("utf-8", "replace")
            if started:
                splited = line.split()
                if len(splited) == 9:
                    percentage = splited[6]
                    speed = splited[7]
                    remaining = splited[8]
                    print("Downloaded {} with {} per second and {} left.".format(percentage, speed, remaining), end='\r')
            elif line == os.linesep:
                started = True

        ##Should probably be a subprocess.call(cmd) - not quite sure why but that is the perfered method
    else:
        print(filename + " already exists. Skipping download ..")

    print("done")
    
    # unzip

    for file in glob.glob("/tmp/*.zip"):
        with zipfile.ZipFile(file, "r") as zip_ref:
            print('Extracting all the files now...')
            zip_ref.extractall('/tmp')
            print("done")
    
    return data_dir

# folder is path to a folder with an .ann (or .txt) and .grd files (.amp1, .amp2, .cor, .unw, .int)

def uavsar_tiff_convert(folder, verbose = False):
    """
    Builds a header file for the input UAVSAR .grd file,
    allowing the data to be read as a raster dataset.
    :param folder:   the folder containing the UAVSAR .grd and .ann files
    """

    os.chdir(folder)
    int_file = glob.glob(os.path.join(folder, 'int.grd'))

    # Empty lists to put information that will be recalled later.
    Lines_list = []
    Samples_list = []
    Latitude_list = []
    Longitude_list = []
    Files_list = []

    # Step 1: Look through folder and determine how many different flights there are
    # by looking at the HDR files.
    for files in os.listdir(folder):
        if files [-4:] == ".grd":
            newfile = open(files[0:-4] + ".hdr", 'w')
            newfile.write("""ENVI
description = {DESCFIELD}
samples = NSAMP
lines = NLINE
bands = 1
header offset = 0
data type = DATTYPE
interleave = bsq
sensor type = UAVSAR L-Band
byte order = 0
map info = {Geographic Lat/Lon, 
            1.000, 
            1.000, 
            LON, 
            LAT,  
            0.0000555600000000, 
            0.0000555600000000, 
            WGS-84, units=Degrees}
wavelength units = Unknown
                """
                          )
            newfile.close()
            if files[0:18] not in Files_list:
                Files_list.append(files[0:18])

    #Variables used to recall indexed values.
    var1 = 0

    #Step 2: Look through the folder and locate the annotation file(s).
    # These can be in either .txt or .ann file types.
    for files in os.listdir(folder):
        if Files_list[var1] and files[-4:] == ".txt" or files[-4:] == ".ann":
            #Step 3: Once located, find the info we are interested in and append it to
            # the appropriate list. We limit the variables to <=1 so that they only
            # return two values (one for each polarization of
            searchfile = codecs.open(files, encoding = 'windows-1252', errors='ignore')
            for line in searchfile:
                if "Ground Range Data Latitude Lines" in line:
                    Lines = line[65:70]
                    if verbose:
                        print(f"Number of Lines: {Lines}")
                    if Lines not in Lines_list:
                        Lines_list.append(Lines)

                elif "Ground Range Data Longitude Samples" in line:
                    Samples = line[65:70]
                    if verbose:
                        print(f"Number of Samples: {Samples}")
                    if Samples not in Samples_list:
                        Samples_list.append(Samples)

                elif "Ground Range Data Starting Latitude" in line:
                    Latitude = line[65:85]
                    if verbose:
                        print(f"Top left lat: {Latitude}")
                    if Latitude not in Latitude_list:
                        Latitude_list.append(Latitude)

                elif "Ground Range Data Starting Longitude" in line:
                    Longitude = line[65:85]
                    if verbose:
                        print(f"Top left Lon: {Longitude}")
                    if Longitude not in Longitude_list:
                        Longitude_list.append(Longitude)
    
                        
                 
            #Reset the variables to zero for each different flight date.
            var1 = 0
            searchfile.close()


    # Step 3: Open .hdr file and replace data for all type 4 (real numbers) data
    # this all the .grd files expect for .int
    for files in os.listdir(folder):
        if files[-4:] == ".hdr":
            with open(files, "r") as sources:
                lines = sources.readlines()
            with open(files, "w") as sources:
                for line in lines:
                    if "data type = DATTYPE" in line:
                        sources.write(re.sub(line[12:19], "4", line))
                    elif "DESCFIELD" in line:
                        sources.write(re.sub(line[15:24], folder, line))
                    elif "lines" in line:
                        sources.write(re.sub(line[8:13], Lines, line))
                    elif "samples" in line:
                        sources.write(re.sub(line[10:15], Samples, line))
                    elif "LAT" in line:
                        sources.write(re.sub(line[12:15], Latitude, line))
                    elif "LON" in line:
                        sources.write(re.sub(line[12:15], Longitude, line))
                    else:
                        sources.write(re.sub(line, line, line))
    
    # Step 3: Open .hdr file and replace data for .int file date type 6 (complex)                 
    for files in os.listdir(folder):
        if files[-8:] == ".int.hdr":
            with open(files, "r") as sources:
                lines = sources.readlines()
            with open(files, "w") as sources:
                for line in lines:
                    if "data type = 4" in line:
                        sources.write(re.sub(line[12:13], "6", line))
                    elif "DESCFIELD" in line:
                        sources.write(re.sub(line[15:24], folder, line))
                    elif "lines" in line:
                        sources.write(re.sub(line[8:13], Lines, line))
                    elif "samples" in line:
                        sources.write(re.sub(line[10:15], Samples, line))
                    elif "LAT" in line:
                        sources.write(re.sub(line[12:15], Latitude, line))
                    elif "LON" in line:
                        sources.write(re.sub(line[12:15], Longitude, line))
                    else:
                        sources.write(re.sub(line, line, line))
                        
    
    # Step 4: Now we have an .hdr file, the data is geocoded and can be loaded into python with rasterio
    # once loaded in we use gdal.Translate to convert and save as a .tiff
    
    data_to_process = glob.glob(os.path.join(folder, '*.grd')) # list all .grd files
    for data_path in data_to_process: # loop to open and translate .grd to .tiff, and save .tiffs using gdal
        raster_dataset = gdal.Open(data_path, gdal.GA_ReadOnly)
        raster = gdal.Translate(os.path.join(folder, os.path.basename(data_path) + '.tiff'), raster_dataset, format = 'Gtiff', outputType = gdal.GDT_Float32)
    
    # Step 5: Save the .int raster, needs separate save because of the complex format
    data_to_process = glob.glob(os.path.join(folder, '*.int.grd')) # list all .int.grd files (only 1)
    for data_path in data_to_process:
        raster_dataset = gdal.Open(data_path, gdal.GA_ReadOnly)
        raster = gdal.Translate(os.path.join(folder, os.path.basename(data_path) + '.tiff'), raster_dataset, format = 'Gtiff', outputType = gdal.GDT_CFloat32)

    print(".tiffs have been created")
    return

def a3_bucket_transfer(folder):
    """
    transfers converted .tiff files to the a3 cloud
    :param folder:  (filepath) to folder containing the UAVSAR .tiff and .ann files
    :param region: (string) the region the flight is from
    """
    num_tiffs = len(glob.glob(folder+ "*.tiff"))
    
    for tiff in glob.glob(folder+ "*.tiff"):
        base_name = tiff.split('/')[-1]
        
        region = base_name.split('_')[0]
        year = '20' + base_name.split('_')[2][0:2]
        flight_num = base_name.split('_')[2][2:5]
        folder_name = '{}_{}_{}/'.format(region,year,flight_num)
        
        tiff_folder_fp = os.path.join(folder, folder_name)
        if not os.path.exists(tiff_folder_fp):
            os.mkdir(tiff_folder_fp)
        

        os.replace(tiff,tiff_folder_fp+base_name )
    cmd = 'aws s3 cp {} s3://snowex-data/uavsar-project/UAVSAR_images/{} --recursive'.format(tiff_folder_fp,folder_name)
    os.system(cmd)
    
    ###now check for upload complete (probably a smoother way to do this. This is has a danger of hanging)
    upload_incomplete = True
    if upload_incomplete:
        cmd = 'aws s3 ls s3://snowex-data/uavsar-project/UAVSAR_images/{} | wc -l'.format(folder_name)
        stream = os.popen(cmd)
        output = stream.read()
        if int(output) == num_tiffs:
            upload_incomplete = False
            print('upload complete')
    
    return folder_name, output

def clear_folder(folder):
    if os.path.exists(folder):
        shutil.rmtree(folder, ignore_errors = True)
    return

def main(zip_url, clear_temp = True):
    zip_num = len(zip_url)
    count = 0
    for url in zip_url:
        print('Starting {} of {} zips'.format(count, zip_num))
        data_dir = downloading(url)
        uavsar_tiff_convert(data_dir)
        folder_name, number_uploaded = a3_bucket_transfer(data_dir)
        if clear_temp:
            clear_folder(data_dir)
        print('Created folder {} with {} images'.format(folder_name, number_uploaded.strip()))

        