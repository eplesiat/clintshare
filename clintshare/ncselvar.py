import argparse
import xarray as xr
from tqdm import tqdm
from multiprocessing import Process, Manager
import glob
import os
import numpy as np
import warnings

def split_list(lst, n):
    return [lst[i * n:(i + 1) * n] for i in range((len(lst) + n - 1) // n )]

def copy_ds(input, output, variable, res, i):
    
    try:
        ds = xr.open_dataset(input)
    except Exception as e:
        res[i] = e

    if "ds" in locals():
        vars = list(ds.keys())
        if variable not in vars:
            res[i] = "Variable {} not found in {}".format(variable, input)
        else:
            vars = [var for var in vars if "_bnd" in var or var == variable]
            try:
                ds[vars].to_netcdf(output)
            except Exception as e:
                res[i] = e
    return res

def ncselvar():
    
    warnings.simplefilter("ignore")

    parser = argparse.ArgumentParser()
    parser.add_argument('data_path', type=str, help="Path or pattern of paths of the directories or NetCDF files to be shared")
    parser.add_argument("variables", type=str, help="Comma separated list of variables name to be selected")
    parser.add_argument("out_path", type=str, help="Path of the output files")
    parser.add_argument("-n","--ncpus", type=int, default=32, help="Number of cpus")
    args = parser.parse_args()

    try:
        os.makedirs(args.out_path)
        print("* Output directory has been created")
    except:
        print("* Output directory already exists")

    if os.path.isdir(args.data_path):
        args.data_path += "/**"
    files = [file for file in glob.glob(args.data_path, recursive=True) if file[-3:] == ".nc"]
    files = [os.path.abspath(file) for file in files]

    num_files = len(files)
    print("* {} files found".format(num_files))

    batches = split_list(np.arange(num_files), args.ncpus)
    
    logfile = args.out_path + "/ncselvar.log" 
    f = open(logfile, "w")
    for variable in args.variables.split(","):
        print("* Selecting {}...".format(variable))

        num_errors = 0
        pbar = tqdm(range(len(batches)))
        for i in pbar:

            pbar.set_description("# errors = {}".format(num_errors))

            res = Manager().dict()
            procs = []
            k = 0
            for idx in batches[i]:
                k += 1
                output = "{}/{}_{}".format(args.out_path, variable, os.path.basename(files[idx]))
                procs.append(Process(target=copy_ds, args=[files[idx], output, variable, res, k]))
                procs[-1].start()

            for proc in procs:
                proc.join()
            
            errors = res.values()
            for error in errors:
                print(error, file=f)
            num_errors += len(errors)

        pbar.set_description("# errors = {}".format(num_errors))
        pbar.update()
    f.close()
    if num_errors == 0:
        print("* Variables have been selected successfully!")
    else:
        print("* {} errors reported in {}".format(num_errors, logfile))

if __name__ == "__main__":
    ncselvar()
