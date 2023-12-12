import xarray as xr
from tqdm import tqdm
import glob
import os
import warnings

def get_dimvars(file):
    warnings.simplefilter("ignore")
    ds = xr.open_dataset(file)
    return [dict(ds.dims)], list(ds.keys())

def check_dict(dicts):
    uniq_dicts = [dicts[0]]
    for dict in dicts:
        for i in range(len(uniq_dicts)):
            doublon = True
            for key, val in dict.items():
                if key in uniq_dicts[i]:
                    if val != uniq_dicts[i][key]:
                        doublon = False
                else:
                    doublon = False

            if doublon:
                break
        if not doublon:
            uniq_dicts += [dict]
    return uniq_dicts


def check_list(lst, name):
    print("\n* {} found in files:".format(name))
    for val in lst:
        print("  - {}".format(val))
    return lst

def check_files(files):
    num_files = len(files)
    dims, vars = get_dimvars(files[0])
    print("\n* Checking variables in files... Process can be interrupted using CTRL+C")
    try:
        for i in tqdm(range(1, num_files)):
            dims_i, vars_i = get_dimvars(files[i])
            dims += dims_i
            vars += vars_i
    except KeyboardInterrupt:
        print("Checking interrupted.")

    check_list(check_dict(dims), "Dimensions")
    vars = list(set([var for var in vars if "_bnd" not in var]))
    check_list(vars, "Variables")

    if len(vars) == 1:
        return vars[0]
    else:
        print("Cannot use netcdf files with multiple variables. You can use ncselvar to select variables.")
        exit()

