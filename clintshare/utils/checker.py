import xarray as xr
from tqdm import tqdm
import glob
import os
import warnings

def time_table():
    return {
        315360000: "dec",
        31104000: "yr",
        2419200: "mon",
        1296000: "sem",
        86400: "day",
        21600: "6h",
        10800: "3h",
        3600: "hr",
        1: "subhr",
    }

def dt_to_cmor_frequency(dts):
    frequencies = []
    for dt in dts:
        for total_seconds, frequency in time_table().items():
            if dt >= total_seconds:
                print(dt, frequency)
                frequencies.append(frequency)
                break
    frequencies = list(set(frequencies))
    if len(frequencies) == 1:
        return frequencies
    else:
        return ["fx"]

def get_time_frequency(ds):

    time_freq = ds.attrs.get("frequency", "")

    try:
        times = ds["time"].values[:]
    except:
        times = []

    nt = len(times)
    if nt > 1:
        dts = [abs(times[i+1]-times[i]).total_seconds() for i in range(nt-1)]
    else:
        dts = [0]

    if time_freq in time_table().values():
        return [time_freq]
    else:
        return dt_to_cmor_frequency(dts)

def get_dimvars(file):
    warnings.simplefilter("ignore")
    ds = xr.open_dataset(file, use_cftime=True)
    return [dict(ds.dims)], list(ds.keys()), get_time_frequency(ds)

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


def print_list(lst, name):
    print("\n* {} found in files:".format(name))
    for val in lst:
        print("  - {}".format(val))
    return lst

def check_list(lst, name):
    lst = list(set(lst))
    print_list(lst, name)
    if len(lst) == 1:
        return lst[0]
    else:
        print("Cannot use netcdf files with multiple {}.".format(name.lower()))
        exit()

def check_files(files):
    num_files = len(files)
    dims, vars, freqs = get_dimvars(files[0])
    print("\n* Checking variables in files... Process can be interrupted using CTRL+C")
    try:
        for i in tqdm(range(1, num_files)):
            dims_i, vars_i, freqs_i = get_dimvars(files[i])
            dims += dims_i
            vars += vars_i
            freqs += freqs_i
    except KeyboardInterrupt:
        print("Checking interrupted.")

    print_list(check_dict(dims), "Dimensions")
    var = check_list([var for var in vars if "_bnd" not in var], "Variables")
    freq = check_list(freqs, "Time frequencies")

    return var, freq
