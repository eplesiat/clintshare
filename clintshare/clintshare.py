
import argparse
import os
import glob
import pathlib
import yaml
from datetime import datetime
from .utils.frevasub import subfreva
from .utils.mdio import read_data, write_data
from .utils.parser import confparser, remember

def quitkeep(text):
    choice = input(text + " (y/n)\n")
    if choice.lower() == "n" or choice.lower() == "no":
        exit()

def clintshare():

    date = datetime.now()

    parser = argparse.ArgumentParser()
    parser.add_argument('path_data', type=str, help="Path of the directory containing the NetCDF files to be shared")
    parser.add_argument('-u', '--update', type=str, default=None, help="Update data using data_id")
    parser.add_argument("-a", "--account", type=str, default=None, help="Account name")
    parser.add_argument("--mem", type=str, default=None, help="Memory in MB")
    parser.add_argument("-t", "--time", type=str, default=None, help="Time in h:mn:s")
    parser.add_argument("-l", "--nodelist", type=str, default=None, help="NODES")
    parser.add_argument("-p", "--partition", type=str, default=None, help="Partition name")
    parser.add_argument("-n", "--nthreads", type=int, default=None, help="Number of threads")
    parser.add_argument("-v", "--varpar", type=str, default="r", choices=["r", "i", "p"], help="Character defining the varying member parameter")
    parser.add_argument("-r", "--regex", type=str, default=None, help="Regex expression to parse ensemble member "
                                                                      "from filenames (e.g., .*mem(\d+).*)")
    parser.add_argument("-m", "--member", type=str, default=None, help="Ensemble member used for all files")
    args = parser.parse_args()

    path_dir = pathlib.Path(__file__).parent
    conf_dir = path_dir / "config"
    query_dict = yaml.safe_load(open(conf_dir / "query.yml", "r"))

    keys = list(query_dict.keys())
    n_queries = len(keys)

    print("CLINTshare: Data sharing tool for CLINT members")

    dataid = date.strftime("%d%m%Y.%H%M%S")
    userid = os.popen("echo $USER").read().strip()
    username = os.popen("pinky -lb $USER").read().strip().split(":")[-1].strip()

    conf_dict = yaml.safe_load(open(conf_dir / "config.yml", "r"))
    conf_dict = confparser(conf_dict, args)
    conf_dict["frevadd"] = pathlib.Path(__file__).parent.parent

    help_dict = yaml.safe_load(open(conf_dir / "help.yml", "r"))

    md_text, idx = None, None
    
    if args.update is None:
        print("\nTo proceed with data sharing, it is required to answer the {} following questions.".format(n_queries))
        print("For each question, you can enter empty field for help or 'back' to go to the previous question.")
        quitkeep("Do you want to continue?")
        ans_dict = {"Dataid": dataid}
    else:
        md_text, idx, ans_dict = read_data(conf_dict["path_registry"], args.update)
        if idx is not None and userid == ans_dict["Userid"]:

            print("Found data registered with the following information:\n", ans_dict)
            quitkeep("\nDo you want to update this data?")
        else:
            print("Error! Did find any data registered with dataid {} and userid {}.".format(args.update, userid))
            exit()

    files = glob.glob(args.path_data + "/*.nc")
    num_files = len(files)

    assert num_files != 0, "No NetCDF files found in {}".format(args.path_data)
    files = [os.path.abspath(file) for file in files]
    size_files = sum(os.path.getsize(file) for file in files) / (1024 ** 2)
    
    if conf_dict["nthreads"] == 0:
        conf_dict["nthreads"] = num_files

    members = ["" for i in range(num_files)]
    if args.member is not None:
        members = [args.member for i in range(num_files)]

    if args.regex:
        assert conf_dict["nthreads"] == num_files, "regex option requires nthreads = 0"
        members = remember(files, members, args.regex, args.varpar)

    ans_dict.update({"Modified date": date.strftime("%d/%m/%Y %H:%M:%S"),
                "Userid": userid,
                "username": username,
                "Data path": args.path_data,
                "Number of files": num_files,
                "Total size (in Mb)": round(size_files),
                "CMORized": "No",
                "Indexed": "No"
                })

    if args.update is None:
        ans_dict.update({key: "" for key in keys})
    k = 0
    while k < n_queries:
        key = keys[k]
        ans = input("\n[{}/{}] {}:\n{}\r".format(k + 1, n_queries, query_dict[key], ans_dict[key])).strip()
        if ans == "":
            if ans_dict[key] == "":
                print(help_dict[key])
            else:
                k += 1
        elif ans == "back":
            k -= 1
        else:
            ans_dict[key] = ans
            k += 1

    if ans_dict["Product"][:6] != "clint-":
        ans_dict["Product"] = "clint-" + ans_dict["Product"]

    write_data(conf_dict["path_registry"], ans_dict, md_text, idx)

    print("Data have been successfully registered!")

    subfreva(conf_dict, ans_dict, files, userid, members)

    print("Data ingestion to Freva is running in the background...")

if __name__ == "__main__":
    clintshare()
