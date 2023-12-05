
import argparse
import os
import glob
import pathlib
import yaml
from datetime import datetime
from .utils.frevasub import subfreva
from .utils.mdio import read_data, write_data
from .utils.interactive import quitkeep, form
from .utils.parser import confparser, remember, create_dict
from .utils.frevacheck import frevacheck
from .utils.commit import commit_registry

def clintshare():

    date = datetime.now()

    parser = argparse.ArgumentParser()
    parser.add_argument('data_path', type=str, help="Path or pattern of paths of the directories or NetCDF files to be shared")
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
    query_dict, filter_dict = yaml.safe_load(open(conf_dir / "query.yml", "r"))

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
   
    if os.path.isdir(args.data_path):
        args.data_path += "/**"
    files = [file for file in glob.glob(args.data_path, recursive=True) if file[-3:] == ".nc"]
    num_files = len(files)

    assert len(files) != 0, "No NetCDF files found in provided paths."
    print("\n* {} NetCDF files found in provided paths.".format(num_files))

    if conf_dict["nthreads"] == 0:
        conf_dict["nthreads"] = num_files
    if conf_dict["nthreads"] > conf_dict["max_threads"]:
        conf_dict["nthreads"] = conf_dict["max_threads"]
        print("* Number of threads has been automatically reduced to the maximum value {}".format(conf_dict["max_threads"]))

    files = [os.path.abspath(file) for file in files]

    md_text, idx = None, None
    
    if args.update is None:
        print("\nTo proceed with data sharing, it is required to answer the {} following questions.".format(n_queries))
        print("For each question, you can enter empty field for help or 'back' to go to the previous question.")
        quitkeep("Do you want to continue?")
        ans_dict = {"Dataid": dataid}
        index_data = True
    else:
        md_text, idx, ans_dict = read_data(conf_dict["path_registry"], args.update)
        if idx is not None and userid == ans_dict["Userid"] and args.data_path == ans_dict["Data path"]:
            print("\nFound data registered with the following information:\n", ans_dict)
            quitkeep("\nDo you want to update this data?")
        else:
            raise Exception("Did not find any data registered with dataid {}, userid {} and data path {}."
                    .format(args.update, userid, args.data_path))

        keys = [key for key in keys if key not in filter_dict]
    
    members = remember(files, args.member, args.regex, args.varpar)
    ans_dict = create_dict(ans_dict, date, userid, username, args.data_path, files, keys)
    ans_dict = form(query_dict, ans_dict, help_dict, keys, userid)

    frevacheck(ans_dict, conf_dict["project"])
    write_data(conf_dict["path_registry"], ans_dict, md_text, idx)
    commit_registry(conf_dict["path_repo"], conf_dict["path_registry"], username, update=args.update, verbose=False)

    print("\n* Data have been successfully registered!")

    quitkeep("Do you want to (re-)ingest the files using Freva?")
    subfreva(conf_dict, ans_dict, files, members, username, dataid)
    print("* Data ingestion is running in the background...")

if __name__ == "__main__":
    clintshare()
