
import argparse
import os
import glob
import pathlib
import yaml
from .utils.freva_sub import subfreva
from .utils.catalog_io import get_data, write_data
from .utils.interactive import get_id, quitkeep, form
from .utils.parser import confparser
from .utils.checker import check_files
from .utils.dict_utils import remember, create_dict
from .utils.freva_check import freva_check
from .utils.commit import commit_catalog

def clintshare():

    parser = argparse.ArgumentParser()
    parser.add_argument('data_path', type=str, help="Path or pattern of paths of the directories or NetCDF files to be shared")
    parser.add_argument('-u', '--update', type=str, default=None, help="Update data using data_id")
    parser.add_argument("--mem", type=str, default=None, help="Memory in MB")
    parser.add_argument("-t", "--time", type=str, default=None, help="Time in h:mn:s")
    parser.add_argument("-l", "--nodelist", type=str, default=None, help="NODES")
    parser.add_argument("-p", "--partition", type=str, default=None, help="Partition name")
    parser.add_argument("-n", "--nthreads", type=int, default=None, help="Number of threads")
    parser.add_argument("-a", "--add_method", type=str, default=None, help="Method to add files with Freva (copy,move,symlink,link)")
    parser.add_argument("-v", "--varpar", type=str, default="r", choices=["r", "i", "p"], help="Character defining the varying member parameter")
    parser.add_argument("-e", "--ens_regex", type=str, default=None, help="Regex expression to parse ensemble member "
                                                                      "from filenames (e.g., .*mem(\d+).*)")
    parser.add_argument("-m", "--member", type=str, default=None, help="Ensemble member used for all files")
    parser.add_argument("-r", "--path_repo", type=str, default=None, help="Path of the repository")
    args = parser.parse_args()

    path_dir = pathlib.Path(__file__).parent
    conf_dir = path_dir / "config"
    query_dict, filter_dict = yaml.safe_load(open(conf_dir / "query.yml", "r"))

    keys = list(query_dict.keys())
    n_queries = len(keys)

    print("CLINTshare: Data sharing tool for CLINT members")

    date, username, userid, dataid = get_id()
    
    conf_dict = yaml.safe_load(open(conf_dir / "config.yml", "r"))
    conf_dict = confparser(conf_dict, args)

    help_dict = yaml.safe_load(open(conf_dir / "help.yml", "r"))
    
    for path in ("path_catalog", "path_markdown", "path_header"):
        conf_dict[path] = "{}/{}".format(conf_dict["path_repo"], conf_dict[path])
    catalog = yaml.safe_load(open(conf_dict["path_catalog"]))

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
        ans_dict = {}
    else:
        ans_dict = get_data(catalog, args.update, userid=userid, data_path=args.data_path)
        dataid = args.update
        keys = [key for key in keys if key not in filter_dict]
    
    variable, time_freq = check_files(files)
    members = remember(files, args.member, args.ens_regex, args.varpar)
    ans_dict = create_dict(ans_dict, date, userid, username, conf_dict["contacts_url"], variable, time_freq, args.data_path, files, keys)
    ans_dict = form(query_dict, ans_dict, help_dict, keys, userid)

    freva_check(ans_dict, conf_dict["project"], conf_dict["modules"])
    catalog[dataid] = ans_dict
    write_data(conf_dict["path_catalog"], conf_dict["path_markdown"], conf_dict["path_header"], catalog)
    commit_catalog(conf_dict["path_repo"], conf_dict["path_catalog"], conf_dict["path_markdown"],
                   username, "Add new data to catalog", update=args.update, verbose=False)

    print("\n* Data have been successfully registered!")

    quitkeep("Do you want to (re-)ingest the files using Freva?")
    subfreva(conf_dict, ans_dict, files, members, dataid)
    print("* Data ingestion is running in the background...")

if __name__ == "__main__":
    clintshare()
