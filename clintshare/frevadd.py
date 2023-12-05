
import yaml
from datetime import datetime
import argparse
from .utils.parser import strparser
import os
from threading import Thread
import numpy as np
from .utils.mdio import read_data, write_data
from .utils.interactive import exec
from .utils.parser import argvar
from .utils.commit import commit_registry

def split_list(lst, n):
    return [lst[i * n:(i + 1) * n] for i in range((len(lst) + n - 1) // n )]

def frevadd():

    parser = argparse.ArgumentParser()
    parser.add_argument('ymlfile', type=str, help="Yaml containing the NetCDF files and their associated members to be ingested with Freva")
    parser.add_argument('-p', '--product', type=str, default=None, help="Product of original data")
    parser.add_argument("-i", "--institute", type=str, default=None, help="Institute of original data")
    parser.add_argument("-m", "--model", type=str, default=None, help="Model of original data")
    parser.add_argument("-e", "--experiment", type=str, default=None, help="Experiment of original data")
    parser.add_argument("-v", "--variable", type=str, default=None, help="Variable of original data")
    parser.add_argument("-n", "--nthreads", type=int, default=None, help="Number of threads")
    parser.add_argument("-j", "--project", type=str, default=None, help="Project")
    parser.add_argument("-g", "--path_repo", type=str, default=None, help="Path of the git repository")
    parser.add_argument("-r", "--path_registry", type=str, default=None, help="Path of the registry")
    parser.add_argument("-d", "--dataid", type=str, default=None, help="Dataid")
    parser.add_argument("-u", "--username", type=str, default=None, help="Username")
    args = parser.parse_args()
    
    with open(args.ymlfile, "r") as f:
        memfiles = yaml.safe_load(f)

    num_files = len(memfiles)
    print("\n* Number of files: {}".format(num_files))
    #batches = np.array_split(np.arange(num_files), args.nthreads)
    batches = split_list(np.arange(num_files), args.nthreads)

    def check_status(threads, ok_add, count_add):
        print("\n* Log freva add:")
        for thread in threads:
            print(thread.res)
            if "ok" in thread.res:
                count_add += 1
            else:
                ok_add = False
        return ok_add, count_add

    def get_status(count, num_files, time):
        date = time.strftime("%d/%m/%Y %H:%M:%S")
        if count >= num_files:
            return True, "{} files - {}".format(count, date)
        else:
            return False, "Failed ({}) - {}".format(count, date)


    class add_data(Thread):
        def __init__(self, attributes, variable, member, file):
            Thread.__init__(self)
            self.res = None
            self.attributes = attributes
            self.variable = variable
            self.member = member
            self.file = file

        def run(self):
            self.res = exec("freva user-data add {} --project {} --institute {} --model {} --experiment {} --variable '{}' --ensemble '{}' {}"
                    .format(*self.attributes, self.variable, self.member, self.file))

    attributes = [args.product, args.project, args.institute, args.model, args.experiment]

    start_time = datetime.now()

    ok_add, count_add = True, 0
    for batch in batches:
        threads = []
        for idx in batch:
            threads.append(add_data(attributes, args.variable, *memfiles[idx]))
            threads[-1].start()

        for thread in threads:
            thread.join()

        ok_add, count_add = check_status(threads, ok_add, count_add)

    print("\n* Number of files CMORized: {}".format(count_add))
    md_text, idx, ans_dict = read_data(args.path_registry, args.dataid)

    if not ok_add:
        ans_dict["Indexed"] = get_status(count_add, num_files, start_time)
    else:
        files = exec("freva databrowser product={} project={} institute={} model={} experiment={} {}"
                    .format(*attributes, argvar(args.variable))).split()
        
        end_time = datetime.now()
        print("\n* Start indexing time:", start_time)
        print("* End indexing time:", end_time)

        count_index = 0
        for file in files:
            date = datetime.fromtimestamp(os.path.getmtime(file))
            if date > start_time and date < end_time:
                count_index += 1

        print("\n* Number of files indexed: {}".format(count_index))

        ok_index, ans_dict["Indexed"] = get_status(count_index, num_files, end_time)

    write_data(args.path_registry, ans_dict, md_text, idx)
    commit_registry(args.path_repo, args.path_registry, args.username, ingest=True) 
    
    if not ok_add or not ok_index:
        exit()

if __name__ == "__main__":
    frevadd()
