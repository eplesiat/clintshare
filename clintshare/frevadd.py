
import yaml
from datetime import datetime
import argparse
from .utils.parser import strparser
import os
from threading import Thread
import numpy as np
from .utils.mdio import read_data, write_data

def split_list(lst, n):
    return [lst[i * n:(i + 1) * n] for i in range((len(lst) + n - 1) // n )]

def frevadd():

    parser = argparse.ArgumentParser()
    parser.add_argument('ymlfile', type=str, help="Yaml containing the NetCDF files and their associated members to be ingested with Freva")
    parser.add_argument('-p', '--product', type=str, default=None, help="Product of original data")
    parser.add_argument("-i", "--institute", type=str, default=None, help="Institute of original data")
    parser.add_argument("-o", "--model", type=str, default=None, help="Model of original data")
    parser.add_argument("-e", "--experiment", type=str, default=None, help="Experiment of original data")
    parser.add_argument("-n", "--nthreads", type=int, default=None, help="Number of threads")
    parser.add_argument("-c", "--path_crawl", type=str, default=None, help="Path where the crawl data will be stored")
    parser.add_argument("-r", "--path_registry", type=str, default=None, help="Path of the registry")
    parser.add_argument("-d", "--dataid", type=str, default=None, help="Dataid")
    parser.add_argument("-u", "--userid", type=str, default=None, help="Userid")
    args = parser.parse_args()
    
    with open(args.ymlfile, "r") as f:
        memfiles = yaml.safe_load(f)

    num_files = len(memfiles)
    print("\n* Number of files: {}".format(num_files))
    #batches = np.array_split(np.arange(num_files), args.nthreads)
    batches = split_list(np.arange(num_files), args.nthreads)

    def exec(instruc):
        print(">", instruc)
        return os.popen(instruc).read()

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
        if count == num_files:
            return "Yes - {}".format(date)
        elif count > num_files:
            return "Yes ({}) - {}".format(count, date)
        else:
            return "Failed ({}) - {}".format(count, date)


    class add_data(Thread):
        def __init__(self, attributes, member, file):
            Thread.__init__(self)
            self.res = None
            self.attributes = attributes
            self.member = member
            self.file = file

        def run(self):
            self.res = exec("freva user-data add {} --institute {} --model {} --experiment {} --ensemble '{}' {}"
                    .format(*self.attributes, self.member, self.file))

    attributes = [args.product, args.institute, args.model, args.experiment]

    start_time = datetime.now()

    ok_add, count_add = True, 0
    for batch in batches:
        threads = []
        for idx in batch:
            threads.append(add_data(attributes, *memfiles[idx]))
            threads[-1].start()

        for thread in threads:
            thread.join()

        ok_add, count_add = check_status(threads, ok_add, count_add)

    count_index = 0

    if ok_add:
        res = exec("freva user-data index {}".format(args.path_crawl))

        print("\n* Log freva index:\n {}".format(res))
        
        if "ok" in res:
            files = exec("freva databrowser project=user-{} product={} institute={} model={} experiment={}"
                    .format(args.userid, *attributes)).split()
            end_time = datetime.now()
            
            print("\n* Start indexing time:", start_time)
            print("* End indexing time:", end_time)
            
            for file in files:
                date = datetime.fromtimestamp(os.path.getmtime(file))
                if date > start_time and date < end_time:
                    count_index += 1

            print("\n* Number of files indexed: {}".format(count_index))

    md_text, idx, ans_dict = read_data(args.path_registry, args.dataid)
    ans_dict["CMORized"] = get_status(count_add, num_files, end_time)
    ans_dict["Indexed"] = get_status(count_index, num_files, end_time)
    write_data(args.path_registry, ans_dict, md_text, idx)

if __name__ == "__main__":
    frevadd()
