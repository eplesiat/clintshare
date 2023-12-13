
import yaml
from datetime import datetime
import argparse
from .utils.parser import strparser
import os
from threading import Thread
import numpy as np
from .utils.catalog_io import get_data, write_data
from .utils.interactive import exec
from .utils.parser import argvar
from .utils.commit import commit_catalog

def split_list(lst, n):
    return [lst[i * n:(i + 1) * n] for i in range((len(lst) + n - 1) // n )]

def frevadd():

    parser = argparse.ArgumentParser()
    parser.add_argument('memfile', type=str, help="Yaml containing the NetCDF files and their associated members to be ingested with Freva")
    parser.add_argument('ansfile', type=str, help="Yaml containing the information about the data")
    parser.add_argument('confile', type=str, help="Yaml containing the configuration parameters")
    parser.add_argument('dataid', type=str, help="Dataid")
    args = parser.parse_args()
    
    with open(args.memfile, "r") as f:
        memfiles = yaml.safe_load(f)
    with open(args.ansfile, "r") as f:
        ans_dict = yaml.safe_load(f)
    with open(args.confile, "r") as f:
        conf_dict = yaml.safe_load(f)

    num_files = len(memfiles)
    print("\n* Number of files: {}".format(num_files))
    #batches = np.array_split(np.arange(num_files), args.nthreads)
    batches = split_list(np.arange(num_files), conf_dict["nthreads"])

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
        def __init__(self, attributes, member, file, how):
            Thread.__init__(self)
            self.res = None
            self.attributes = attributes
            self.member = member
            self.file = file
            self.how = how

        def run(self):
            self.res = exec("freva user-data add {} --project {} --institute {} --model {} --experiment {} --variable {} --ensemble '{}' --how {} {}"
                    .format(*self.attributes, self.member, self.how, self.file))

    attributes = [ans_dict["Product"], conf_dict["project"], ans_dict["Institute"], ans_dict["Model"], ans_dict["Experiment"], ans_dict["Variable"]]
    link = " - [Link Freva]({}/solr/data-browser/?product={}&project={}&institute={}&model={}&experiment={}&variable={})"
    link = link.format(conf_dict["freva_url"], *attributes)

    start_time = datetime.now()
    ok_add, count_add = True, 0
    for batch in batches:
        threads = []
        for idx in batch:
            threads.append(add_data(attributes, *memfiles[idx], conf_dict["add_method"]))
            threads[-1].start()

        for thread in threads:
            thread.join()

        ok_add, count_add = check_status(threads, ok_add, count_add)

    print("\n* Number of files CMORized: {}".format(count_add))
    catalog = yaml.safe_load(open(conf_dict["path_catalog"]))
    
    files = exec("freva databrowser product={} project={} institute={} model={} experiment={} variable={}"
                .format(*attributes)).split()
        
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

    ans_dict["Indexed"] += link
    catalog[args.dataid] = ans_dict
    write_data(conf_dict["path_catalog"], conf_dict["path_markdown"], conf_dict["path_header"], catalog)
    commit_catalog(conf_dict["path_repo"], conf_dict["path_catalog"], conf_dict["path_markdown"], ans_dict["Username"], ingest=True)
    
    exec("chmod -R g+rw {}/{}".format(conf_dict["path_storage"], ans_dict["Product"]))

    if not ok_add or not ok_index:
        exit()
    
    if conf_dict["clean_tmp"]:
        os.remove(args.memfile)
        os.remove(args.ansfile)
        os.remove(args.confile)

if __name__ == "__main__":
    frevadd()
