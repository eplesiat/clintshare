
from datetime import datetime, timezone
import argparse
import os
from threading import Thread
import numpy as np
from .utils.mdio import read_data, write_data

def frevadd():

    parser = argparse.ArgumentParser()
    parser.add_argument('files', type=str, help="NetCDF files to be ingested with Freva", nargs="+")
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

    status = {True: "Yes", False: "Failed"}

    num_files = len(args.files)
    print("\n* Number of files: {}".format(num_files))
    files = np.array_split(np.array(args.files), args.nthreads)

    def check_status(threads):
        print("\n* Log freva add:")
        status = True
        for thread in threads:
            print(thread.res)
            if "ok" not in thread.res:
                status = False
        return status


    class add_data(Thread):
        def __init__(self, attributes, batch):
            Thread.__init__(self)
            self.res = None
            self.attributes = attributes
            self.batch = batch

        def run(self):
            self.res = os.popen("freva user-data add {} --institute {} --model {} --experiment {} {}"
                     .format(*self.attributes, " ".join(map(str, self.batch)))).read()

    attributes = [args.product, args.institute, args.model, args.experiment]

    start_time = datetime.now(timezone.utc)

    threads = []
    for batch in files:
        threads.append(add_data(attributes, batch))
        threads[-1].start()

    for thread in threads:
        thread.join()

    ok_add = check_status(threads)
    ok_index = False
    
    if ok_add:
        res = os.popen("freva user-data index {}".format(args.path_crawl)).read()

        print("\n* Log freva index:\n {}".format(res))
        
        if "ok" in res:
            res = os.popen("freva databrowser project=user-{} --facet creation_time".format(args.userid)).read()
            res = res.replace("creation_time: ","").split(",")

            end_time = datetime.now(timezone.utc)
            print("\n* Start indexing time:", start_time)
            print("* End indexing time:", end_time)
            print("* List of retrieved dates:", *res)

            k = 0
            for date in res:
                date = datetime.strptime(date.strip(), '%Y-%m-%dT%H:%M:%S.%f%z')
                if date > start_time and date < end_time:
                    k += 1

            print("\n* Number of files indexed: {}".format(k))
            if k == num_files:
                ok_index = True

    md_text, idx, ans_dict = read_data(args.path_registry, args.dataid)
    ans_dict["CMORized"] = status[ok_add]
    ans_dict["Indexed"] = status[ok_index]
    write_data(args.path_registry, ans_dict, md_text, idx)

if __name__ == "__main__":
    frevadd()
