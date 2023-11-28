
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
    args = parser.parse_args()

    files = np.array_split(np.array(args.files), args.nthreads)

    def check_status(threads):
        status = "Done"
        for thread in threads:
            if "ok" not in thread.res:
                status = "Failed"
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

    threads = []
    for batch in files:
        threads.append(add_data(attributes, batch))
        threads[-1].start()

    for thread in threads:
        thread.join()

    status_add = check_status(threads)

    res = os.popen("echo freva user-data index {}".format(args.path_crawl)).read()
    if "ok" in res:
        status_index = "Done"
    else:
        status_index = "Failed"

    md_text, idx, ans_dict = read_data(args.path_registry, args.dataid)
    ans_dict["CMORized"] = status_add
    ans_dict["Indexed"] = status_index
    write_data(args.path_registry, ans_dict, md_text, idx)

if __name__ == "__main__":
    frevadd()