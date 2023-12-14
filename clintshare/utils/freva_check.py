
import os
from .interactive import quitkeep, printfiles
from .parser import argvar

def freva_check(ans_dict, project, modules):

    print("\n* Checking for indexed files...")
    files = os.popen("module load {} 2>&1 >/dev/null;"\
            "freva databrowser project={} product={} institute={} model={} experiment={} variable={} time_frequency={}"
                    .format(modules, project, ans_dict["Product"], ans_dict["Institute"], ans_dict["Model"],
                        ans_dict["Experiment"], ans_dict["Variable"], ans_dict["Time frequency"])).read().split()
    
    num_files = len(files)
    if num_files > 0:
        print("\n* {} files are already indexed with the same facets:".format(num_files))
        printfiles(files)

        print("\nThese indexed files might differ from the new files by dates, members, variables.")
        print("These indexed files will be overwritten if the new files have the same metadata.")
        quitkeep("Do you want to continue?")

