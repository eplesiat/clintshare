
import os
from .interactive import quitkeep

def frevacheck(ans_dict, userid):

    n_print = 5
    n_chars = 100
    
    print("\n* Checking for indexed files...")
    files = os.popen("module load clint xces 2>&1 >/dev/null; freva databrowser project=user-{} product={} institute={} model={} experiment={}"
                    .format(userid, ans_dict["Product"], ans_dict["Institute"], ans_dict["Model"], ans_dict["Experiment"])).read().split()
    
    num_files = len(files)
    if num_files > 0:
        print("\n* {} files are already indexed with the same facets:".format(num_files))
        if num_files <= n_print * 2:
            for file in files:
                print("..." + file[-n_chars:])
        else:
            for file in files[:n_print]:
                print("..." + file[-n_chars:])
            print("   ...   ")
            for file in files[-n_print:]:
                print("..." + file[-n_chars:])

        print("\nThese indexed files might differ from the new files by dates, members, variables.")
        print("These indexed files will be overwritten if the new files have the same metadata.")
        quitkeep("Do you want to continue?")

