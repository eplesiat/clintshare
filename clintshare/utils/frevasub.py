import os
import yaml

def subfreva(conf_dict, ans_dict, files, members, username, projectdir):
    
    ymlfile = "memfiles.yml"
    with open(ymlfile, "w") as f:
        yaml.dump([list(i) for i in zip(members, files)], f)

    header = f"""#!/usr/bin/env bash
    
#SBATCH -J frevadd
#SBATCH --output frevadd_%j.log
#SBATCH --account={conf_dict["account"]}
#SBATCH --cpus-per-task={conf_dict["nthreads"]}
#SBATCH --time={conf_dict["time"]}
#SBATCH --mem={conf_dict["mem"]}G
#SBATCH --partition={conf_dict["partition"]}

module load clint xces

cd {conf_dict["frevadd"]}

"""

    cmd = "python -m clintshare.frevadd"
    if ans_dict["Variable"] is None:
        variable = ""
    else:
        variable = "-v '{}'".format(ans_dict["Variable"])
    
    f = open("freva-slurm.sh", "w")
    print(header, file=f)
    print("{} {} -p {} -i {} -m {} -e {} {} -n {} -c {} -g {} -r {} -l {} -u '{}' -d {}".format(cmd, os.path.abspath(ymlfile),
                ans_dict["Product"], ans_dict["Institute"], ans_dict["Model"], ans_dict["Experiment"], variable,
                conf_dict["nthreads"], conf_dict["path_crawl"], conf_dict["path_repo"], conf_dict["path_registry"],
                ans_dict["Dataid"], username, projectdir), file=f)
    f.close()

    os.system("sbatch freva-slurm.sh")
