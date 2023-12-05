import os
import yaml

def subfreva(conf_dict, ans_dict, files, members, username):
    
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
#SBATCH --mail-type=ALL
#SBATCH --mail-user={conf_dict["email"]}

module load clint xces share

"""

    cmd = "python -m clintshare.frevadd"
    if ans_dict["Variable"] is None:
        variable = ""
    else:
        variable = "-v '{}'".format(ans_dict["Variable"])
    
    f = open("freva-slurm.sh", "w")
    print(header, file=f)
    print("{} {} -p {} -i {} -m {} -e {} {} -n {} -j {} -g {} -r {} -d {} -u '{}'".format(cmd, os.path.abspath(ymlfile),
                ans_dict["Product"], ans_dict["Institute"], ans_dict["Model"], ans_dict["Experiment"], variable,
                conf_dict["nthreads"], conf_dict["project"], conf_dict["path_repo"], conf_dict["path_registry"],
                ans_dict["Dataid"], username), file=f)
    f.close()

    os.system("sbatch freva-slurm.sh")
