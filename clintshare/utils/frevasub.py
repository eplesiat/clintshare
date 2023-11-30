import os
import yaml

def subfreva(conf_dict, ans_dict, files, members, userid):
    
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

    exec = "python -m clintshare.frevadd"
    
    f = open("freva-slurm.sh", "w")
    print(header, file=f)
    print("{} {} -p {} -i {} -o {} -e {} -n {} -c {} -r {} -d {} -u {}".format(exec, os.path.abspath(ymlfile),
                ans_dict["Product"], ans_dict["Institute"], ans_dict["Model"], ans_dict["Experiment"],
                conf_dict["nthreads"], conf_dict["path_crawl"] + userid, conf_dict["path_registry"], ans_dict["Dataid"], userid), file=f)
    f.close()

    os.system("sbatch freva-slurm.sh")
