import os
import yaml

def subfreva(conf_dict, ans_dict, files, members, username, dataid):
    
    ymlfile = "{}/memfiles_{}.yml".format(conf_dict["path_tmp"], dataid)
    with open(ymlfile, "w") as f:
        yaml.dump([list(i) for i in zip(members, files)], f)

    header = f"""#!/usr/bin/env bash
    
#SBATCH -J frevadd
#SBATCH --output {conf_dict["path_log"]}/frevadd_%j.log
#SBATCH --account={conf_dict["account"]}
#SBATCH --cpus-per-task={conf_dict["nthreads"]}
#SBATCH --time={conf_dict["time"]}
#SBATCH --mem={conf_dict["mem"]}G
#SBATCH --partition={conf_dict["partition"]}
#SBATCH --mail-type=ALL
#SBATCH --mail-user={conf_dict["email"]}

module load {conf_dict["modules"]}

"""

    if conf_dict["clean_tmp"]:
        clean_tmp = "-t"
    else:
        clean_tmp = ""

    slurmfile = "{}/slurm_{}.sh".format(conf_dict["path_tmp"], dataid)
    f = open(slurmfile, "w")
    print(header, file=f)
    
    args = [ymlfile, ans_dict["Product"], ans_dict["Institute"], ans_dict["Model"], ans_dict["Experiment"], ans_dict["Variable"],
                conf_dict["nthreads"], conf_dict["project"], conf_dict["path_repo"], conf_dict["path_catalog"],
                conf_dict["path_header"], conf_dict["path_markdown"], conf_dict["path_storage"], dataid, conf_dict["add_method"], clean_tmp]
    args = [str(arg).replace(" ", "-") for arg in args]
    
    print("frevadd {} -p {} -i {} -m {} -e {} -v '{}' -n {} -j {} -r {} -c {} -a {} -k {} -s{} -d {} -w {} {} -u '{}'".format(*args, username), file=f)
    f.close()

    os.system("sbatch {}".format(slurmfile))
    if conf_dict["clean_tmp"]:
        os.remove(slurmfile)
