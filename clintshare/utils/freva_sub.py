import os
from .catalog_io import write_yaml

def subfreva(conf_dict, ans_dict, files, members, dataid):
    
    memfile = write_yaml([list(i) for i in zip(members, files)],
            conf_dict["path_tmp"], "memfile", dataid)
    ansfile = write_yaml(ans_dict, conf_dict["path_tmp"], "ansfile", dataid)
    confile = write_yaml(conf_dict, conf_dict["path_tmp"], "confile", dataid)

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
source activate {os.environ["CONDA_PREFIX"]}

"""

    slurmfile = "{}/slurm_{}.sh".format(conf_dict["path_tmp"], dataid)
    f = open(slurmfile, "w")
    print(header, file=f)
    print("frevadd {} {} {} {}".format(memfile, ansfile, confile, dataid), file=f)
    f.close()

    os.system("sbatch {}".format(slurmfile))
    if conf_dict["clean_tmp"]:
        os.remove(slurmfile)
