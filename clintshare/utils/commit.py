
from .interactive import exec

def commit_catalog(path_repo, path_catalog, path_markdown, username, message, update=None, verbose=True):
    print("\n* Committing changes to catalog")
    
    if update is not None:
        message =  "Update existing data in catalog"
   
    cmd = "git -C {}".format(path_repo)
    cmd = "module load git; {} config user.name '{}' ; {} config user.email None; {}".format(cmd, username, cmd, cmd)

    exec("{} add {} {}".format(cmd, path_catalog, path_markdown), verbose=verbose)
    exec("{} commit -m '{}'".format(cmd, message), verbose=verbose)
