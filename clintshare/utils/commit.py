
from .interactive import exec

def commit_registry(path_repo, path_registry, username, update=None, ingest=False, verbose=True):
    print("\n* Committing changes to registry")
    
    assert update is None or ingest is False
    
    if ingest is False:
        if update is None:
            message =  "Add new data to registry"
        else:
            message =  "Update existing data in registry"
    else:
        message =  "Change status of data in registry"
   
    cmd = "git -C {}".format(path_repo)
    cmd = "module load git; {} config user.name '{}' ; {} config user.email None; {}".format(cmd, username, cmd, cmd)

    exec("{} add {}".format(cmd, path_registry), verbose=verbose)
    exec("{} commit -m '{}'".format(cmd, message), verbose=verbose)
