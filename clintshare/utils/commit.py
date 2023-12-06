
from .interactive import exec

def commit_catalog(path_repo, path_catalog, path_markdown, username, update=None, ingest=False, verbose=True):
    print("\n* Committing changes to catalog")
    
    assert update is None or ingest is False
    
    if ingest is False:
        if update is None:
            message =  "Add new data to catalog"
        else:
            message =  "Update existing data in catalog"
    else:
        message =  "Change status of data in catalog"
   
    cmd = "git -C {}".format(path_repo)
    cmd = "module load git; {} config user.name '{}' ; {} config user.email None; {}".format(cmd, username, cmd, cmd)

    exec("{} add {} {}".format(cmd, path_catalog, path_markdown), verbose=verbose)
    exec("{} commit -m '{}'".format(cmd, message), verbose=verbose)
