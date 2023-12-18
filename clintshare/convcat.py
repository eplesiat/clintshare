import argparse
import os
import pathlib
import yaml
from .utils.catalog_io import write_data
from .utils.commit import commit_catalog
from .utils.parser import confparser
from .utils.interactive import get_id

def convcat():

    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--message", type=str, default=None, help="Commit message")
    parser.add_argument("-r", "--path_repo", type=str, default=None, help="Path of the repository")
    args = parser.parse_args()

    path_dir = pathlib.Path(__file__).parent
    conf_dir = path_dir / "config"

    conf_dict = yaml.safe_load(open(conf_dir / "config.yml", "r"))
    conf_dict = confparser(conf_dict, args)

    for path in ("path_catalog", "path_markdown", "path_header"):
        conf_dict[path] = "{}/{}".format(conf_dict["path_repo"], conf_dict[path])
    catalog = yaml.safe_load(open(conf_dict["path_catalog"]))

    date, username, userid, dataid = get_id()

    write_data(conf_dict["path_catalog"], conf_dict["path_markdown"], conf_dict["path_header"], catalog,
            write_catalog=False)
    if args.message is not None:
        commit_catalog(conf_dict["path_repo"], conf_dict["path_catalog"], conf_dict["path_markdown"],
                       username, message=args.message, verbose=False)

if __name__ == "__main__":
    convcat()

