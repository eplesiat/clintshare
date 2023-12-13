import yaml
import fcntl

def get_data(catalog, dataid, userid=None, data_path=None):
    if dataid in catalog:
        ans_dict = catalog[dataid]
        if (userid is None or userid == ans_dict["Userid"]) and (data_path is None or data_path == ans_dict["Data path"]):
            print("\nFound data registered with the following information:\n", ans_dict)
            return ans_dict
        else:
            raise Exception("Did not find any data registered with userid {} and data path {}."
                    .format(userid, data_path))
    else:
        raise Exception("Could not find dataid {} in catalog.".format(dataid))

def write_yaml(content, path_dir, varname, dataid):
    filename = "{}/{}_{}.yml".format(path_dir, varname, dataid)
    with open(filename, "w") as f:
        if isinstance(content, list):
            yaml.dump(content, f)
        else:
            for key, val in content.items():
                print("{}: {}".format(key, val), file=f)
    return filename


def write_data(path_catalog, path_markdown, path_header, catalog):

    with open(path_catalog, "w") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        # yaml.dump(catalog, f, default_style='', sort_keys=False)
        print("---", file=f)
        for dataid in catalog:
            print(" {}:".format(dataid), file=f)
            for key, val in catalog[dataid].items():
                print("  {}: {}".format(key, str(val or '')), file=f)
        fcntl.flock(f, fcntl.LOCK_UN)

    with open(path_markdown, "w") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        print(open(path_header).read(), file=f)
        for dataid in catalog:
            print("\n## {}".format(catalog[dataid]["Title"]), file=f)
            print("\n- Dataid: {}".format(dataid), file=f)
            for key, val in catalog[dataid].items():
                if key != "Title":
                    print("- {}: {}".format(key, str(val or '')), file=f)
        fcntl.flock(f, fcntl.LOCK_UN)
