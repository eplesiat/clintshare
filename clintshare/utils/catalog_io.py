import yaml
import fcntl

def read_data(path_catalog, dataid):
    catalog = yaml.safe_load(open(path_catalog))
    if dataid in catalog:
        return catalog, catalog[dataid]
    else:
        raise Exception("Could not find dataid {} in catalog.".format(dataid))

def write_data(path_catalog, path_markdown, path_header, catalog):

    with open(path_catalog, "w") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        # yaml.dump(catalog, f, default_style='', sort_keys=False)
        print("---", file=f)
        for dataid in catalog:
            print(" {}:".format(dataid), file=f)
            for key, val in catalog[dataid].items():
                print("  {}: {}".format(key, val), file=f)
        fcntl.flock(f, fcntl.LOCK_UN)

    with open(path_markdown, "w") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        print(open(path_header).read(), file=f)
        for dataid in catalog:
            print("\n## {}".format(catalog[dataid]["Title"]), file=f)
            print("\n- Dataid: {}".format(dataid), file=f)
            for key, val in catalog[dataid].items():
                if key != "Title":
                    print("- {}: {}".format(key, val), file=f)
        fcntl.flock(f, fcntl.LOCK_UN)
