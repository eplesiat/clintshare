import re
from .parser import mdparser, totext
import fcntl

def read_data(path_registry, dataid):
    try:
        md_text = re.split(r'\n(?=#)', open(path_registry).read())
        return md_text, *mdparser(md_text, dataid)
    except:
        print("Error! Could not parse registry.")
        exit()

def write_data(path_registry, ans_dict, md_text, idx):

    data_text = totext(ans_dict)

    if idx is None:
        with open(path_registry, "a") as f:
                fcntl.flock(f, fcntl.LOCK_EX)
                print(data_text, file=f)
                fcntl.flock(f, fcntl.LOCK_UN)
    else:
        md_text[idx] = data_text + "\n"
        md_text[-1] = md_text[-1].strip()
        with open(path_registry, "w") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            for content in md_text:
                print(content, file=f)
            fcntl.flock(f, fcntl.LOCK_UN)

