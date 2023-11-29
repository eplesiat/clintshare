
import os
import re
import numpy as np

def parspar(arg):
    assert len(arg) == 1, "parmem must be a character"
    pos_dict = {"r": 1, "i": 3, "p": 5}
    assert arg in pos_dict.keys(), "parmem must be r, i or p"
    return pos_dict[arg]

def strpars(arg):
    return np.array(arg.split(","))

def remember(files, members, regex, varpar):
    num_files = len(files)
    for i in range(num_files):
        if members[i] == "":
            members[i] = "r1i1p1"
    print("* List of generated ensemble members:")
    for i in range(num_files):
        imem = int(re.search(regex, os.path.basename(files[i])).group(1))
        try:
            imem = int(re.search(regex, os.path.basename(files[i])).group(1))
        except:
            print("Error! Could not extract member index from {}".format(files[i]))
            exit()
        members[i] = members[i][:varpar] + str(imem) + members[i][varpar + 1:]
        print("* {} -> {}".format(files[i], members[i]))

    return members

def mdparser(md_text, dataid):
    k = -1
    for content in md_text:
        k += 1
        if "Dataid" in content:
            content = content.split("\n-")
            content = [["Title", " ".join(content[0].split(" ")[1:])]] + [i.split(":", 1) for i in content[1:]]
            content = {i[0].strip(): i[1].strip() for i in content}
            if content["Dataid"] == dataid:
                return k, content

    return None, None

def totext(ans_dict):
    data_text = "## {}\n".format(ans_dict["Title"])
    del ans_dict["Title"]
    for key in ans_dict.keys():
        data_text += "\n- {}: {}".format(key, ans_dict[key])

    return data_text

def confparser(conf_dict, args):

    args_dict = args.__dict__

    for arg in args_dict:
        if args_dict[arg] is not None:
            if arg in conf_dict.keys():
                conf_dict[arg] = args_dict[arg]

    return conf_dict
