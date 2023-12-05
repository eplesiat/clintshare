
import os
import re
import numpy as np
from .interactive import printfiles

def strparser(arg):
    return arg.split(",")

def argvar(arg):

    if arg is None or arg == "":
        variable = ""
    else:
        variable = "variable={}".format(arg)
    
    return variable

def remember(files, member, regex, varpar):

    i_par = {"r": 1, "i": 3, "p": 5}
    num_files = len(files)

    if member is None:
        if regex is None:
            member = ""
        else:
            member = "r1i1p1"
    
    members = [member for file in files]
    if regex is not None:
        
        print("* List of generated ensemble members:")
        for i in range(num_files):
            imem = int(re.search(regex, os.path.basename(files[i])).group(1))
            try:
                imem = int(re.search(regex, os.path.basename(files[i])).group(1))
            except:
                print("Error! Could not extract member index from {}".format(files[i]))
                exit()
            members[i] = members[i][:i_par[varpar]] + str(imem) + members[i][i_par[varpar] + 1:]
        printfiles(["{} -> {}".format(files[i], members[i]) for i in range(num_files)])
    
    return members

def update_dict(ans_dict, key, val, update=True, keep_same=False):
    if key in ans_dict:
        if update:
            if not keep_same or ans_dict[key] != val:
                ans_dict[key] += ", " + val
    else:
        ans_dict[key] = val
    return ans_dict

def create_dict(ans_dict, date, userid, username, data_path, files, keys):

    num_files = str(len(files))
    #size_files = round(sum(os.path.getsize(file) for file in files) / (1024 ** 2))
    size_files = str(0)

    ans_dict.update({
        "Modified date": date.strftime("%d/%m/%Y %H:%M:%S"),
        "Userid": userid,
        "Username": username,
        "Data path": data_path,
        "Number of added files": num_files,
        "Total size (in Mb)": size_files})

    #ans_dict = update_dict(ans_dict, "Data path", data_path, keep_same=True)
    #ans_dict = update_dict(ans_dict, "Number of added files", num_files, update=index_data)
    #ans_dict = update_dict(ans_dict, "Total size (in Mb)", size_files, update=index_data)

    for key in keys:
        if key not in ans_dict:
            ans_dict[key] = ""
    
    if "Indexed" not in ans_dict:
        ans_dict.update({"Indexed": "No"})

    return ans_dict

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
