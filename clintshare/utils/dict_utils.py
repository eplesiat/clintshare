
import os
import re
from .interactive import printfiles

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
            try:
                imem = int(re.search(regex, os.path.basename(files[i])).group(1))
            except:
                raise Exception("Could not extract member index from {}".format(files[i]))
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