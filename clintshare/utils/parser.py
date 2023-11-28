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