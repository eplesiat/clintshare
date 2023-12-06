
def strparser(arg):
    return arg.split(",")

def argvar(arg):

    if arg is None or arg == "":
        variable = ""
    else:
        variable = "variable={}".format(arg)
    
    return variable

def confparser(conf_dict, args):

    args_dict = args.__dict__

    for arg in args_dict:
        if args_dict[arg] is not None:
            if arg in conf_dict.keys():
                conf_dict[arg] = args_dict[arg]

    return conf_dict
