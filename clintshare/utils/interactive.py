
def exec(instruc, verbose=True):
    import os
    if verbose:
        print(">", instruc)
    return os.popen(instruc).read()

def quitkeep(text):
    choice = input(text + " (y/n)\n")
    if choice.lower() == "n" or choice.lower() == "no":
        exit()

def ans2bool(text):
    choice = input(text + " (y/n)\n")
    if choice.lower() == "n" or choice.lower() == "no":
        return False
    else:
        return True

def inputx(prompt, placeholder):
    import readline
    def hook():
        readline.insert_text(placeholder)
        readline.redisplay()

    readline.set_pre_input_hook(hook)
    user_input = input(prompt)

    return user_input

def form(query_dict, ans_dict, help_dict, keys, userid):
    n_queries = len(keys)
    k = 0
    while k < n_queries:
        key = keys[k]
        ans = inputx("\n[{}/{}] {}:\n".format(k + 1, n_queries, query_dict[key]), str(ans_dict[key] or '')).strip()
        if ans == "":
            if help_dict[key] is None:
                ans_dict[key] = ans
                k += 1
            else:
                if ans_dict[key] == "":
                    print(help_dict[key])
                else:
                    k += 1
        elif ans == "back":
            k -= 1
        else:
            ans_dict[key] = ans
            k += 1

    if ans_dict["Product"][:len(userid)] != userid:
        ans_dict["Product"] = userid + "-" + ans_dict["Product"]

    return ans_dict

def printfiles(files):

    n_print = 5
    n_chars = 100

    num_files = len(files)

    if num_files <= n_print * 2:
        for file in files:
            print("..." + file[-n_chars:])
    else:
        for file in files[:n_print]:
            print("..." + file[-n_chars:])
        print("   ...   ")
        for file in files[-n_print:]:
            print("..." + file[-n_chars:])
