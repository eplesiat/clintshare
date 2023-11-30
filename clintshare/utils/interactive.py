
def quitkeep(text):
    choice = input(text + " (y/n)\n")
    if choice.lower() == "n" or choice.lower() == "no":
        exit()

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
