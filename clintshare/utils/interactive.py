
def quitkeep(text):
    choice = input(text + " (y/n)\n")
    if choice.lower() == "n" or choice.lower() == "no":
        exit()

