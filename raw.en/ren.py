sw = open("stopwords.dat")
ssw = []
for word in sw:
    ssw += [word[:-1]]
sw.close()

for i in range(0,170):
    lines = open(str(i), "r")
    fils = open("a"+str(i), "w")

    flag = 0
    for line in lines:
        if(line == '\n'):
            continue
        for word in line.split():
            if(word[-1] == ">"):
                fils.write(word+" ")
                flag = 1
                continue

            if flag == 0:
                fils.write(word+" ")
                continue

            if word.lower() not in ssw:
                fils.write(word+" ")
        fils.write("\n")
    lines.close()
    fils.close()
