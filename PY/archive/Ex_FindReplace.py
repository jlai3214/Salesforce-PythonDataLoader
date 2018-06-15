import csv

dir = 'c:/kenandy/python/stageCSV/'
file = dir + "account" + ".csv"
print (file)
ifile = open(file, 'r')
reader = csv.reader(ifile)
ofile = open(dir + 'file.csv', 'w')
writer = csv.writer(ofile)


findlist = ['JEFF', 'G', 'C', 'T', 'Y', 'R', 'W', 'S', 'K', 'M', 'X', 'N', '-']
replacelist = ['AAJEFF', 'GG', 'CC', 'TT', 'CT', 'AG', 'AT', 'GC', 'TG', 'CA',
'NN', 'NN', '-']

rep = dict(zip(findlist, replacelist))

print (rep)

def findReplace(find, replace):
    s = ifile.read()
    print (s)
    for item, replacement in zip(findlist, replacelist):
        print (item)
        print (replacement)
        s = s.replace(item, replacement)
        print ("s*****", s)
    ofile.write(s)

for item in findlist:
    findReplace(item, rep[item])

ifile.close()
ofile.close()