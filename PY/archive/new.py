import os,sys
import csv



#print("Only one percentage sign: %% " % (l1))


def IsFileOpen(file):

    mode = False
    fileobj=open(file,"wb+")

    if not fileobj.closed:
        mode = True
        print("file is already opened")

    IsFileOpen = mode
    return IsFileOpen

file = 'C:\Kenandy\Python\Configuration\ID_Dictionary.csv'

#IsFileOpen(file)



def IsFileOpen(file):

    mode = True
    if os.path.exists(file):
        try:
            os.rename(file, file)
            #print ('Access on file "' + file +'" is available!')
            mode = False
        except OSError as e:
            print ('Access-error on file "' + file + '"! \n' + str(e))
            mode = True
    IsFileOpen = mode
    return IsFileOpen


#print (IsFileOpen(file))

file = 'C:\Kenandy\Python\Configuration\ID_Dictionary.csv'



def rowsInCSV(file):

    #count = none
    try:
        with open(file, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            count = (len(list(reader)))
            rowsInCSV = count
            return rowsInCSV
    except:
        print('           *** file has empty row *** %s' % (file))


print (rowsInCSV(file))