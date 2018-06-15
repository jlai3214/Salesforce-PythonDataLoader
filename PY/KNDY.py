import csv
from simple_salesforce import Salesforce
from salesforce_bulk import SalesforceBulk
from salesforce_bulk import CsvDictsAdapter
from datetime import date
from pathlib import Path
from shutil import copyfile
import os,sys

sObject_src,sObject_stg,sObject_loc = '','',''
dest_CSVDir = 'c:/kenandy/python/destCSV/'
src_CSVDir = 'c:/kenandy/python/sourceCSV/'
stg_CSVDir = 'c:/kenandy/python/stageCSV/'
configDir = 'c:/kenandy/python/Configuration/'


def init_credential():

    myFuncName = sys._getframe().f_code.co_name
    print("\n")
    print("****** %s *** processing ************" % (myFuncName))

    file  = configDir + 'credentials' + '.csv'
    copyfile(file,file.replace(configDir, configDir + 'backup/'))

    src_credential = []
    dest_credential = []

    with open(file, "r") as csvfile:
        dataset = csv.DictReader(csvfile)
        for row in dataset:
            if row['src_dest'].upper() == 'SRC':
                src_credential = row
            elif row['src_dest'].upper() == 'DEST':
                dest_credential = row

        print('      *** credential: ', dict(src_credential), dict(dest_credential))
        return (src_credential,dest_credential)

def init_sObjectList(src_namespace,dest_namespace):

    myFuncName = sys._getframe().f_code.co_name
    print("\n")
    print("****** %s *** processing ************" % (myFuncName))

    sObjectList = []
    file = configDir + 'sObjects' + '.csv'
    with open(file, "r") as csvfile:
        dataset = csv.DictReader(csvfile)
        for row in dataset:
            if row['src_namespace'].upper() == 'NA' or src_namespace ==  '':
                row['src_namespace'] = ''
            else:
                row['src_namespace'] = src_namespace + '__'
            if row['dest_namespace'].upper() == 'NA' or dest_namespace == '':
                row['dest_namespace'] = ''
            else:
                row['dest_namespace'] = src_namespace + '__'
            sObjectList.append(row)
        return sObjectList

def init_sObject(sObject, src_namespace,dest_namespace):

    #11/2/2017
    myFuncName = sys._getframe().f_code.co_name
    print("\n")
    print("****** %s(%s) *** processing ************" % (myFuncName,sObject))

    if src_namespace == '' and src_namespace == dest_namespace:
        src_sObjectCSV = sObject + '_src'
        stg_sObjectCSV = sObject + '_stg'
        dest_sObjectCSV = sObject + '_dest'
    elif src_namespace == '' and dest_namespace != '':
        src_sObjectCSV = sObject + '_src'
        stg_sObjectCSV = dest_namespace +   sObject + '_stg'
        dest_sObjectCSV = dest_namespace +  sObject + '_dest'
    elif src_namespace != '' and src_namespace == dest_namespace:
        src_sObjectCSV = src_namespace + sObject +'_src'
        stg_sObjectCSV = dest_namespace + sObject + '_stg'
        dest_sObjectCSV = dest_namespace + sObject + '_dest'
    elif src_namespace != '' and dest_namespace == '':
        src_sObjectCSV = src_namespace  + sObject + '_src'
        stg_sObjectCSV = sObject + '_stg'
        dest_sObjectCSV = sObject + '_dest'
    elif src_namespace != '' and src_namespace != dest_namespace:
        src_sObjectCSV = src_namespace  + sObject + '_src'
        stg_sObjectCSV = dest_namespace  + sObject + '_stg'
        dest_sObjectCSV = dest_namespace + sObject + '_dest'
    else:
        print ('unknown error')
        pass
    #print (src_sObjectCSV)
    src_sObjectFile =  src_CSVDir + src_sObjectCSV  + '.csv'
    stg_sObjectFile =  stg_CSVDir + stg_sObjectCSV  + '.csv'
    dest_sObjectFile =  dest_CSVDir + dest_sObjectCSV  + '.csv'

    print('       *** sObject:',sObject, src_namespace, dest_namespace, src_sObjectCSV, stg_sObjectCSV,dest_sObjectCSV,src_sObjectFile, stg_sObjectFile, dest_sObjectFile)
    return sObject, src_namespace, dest_namespace, src_sObjectCSV, stg_sObjectCSV, dest_sObjectCSV,src_sObjectFile, stg_sObjectFile, dest_sObjectFile


def sf_export_object2CSV(sf, sObject, namespace, sObjectFile,whereCondition):

    myFuncName = sys._getframe().f_code.co_name
    print("\n")
    print("****** %s(%s *** processing ************" % (myFuncName,sObject),end = "" )

    myExtID = namespace +'ExtId__c'
    sObject = namespace + sObject

    'Call Salesforce and fetch records'
    try:
        salesforceObject = sf.__getattr__(sObject)
        fieldNames = [field['name'] for field in salesforceObject.describe()['fields']]

        print("\n")
        print ("recordtrype: %s" %(whereCondition))
        print (whereCondition[0:2].lower())
        if whereCondition == '' :
            soqlQuery = "SELECT " + ", ".join(fieldNames) + " FROM " + sObject
        elif whereCondition[0:2].lower() == 'me':
            soqlQuery = "SELECT " + ", ".join(fieldNames) + " FROM " + sObject + " WHERE " + namespace+  whereCondition[3:]
        else:
            soqlQuery = "SELECT " + ", ".join(fieldNames) + " FROM " + sObject + " WHERE " + "recordtypeid IN (SELECT Id FROM RecordType WHERE Name = " + whereCondition[whereCondition.find("=")+1:] + ")"
        print("\n")
        print ('SOQL=:', soqlQuery)
        sf_dataset = sf.query_all(soqlQuery)['records']
    except:
        print ('*** exception error')
        pass

    with open(sObjectFile, 'w', newline = '') as csvfile:
        dataset = csv.DictWriter(csvfile, fieldnames=fieldNames)
        dataset.writeheader()
        for row in sf_dataset:
            if myExtID in fieldNames and 'src' in sObjectFile:
                row[myExtID] = '&' + reversed_string(row['Id'])
                # each row has a strange attributes key we don't want
            row.pop('attributes',None)
            dataset.writerow(row)
        print ('   [No of rows exported]:= ', len(list(sf_dataset)), end="")
    csvfile.close()


def z_src_add_2_ID_Dict(sObject, src_namespace, src_sObjectFile):

    # write source object to master object record
    # Sourcedir = "c:/kenandy/python/sourceCSV/"
    myFuncName = sys._getframe().f_code.co_name
    print("\n")
    print("[SRC-01]****** %s *** processing ************" % (myFuncName), end="")

    file = configDir + "ID_Dictionary" + '.csv'
    copyfile(file, file.replace(configDir, configDir + 'backup/'))

    mysObject = src_namespace + sObject
    myExtID = src_namespace + 'ExtId__c'
    print('      *** sOBject:=', mysObject, src_sObjectFile)

    with open(file, 'r') as csvfile:
        dataset_config = csv.reader(csvfile)
        next(dataset_config)
        #list = [records for records in dataset_config if records[0] != sObject or (records[0] == sObject and records[4][0] != '#')]
        list = [records for records in dataset_config if
            records[0] != sObject or (records[0] == sObject and records[4][0] != '#')]

    dataset_tmp = []
    if IsCSVEmpty(src_sObjectFile) == True:
        print('      *** File not exist or file with empty row')
    else:
        with open(src_sObjectFile) as csvfile:
            dataset_src = csv.DictReader(csvfile)
            fieldNames = dataset_src.fieldnames
            for row in dataset_src:
                #print (dict(row))
                src_ID = row["Id"]
                name = row ["Name"]
                if myExtID in fieldNames:
                    extID =  row[myExtID]
                    dest_ID = extID.replace('&','#')
                else:
                    extID = '&'+ row["Name"].replace(","," ")
                    dest_ID = '#' + row["Name"].replace(","," ")

                record = [sObject, extID, name, src_ID,  dest_ID]
                print('      *** ID Dict:=', record, end="")

                print (record)
                dataset_tmp.append(record)
        #print ('*** source ID *** ',dataset_tmp)

        #write back to recordId csv
        #file = configDir + "ID_Dictionary" + '.csv'
        with open(file, 'w', newline='') as csvfile:
            fieldNames = ['sObject', 'extID', 'name', 'src_ID', 'dest_ID']
            writer = csv.writer(csvfile)
            writer.writerow(fieldNames)
            writer.writerows(list)
            writer.writerows(dataset_tmp)




def src_add_2_ID_Dict(sObject, src_namespace, src_sObjectFile):

    # write source object to master object record
    # Sourcedir = "c:/kenandy/python/sourceCSV/"
    myFuncName = sys._getframe().f_code.co_name
    print("\n")

    mysObject = src_namespace + sObject
    myExtID = src_namespace + 'ExtId__c'

    print("[SRC-01]****** %s *** processing ************ sObject=:%s , sObjetFile=:%s "  % (myFuncName, mysObject, src_sObjectFile ))

    src_ID_Dict_tmp = []
    src_IDs = []
    if IsCSVEmpty(src_sObjectFile) == True:
        print('      *** File not exist or file with empty row')
    else:
        with open(src_sObjectFile) as csvfile:
            dataset_src = csv.DictReader(csvfile)
            fieldNames = dataset_src.fieldnames
            for row in dataset_src:
                #print (dict(row))
                src_ID = row["Id"]
                name = row ["Name"]
                if myExtID in fieldNames:
                    extID =  row[myExtID]
                    dest_ID = extID.replace('&','#')
                else:
                    extID = '&'+ row["Name"].replace(","," ")
                    dest_ID = '#' + row["Name"].replace(","," ")

                record = [sObject, extID, name, src_ID,  dest_ID]
                src_IDs.append(src_ID)
                print('      *** ID Dict:=', record)
                src_ID_Dict_tmp.append(record)
    print (src_IDs)


    print("[SRC-02]****** %s *** processing ************ sObject=:%s , sObjetFile=:%s " % (
    myFuncName, mysObject, src_sObjectFile))

    file = configDir + "ID_Dictionary" + '.csv'
    copyfile(file, file.replace(configDir, configDir + 'backup/'))
    with open(file, 'r') as csvfile:
        dataset_ID_Dict = csv.reader(csvfile)
        next(dataset_ID_Dict)
        ID_Dict = [item for item in dataset_ID_Dict if item[3] not in src_IDs or (item[3]  in src_IDs and item[4][0] !='#' ) ]
        IDs = [item[3]for item in ID_Dict]
        print ('xxx',IDs)

    src_ID_Dict = [item for item in src_ID_Dict_tmp if item[3] not in IDs]
    print ('src_ID_Dict',src_ID_Dict)
    print('ID_DIct', ID_Dict)


    #write back to recordId csv
    with open(file, 'w', newline='') as csvfile:
        fieldNames = ['sObject', 'extID', 'name', 'src_ID', 'dest_ID']
        writer = csv.writer(csvfile)
        writer.writerow(fieldNames)
        writer.writerows(src_ID_Dict)
        writer.writerows(ID_Dict)



def stg_namespace2fieldnames(src_namespace,dest_namespace,stg_sObjectFile):

    myFuncName = sys._getframe().f_code.co_name
    print("\n")
    print("[STG-01] ****** %s(%s) *** processing ************" % (myFuncName, stg_sObjectFile))

    copyfile(stg_sObjectFile, stg_sObjectFile.replace(stg_CSVDir, stg_CSVDir +'backup/'))
    file = stg_CSVDir + '_tmp_1.csv'

    print("[STG-01] ****** No of rows processed = %s************" % (rowsInCSV(stg_sObjectFile)))
    with open(stg_sObjectFile, 'r', ) as csvfile:
        dataset_stg = csv.reader(csvfile)
        'header'
        stg_fieldNames = next(dataset_stg)
        print ('      **** header[fieldnames]:(Before)' ,stg_fieldNames)
        for i in range(len(stg_fieldNames)):
            if stg_fieldNames[i][-3:] == '__c' and len(src_namespace) > 0 and len(dest_namespace) > 0  :
                stg_fieldNames[i] = stg_fieldNames[i].replace(src_namespace,dest_namespace)
            elif stg_fieldNames[i][-3:] == '__c' and len(src_namespace) > 0 and len(dest_namespace) == 0:
                stg_fieldNames[i] = stg_fieldNames[i].replace(src_namespace+'__', dest_namespace)
            elif stg_fieldNames[i][-3:] == '__c' and len(src_namespace) == 0 and len(dest_namespace) > 0:
                stg_fieldNames[i] = dest_namespace + '__' +stg_fieldNames[i]

        #elif stg_fieldNames[i][-3:] == '__c' and len(src_namespace) == 0:
            #    stg_fieldNames[i] = dest_namespace + stg_fieldNames[i]
        print('      **** header[fieldnames]:(After) ', stg_fieldNames)
        #exit()

        with open(file, 'w', newline='') as csvfile_1:
            dataset_staging = csv.writer(csvfile_1)
            dataset_staging.writerow(stg_fieldNames)
            dataset_staging.writerows(dataset_stg)

    copyfile(file, stg_sObjectFile)
    print("[STG-01] ****** No of rows processed = %s************" % (rowsInCSV(stg_sObjectFile)))


def stg_fieldnamesExcluded(dest_namespace, sObject):

    myFuncName = sys._getframe().f_code.co_name
    print("****** %s *** processing ************" % (myFuncName))

    fldsExcluded = []
    file = configDir + "fieldnamesExcluded" + '.csv'
    with open(file, 'r') as csvfile:
        dataset_stg = csv.reader(csvfile)
        try:
            #fieldnames = [r[1] for r in dataset_stg if sObject in r]
            fieldnames = [r[1] for r in dataset_stg if sObject == r[0]]
            print ('xxxxx',sObject, fieldnames)
            fldsExcluded = fieldnames[0].split(',')
            for i in range(len(fldsExcluded)):
                fldsExcluded[i] = dest_namespace + fldsExcluded[i].strip()
        except IndexError:
            fldsExcluded=[]
    print('       *** fieldnames excluded: ', fldsExcluded)
    return fldsExcluded


def stg_removeValueByFieldname(stg_sObjectFile,dest_namespace,sObject):

    myFuncName = sys._getframe().f_code.co_name
    print("\n")
    print("[STG-02] ****** %s(%s) *** processing ************" % (myFuncName, sObject))
    copyfile(stg_sObjectFile, stg_sObjectFile.replace(stg_CSVDir, stg_CSVDir +'backup/'))

    file = stg_CSVDir + '_tmp_1.csv'
    copyfile(stg_sObjectFile,file)
    dataset_tmp =[]

    print("[STG-02] ****** No of rows processed = %s************" % (rowsInCSV(file)))
    with open(file) as csvfile:
        dataset_stg = csv.DictReader(csvfile)
        stg_fieldNames = dataset_stg.fieldnames
        common_fieldNames = ['Id','OwnerId', 'IsDeleted', 'CreatedDate', 'CreatedById', 'LastModifiedDate','LastModifiedById', 'SystemModstamp', 'LastViewedDate', 'LastReferencedDate']
        std_fieldNames = [fieldName for fieldName in common_fieldNames if fieldName in stg_fieldNames]
        fieldNamesBysObject = stg_fieldnamesExcluded(dest_namespace, sObject)
        unique_fieldNames = [fieldName for fieldName in stg_fieldNames if 'Identifier' in fieldName or 'Unique' in fieldName or 'Key' in fieldName]
        #print('         *** stg fieldNames:=',stg_fieldNames)
        #print('         *** std fieldNames:=', std_fieldNames)
        #print('         *** unique_fieldNames:=', unique_fieldNames)
        fieldNamesWithNullValue = unique_fieldNames + fieldNamesBysObject + std_fieldNames
        print('       *** fieldNames with Null value', fieldNamesWithNullValue)

        for row in dataset_stg:
            for i in range(len(fieldNamesWithNullValue)):
                row[fieldNamesWithNullValue[i]] = ''
            #print('         *** ', dict(row))
            dataset_tmp.append(row)
        #print ('         ***', dataset_tmp)

        #copyfile(stageCSVFile,stageCSVFile_bak)
        with open(stg_sObjectFile, 'w', newline='') as csvfile1:
            writer = csv.DictWriter(csvfile1, fieldnames=stg_fieldNames)
            #print(dataset_tmp)
            writer.writeheader()
            writer.writerows(dataset_tmp)

    print("[STG-02] ****** No of rows processed = %s************" % (rowsInCSV(stg_sObjectFile)))


def stg_getNewReferenceId(stg_sObjectFile):

    myFuncName = sys._getframe().f_code.co_name
    print("\n")
    print("[STG-03] ****** %s(%s) *** processing ************" % (myFuncName, stg_sObjectFile))

    copyfile(stg_sObjectFile, stg_sObjectFile.replace(stg_CSVDir, stg_CSVDir +'backup/'))

    src_IDs = []
    dest_IDs = []

    print("[STG-03] ****** No of rows processed = %s************" % (rowsInCSV(stg_sObjectFile)))
    stg_sObjectCSV = open(stg_sObjectFile, 'r')
    #tmp_3 = stg_CSVDir + '_tmp_3' + ".csv"
    file = stg_CSVDir + '_tmp_3' + ".csv"

    #tmp = open(_tmp_3, 'w')
    tmp = open(file, 'w')
    #src_dest_IDs = getIDfromID_Dictionary()
    src_IDs, dest_IDs = getIDfromID_Dictionary()

    rep = dict(zip(src_IDs, dest_IDs))
    #print('xxxx',rep)
    print("      ****[Rep]:= %s *" % (rep))
    def findReplace(find, replace):
        dataset_stg = stg_sObjectCSV.read()
        for item, replacement in zip(src_IDs, dest_IDs):
            dataset_stg = dataset_stg.replace(item, replacement)
            #print('')
        tmp.write(dataset_stg)


    for item in src_IDs:
        #findReplace(item, src_dest_IDs[item])
        findReplace(item, rep[item])


    stg_sObjectCSV.close()
    tmp.close()

    # Copy from 'file to staging file
    copyfile(file, stg_sObjectFile)
    print("[STG-03] ****** No of rows processed = %s************" % (rowsInCSV(stg_sObjectFile)))


def stg_sfBulkUpdate(sfBulk,sObject,stg_sObjectFile ):

    myFuncName = sys._getframe().f_code.co_name
    print("\n")
    print("[STG-04] ****** %s *** processing (%s) ************" % (myFuncName,sObject))

    if IsCSVEmpty(stg_sObjectFile) == True:
        pass
    else:
        job = sfBulk.create_insert_job(sObject, contentType='CSV', concurrency='Parallel')
        with open(stg_sObjectFile) as csvfile:
            reader = csv.DictReader(csvfile)
            #print('   [No of rows import]:= ', len(list(reader)), end="")
            #print (reader.fieldnames)
            rows = []
            for row in reader:
                print("row****", dict(row))
                rows.append(dict(row))

            csv_iter = CsvDictsAdapter(iter(rows))
            print("rows****", rows)
            batch = sfBulk.post_batch(job, csv_iter)
            sfBulk.wait_for_batch(job, batch)
            sfBulk.close_job(job)

        #while not sfBulk.is_batch_done(batch):
        #    sleep(10)

        """
        for result in sfBulk.get_all_results_for_query_batch(batch):
            reader = unicodecsv.DictReader(result, encoding='utf-8')
            for row in reader:
                print (row)  # dictionary rows

        """
        print("Done. Data Uploaded.")


def dest_get_Dest_Id(dest_sObjectFile):

    myFuncName = sys._getframe().f_code.co_name
    print("\n")
    print("[DEST-02] ****** %s *** processing ************" % (myFuncName))

    if IsCSVEmpty(dest_sObjectFile) == True:
        print('           *** file has empty row *** %s' % (dest_sObjectFile))
        pass
    else:
        config = 'ID_Dictionary'
        configFile = configDir + config + '.csv'
        copyfile(configFile, configFile.replace(configDir, configDir +'backup/'))

        extIDs = []
        dest_IDs = []

        temp_ID_Dict = open(configFile, 'r')
        # dataset_stg = csv.reader(stg_sObjectCSV)
        file = configDir + '_tmp_1' + '.csv'
        temp = open(file, 'w')

        extIDs, dest_IDs = get_dest_IDs(dest_sObjectFile)
        print("xxxx", extIDs, dest_IDs)

        rep = dict(zip(extIDs, dest_IDs))
        print('rep', rep)

        def findReplace(find, replace):
            dataset = temp_ID_Dict.read()
            for item, replacement in zip(extIDs, dest_IDs):
                dataset = dataset.replace(item, replacement)
                #print (dataset)
            temp.write(dataset)


        for item in extIDs:
            findReplace(item, rep[item])


        temp_ID_Dict.close()
        temp.close()

        print(file)
        #Copy from 'file to staging file
        copyfile(file, configFile)


#######################################################################################################################
# Other functon
#######################################################################################################################

def sf_Login(username,password,security_token):

    sf = Salesforce(username=username, password=password, security_token=security_token)
    print ('login sucessfully')
    return sf


def sf_Bulk_Login(username, password,security_token):

    sfBulk = SalesforceBulk(username=username, password=password,security_token=security_token)
    print ('login sucessfully')
    return sfBulk

def reversed_string(string):

    string = string[::-1]
    return string


def getIDfromID_Dictionary():

    myFuncName = sys._getframe().f_code.co_name
    #print("\n")
    print("       *** %s *** processing ************" % (myFuncName))

    dir = 'c:/kenandy/python/Configuration/'
    config = 'ID_Dictionary'
    configFile = configDir + config + '.csv'
    openFile = open(configFile, "r")
    reader = csv.DictReader(openFile)
    src_IDs = ['']
    dest_IDs = ['']
    for row in reader:
        if row['dest_ID'] != 'New ID':
            #print(row['Record_ID'], row['New_Record_ID'])
            src_IDs.append(row['src_ID'])
            dest_IDs.append(row['dest_ID'])
    #print('SRC_ID',src_IDs)
    #print('DEST_ID', dest_IDs)
    #src_dest_IDs = dict(zip(src_IDs, dest_IDs))
    #print('       *** src_ID_map', src_dest_IDs)

    return src_IDs,dest_IDs
    #return src_dest_IDs



def get_IdList(file):

    myFuncName = sys._getframe().f_code.co_name
    #print("\n")
    print("       *** %s *** processing ************" % (myFuncName))


    temp = open(file, "r")
    reader = csv.DictReader(temp)

    my_dict = {'name': 'Jack', 'age': 26}

    d1 = ({'first_name': 'Baked', 'last_name': 'Beans'})
    #d2 = {[first_name': 'Lovely', 'last_name': 'Spam']}
    d3 = ({'first_name': 'Wonderful', 'last_name': 'Spam'})

    #d1 = d1 + d3

    d1.update(d3)
    # d1.append(d2)
    print (d1)

    #IDList = ['extid':'1','item':'2')
    #IDList_tmp = dict(extid='3', id='4')

    #IDList = IDList + IDList_tmp
    print (my_dict)

    """
    id = 'src_ID'
    for row in reader:
        IDList.update(
        IDList_tmp={'extID': 'xxxx', 'ID': 'xxx'}
        IDList.update(IDList_tmp)


    print (Ids)
    get_IdList = Ids
    #return src_IDs
    """
    return


def get_dest_IDs(dest_sObjectFile):

    myFuncName = sys._getframe().f_code.co_name
    #print("\n")
    print("       *** %s(%s) *** processing ************" % (myFuncName,dest_sObjectFile))

    dir = 'c:/kenandy/python/Configuration/'
    config = 'ID_Dictionary'
    configFile = configDir + config + '.csv'

    dest_extIDs = []
    dest_IDs = []

    with open(dest_sObjectFile) as csvfile:
        dataset_dest = csv.DictReader(csvfile)
        fieldNames = dataset_dest.fieldnames
        for row in dataset_dest:
            dest_IDs.append(row['Id'])
            if 'ExtId__c' not in fieldNames:
                dest_extIDs.append('#'+row['Name'].replace(',',' '))
            else:
                if row['ExtId__c'] != '':
                    dest_extIDs.append(row['ExtId__c'].replace('&','#'))
                else:
                    dest_extIDs.append('&&' + reversed_string(row['Id']))

        print ('ccccc', dest_extIDs,dest_IDs)

    return dest_extIDs,dest_IDs


def IsFileExist(file):

    mode = False
    checkFile = Path(file)
    if checkFile.exists():
        mode = True
    else:
        mode = False

    IsFileExist = mode
    return IsFileExist


def IsFileOpen(file):

    mode = False
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



def IsCSVEmpty(file):


    mode = False
    if IsFileExist(file) == True:
        with open(file, 'r') as csvfile:
            csvreader = csv.reader(csvfile)
            if (len(list(csvreader))) < 2:
                mode = True
            else:
                mode = False

    IsCSVEmpty = mode
    return IsCSVEmpty


###################################################################################################

