import csv
from simple_salesforce import Salesforce
from salesforce_bulk import SalesforceBulk
from salesforce_bulk import CsvDictsAdapter
from datetime import date
from pathlib import Path
from shutil import copyfile


username_src = 'jl-auto-1@myorg.com'
security_token_src = 'fNPrxjsjFX1rTyzgaHtmDulvJ'
password_src = 'kyqa2017'

username_loc = 'jl-auto-1@myorg.com'
security_token_loc = 'fNPrxjsjFX1rTyzgaHtmDulvJ'
password_loc = 'kyqa2017'


#username = 'jl-match2rd@myorg.com'
#security_token = 'lOtpc5VNUQf9dpWiuVIewdv4'

#sf = Salesforce(username=username,password=password, security_token=security_token)

sObject_src,sObject_stg,sObject_loc = '','',''
dest_CSVDir = 'c:/kenandy/python/destCSV/'
src_CSVDir = 'c:/kenandy/python/sourceCSV/'
stg_CSVDir = 'c:/kenandy/python/stageCSV/'
configDir = 'c:/kenandy/python/Configuration/'

sOBject_src, sObject_stg = '', ''
src_namespace, dest_namespace = '',''


def sf_login_src():

    #11/2/2017
    credential_src, credential_dest, listofsObject = init_config()
    print('*** credential: ***' , credential_src)
    sf = sf_Login(credential_src[0], credential_src[1], credential_src[2])
    return sf

def sf_login_dest():

    #11/2/2017
    credential_src, credential_dest, listofsObject = init_config()
    print(credential_dest)
    sf = sf_Login(credential_dest[0], credential_dest[1], credential_dest[2])
    return sf

def init_config():

    print ('*** init_config *** processing')

    objectList = []
    credential_src = []
    credential_dest = []
    cnt = 0

    config = configDir + 'Configuration' + '.csv'
    #print (config)
    with open(config, "r") as csvfile:
        dataset = csv.reader(csvfile)
        for row in dataset:
            cnt = cnt +1
            if row[0].upper() == "USERNAME_SRC":
                credential_src.append(row[1])
                credential_dest.append(row[3])
            if row[0].upper() == "PASSWORD_SRC":
                credential_src.append(row[1])
                credential_dest.append(row[3])
            if row[0].upper() == 'SECURITY TOKEN_SRC':
                credential_src.append(row[1])
                credential_dest.append(row[3])
            if row[0].upper() == 'src_namespace':
                credential_src.append(row[1])
                credential_dest.append(row[3])
            if cnt > 5:
                if  row[5].upper() !="NA":
                    row[5] = credential_src[3]
                    row[6] = credential_dest[3]
                    objectList.append(row)
                elif row[5] == 'NA':
                    row[5] = ''
                    row[6] = ''
                    objectList.append(row)
        print('*** credential: ', credential_src, credential_dest)
        return (credential_src,credential_dest,objectList)


def init_sObject(src_namespace,dest_namespace,sObject):

    #11/2/2017
    print ('*** init_sObjectFile *** processing')
    if src_namespace == '' and src_namespace == dest_namespace:
        src_sObject = sObject + '_src'
        stg_sObject = sObject + '_stg'
        dest_dest = sObject + '_dest'
    elif src_namespace == '' and dest_namespace != '':
        dest_namespace = dest_namespace + '__'
        src_sObjectCSV = sObject + '_src'
        stg_sObjectCSV = dest_namespace +   sObject + '_stg'
        dest_sObjectCSV = dest_namespace +  sObject + '_dest'
    elif src_namespace != '' and src_namespace == dest_namespace:
        src_namespace = src_namespace.upper() + '__'
        dest_namespace = dest_namespace.upper() + '__'
        src_sObjectCSV = src_namespace + sObject +'_src'
        stg_sObjectCSV = dest_namespace + sObject + '_stg'
        dest_sObjectCSV = dest_namespace + sObject + '_dest'
    elif src_namespace != '' and dest_namespace == '':
        src_namespace = src_namespace.upper() + '__'
        src_sObjectCSV = src_namespace  + sObject + '_src'
        stg_sObjectCSV = sObject + '_stg'
        dest_sObjectCSV = sObject + '_dest'
    elif src_namespace != '' and src_namespace != dest_namespace:
        src_namespace = src_namespace.upper() + '__'
        dest_namespace = dest_namespace.upper() + '__'
        src_sObjectCSV = src_namespace  + sObject + '_src'
        stg_sObjectCSV = dest_namespace  + sObject + '_stg'
        dest_sObjectCSV = dest_namespace + sObject + '_dest'
    else:
        print ('unknown error')
        pass

    src_sObjectFile =  src_CSVDir + src_sObjectCSV  + '.csv'
    stg_sObjectFile =  stg_CSVDir + stg_sObjectCSV  + '.csv'
    dest_sObjectFile =  dest_CSVDir + dest_sObjectCSV  + '.csv'

    print('*** sObject:',sObject, src_namespace, dest_namespace, src_sObjectCSV, stg_sObjectCSV,dest_sObjectCSV,src_sObjectFile, stg_sObjectFile, dest_sObjectFile)
    return sObject, src_namespace, dest_namespace, src_sObjectCSV, stg_sObjectCSV, dest_sObjectCSV,src_sObjectFile, stg_sObjectFile, dest_sObjectFile


def sf_export_object2CSV(sf, namespace, sObject, src_dest):

    # call init_sObject
    #11/2/2017
    myExtID = 'ExtId__c'
    mysObject = sObject

    if len(namespace) == 0 :
        if src_dest == 'src':
            mysObjectFile = sourceCSVDir + mysObject + '_src'  + '.csv'
        elif src_dest == 'dest':
            mysObjectFile = destCSVDir + mysObject + '_dest' + src_dest + '.csv'
    else:
        mysObject = namespace.upper() + '__' + sObject
        myExtID = namespace.upper() + '__' + 'ExtId__c'
        if src_dest == 'src':
            mysObjectFile = sourceCSVDir + mysObject + '_src'  + '.csv'
        elif src_dest == 'dest':
            mysObjectFile = destCSVDir + mysObject + '_dest'  + '.csv'
    print(mysObjectFile)


    'Call Salesforce and fetch records'
    try:
        salesforceObject = sf.__getattr__(mysObject)
        fieldNames = [field['name'] for field in salesforceObject.describe()['fields']]
        print (fieldNames)
        soqlQuery = "SELECT " + ", ".join(fieldNames) + " FROM " + mysObject
        sf_dataset = sf.query_all(soqlQuery)['records']
        print (sf_dataset)
    except:
        print ('*** exception error')
        exit()

    with open(mysObjectFile, 'w', newline = '') as csvfile:
        dataset = csv.DictWriter(csvfile,fieldnames=fieldNames)
        dataset.writeheader()
        for row in sf_dataset:
            if myExtID in fieldNames:
                row[myExtID] = reversed_string(row['Id'])
            # each row has a strange attributes key we don't want
            row.pop('attributes',None)
            dataset.writerow(row)
    csvfile.close()


def src_add_RecordIDs2Config(src_namespace, sObject):

    # write source object to master object record
    # Sourcedir = "c:/kenandy/python/sourceCSV/"

    myExtID = 'ExtId__c'
    myObject = sObject

    if len(src_namespace) > 0:
        myObject = src_namespace.upper() + '__' + sObject
        myExtID = src_namespace.upper() + '__'+ 'ExtId__c'
    print(myObject)
    myObjectFile = sourceCSVDir + myObject + '_src.csv'
    print(myObjectFile)
    dataset_tmp = []

    file = configDir + "recordIDs" + '.csv'
    with open(file, 'r') as csvfile:
        dataset_config = csv.reader(csvfile)
        next(dataset_config)
        list = [records for records in dataset_config if records[0] != sObject or (records[0] == sObject and records[4].upper() != 'NEW')]
    print('xxxx', list)

    with open(myObjectFile) as csvfile:
        source = csv.DictReader(csvfile)
        fieldNames = source.fieldnames
        #print ('***7777777fieldnames:', fieldNames)
        for row in source:
            print (row)
            id = row["Id"]
            name = row ["Name"]
            if myExtID in fieldNames:
                extID = row [myExtID]
            else:
                extID = row["Name"]
            record = [sObject, extID, row['Name'], row ["Id"], 'New']
            print (record)
            dataset_tmp.append(record)
    print ('*** source ID *** ',dataset_tmp)

    #write back to recordId csv
    file = configDir + "recordIDs" + '.csv'
    with open(file, 'w', newline='') as csvfile:
        fieldNames = ['Object', 'ExtID', 'Name', 'Record_ID', 'New_Record_ID']
        writer = csv.writer(csvfile)
        writer.writerow(fieldNames)
        writer.writerows(list)
        writer.writerows(dataset_tmp)


def stg_copy2StageCSV(src_sObjectFile,stg_sObjectFile):

    print ('*** source file, stage File ***',src_sObjectFile,stg_sObjectFile)
    copyfile(src_sObjectFile, stg_sObjectFile)


def stg_namespace2fieldnames(src_namespace,dest_namespace,stg_sObjectFile):

    copyfile(stg_sObjectFile, stg_sObjectFile.replace('.csv', '_bak.csv'))
    tmp = stg_CSVDir + 'tmp_1.csv'


    #namespace in source  and target are the same, exit function
    if src_namespace != dest_namespace:
        #myStageCSV = stageCSVDir + stg_sObject + '.csv'
        #myStageCSV = stg_sObjectFile
        print ('myStageCSV', stg_sObjectFile )

        with open(stg_sObjectFile, 'r', ) as csvfile:
            dataset = csv.reader(csvfile)
            'header'
            fieldnames = next(dataset)
            print ('header[fieldnames]:(Before)' ,fieldnames)
            for i in range(len(fieldnames)):
                print (fieldnames[i][-3:])
                if fieldnames[i][-3:] == '__c' and len(src_namespace) > 0  :
                    print('*** namespace(src,stg) ***', src_namespace, dest_namespace, fieldnames[i])
                    fieldnames[i] = fieldnames[i].replace(src_namespace,dest_namespace)
                    print('*** namespace(src,stg) ***', src_namespace, dest_namespace,fieldnames[i])
                elif fieldnames[i][-3:] == '__c' and len(src_namespace) == 0:
                    fieldnames[i] = dest_namespace + fieldnames[i]
            print('header[fieldnames]:(After)', fieldnames)

            with open(tmp, 'w', newline='') as csvfile_1:
                dataset_staging = csv.writer(csvfile_1)
                dataset_staging.writerow(fieldnames)
                dataset_staging.writerows(dataset)
        copyfile(tmp, stg_sObjectFile)

    else:
        print('System log:' + 'Namespace is the same, Exit Functon')



def stg_fieldnamesExcluded(dest_namespace, sObject):

    fldsExcluded = []
    file = configDir + "fieldnamesExcluded" + '.csv'
    with open(file, 'r') as csvfile:
        dataset_stg = csv.reader(csvfile)
        try:
            fieldnames = [r[1] for r in dataset_stg if sObject in r]
            fldsExcluded = fieldnames[0].split(',')
            for i in range(len(fldsExcluded)):
                fldsExcluded[i] = dest_namespace +fldsExcluded[i]
        except IndexError:
            fldsExcluded=[]
    print('fieldnames excluded: ', fldsExcluded)
    return fldsExcluded


def stg_removeValueByFieldname(stg_sObjectFile,dest_namespace,sObject):

    copyfile(stg_sObjectFile, stg_sObjectFile.replace('.csv', '_bak.csv'))

    tmp = stg_CSVDir + 'tmp_2.csv'
    copyfile(stg_sObjectFile,tmp)
    dataset_tmp =[]

    with open(tmp) as csvfile:
        dataset_stg = csv.DictReader(csvfile)
        fieldNames = dataset_stg.fieldnames
        fldnames_default = ['OwnerId', 'IsDeleted', 'CreatedDate', 'CreatedById', 'LastModifiedDate','LastModifiedById', 'SystemModstamp', 'LastViewedDate', 'LastReferencedDate']
        fldnames_specifyByObject = stg_fieldnamesExcluded(dest_namespace,sObject)
        unique_fldnames = [r.upper() for r in fieldNames if
                           'IDENTIFIER' in r or 'UNIQUE' in r or 'KEY' in r]
        delete_fieldnames = unique_fldnames + fldnames_specifyByObject + fldnames_default
        print ('delete', delete_fieldnames)
        for row in dataset_stg:
            for i in range(len(delete_fieldnames)):
                row[delete_fieldnames[i]] = ''
            print(row)
            dataset_tmp.append(row)
        print (dataset_tmp)

        #copyfile(stageCSVFile,stageCSVFile_bak)
        with open(stg_sObjectFile, 'w', newline='') as csvfile1:
            writer = csv.DictWriter(csvfile1, fieldnames=fieldNames)
            print(dataset_tmp)
            writer.writeheader()
            writer.writerows(dataset_tmp)


def stg_getNewReferenceId(stg_sObjectFile):

    src_IDs = []
    dest_IDs = []


    copyfile(stg_sObjectFile, stg_sObjectFile.replace('.csv','_bak.csv'))

    stg_sObjectCSV = open(stg_sObjectFile, 'r')
    #dataset_stg = csv.reader(stg_sObjectCSV)
    tmp_3 = stg_CSVDir + 'tmp_3' + ".csv"
    tmp = open(tmp_3, 'w')
    #dataset_tmp = csv.writer(tmp)

    src_IDs, dest_IDs = config_RecordIdsMapping()
    #print (src_IDs, dest_IDs)

    rep = dict(zip(src_IDs, dest_IDs))
    print('rep', rep)

    def findReplace(find, replace):
        dataset_stg = stg_sObjectCSV.read()
        for item, replacement in zip(src_IDs, dest_IDs):
            dataset_stg = dataset_stg.replace(item, replacement)
        tmp.write(dataset_stg)

    for item in src_IDs:
        findReplace(item, rep[item])

    stg_sObjectCSV.close()
    tmp.close()

    # Copy from 'file to staging file
    copyfile(tmp_3, stg_sObjectFile)



def stg_sfBulkUpdate(sfBulk,namespace,sObject):

    myObject =sObject
    if len(namespace) > 0:
        myObject = namespace.upper() + '__' + sObject

    stageCSV = stageCSVDir + myObject  + '_stg.csv'
    print(stageCSV)
    #print (sObject)

    job = sfBulk.create_insert_job(myObject, contentType='CSV', concurrency='Parallel')


    with open(stageCSV) as csvfile:
        reader = csv.DictReader(csvfile)
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
        print("Done. Data Uploaded.")


def dest_update_dest_IDs2Config(dest_namespace,sObject):

    #config = 'records_id'
    newIDs = []
    configRecords = []

    tmp = configDir + 'tmp' + '.csv'
    configFile = configDir + 'recordIDs' + '.csv'
    print (configFile,staging)
    copyfile(configFile,tmp)

    ##Object new record ids after importing
    dest_newIDs = getObjectNewRecordIds(namespace_dest,sObject,dest_sObjectFile)
    print (newObjectRecordIds)

    with open(tmp, 'r') as csvfile:
        dataset_tmp = csv.reader(csvfile)

        for item in dest_newIDs:
            print ("zzzzzzzzzzzz",item)
            myExtID = item['ExtID']
            print ("EXTID", myExtID)
            dest_ID = newRecord['New_Record_ID']

            # open config file and filter by extid
            found_src_ID = [item for item in dataset_tmp if myExtID in item]
            print("Found", found_src_ID)
            found_src_ID[0][4] = dest_ID
            print(findRecord)
            print(configRecords)

        with open(configFile, 'w', newline='') as csvFile:
            writer = csv.writer(tmpFile)
            writer.writerows(dataset_tmp)

    csvFile.close
    tmpFile.close

    #copyfile (tmpFile,configFile)


def Zdest_update_dest_IDs2Config(namespace_dest,dest_sObjectFile):

    myObject = sObject
    if len(namespace) > 0:
        myObject = namespace.upper() + '__'+ sObject
        #extID = namespace.upper() + '__' + 'ExtId__c'
    print("myObject:", myObject)

    'Open Loc sObject'
    loc_ObjectFile = localCSVDir + myObject + '_dest.csv'
    print(loc_ObjectFile)

    config = 'records_id'
    newRecordIds = []
    configRecords = []
    #newObjectRecordIds =[]

    staging = configDir + 'staging' + '.csv'
    configFile = configDir + 'recordIDs' + '.csv'
    print (configFile,staging)
    copyfile(configFile,staging)

    ##Object new record ids after importing
    getObjectNewRecordIds(namespace_dest,sObject)
    print (newObjectRecordIds)

    with open(configFile, 'r') as csvfile:
        dataset_config = csv.reader(csvfile)
        for row in config:
            configRecords.append(row)
            #print(configRecords)

        for newRecord in newObjectRecordIds:
            print ("zzzzzzzzzzzz",newRecord)
            myExtID = newRecord['ExtID']
            print ("EXTID", myExtID)

            # with open(sObjectFile) as csvfile:
            #extId = 'a0q1I000000Xl3XQAS'
            dest_IDs = newRecord['New_Record_ID']

            # open config file and filter by extid
            findRecord = [r for r in configRecords if myExtID in r]
            print("Found", findRecord)
            findRecord[0][4] = dest_IDs
            print(findRecord)
            print(configRecords)

        with open(staging, 'w', newline='') as csvfile2:
            writer = csv.writer(csvfile2)
            writer.writerows(configRecords)
    csvfile2.close

    copyfile (staging,configFile)




#######################################################################################################################
# Other functon
#######################################################################################################################


def sf_Login(username,password,security_token):

    sf = Salesforce(username=username, password=password, security_token=security_token)
    return sf


def sf_Bulk_Login(username,password,security_token):

    sfBulk = SalesforceBulk(username=username, password=password,security_token=security_token)
    return sfBulk


def reversed_string(string):

    string = string[::-1]
    return string


def getdest_IDs(IdList, Id, dest_IDs):

    row = [r for r in IdList if Id in r]
    print(row)
    #row[0][4] = dest_IDs
    #print(row)
    #print(IdList)
    #return IdList


def config_RecordIdsMapping():

    dir = 'c:/kenandy/python/Configuration/'
    config = 'recordIDs'
    configFile = configDir + config + '.csv'
    openFile = open(configFile, "r")
    reader = csv.DictReader(openFile)
    src_IDs = ['']
    dest_IDss = ['']
    for row in reader:
        if row['New_Record_ID'] != 'New ID':
            #print(row['Record_ID'], row['New_Record_ID'])
            src_IDs.append(row['Record_ID'])
            dest_IDss.append(row['New_Record_ID'])
    #print (src_IDs,dest_IDss)
    return src_IDs,dest_IDss


def get_dest_IDs(namespace, sObject):

    # write source object to master object record
    # Sourcedir = "c:/kenandy/python/sourceCSV/"
    myObject = sObject
    dest_IDs = []

    if len(namespace) > 0:
        myObject = namespace.upper()+ '__' + sObject
        extID = namespace.upper()+ '__' + 'ExtId__c'
    else:
        extID = 'ExtId__c'
    print("ssssssssssss", myObject)
    myObjectFile = localCSVDir + myObject + '_loc.csv'
    print("xxxxnnnn",myObjectFile)


    with open(dest_sObjectFile) as csvfile:
        dataset_dest = csv.DictReader(csvfile)
        for row in dataset_dest:
            id = row["Id"]
            name = row ["Name"]
            dest_ID = {'Object' : sObject, 'ExtID' :row[extID], 'Name': row ['Name'], 'Record_ID': "", 'New_Record_ID': row["Id"]}
            dest_IDs.append(dest_ID)

        print ('dest IDs',dest_IDs)
        return dest_IDs

####################################################################################################

