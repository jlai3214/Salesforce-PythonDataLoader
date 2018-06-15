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
destCSVDir = 'c:/kenandy/python/destCSV/'
sourceCSVDir = 'c:/kenandy/python/sourceCSV/'
stageCSVDir = 'c:/kenandy/python/stageCSV/'
configDir = 'c:/kenandy/python/Configuration/'

sOBject_src, sObject_stg = '', ''
namespace_src, namespace_stg = '',''


def sf_login_source():

    credential_src, credential_dest, listofsObject = init_config()
    print(credential_src)
    sf = sf_Login(credential_src[0], credential_src[1], credential_src[2])
    return sf

def sf_login_dest():

    credential_src, credential_dest, listofsObject = init_config()
    print(credential_dest)
    sf = sf_Login(credential_dest[0], credential_dest[1], credential_dest[2])
    return sf

def init_config():

    objectList = []
    credential_src = []
    credential_dest = []
    cnt = 0

    config = configDir + 'Configuration' + '.csv'
    print (config)
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
            if row[0].upper() == 'NAMESPACE_SRC':
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
    #print(properties)
    #print (credential_src, credential_dest)
    #print (objectList)
    return (credential_src,credential_dest,objectList)


def init_sObjectFile(namespace_src,namespace_stg,sObject):

    print ('*** init_sObjectFile *** processing')
    if namespace_src == namespace_stg and  namespace_src == '':
        sObject_src = sObject + '_src'
        sObject_stg = sObject + '_stg'
    elif namespace_src == '' :
        sObject_src = sObject +'_src'
        sObject_stg = namespace_stg + '__' + sObject + '_stg'
    elif namespace_stg == '':
        sObject_src = namespace_src + '__' + sObject +'_src'
        sObject_stg =  sObject +'_stg;'
    else:
        sObject_src = namespace_src + '__' + sObject + '_src'
        sObject_stg = namespace_stg + '__' + sObject + '_stg'

    print("#####", sObject_src, sObject_stg)
    return sObject_src, sObject_stg


def update_namespace2fieldnames(namespace_src,namespace_stg,sObject):

    print (namespace_src)
    #namespace in source  and target are the same, exit function
    if namespace_src == namespace_stg:
        print ('System log:' + 'Namespace is the same, Exit Functon')
        exit()

    #StageCSV file
    if namespace_stg == "":
        myStageCSV = stageCSVDir + sObject + '_stg.csv'
    else:
        myStageCSV = stageCSVDir + namespace_stg + '__' +sObject + '_stg.csv'

    myStaging = stageCSVDir + 'tmp.csv'

    with open(myStageCSV, 'r', ) as csvfile:
        dataset = csv.reader(csvfile)
        'header'
        fieldnames = next(dataset)
        print ('header[fieldnames]:' ,fieldnames)
        if namespace_src == namespace_stg :
            pass
        elif namespace_src == '' and namespace_stg != '':
            fieldnames__c = [r for r in fieldnames if '__c' in r]
            print (fieldnames__c)
        elif namespace_src != '' and namespace_stg == '':
            fieldnames__c = [r for r in fieldnames if '__c' in r]
            print (fieldnames__c)


           """
            for i in range(len(fieldnames)):
            #if header[i].find('__c') > -1:

            if fieldnames[i][-3:] == '__c':
                    pass
                elif namespace_src == '' and namespace_stg != '' :
                    fieldnames[i] = header[i].replace(namespace_src,namespace_stg)
                print(i, header[i])
        """
        with open(myStaging, 'w', newline='') as csvfile_1:
            dataset_staging = csv.writer(csvfile_1)
            dataset_staging.writerow(header)
            dataset_staging.writerows(dataset)

    copyfile(myStaging,myStageCSV)


def sf_export_object2CSV(sf,namespace,sObject,src_dest):


    myObject = sObject
    myExtID = 'ExtId__c'

    if len(namespace) > 0:
        myObject = namespace.upper() + '__' + sObject
        myExtID = namespace.upper() + '__' + 'ExtId__c'

    if src_dest == 'dest':
        myObjectFile = destCSVDir + myObject + '_dest.csv'
    else:
        myObjectFile = sourceCSVDir + myObject + '_src.csv'
    print(myObject)
    print(myObjectFile)

    salesforceObject = sf.__getattr__(myObject)
    fieldNames = [field['name'] for field in salesforceObject.describe()['fields']]
    print (fieldNames)
    soqlQuery = "SELECT " + ", ".join(fieldNames) + " FROM " + myObject
    records = sf.query_all(soqlQuery)['records']
    print (records)

    with open(myObjectFile, 'w', newline = '') as csvfile:
        dataset_src = csv.DictWriter(csvfile,fieldnames=fieldNames)
        dataset_src.writeheader()
        for row in records:
            if myExtID in fieldNames:
                row[myExtID] = reversed_string(row['Id'])
            # each row has a strange attributes key we don't want
            row.pop('attributes',None)
            dataset_src.writerow(row)
        csvfile.close()


def z_config_recordIDs(namespace, sObject):

    # write source object to master object record
    # Sourcedir = "c:/kenandy/python/sourceCSV/"

    myExtID = 'ExtId__c'
    myObject = sObject

    if len(namespace) > 0:
        myObject = namespace.upper() + '__' + sObject
        myExtID = namespace.upper() + '__' + 'ExtId__c'
        print(myObject)
        myObjectFile = sourceCSVDir + myObject + '_src.csv'
        print(myObjectFile)
        dataset = []

    with open(myObjectFile) as csvfile:
        dataset_src = csv.DictReader(csvfile_1)
        fieldNames = csv.DictReader(csvfile_1).fieldnames
        for row in dataset_src:
            id = row["Id"]
            name = row["Name"]
            if myExtID in fieldNames:
                extID = row[myExtID]
            else:
                extID = row["Name"]
            record = {'Object': sObject, 'ExtID': extID, 'Name': row["Name"], 'Record_ID': row["Id"],
                              'New_Record_ID': "New"}
            dataset.append(record)
        print('*** source *** ', dataset)

        file = configDir + "recordIDs" + '.csv'
        with open(file, 'a', newline='') as csvfile_2:
            fieldNames = ['Object', 'ExtID', 'Name', 'Record_ID', 'New_Record_ID']
            writer = csv.DictWriter(csvfile_2, fieldnames=fieldNames)
            # writer.writeheader()
            writer.writerows(dataset)


def config_sourceIDs(namespace, sObject):

    # write source object to master object record
    # Sourcedir = "c:/kenandy/python/sourceCSV/"

    myExtID = 'ExtId__c'
    myObject = sObject

    if len(namespace) > 0:
        myObject = namespace.upper() + '__' + sObject
        myExtID = namespace.upper() + '__'+ 'ExtId__c'
    print(myObject)
    myObjectFile = sourceCSVDir + myObject + '_src.csv'
    print(myObjectFile)
    dataset_tmp = []

    with open(myObjectFile) as csvfile:
        source = csv.DictReader(csvfile)
        fieldNames = source.fieldnames
        print ('***7777777fieldnames:', fieldNames)
        for row in source:
            print (row)
            id = row["Id"]
            name = row ["Name"]
            if myExtID in fieldNames:
                extID = row [myExtID]
            else:
                extID = row["Name"]
            record = {'Object':sObject,'ExtID': extID,'Name':row["Name"],'Record_ID': row ["Id"], 'New_Record_ID': "New"}
            print (record)
            dataset_tmp.append(record)

        print ('*** source *** ',dataset_tmp)

        file = configDir + "recordIDs" + '.csv'
        with open(file, 'a',  newline = '') as csvfile:
            fieldNames = ['Object', 'ExtID', 'Name', 'Record_ID', 'New_Record_ID']
            writer = csv.DictWriter(csvfile, fieldnames=fieldNames)
            #writer.writeheader()
            writer.writerows(dataset_tmp)




def importsObject2Dest(namespace_src, namespace_dest,sObject):

    """
    1) Copy to staging
    2) Prep staging
        1) get new reference id
        2) Empty columns
    3) SF Bulk
    4) Export to Local
    5) Update New record ID

    """
    #copy2StageCSV(namespace_src,namespace_dest, sObject)
    replaceNewReferenceId(namespace_dest, sObject)
    removeValueByFieldname(namespace_dest, sObject)



def copy2StageCSV(namespace_src,namespace_dest, sObject):

    if len(namespace_src) > 0:
        sourceFile = sourceCSVDir + namespace_src + '__' + sObject + '_src' + '.csv'
    else:
        sourceFile = sourceCSVDir + sObject + '_src' + '.csv'

    if len(namespace_dest) > 0:
        stageFile = stageCSVDir + namespace_src + '__' + sObject + '_stg '+ '.csv'
        stageFileBak = stageCSVDir + namespace_src + '__' + sObject + '_stg_bak '+ '.csv'
    else:
        stageFile = stageCSVDir + sObject + '_stg' + '.csv'
        stageFileBak = stageCSVDir +  sObject + '_stg_bak ' + '.csv'

    copyfile(sourceFile, stageFile)
    copyfile(sourceFile, stageFileBak)


def fieldnamesExcluded(sObject):

    fldsExcluded = []
    file = configDir + "fieldnamesExcluded" + '.csv'
    with open(file, 'r') as csvfile:
        dataset_stg = csv.reader(csvfile)
        try:
            fieldnames = [r[1] for r in dataset_stg if sObject in r]
            print ('Fieldnames:' ,fieldnames)
            fldsExcluded = fieldnames[0].split(',')
            print ('fieldnames excluded: ', fldsExcluded)
        except IndexError:
            fldsExcluded=[]
        print('fieldnames excluded: ', fldsExcluded)

    return fldsExcluded


def removeValueByFieldname(namespace_dest,sObject):

    myObject = sObject
    if len(namespace_dest) > 0:
        myObject = namespace_dest.upper() +'__' + sObject
    print(myObject)
    stagingFile = stageCSVDir + 'staging' + ".csv"
    stageFile = stageCSVDir + myObject + '_stg' + ".csv"
    dataset_tmp =[]

    with open(stagingFile) as csvfile:
        dataset_stg = csv.DictReader(csvfile)
        fieldNames = dataset_stg.fieldnames

        fldnames_default = ['OwnerId', 'IsDeleted', 'CreatedDate', 'CreatedById', 'LastModifiedDate','LastModifiedById', 'SystemModstamp', 'LastViewedDate', 'LastReferencedDate']
        fldnames_specifyByObject = fieldnamesExcluded(sObject)
        unique_fldnames = [r.upper() for r in fieldNames if
                           'IDENTIFIER' in r or 'UNIQUE' in r or 'KEY' in r]
        print('STG_fieldnames', fieldNames)
        print ('UNIQUE FieldNames', unique_fldnames)
        delete_fieldnames = unique_fldnames + fldnames_specifyByObject + fldnames_default
        delete_fieldnames = [x.upper() for x in delete_fieldnames]
        print ('delete', delete_fieldnames)
        for row in dataset_stg:
            for i in range(len(delete_fieldnames)):
                #print(delete_fieldnames[i])
                row[delete_fieldnames[i]] = ''
            # for if "*Identifierf__C in FieldNames In
            print(row)
            dataset_tmp.append(row)

        print (stageFile)
        with open(stageFile, 'w', newline='') as csvfile1:
            writer = csv.DictWriter(csvfile1, fieldnames=fieldNames)
            writer.writeheader()
            writer.writerows(dataset_tmp)


def replaceNewReferenceId(namespace_dest,sObject):

    sourceId = []
    newId = []
    myObject = sObject

    if len(namespace_dest) > 0:
        myObject = namespace_dest.upper() +'__' + sObject
    print(myObject)

    stageFile = stageCSVDir + myObject + '_stg' + ".csv"
    stageFile_bak = stageCSVDir + myObject + '_stg_bak' + ".csv"
    stagingFile = stageCSVDir + 'staging' + ".csv"
    print(stageFile)
    copyfile(stageFile, stageFile_bak)


    stgCVSFile = open(stageFile, 'r')
    reader = csv.reader(stgCVSFile)
    staging = open(stagingFile, 'w')
    writer = csv.writer(staging)

    sourceIDs, newId = config_RecordIdsMapping()
    print (sourceIDs)
    print(newId)

    rep = dict(zip(sourceIDs, newId))
    print(rep)

    def findReplace(find, replace):
        stg = stgCVSFile.read()
        print('ssssss', stg)
        for item, replacement in zip(sourceIDs, newId):
            print(item)
            print(replacement)
            stg = stg.replace(item, replacement)
            print("s*****", stg)
        staging.write(stg)

    for item in sourceIDs:
        findReplace(item, rep[item])

    stgCVSFile.close()
    staging.close()

    # Copy from 'file to staging file
    copyfile(stagingFile,stageFile)



def sfBulkUpdate(sfBulk,namespace,sObject):

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




def UpdateObjectNewIds(namespace,sObject):

    myObject = sObject
    if len(namespace) > 0:
        myObject = namespace.upper() + '__'+ sObject
        #extID = namespace.upper() + '__' + 'ExtId__c'
    print("myObject:", myObject)

    'Open Loc sObject'
    loc_ObjectFile = localCSVDir + myObject + '_loc.csv'
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
    getObjectNewRecordIds(namespace,sObject)
    print (newObjectRecordIds)

    with open(configFile, 'r') as csvfile:
        config = csv.reader(csvfile)
        for row in config:
            configRecords.append(row)
            #print(configRecords)

        for newRecord in newObjectRecordIds:
            print ("zzzzzzzzzzzz",newRecord)
            myExtID = newRecord['ExtID']
            print ("EXTID", myExtID)

            # with open(sObjectFile) as csvfile:
            #extId = 'a0q1I000000Xl3XQAS'
            newId = newRecord['New_Record_ID']

            # open config file and filter by extid
            findRecord = [r for r in configRecords if myExtID in r]
            print("Found", findRecord)
            findRecord[0][4] = newId
            print(findRecord)
            print(configRecords)

        with open(staging, 'w', newline='') as csvfile2:
            writer = csv.writer(csvfile2)
            writer.writerows(configRecords)
    csvfile2.close

    copyfile (staging,configFile)



def getObjectNewRecordIds(namespace, sObject):

    # write source object to master object record
    # Sourcedir = "c:/kenandy/python/sourceCSV/"
    myObject = sObject

    if len(namespace) > 0:
        myObject = namespace.upper()+ '__' + sObject
        extID = namespace.upper()+ '__' + 'ExtId__c'
    else:
        extID = 'ExtId__c'
    print("ssssssssssss", myObject)
    myObjectFile = localCSVDir + myObject + '_loc.csv'
    print("xxxxnnnn",myObjectFile)


    with open(myObjectFile) as csvfile:
        source = csv.DictReader(csvfile)
        for row in source:
            id = row["Id"]
            name = row ["Name"]
            record = {'Object' : sObject, 'ExtID' :row[extID], 'Name': row ['Name'], 'Record_ID': "", 'New_Record_ID': row["Id"]}
            newObjectRecordIds.append(record)

        print ('Local',newObjectRecordIds)
        return newObjectRecordIds







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


def getNewId(IdList, Id, newId):

    row = [r for r in IdList if Id in r]
    print(row)
    #row[0][4] = newId
    #print(row)
    #print(IdList)
    #return IdList


def config_RecordIdsMapping():

    dir = 'c:/kenandy/python/Configuration/'
    config = 'recordIDs'
    configFile = configDir + config + '.csv'
    openFile = open(configFile, "r")
    reader = csv.DictReader(openFile)
    sourceIDs = ['']
    newIDs = ['']
    for row in reader:
        if row['New_Record_ID'] != 'New ID':
            print(row['Record_ID'], row['New_Record_ID'])
            sourceIDs.append(row['Record_ID'])
            newIDs.append(row['New_Record_ID'])
    print (sourceIDs,newIDs)
    return sourceIDs,newIDs


####################################################################################################

