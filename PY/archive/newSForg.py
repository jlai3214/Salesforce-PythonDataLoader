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
localCSVDir = 'c:/kenandy/python/localCSV/'
sourceCSVDir = 'c:/kenandy/python/sourceCSV/'
stageCSVDir = 'c:/kenandy/python/stageCSV/'
configDir = 'c:/kenandy/python/Configuration/'

sOBject_src, sObject_stg = '', ''
namespace_src, namespace_stg = '',''


def sfLogin(username,password,security_token):

    sf = Salesforce(username=username_src, password=password_src, security_token=security_token_src)
    return sf




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

#init_sObjectFile('xyx','abc','Account')
#Declare vaiables




def export_object2CSV(sf,namespace,sObject,dir):


    myObject = sObject
    myExtID = 'ExtId__c'

    if dir.upper() == 'SOURCE':
        myObjectFile = sourceCSVDir + myObject + '_src.csv'
    elif dir.upper() == 'LOCAL' or dir == '':
        myObjectFile = localCSVDir + myObject + '_loc.csv'
    print(myObjectFile)

    if len(namespace) > 0:
        myObject = namespace.upper() +'__' + sObject
        myExtID = namespace.upper() +'__' + 'ExtId__c'
    print(myObject)
    print(myObjectFile)

    salesforceObject = sf.__getattr__(myObject)
    fieldNames = [field['name'] for field in salesforceObject.describe()['fields']]
    print (fieldNames)
    soqlQuery = "SELECT " + ", ".join(fieldNames) + " FROM " + myObject
    records = sf.query_all(soqlQuery)['records']
    print (records)

    with open(myObjectFile, 'w', newline = '') as csvfile:
        writer = csv.DictWriter(csvfile,fieldnames=fieldNames)
        writer.writeheader()
        for row in records:
            #row['KNDY4__ExtId__c'] = 'ExtID-' + row['Id']
            row[myExtID] = reversed_string(row['Id'])
            # each row has a strange attributes key we don't want
            #del row['Id']
            row.pop('attributes',None)
            #emptyfieldnames = ['OwnerId', 'IsDeleted', 'CreatedDate', 'CreatedById', 'LastModifiedDate','LastModifiedById', 'SystemModstamp', 'LastViewedDate', 'LastReferencedDate']
            emptyfieldnames = ['IsDeleted','CreatedDate','CreatedById','LastModifiedDate','LastModifiedById','SystemModstamp','LastViewedDate','LastReferencedDate']
            for i in range(len(emptyfieldnames)):
                del row[emptyfieldnames[i]]
            writer.writerow(row)
        csvfile.close()


def namespace_fieldnames(namespace_src,namespace_stg,sObject):

    print (namespace_src)
    if namespace_src == namespace_stg:
        exit()

    if namespace_stg == "":
        myStageCSV = stageCSVDir + sObject + '_stg.csv'
    else:
        myStageCSV = stageCSVDir + namespace_stg + '__' +sObject + '_stg.csv'

    myStaging = stageCSVDir + 'staging.csv'

    with open(myStageCSV, 'r', ) as csvfile:
        records = csv.reader(csvfile)
        'header'
        header = next(records)
        print ('header[fieldnames]:' ,header)
        for i in range(len(header)):
            if header[i].find('__c') > -1:
                if namespace_src == '':
                    header[i] = namespace_stg + '__' + header[i]
                    print(i, header[i])
                else:
                    header[i] = header[i].replace(namespace_src,namespace_stg)
                print(i, header[i])



        with open(myStaging, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(header)
            writer.writerows(records)

    copyfile(myStaging,myStageCSV)

#namespace_fieldnames("","KNDY4",'GL_Type__c')

"""
        
        for row in records:
            row[myExtID] = reversed_string(row['Id'])
            # each row has a strange attributes key we don't want
            row.pop('attributes',None)
            #emptyfieldnames = ['OwnerId', 'IsDeleted', 'CreatedDate', 'CreatedById', 'LastModifiedDate','LastModifiedById', 'SystemModstamp', 'LastViewedDate', 'LastReferencedDate']
            emptyfieldnames = ['IsDeleted','CreatedDate','CreatedById','LastModifiedDate','LastModifiedById','SystemModstamp','LastViewedDate','LastReferencedDate']
            for i in range(len(emptyfieldnames)):
                del row[emptyfieldnames[i]]
            writer.writerow(row)
        csvfile.close()

"""




def sfBulkUpdate(namespace,sObject):

    myObject =sObject
    if len(namespace) > 0:
        myObject = namespace.upper() + '__' + sObject

    stageCSV = stageCSVDir + myObject  + '_stg.csv'
    print(stageCSV)
    #print (sObject)

    sfBulk = SalesforceBulk(username=username_loc, password=password_loc, security_token=security_token_loc)
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


def replaceNewReferenceId(namespace,sObject):

    sourceId = []
    newId = []

    myObject = sObject
    if len(namespace) > 0:
        myObject = namespace.upper() +'__' + sObject
    print(myObject)
    stageFile = stageCSVDir + myObject + '_stg' + ".csv"
    stageFile_bak = stageCSVDir + myObject + '_stg_bak' + ".csv"
    staging = stageCSVDir + 'staging' + ".csv"
    print(stageFile)

    copyfile(stageFile, stageFile_bak)

    input = open(stageFile, 'r')
    reader = csv.reader(input)
    output = open(staging, 'w')
    writer = csv.writer(output)

    sourceIDs, newId = configRecordIdList()
    print (sourceIDs)
    print(newId)

    #extIDs = ['0011I000002S2kuQAC', 'LTE', 'a0g1I000000TUbHQAW', 'a0q1I000000Xl3XQAS', 'a0q1I000000Xl3YQAS', 'a0q1I000000Xl3ZQAS', 'a0q1I000000Xl3aQAC', 'a0q1I000000Xl3bQAC']
    #newId = ['0011I000002S2kuQACJEFFxxx', 'XXXXXG', 'a0g1I000000TUbHQAW_new', '1-a0q1I000000Xl3XQAS', '2-a0q1I000000Xl3YQAS', '3-a0q1I000000Xl3ZQAS', '4-a0q1I000000Xl3aQAC', '5-a0q1I000000Xl3bQAC']

    #rep = dict(zip(sourceId, newId))
    rep = dict(zip(sourceIDs, newId))

    print(rep)

    def findReplace(find, replace):
        s = input.read()
        print('ssssss', s)
        for item, replacement in zip(sourceIDs, newId):
            print(item)
            print(replacement)
            s = s.replace(item, replacement)
            print("s*****", s)
        output.write(s)

    for item in sourceIDs:
        findReplace(item, rep[item])

    input.close()
    output.close()

    # Copy from 'file to staging file

    copyfile(staging,stageFile)



def prestaging(namespace,sObject):

    sourceId = []
    newId = []

    myObject = sObject
    if len(namespace) > 0:
        myObject = namespace.upper() +'__' + sObject
    print(myObject)
    stageFile = stageCSVDir + myObject + '_stg' + ".csv"
    stageFile_bak = stageCSVDir + myObject + '_stg_bak' + ".csv"
    staging = stageCSVDir + 'staging' + ".csv"
    print(stageFile)

    copyfile(stageFile, stageFile_bak)

    input = open(stageFile, 'r')
    reader = csv.reader(input)
    output = open(staging, 'w')
    writer = csv.writer(output)

    sourceIDs, newId = configRecordIdList()
    print (sourceIDs)
    print(newId)

    #extIDs = ['0011I000002S2kuQAC', 'LTE', 'a0g1I000000TUbHQAW', 'a0q1I000000Xl3XQAS', 'a0q1I000000Xl3YQAS', 'a0q1I000000Xl3ZQAS', 'a0q1I000000Xl3aQAC', 'a0q1I000000Xl3bQAC']
    #newId = ['0011I000002S2kuQACJEFFxxx', 'XXXXXG', 'a0g1I000000TUbHQAW_new', '1-a0q1I000000Xl3XQAS', '2-a0q1I000000Xl3YQAS', '3-a0q1I000000Xl3ZQAS', '4-a0q1I000000Xl3aQAC', '5-a0q1I000000Xl3bQAC']

    #rep = dict(zip(sourceId, newId))
    rep = dict(zip(sourceIDs, newId))

    print(rep)

    def findReplace(find, replace):
        s = input.read()
        print('ssssss', s)
        for item, replacement in zip(sourceIDs, newId):
            print(item)
            print(replacement)
            s = s.replace(item, replacement)
            print("s*****", s)
        output.write(s)

    for item in sourceIDs:
        findReplace(item, rep[item])

    input.close()
    output.close()

    # Copy from 'file to staging file

    copyfile(staging,stageFile)



def configRecordIdList():
    dir = 'c:/kenandy/python/Configuration/'
    config = 'recordIDs'
    configFile = configDir + config + '.csv'
    openFile = open(configFile, "r")
    reader = csv.DictReader(openFile)
    sourceIDs = ['']
    newIDs = ['']
    for row in reader:
        print(row['Record_ID'], row['New_Record_ID'])
        sourceIDs.append(row['Record_ID'])
        newIDs.append(row['New_Record_ID'])
    print (sourceIDs,newIDs)
    return sourceIDs,newIDs


def init_config():

    listofsObjects = []
    credential_src = []
    credential_dest = []
    cnt = 0
    objectlist = 'false'

    config = configDir + 'Configuration' + '.csv'
    print (config)
    with open(config, "r") as csvfile:
        records = csv.reader(csvfile)
        for row in records:
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
                    listofsObjects.append(row)
                elif row[5] == 'NA':
                    row[5] = ''
                    row[6] = ''
                    listofsObjects.append(row)
    #print(properties)
    #print (credential_src, credential_dest)
    #print (listofsObjects)
    return (credential_src,credential_dest,listofsObjects)


def config_recordIDList(namespace, sObject):

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
    records = []

    with open(myObjectFile) as csvfile:
        source = csv.DictReader(csvfile)
        for row in source:
            id = row["Id"]
            name = row ["Name"]
            row[myExtID] = row [myExtID]
            #if row[myExtID] == '':
            #    row[myExtID] =  row ["Id"]
            record = {'Object':sObject,'ExtID':row[myExtID],'Name':row["Name"],'Record_ID': row ["Id"], 'New_Record_ID': "New"}
            records.append(record)

    print ('*** source *** ',records)

    file = configDir + "recordIDs" + '.csv'
    with open(file, 'a',  newline = '') as csvfile:
        fieldNames = ['Object', 'ExtID', 'Name', 'Record_ID', 'New_Record_ID']
        writer = csv.DictWriter(csvfile, fieldnames=fieldNames)
        #writer.writeheader()
        writer.writerows(records)


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


def copy2StageCSV(namespace_src,namespace_stg, sObject):

    sObject_src,sObject_stg = init_sObjectFile(namespace_src,namespace_stg,sObject)

    sourceFile = sourceCSVDir + sObject_src + '.csv'
    stageFile = stageCSVDir + sObject_stg + '.csv'

    copyfile(sourceFile,stageFile)



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

def getNewId(IdList, Id, newId):

    row = [r for r in IdList if Id in r]
    print(row)
    #row[0][4] = newId
    #print(row)
    #print(IdList)
    #return IdList




def run_sObject_stg():

    credential_src, credential_src, listofsObject = init_config()
    sf = sfLogin(credential_src[0], credential_src[1], credential_src[2])
    for row in listofsObject:
        if row[2].upper() == 'Y':  #Have ExtID
            print ("****", row)
            sObject = row[1]
            namespace = row[5]
            copy2StageCSV(row[5], row[6], row[1])
            namespace_fieldnames(row[5], row[6], row[1])




def run_export_srcOrg():

    """
    ############################################################################
    #1) export_object2CSV('KNDY4', 'GL_Type__c', 'source')
    #2) config_recordIDList('KNDY4', 'GL_Type__c')
    #3) copy2StageCSV('KNDY4', 'GL_Type__c')
    #4) replaceNewReferenceId('KNDY4', 'GL_Type__c')
    #5) Buil Update
    #6) export_object2CSV('KNDY4', 'GL_Type__c', 'local')
    #7) newObjectRecordIds = []
    #8) UpdateObjectNewIds('KNDY4', 'GL_Type__c')
    ############################################################################
    """

    # 1) 'Generate csv to LocalCSV'
    credential_src,credential_src,listofsObject = init_config()
    sf = sfLogin(credential_src[0],credential_src[1],credential_src[2])
    print ('*** export_srcOrg *** processing')
    for row in listofsObject:
        if row[2].upper() == 'Y':  #Have ExtID
            print ("****", row)
            sObject = row[1]
            namespace = row[5]
            export_object2CSV(sf, namespace, sObject,"source")
            config_recordIDList(row[5], row[1])
            #copy2StageCSV(row[5], row[6], row[1])



def Import():

    ############################################################################
    #1) export_object2CSV('KNDY4', 'GL_Type__c', 'source')
    #2) config_recordIDList('KNDY4', 'GL_Type__c')
    #3) copy2StageCSV('KNDY4', 'GL_Type__c')
    #4) replaceNewReferenceId('KNDY4', 'GL_Type__c')
    #5) Buil Update
    #6) export_object2CSV('KNDY4', 'GL_Type__c', 'local')
    #7) newObjectRecordIds = []
    #8) UpdateObjectNewIds('KNDY4', 'GL_Type__c')
    ############################################################################
    """

    # 1) 'Generate csv to LocalCSV'
    properties, listofsObject = configuration()
    print("outside:properties", properties)
    print ("outside:List of Objects", listofsObject)
    for row in listofsObject:
        if row[2].upper() == 'Y':
            print ("****", row)
            sObject = row[1]
            namespace = row[5]
            export_object2CSV(namespace, sObject,"source")


    # 2) 'Record ID to COnfiguration/recordsID'
    for row in listofsObject:
        print("****", row)
        sObject = row[1]
        namespace = row[5]
        sourceRecords(namespace, sObject)


    #3) update objects without any reference id
    #3.1    Copy to Staging folder
    for row in listofsObject:
        print("****", row)
        if row[3].upper() == 'N':
            print ('******','no ref')
            namespace = row[5]
            sObject = row[1]
            print (namespace,sObject)
            Copy2StageCSV(namespace,sObject)


            #3.2) Bullk Update

            #3.3) Call export to Local
            export_object2CSV(namespace, sObject, "local")

            #3.4) Update new record ID into config
            UpdateObjectNewIds(namespace,sObject)


    #4) update objects with refernce ids
    #4.1) Copy to Staging folder
    #newStageCSV('', 'sObject')

    #4.2) Update new reference ID

    #4.3) Bullk Update         # i) update objects without any reference id

    #4.4) Export to local folder
    #get_records2CSV(namespace, sObject, "local")

    #4.5) Update new record ID into config

    #5) Repeat step#4

"""


def reversed_string(string):
    string = string[::-1]
    #print(string)
    return string

#s = reversed_string('kkkkasdafdafsdafda')
#print (s)

####################################################################################################


credential_src,credential_dest,listofObject =init_config()
print (credential_src)
print (credential_dest)
print (listofObject)
x = init_sObjectFile(listofObject[10][5],listofObject[10][6],listofObject[10][1])
print (x)

run_export_srcOrg()
run_sObject_stg()
#config_recordIDList(listofObject[10][5],listofObject[10][1])
#copy2StageCSV(namespace,sobject)
#copy2StageCSV('','','account')
#configRecordIdList()
#replaceNewReferenceId(namespace,sobject)
sfBulkUpdate(namespace,sobject)
#sf = sfLogin('local')
#export_object2CSV(namespace,sobject,'local')
#newObjectRecordIds = []
#UpdateObjectNewIds(namespace,sobject)

#prestagingprep(namespace,sobject)

#x = namespace_sObject("","x",'account')
#print (x)