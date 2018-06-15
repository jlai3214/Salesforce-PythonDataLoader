import os,sys
sys.path.append(os.path.abspath("C:\Kenandy\Python\PY"))
from pathlib import Path
from KNDY import *


def run_export_srcData():

    myFuncName = sys._getframe().f_code.co_name
    print("\n")
    print ("****** %s *** processing ************"  % (myFuncName), end="")

    ID_Dict = 'C:\Kenandy\Python\Configuration\ID_Dictionary.csv'
    if IsFileOpen(ID_Dict):
        print("\n")
        print ('***************************** Job Aborted ****************************')
        exit()

    src_credential, dest_credential = init_credential()
    sObjectList =  init_sObjectList(src_credential['namespace'],dest_credential['namespace'])
    sf = sf_Login(src_credential['username'],src_credential['password'],src_credential['security_token'])

    for row in sObjectList:
        if row['src_status'].upper() == 'PENDING' and row['select'].upper() == 'Y':
            print("\n")
            print('****** exporting sObject:=%s *** processing ************, %s' % (row['sObject'], dict(row)),end = "" )
            sObject = row['sObject']
            src_namespace = row['src_namespace']
            dest_namespace = row['dest_namespace']
            whereCondition = row['whereCondition']
            print("\n")
            filesuffix = whereCondition[whereCondition.find("=")+2:].replace("'","")
            print(filesuffix)
            sObject, src_namespace, dest_namespace, src_sObjectCSV, stg_sObjectCSV, dest_sObejectCSV, src_sObjectFile, stg_sObjectFile, dest_sObjectFile = init_sObject(sObject, src_namespace, dest_namespace)
            if whereCondition != '':
                src_sObjectFile = src_sObjectFile.replace(sObject,sObject +'-' + filesuffix)

            sf_export_object2CSV(sf, sObject, src_namespace, src_sObjectFile,whereCondition)
            src_add_2_ID_Dict(sObject, src_namespace, src_sObjectFile)



def run_import_srcdata2Dest():


    myFuncName = sys._getframe().f_code.co_name
    print("\n")
    print ("****** %s *** processing ************"  % (myFuncName), end="")

    src_credential, dest_credential = init_credential()
    sObjectList = init_sObjectList(src_credential['namespace'], dest_credential['namespace'])
    sf = sf_Login(dest_credential['username'], dest_credential['password'], dest_credential['security_token'])
    sfBulk = sf_Bulk_Login(dest_credential['username'], dest_credential['password'], dest_credential['security_token'])

    for row in sObjectList:
        #if row['select'].upper() == 'Y':
        if row['dest_status'].upper() == 'PENDING' and row['select'].upper() == 'Y':
            print("\n")
            print("[STG**] %s (%s) *** processing ************" % (myFuncName, row['sObject']))
            print("****** %s *** processing ************" % (dict(row)))
            #recordType = row['recordType']
            whereCondition = row['whereCondition']
            print("\n")
            filesuffix = whereCondition[whereCondition.find("=") + 2:].replace("'", "")
            print(filesuffix)
            sObject, src_namespace, dest_namespace, src_sObjectCSV, stg_sObjectCSV, dest_sObejectCSV, src_sObjectFile,stg_sObjectFile, dest_sObjectFile = init_sObject(row['sObject'], row['src_namespace'], row['dest_namespace'])
            if whereCondition != '':
                src_sObjectFile = src_sObjectFile.replace(sObject, sObject + '-' + filesuffix)

            if IsFileExist(src_sObjectFile) == True:
                copyfile(src_sObjectFile, stg_sObjectFile)
                if IsCSVEmpty(stg_sObjectFile) == True:
                    print ('           *** file has empty row *** %s' %(stg_sObjectFile))
                    pass
                else:
                    stg_namespace2fieldnames(src_namespace, dest_namespace, stg_sObjectFile)
                    stg_namespace2fieldnames(src_credential['namespace'], dest_credential['namespace'], stg_sObjectFile)

                    stg_removeValueByFieldname(stg_sObjectFile, dest_namespace, sObject)
                    stg_getNewReferenceId(stg_sObjectFile)
                    stg_sfBulkUpdate(sfBulk,sObject,stg_sObjectFile)

                    ##dest
                    #sf_export_object2CSV(sf, sObject, dest_namespace, dest_sObjectFile,whereCondition)
                    sf_export_object2CSV(sf, sObject, dest_namespace, dest_sObjectFile, "")

                    if IsCSVEmpty(dest_sObjectFile) == True:
                        print('           *** file has empty row *** %s, job aborted' % (dest_sObjectFile))
                        pass
                    else:
                        print ('')
                        dest_get_Dest_Id(dest_sObjectFile)

            else:
                print('            *** File does not exist *** %s' %(src_sObjectFile))
                exit()




def run_rebuild_ID_Dict():

    myFuncName = sys._getframe().f_code.co_name
    print("\n")
    print ("****** %s *** processing ************"  % (myFuncName), end="")

    ID_Dict = 'C:\Kenandy\Python\Configuration\ID_Dictionary.csv'
    if IsFileOpen(ID_Dict):
        print("\n")
        print ('***************************** Job Aborted ****************************')
        exit()

    src_credential, dest_credential = init_credential()
    sObjectList =  init_sObjectList(src_credential['namespace'],dest_credential['namespace'])
    sf = sf_Login(dest_credential['username'], dest_credential['password'], dest_credential['security_token'])

    for row in sObjectList:
        if row['dest_status'].upper() == 'REBUILD' and row['select'].upper() == 'Y':
            print("\n")
            print("[STG**] %s (%s) *** processing ************" % (myFuncName, row['sObject']))
            print("****** %s *** processing ************" % (dict(row)))
            sObject, src_namespace, dest_namespace, src_sObjectCSV, stg_sObjectCSV, dest_sObejectCSV, src_sObjectFile, stg_sObjectFile, dest_sObjectFile = init_sObject(
                row['sObject'], row['src_namespace'], row['dest_namespace'])

            sf_export_object2CSV(sf, sObject, dest_namespace, dest_sObjectFile,"")
            dest_get_Dest_Id(dest_sObjectFile)








##################################################################################

run_export_srcData()
run_import_srcdata2Dest()


#run_rebuild_ID_Dict()

##################################################################################





