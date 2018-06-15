import os,sys

sys.path.append(os.path.abspath("C:\Kenandy\Python\PY"))

from KNDY import *

import collections



#run_export_srcOrg()
#run_sObject_stg()
#run_export2Dest()


#######
#credential_src, credential_dest, listofsObject = init_config()

# Export to source
#sf = sf_login_src()

sf = sf_Login('kan-sierra@myorg.com','kyqa2018','GdlyZvrqahHCEz2kKRl7rhHlC'

'')
#sf_export_object2CSV(sf,'KNDY4','GL_Account', 'src')
#src_add_RecordIDs2Config('KNDY4','GL_Account')



#Import
src_namespace = 'KNDY4'
dest_namespace = 'KNDY4'
sObject = 'GL_Account__c'

sObject, src_namespace, dest_namespace, src_sObjectCSV, stg_sObjectCSV, dest_sObejectCSV, src_sObjectFile,stg_sObjectFile, dest_sObjectFile = init_sObject(src_namespace,dest_namespace,sObject)
#stg_copy2StageCSV(src_sObjectFile,stg_sObjectFile)
#stg_namespace2fieldnames(src_namespace,dest_namespace,stg_sObjectFile)
#stg_fieldnamesExcluded(sObject)
#stg_removeValueByFieldname(stg_sObjectFile,sObject)
#stg_replaceNewReferenceId(stg_sObjectFile)
#sfBulkUpdate

## Export after loading to org
#sf_export_object2CSV(sf,'KNDY4','Company__c','dest')
#UpdateObjectNewIds2Config

#print (sf.KNDY4__GL_Account__c.metadata())

#print (sf.KNDY4__GL_Account__c.describe())

#sf.describe()

desc = sf.KNDY4__GL_Account__c.describe()
print ('desc', desc)

for key,value in desc.items():
    print (key, value)

