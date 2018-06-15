

import csv
from simple_salesforce import Salesforce
from salesforce_bulk import SalesforceBulk
from salesforce_bulk import CsvDictsAdapter
from datetime import date
from pathlib import Path
from shutil import copyfile



username = 'cashdemo_qa@myorg.com'
password = 'kyqa2017'
security_token = '3ojna9b4VIY7grIPp0ZmNhlD'

sf = Salesforce(username=username,password=password, security_token=security_token)

#Declare vaiables
localCSVDir = 'c:/kenandy/python/localCSV/'
sourceCSVDir = 'c:/kenandy/python/sourceCSV/'
stageCSVDir = 'c:/kenandy/python/stageCSV/'
configDir = 'c:/kenandy/python/Configuration/'

myObject = "KSPRING17__Customer__c"
#myObject = "KSPRING17__Supplier__c"


salesforceObject = sf.__getattr__(myObject)
fieldNames = [field['name'] for field in salesforceObject.describe()['fields']]
print(fieldNames)
#soqlQuery = "SELECT " + ", ".join(fieldNames) + " FROM " + myObject "WHERE RecordTypeId = '0121I0000009IBkQAM'"

recordTypeName = "'Sold To'"
recordTypeName = recordTypeName.replace('Name=','')

#subquery recordtype
#SELECT Id, name FROM KNDY4__Customer__c WHERE recordtypeID  IN (SELECT Id FROM RecordType WHERE Name = 'Remit To')
soqlQuery = "SELECT " + ", ".join(fieldNames) + " FROM " + myObject + " WHERE " +  "recordtypeid IN (SELECT Id FROM RecordType WHERE Name = " + recordTypeName +  ")"
print (soqlQuery)

records = sf.query_all(soqlQuery)['records']

for row in records:
    print (dict(row))

#print(records)

"""
Catalog,Request Item,Bill To,Corporate Parent,Ship To,Site,Sold To,Purchase From,Remit To,


#where RecordTypeId = (select id  from RecordType where name = 'Bill To')
"""


list_sObject = {['id':12333', 'extid': 'sasassas'']}
