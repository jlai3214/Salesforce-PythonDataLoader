import csv
from simple_salesforce import Salesforce
from salesforce_bulk import SalesforceBulk
from salesforce_bulk import CsvDictsAdapter
from datetime import date
from pathlib import Path


username = 'jl-auto@myorg.com'
password = 'kyqa2017'
security_token = 'VJMrCkGcuEERf5thOsjZwPfIZ'
sf = Salesforce(username=username,password=password, security_token=security_token)


def get_records2CSV(namespace,sObject):

    dir = "c:/kenandy/python/localCSV/"
    object = namespace + sObject
    print(object)
    # datapath = Path(args.datadir) / date.today().isoformat()
    # datapath = Path(dir) / date.today().isoformat()
    datapath = dir + '/' + date.today().isoformat()
    print(datapath)
    sourceCSV = dir + sObject + '.csv'
    print(sourceCSV)

    salesforceObject = sf.__getattr__(object)
    fieldNames = [field['name'] for field in salesforceObject.describe()['fields']]
    print (fieldNames)
    soqlQuery = "SELECT " + ", ".join(fieldNames) + " FROM " + object
    records = sf.query_all(soqlQuery)['records']
    print (records)

    with open(sourceCSV, 'w') as csvfile:
        writer = csv.DictWriter(csvfile,fieldnames=fieldNames)
        writer.writeheader()
        for row in records:
            # each row has a strange attributes key we don't want
            row.pop('attributes',None)
            writer.writerow(row)
    csvfile.close()



def import_csv2org():
    with open('Currency__c.csv') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
      #''      print(row['id'], row['name'])
            #print(row)
    #OrderedDict([('first_name', 'John'), ('last_name', 'Cleese')])
    #print(reader)
    
    # Bulk insert
    #data = [{'LastName':'Smith','Email':'example@example.com'}, {'LastName':'Jones','Email':'test@test.com'}]
    #sf.bulk.Contact.insert(data)
    #-----------------------------------------------------------------------------------------------------------
    #data = [{'Name':'LTE-CA'},{'Name':'LTE-NY'}]
    #print (data)
            sf.bulk.account.insert(row)

    

def objectsExport2CSV():
    namespace = "KNDY4__"
    objects = ["Currency__c","Company__c","UOM__c","Facility__c","Customer__c","Supplier__c"]
    for object in objects:
        print (object)
        get_records2CSV(namespace,object)

def bulkUpdate(sObject):

    sfBulk = SalesforceBulk(username=username, password=password, security_token=security_token)
    job = sfBulk.create_insert_job(sObject, contentType='CSV', concurrency='Parallel')

    dir = "c:/kenandy/python/stageCSV/"
    stageCSV = dir + sObject + '.csv'
    print(stageCSV)

    with open(stageCSV) as csvfile:
        reader = csv.DictReader(csvfile)
        #print (reader.fieldnames)
        rows = []

        for row in reader:
            print("row****", dict(row))
            #print(row['Id'], row['Name'])
            # print(row['Id'], row['Name'])
            rows.append(dict(row))
            #print("rows****", rows)

        csv_iter = CsvDictsAdapter(iter(rows))
        #print("csv_iter**** ", csv_iter)
        print("rows****", rows)
        batch = sfBulk.post_batch(job, csv_iter)
        sfBulk.wait_for_batch(job, batch)
        sfBulk.close_job(job)
        print("Done. Data Uploaded.")


####################################################################################################
# import unicodecsv
#objectsExport2CSV()
#get_records2CSV("", "account")
#csv_DictReader()
bulkUpdate("account")
#objectsExport2CSV()
get_records2CSV("", "account")
#objectsExport2CSV()
#import_csv2org()
#csv_reader()
#csv_DictReader()
