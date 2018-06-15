from simple_salesforce import Salesforce
from salesforce_bulk import SalesforceBulk
from salesforce_bulk import CsvDictsAdapter
from csv import DictWriter
from csv import DictReader

username = 'jl-sierra-cash-j1@myorg.com'
password = 'kyqa2017'
security_token = 'HxK2ciSHbsjN5PvAE8psL9w9F'


# Login
sf = Salesforce(username=username,password=password, security_token=security_token)

#bulk = SalesforceBulk(username=username, password=password, security_token=security_token)
print ("xxx")

#bulk = SalesforceBulk(username=username, password=password)
#job = bulk.create_insert_job("currency__c", contentType='CSV', concurrency='Parallel')
#job = sf.create_insert_job("currency__c", contentType='CSV', concurrency='Parallel')

rootDir =   "c:/python/kenandy/StageCSV/"
objectName = "Account"
stageCSV = rootDir + objectName + ".csv"
print (stageCSV)


#from salesforce_bulk import CsvDictsAdapter
def zbulk():
    job = bulk.create_insert_job("Account", contentType='CSV')
    accounts = [dict(Name="Account%d" % idx) for idx in range(10)]
    csv_iter = CsvDictsAdapter(iter(accounts))
    batch = bulk.post_batch(job, csv_iter)
    bulk.wait_for_batch(job, batch)
    bulk.close_job(job)
    print ("Done. Accounts uploaded.")



    
    
def csv_DictWriter(): 

    rootDir = "c:/python/kenandy/stageCSV/"
    objectName = "Account"
    #stageCSV = rootDir + objectName + '.csv'
    print (stageCSV)
    with open(stageCSV) as csvfile:
        reader = DictReader(csvfile)
        print(reader)
        print ("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
        for row in reader:
            print("********* ", row['Name'], row['Id'])
            print(row)
            #OrderedDict([('first_name', 'John'), ('last_name', 'Cleese')])
            sf.bulk.account.insert(row)

        print('xxxxxxxxxxxxxxxxxxxxxxxx')
        
        print(reader)
        #sf.account.create(row)

csv_DictWriter()
