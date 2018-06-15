import csv
from salesforce_bulk import SalesforceBulk
from salesforce_bulk import CsvDictsAdapter


username = 'jl-sierra-cash-j1@myorg.com'
password = 'kyqa2017'
security_token = 'HxK2ciSHbsjN5PvAE8psL9w9F'


bulk = SalesforceBulk(username=username, password=password, security_token=security_token)
job = bulk.create_insert_job("account", contentType='CSV', concurrency='Parallel')


rootDir = "c:/python/kenandy/stageCSV/"
objectName = "Account"
stageCSV = rootDir + objectName + '.csv'
print (stageCSV)
with open(stageCSV) as csvfile:

    reader = csv.DictReader(stageCSV)
    account = [dict(Name="Account%d" % idx) for idx in xrange(5)]
    #disbursals = []
    #for row in reader:
    #    disbursals.append(row)
    #print (disbursals)
    print (account)
    csv_iter = CsvDictsAdapter(iter(account))
    
    #csv_iter = CsvDictsAdapter(iter(disbursals))
    batch = bulk.post_batch(job, csv_iter)
    bulk.wait_for_batch(job, batch)
    bulk.close_job(job)
    print("Done. Data Uploaded.")
    
    
    
    
#job = bulk.create_insert_job("Account", contentType='CSV')
#accounts = [dict(Name="Account%d" % idx) for idx in xrange(5)]
#csv_iter = CsvDictsAdapter(iter(accounts))
#batch = bulk.post_batch(job, csv_iter)
#bulk.wait_for_batch(job, batch)
#bulk.close_job(job)
#print "Done. Accounts uploaded."