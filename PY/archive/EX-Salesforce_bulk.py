from salesforce_bulk import CsvDictsAdapter
from salesforce_bulk import SalesforceBulk


job = bulk.create_insert_job("Account", contentType='CSV')
accounts = [dict(Name="Account%d" % idx) for idx in xrange(5)]
csv_iter = CsvDictsAdapter(iter(accounts))
batch = bulk.post_batch(job, csv_iter)
bulk.wait_for_batch(job, batch)
bulk.close_job(job)
print ("Done. Accounts uploaded.")