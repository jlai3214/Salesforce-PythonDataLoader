import os,sys
sys.path.append(os.path.abspath("C:\Kenandy\Python\PY"))
from pathlib import Path
from KNDY import *
from salesforce_bulk import CsvDictsAdapter
import csv
from simple_salesforce import Salesforce
from salesforce_bulk import SalesforceBulk
from salesforce_bulk import CsvDictsAdapter
from datetime import date
from pathlib import Path
from shutil import copyfile


sObject_src,sObject_stg,sObject_loc = '','',''
dest_CSVDir = 'c:/kenandy/python/destCSV/'
src_CSVDir = 'c:/kenandy/python/sourceCSV/'
stg_CSVDir = 'c:/kenandy/python/stageCSV/'
configDir = 'c:/kenandy/python/Configuration/'


src_credential, dest_credential = init_credential()
sObjectList = init_sObjectList(src_credential['namespace'], dest_credential['namespace'])
sf = sf_Login(src_credential['username'], src_credential['password'], src_credential['security_token'])
sfBulk = sf_Bulk_Login(dest_credential['username'], dest_credential['password'], dest_credential['security_token'])


del_list = [dict(Id="0011I000009Es7b")]


def deleteBysOBject(sObject):

    #job = sfBulk.create_delete_job("Account", contentType='CSV')
    job = sfBulk.create_delete_job(sObject, contentType='CSV')
    del_list = [dict(Id="a0q1I000000fXEH"),dict(Id="a0q1I000000fXEG")]
    #accounts = [dict(Name="Account%d" % idx) for idx in range(5)]
    csv_iter = CsvDictsAdapter(iter(del_list))
    batch = sfBulk.post_batch(job, csv_iter)
    sfBulk.wait_for_batch(job, batch)
    sfBulk.close_job(job)

    while not sfBulk.is_batch_done(batch):
        print ('processing')
        sleep(1)

    """
    for result in sfBulk.get_all_results_for_query_batch(batch):
        reader = unicodecsv.DictReader(result, encoding='utf-8')
        for row in reader:
            print
            row  # dictionary rows

    """

deleteBysOBject("Currency__c")
