import os,sys
sys.path.append(os.path.abspath("C:\Kenandy\Python\PY"))
from pathlib import Path
from KNDY import *

dir = 'c:/kenandy/python/Configuration/'
config = 'ID_Dictionary'
configFile = configDir + config + '.csv'

#get_IdList(configFile)


sObject = "Account_src"
src_namespace = ""
src_sObjectFile = 'C:/Kenandy/Python/SourceCSV/Account_src.csv'

dev_src_add_2_ID_Dict(sObject, src_namespace, src_sObjectFile)


