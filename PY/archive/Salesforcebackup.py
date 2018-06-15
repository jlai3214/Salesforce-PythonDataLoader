from simple_salesforce import Salesforce, SalesforceMalformedRequest
from argparse import ArgumentParser
from csv import DictWriter
from datetime import date
from pathlib import Path

production_instance = 'yourinstance.salesforce.com'

parser = ArgumentParser(description="Backs up all Salesforce objects to csv files")
parser.add_argument("username", help="User to authenticate as. Should be part of an 'integration_user' profile or some profile with no ip range restriction")
parser.add_argument("password", help="Password for the user")
parser.add_argument("security_token", help="Any API authenticating user needs to supply a security_token. You must log in as the user to get the security token from their profile")
parser.add_argument("instance", help="The instance to connect to")
parser.add_argument("datadir", help="The directory to store csv files in")
parser.add_argument("--objectNames", help="Space separated names of SalesForce objects or by default we backup all queryable objects", nargs='*')
args = parser.parse_args()

#create a subdirectory with todays date
datapath = Path(args.datadir) / date.today().isoformat() 

try:
  datapath.mkdir(parents=True) #in python 3.5 we can switch to using  exist_ok=True
except FileExistsError:
  pass

#log into salesforce
#sf = Salesforce(username=args.username,password=args.password, security_token=args.security_token, sandbox=(production_instance!=args.instance), instance=args.instance)


username = 'jl-sierra-cash-j1@myorg.com'
password = 'kyqa2017'
security_token = 'HxK2ciSHbsjN5PvAE8psL9w9F'

sf = Salesforce(username=username,password=password, security_token=security_token)



#get a list of queryable object names we will need to backup
if args.objectNames:
  names = args.objectNames
else:
  #get a description of our global salesforce instance, see layout:
  # https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/dome_describeGlobal.htm
  description = sf.describe()
  names = [obj['name'] for obj in description['sobjects'] if obj['queryable']]

#for every object we'll need all the fields it has that are exportable.
for name in names:
  salesforceObject = sf.__getattr__(name)
  # so get a list of the object fields for this object.
  fieldNames = [field['name'] for field in salesforceObject.describe()['fields']]
  # then build a SOQL query against that object and do a query_all
  try:
    results = sf.query_all( "SELECT " + ", ".join(fieldNames) + " FROM " + name  )['records']
  except SalesforceMalformedRequest as e:
    # ignore objects with rules about getting queried. 
    continue
  outputfile = datapath / (name+".csv")
  with outputfile.open(mode='w', encoding='utf_8_sig) as csvfile:
    writer = DictWriter(csvfile,fieldnames=fieldNames)
    writer.writeheader()
    for row in results:
      # each row has a strange attributes key we don't want
      row.pop('attributes',None)
writer.writerow(row)