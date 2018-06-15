from simple_salesforce import Salesforce

username = 'jl-sierra-cash-j1@myorg.com'
password = 'kyqa2017'
security_token = 'HxK2ciSHbsjN5PvAE8psL9w9F'

# Login
sf = Salesforce(username=username,password=password, security_token=security_token)

# Bulk insert
#data = [{'LastName':'Smith','Email':'example@example.com'}, {'LastName':'Jones','Email':'test@test.com'}]
#sf.bulk.Contact.insert(data)
#-----------------------------------------------------------------------------------------------------------
data = [{'Name':'LTE-CA'},{'Name':'LTE-NY'}]
print (data)
sf.bulk.account.insert(data)


