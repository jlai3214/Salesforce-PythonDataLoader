import cgi
import requests
import json
from simple_salesforce import Salesforce
from pyjavaproperties import Properties
import os.path, time


def main():
    # fields = getFields()
    curApp = 'Sales_Order__c'
    getFields(curApp)


def getFields(single):
    p = Properties()
    p.load(open('../config/config.properties'))
    # p.list()
    # print p
    username = p['Username']
    password = p['Password']
    security_token = p['SecurityToken']
    sfdcUrl = p['sfdcUrl']

    print
    username, " ", password, " ", security_token

    sf = eval(
        'Salesforce(username=\'' + username + '\',  password=\'' + password + '\', security_token=\'' + security_token + '\')')
    # sf = Salesforce(username= username, password= password, security_token= security_token)

    sobjects = {}
    sobjects['fields'] = {}

    if single:
        for x in sf.Sales_Order_line__c.describe()['fields']:
            field = x['name']
            sType = x['type']
            soapType = x['soapType']
            label = x['label']
            formula = x['calculatedFormula']

            sobjects['fields'][field] = {}
            sobjects['fields'][field].append(sType)
            sobjects['fields'][field].append(soapType)
            sobjects['fields'][field].append(label)
            sobjects['fields'][field].append(formula)

        sobjects['name'] = sf.Sales_Order__c.describe()['name']
        sobjects['keyPrefix'] = sf.Sales_Order__c.describe()['keyPrefix']
        sobjects['label'] = sf.Sales_Order__c.describe()['label']
        sobjects['newURL'] = sf.Sales_Order__c.describe()['urls']['uiNewRecord']

        sobject['childR'] = {}
        for x in sf.Sales_Order__c.describe()['childRelationships']:
            sobject['childR'].append(x['childSObject'])




    else:
        for x in sf.describe()['sobjectss']:
            sobj = x['name']
            # sobjects.append(sobj)
            sobjects[sobj] = {}
            sobjects[sobj]['type'] = []
            sobjects[sobj]['name'] = []
            sobjects[sobj]['label'] = []

            sojbList = eval("sf." + sobj + ".describe()['fields']")
            for y in sojbList:
                sobjects[sobj]['type'] = y['type']
                sobjects[sobj]['name'] = y['name']
                sobjects[sobj]['label'] = y['label']

                # print "	", y['type'], y['name'], "// " + y['label']

    print
    sobject
    return sobjects


def createSObject(name, label, oType):
    sobject = Sobject(name, label, oType)
    return sobject


class Sobject(object):
    name = ""
    label = ""
    oType = ""

    def __init__(self, name, label, oType):
        self.name = name
        self.label = label
        self.oType = oType


main()