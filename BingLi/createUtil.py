import os, xml.etree.ElementTree as ET
import sys, itertools
import filecmp, collections

global outputDirectory
global minParamLength
global detailedPrint
global consoleFilePrint
global HardDir
global qaDir

# GLOBAL SETTINGS and CONFIGURATION #
outputDirectory = "../src/salesforce/common/"  # "/Users/balbuque/Documents/workspace/QAAuto/src/salesforce/common/" # leave blank will drop files in current directory /bins
HardDir = "/Users/balbuque/Documents/workspace/git DEV/src/"
qaDir = "/Users/balbuque/Documents/workspace/QAAutomation/"

minParamLength = 0  # keep at 1.
detailedPrint = True  # prints file names created with number of parameters and parameter values
consoleFilePrint = True  # prints files in console.


def main():
    # Dynamically creates a new 'bin' folder after every run in 'bins' folder. Wont replace previous.
    binName = setCreateDirectory()

    # Combined list of fiends from Layouts and Objects
    mergedList = mergeFields(binName)
    empty = []
    mergedList = customAdjustments(mergedList)
    if len(mergedList):
        for each in mergedList:
            size = createUtilClass(each, mergedList[each], binName)
            if size == 0:
                empty.append(each)
    if len(empty):
        print
        "No content found for ", len(empty), " files: ", empty

    cleanupBins(binName)

    # global configure. Prints files in console.
    if consoleFilePrint:
        files = os.listdir(binName)

        for each in files:
            testfile = open(binName + each, 'r+')
            print
            testfile.read()
            testfile.close()


def cleanupBins(binName):
    if os.listdir(binName) == []:
        os.rmdir(binName)
        print
        binName + " was deleted."


def getFileName(userString):
    fileName = os.path.basename(userString)
    return fileName


def mergeFields(binName):
    objXML = []
    fieldsForUtils = {}

    # if single file is passed. Takes only .object and .layout
    if len(sys.argv) > 1:

        # retrive the file name from commandline
        filepath = sys.argv[
                   1:]  # looks like: ['Objects/Sales_Order__c.object', 'Layouts/Sales_Order__c -  Sales Order Layout.layout']

        for each in filepath:
            path = each  # Objects/Sales_Order__c.obejct
            each = getFileName(each)  # looks like: Sales_Order__c.object

            # objXML = extractRequiredFields(each)
            if each.endswith('.object'):
                index = each.find('.object')

            if each.endswith('.layout'):
                index = each.find('-')

            else:
                "Incorrect filetype: " + each

            sfObj = each[:index]

            fieldsForUtils = mergeHelper(path, sfObj, fieldsForUtils)

    # if no file is passed, do all: Layouts and Objects
    else:

        #	create lists using Layouts and Objects combined

        for each in os.listdir(HardDir + "Objects/"):
            if each.endswith(".object"):
                path = HardDir + "Objects/" + each  # Objects/Sales_Order_c.object

                index = each.find(".object")
                sfObj = each[:index]  # Sales_Order__c

                fieldsForUtils = mergeHelper(path, sfObj, fieldsForUtils)

        for each in os.listdir(HardDir + "Layouts/"):
            if each.endswith(".layout"):
                path = HardDir + "Layouts/" + each

                index = each.find("-")
                sfObj = each[:index]
                fieldsForUtils = mergeHelper(path, sfObj, fieldsForUtils)

    fieldsForUtils = collections.OrderedDict(sorted(fieldsForUtils.items()))
    return fieldsForUtils


def mergeHelper(path, sfObj, fieldsForUtils):
    if sfObj in fieldsForUtils:

        extracted = extractRequiredFields(path)
        fieldsForUtils[sfObj]['Req'] += extracted['Req']
        fieldsForUtils[sfObj]['nonReq'] += extracted['nonReq']
        fieldsForUtils[sfObj]['Req'] = list(set(fieldsForUtils[sfObj]['Req']))
        fieldsForUtils[sfObj]['nonReq'] = list(set(fieldsForUtils[sfObj]['nonReq']))

    else:
        fieldsForUtils[sfObj] = extractRequiredFields(path)

    return fieldsForUtils


def setCreateDirectory():
    # uses global settings. If replace Bins
    if len(outputDirectory):
        dirName = outputDirectory
        print
        "Using directory: " + outputDirectory

    else:
        i = 1
        while True:
            i += 1
            dirName = "bins/bin" + str(i) + "/"
            if not os.path.exists(dirName):
                dirName = "bins/bin" + str(i) + "/"
                os.makedirs(dirName)
                print
                "created directory: " + dirName
                break

    return dirName


def customAdjustments(mergedList):
    if 'Cart_Line__c' in mergedList:
        mergedList['Cart_Line__c']['Req'].remove("Shopping___c")
        mergedList['Cart_Line__c']['Req'].append('Shopping_Cart__c')

    autoNumerApps = [];
    with open(qaDir + "scripts/AutoNumberApps.txt", 'r') as f:
        autoNumerApps = f.readlines();

    # index = 0;
    # for each in autoNumerApps:
    # 	autoNumerApps[index] = each.replace("\n","")
    # 	index += 1


    # autoNumerApps = ['Sales_Order__c','Purchase_Order__c']
    for each in autoNumerApps:
        each = each.replace(".object\n", "")

        if each in mergedList:

            if "Name" in mergedList[each]['Req']:
                mergedList[each]['Req'].remove("Name")

    return mergedList


def extractRequiredFields(path):
    tree = ET.parse(path)
    root = tree.getroot()
    meta = "{http://soap.sforce.com/2006/04/metadata}"
    fullName = os.path.basename(path)

    # list that stores fieldsets
    objXML = {}
    reqFieldArray = []
    nonReqFieldArray = []
    bad = ['Description__c', 'Default__c', 'TickerSymbol', 'Ownership', 'BillingAddress', 'Site', 'AccountNumber',
           'NumberOfEmployees', 'Rating', 'Sic']

    for elem in tree.iter():
        # print elem
        if elem.find(meta + "isRequired") != None:
            if elem.find(meta + "isRequired").text == 'true':
                val = elem.find(meta + "field").text
                if val not in bad:
                    if val.find('.') == -1:
                        reqFieldArray.append(val)

        # .object XML structure: Domain_c.object
        if elem.find(meta + "required") != None:
            if elem.find(meta + "required").text == 'true':
                val = elem.find(meta + "fullName").text
                if val not in bad:
                    if val.find('.') == -1:
                        reqFieldArray.append(val)

            if elem.find(meta + "required").text == 'false':
                val = elem.find(meta + "fullName").text
                if val not in bad:
                    if val.find('.') == -1:
                        nonReqFieldArray.append(val)

        if elem.find(meta + "behavior") != None:
            if elem.find(meta + "behavior").text == 'Required':
                val = elem.find(meta + "field").text
                if val not in bad:
                    if val.find('.') == -1:
                        reqFieldArray.append(val)

        if elem.find(meta + "field") != None:
            val = elem.find(meta + "field").text
            if val not in bad:
                if val.find('.') == -1:
                    nonReqFieldArray.append(val)

    objXML['Req'] = sorted(set(reqFieldArray))
    objXML['nonReq'] = sorted(set(nonReqFieldArray))

    objXML['Req'] = [s.replace('Item__r.', "").replace('Cart', '') for s in objXML['Req']]
    objXML['nonReq'] = [s.replace('Item__r.', "") for s in objXML['nonReq']]

    return objXML


def getParams(content, sfObj):
    params = []
    paramDouble = ['Percent', 'Cost', 'Revenue', 'Number', 'discount', 'Total', 'Latency', 'Priority', 'Amount',
                   'Conversion', 'Numeric', 'Rate', 'Price', 'Number', 'Quantity', 'Time', 'Sequence', 'Balance',
                   'Level', 'Value', 'OffsetDays', 'Minimum', 'Quality', 'LifeDays']
    paramBoolean = ['Exception', 'Compare', 'Planned', 'Favorite', 'LotTracked', 'Symbols', 'Display']
    paramCalendar = ['Date']

    exactParamString = ['corporateCurrency', 'lineValue', 'valueField', 'normalBalance', 'timestampField',
                        'bankAccountNumber', 'autonumber', 'decliningBalance', 'amountReference', 'referenceValue',
                        'revenueGLAccount', 'gLAccountReferenceValue', 'pricebook', 'customerNumber', 'value',
                        'fieldValue', 'pricebookUnique']
    exactParamBoolean = ['binTracked', 'isDemand', 'expirationDate', 'active', 'showOnSearchPage', 'accounted',
                         'expirationDateTracked', 'global', 'defaultGLAccount', 'defaultSegmentValue', 'finalized',
                         'creditDebitUnbalanced', 'lotNumber', 'expirationDate']
    exactParamDouble = ['daysPastDue', 'lifeInMonths', 'totalCredit', 'dueDays']
    exactParamCalendar = ['nextRunAfter', 'expirationDate', 'contractStart', 'contractEnd']

    # void = ['tickerSymbol', 'ownership', 'billingAddress', 'site', 'accountNumber', 'numberOfEmployees', 'rating', 'sic']
    # print content
    for x in content:
        x = lowerCase(x).replace("__c", "")
        if x.find("_") + 1:
            x = x.split("_")

            for n in range(1, len(x)):
                x[n] = upperCase(x[n])
            x = "".join(x)

        # if x in void:
        # 	x = " "
        # 	continue


        if sfObj == 'Item_Attribute__c' and x == 'expirationDate':
            x = "Boolean " + x
            params.append(x)
            continue

        elif x in exactParamString and x.find(" ") == -1:
            x = "String " + x
            params.append(x)
            continue

        elif x in exactParamCalendar and x.find(" ") == -1:
            x = "Calendar " + x
            params.append(x)
            continue

        elif x in exactParamDouble and x.find(" ") == -1:
            x = "Double " + x
            params.append(x)
            continue


        elif x in exactParamBoolean and x.find(" ") == -1:
            x = "Boolean " + x
            params.append(x)
            continue

        if x.find(" ") == -1:
            for each in paramCalendar:
                if (x.find(each) + 1 or x.find(lowerCase(each)) + 1) and x.find(" ") == -1:
                    x = "Calendar " + x
                    params.append(x)
                    continue

            for each in paramDouble:
                if (x.find(each) + 1 or x.find(lowerCase(each)) + 1) and x.find(" ") == -1:
                    x = "Double " + x
                    params.append(x)
                    continue

            for each in paramBoolean:
                if (x.find(each) + 1 or x.find(lowerCase(each)) + 1) and x.find(" ") == -1:
                    x = "Boolean " + x
                    params.append(x)
                    continue

        # print x.find(" "), x
        if x.find(" ") == -1:  # single words will return a -1
            x = "String " + x
            params.append(x)
            continue

    paramString = '%s' % ', '.join(map(str, params))

    # returns a single string uses as the method parameters
    return paramString


def createUtilClass(fileName, objXML, binDir):
    sfObj = upperCase(fileName)
    className = getClassName(fileName)  # SalesOrderUtil
    instanceName = getInstanceName(className)  # salesOrder
    methodName = getMethodName(className.replace('Util', ''))  # setSalesOrderInstance

    file = createFile(binDir, className)
    writeClass(sfObj, className, file)

    if len(objXML['Req']) >= minParamLength:  # global setting for number of parameters we want
        content = objXML['Req']

    else:
        content = list(set(objXML['nonReq']) | set(objXML['Req']))

    # content = list(sorted(set(content)))
    params = getParams(content, sfObj)

    writeMethod(methodName, instanceName, params, sfObj, content, file)
    writeGetIDMethod(className, instanceName, sfObj, file)
    writeEndClass(file)

    unfilteredParams = ", ".join(map(str, content)).replace('__c', '').replace('_', '')
    if detailedPrint: print
    "Created file: " + binDir + className + " with ", len(content), len(
        objXML['Req']), " parameters: ", unfilteredParams

    return len(content)


def writeEndClass(file):
    file.write("\n}")
    file.close()


def getContent(objXML):
    content = "bleh"
    return content


def getSfObj(fileName):
    # translates Sales_Order__c.object into Sales_Order__c
    if fileName.endswith('.object'):
        sfObj = fileName.replace('.object', "")

    if fileName.endswith('.layout'):
        index = fileName.find('-')
        fileName = fileName[:index]
        sfObj = fileName.replace('.layout', "")

    return sfObj


def getClassName(fileName):
    className = fileName.replace("_c", "").replace("_", "") + "Util"

    return className


def getMethodName(sections):
    # translates New_Transfer_Sales_Order into setSalesOrderInstance
    methodName = "get" + sections.replace("_", "").replace("New", "").replace(" ", "") + "Instance"
    return methodName


def getInstanceName(className):
    # translates Sales_Order__c.object into salesOrder
    instanceName = lowerCase(className.replace("Util", ""))
    return instanceName


def getParamsHelper(param):
    pass


def createFile(binDir, className):
    javaPath = binDir + className + '.java'
    file = open(javaPath, 'w+')

    return file


def writeClass(sfObj, className, file):
    # sfObj = upperCase(sfObj)

    file.write("package salesforce.common;")
    file.write("\n\nimport com.sforce.soap.enterprise.sobject." + sfObj + ";")
    file.write("\n\nimport java.util.Calendar;")
    file.write("\n\npublic class " + className + ' {')
    file.write("\n\n\tpublic " + className + '(){')
    file.write("\n\n\t}")


def writeMethod(methodName, instanceName, paramString, sfObj, content, file):
    # params = paramString.replace("String","").replace("Double","").replace(" ","").split(",")
    params = []
    paramTrack = 0
    instanceName = instanceName + "Obj"
    file.write("\n\n\tpublic " + sfObj + " " + methodName + "(" + paramString + "){")
    file.write("\n\n\t\t" + sfObj + " " + instanceName + " = new " + sfObj + "();")
    for x in content:
        x = lowerCase(x).replace("__c", "")
        if x.find("_") + 1:
            x = x.split("_")

            for n in range(1, len(x)):
                x[n] = upperCase(x[n])
            x = "".join(x)
        params.append(x)

    for fields in content:
        field = "".join(fields)
        file.write("\n\t\t" + instanceName + ".set" + field + "(" + params[paramTrack] + ");")
        paramTrack += 1
    file.write("\n\n\t\treturn " + instanceName + ";")
    file.write("\n\t}")


def writeGetIDMethod(className, instanceName, sfObj, file):
    className = className.replace("Util", "")
    param = lowerCase(className) + "Obj"

    file.write("\n\n\tpublic String get" + className + "Id(" + sfObj + " " + param + "){")
    file.write("\n\t\tString " + param + "ID = " + param + ".getId();")
    file.write("\n\n\t\treturn " + param + "ID;")
    file.write("\n\t}\n")


def lowerCase(s):
    return s[:1].lower() + s[1:]


def upperCase(s):
    return s[:1].upper() + s[1:]


def find_nth(haystack, needle, n):
    start = haystack.find(needle)
    while start >= 0 and n > 1:
        start = haystack.find(needle, start + len(needle))
        n -= 1
    return start


main()