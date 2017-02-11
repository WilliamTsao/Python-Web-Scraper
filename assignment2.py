# -*- coding: utf-8 -*-
"""
Created on Wed Feb  1 20:27:28 2017

@author: williamtsao

Dependencies:
    Beautiful Soup:
        $ pip install beautifulsoup4
    joblib:
        $ pip install joblib


"""
import re
import xml.etree.ElementTree as ET
import urllib
#import time

#t0 = time.time()

try:
    from bs4 import BeautifulSoup as BS
except ImportError:
    print("Fatal Failure: Please download dependency BeautifulSoup\n pip install beautifulsoup4")
    quit()


def getNewXML(url):
    res = urllib.request.urlopen(url)
    html = BS(res, 'html.parser')
    tag = html.find("a", string=re.compile('.*\.xml$'))
    xmlUrl = 'https://www.sec.gov'+tag['href']
    xmlRes = urllib.request.urlopen(xmlUrl)
    tree = ET.parse(xmlRes)
    root = tree.getroot()
    return root
    
def handleTable1(basicInfo, root):
    transactionList = []
    for transaction in root.findall('nonDerivativeTransaction'):
        #newRecord gets all basicInfo
        newRecord = []
        for ele in basicInfo:
            newRecord.append(ele)
        newRecord.append('N')#for transaction with nonDerivativeSecurities
        
        try:
            #append transaction title
            newRecord.append('"'+transaction.find('securityTitle').find('value').text+'"')
            #append transaction coding
            newRecord.append(transaction.find('transactionCoding').find('transactionCode').text)
        
            transactionAmount = transaction.find('transactionAmounts')
            #append amount of shares
            newRecord.append(transactionAmount.find('transactionShares').find('value').text)
            #append price per share
            newRecord.append(transactionAmount.find('transactionPricePerShare').find('value').text)
            #append Acquired or Disposed of
            newRecord.append(transactionAmount.find('transactionAcquiredDisposedCode').find('value').text)
            #append ownership (Direct or Indirect)
            newRecord.append(transaction.find('ownershipNature').find('directOrIndirectOwnership').find('value').text)
        except AttributeError:
            continue
        else:
            #append 8 N/A to the end for fields meant for table 2 transactions
            for i in range(0,6):
                newRecord.append('N/A')
            #append this transaction to transactionList
            transactionList.append(newRecord)
    return transactionList

def handleTable2(basicInfo, root):
    transactionList = []
    
    for transaction in root.findall('derivativeTransaction'):
        #newRecord gets all basicInfo
        newRecord = []
        for ele in basicInfo:
            newRecord.append(ele)#for transaction with derivativeSecurities
        newRecord.append('D')
        #append 6 N/A in front for fields meant for table 1 transactions
        for i in range(0,6):
            newRecord.append('N/A')
            
        try:
            #append transaction title
            newRecord.append('"'+transaction.find('securityTitle').find('value').text+'"')
            #append transaction coding
            newRecord.append(transaction.find('transactionCoding').find('transactionCode').text)
        
            transactionAmount = transaction.find('transactionAmounts')
            #append amount of shares
            newRecord.append(transactionAmount.find('transactionShares').find('value').text)
            #append price per share
            newRecord.append(transactionAmount.find('transactionPricePerShare').find('value').text)
            #append Acquired or Disposed of
            newRecord.append(transactionAmount.find('transactionAcquiredDisposedCode').find('value').text)
            #append ownership (Direct or Indirect)
            newRecord.append(transaction.find('ownershipNature').find('directOrIndirectOwnership').find('value').text)
        except AttributeError:
            continue;
        else:
            #append this transaction to transactionList
            transactionList.append(newRecord)
        
    return transactionList
    

#=================================================================================================================================
table = []
heading = [
'Reporting Name', 
'Accession Number',
'Issuer Name' , 
'Relationship of Reporting Person to Issuer', 

'Transaction with Derivative (D) or Non-Derivative (N) Securities',

'Non-Derivative Securities: Title', 
'Non-Derivative Securities: Transaction Code',
'Non-Derivative Securities: Amount (shares)',
'Non-Derivative Securities: Price (per share)',
'Non-Derivative Securities: Acquired (A) or Disposed Of (D)',
'Non-Derivative Securities: Ownership Form: Direct (D) or Indirect (I)',

'Derivative Securities: Title', 
'Derivative Securities: Transaction Code',
'Derivative Securities: Amount (shares)',
'Derivative Securities: Price (per share)',
'Derivative Securities: Acquired (A) or Disposed Of (D)',
'Derivative Securities: Ownership Form: Direct (D) or Indirect (I)',
'Year', 'Month', 'Day', 'Time']
#transactionCode: 
#A: Grant, award, or other acquisition of securities from the company (such as an option)
#K: Equity swaps and similar hedging transactions
#P: Purchase of securities on an exchange or from another person
#S: Sale of securities on an exchange or to another person
#D: Sale or transfer of securities back to the company
#F: Payment of exercise price or tax liability using portion of securities received from the company
#M: Exercise or conversion of derivative security received from the company (such as an option)
#G: Gift of securities by or to the insider
#V: A transaction voluntarily reported on Form 4
#J: Other (accompanied by a footnote describing the transaction)
 
table.append(heading)


#Pages to run (100 entries per page)
pageToRun = 10;



# Try to run in parallel
try:
    from joblib import Parallel, delayed


except ImportError:
    #Dependency not fulfilled, run sequencial instead
    #Sequential implementation
    
    print("Warning: Please download dependency joblib for (much) faster performance\n pip install joblib")      
    index = 0
    
    
    while index < pageToRun:
        url = 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&CIK=&type=&company=&dateb=&owner=include&start='+str(index*100)+'&count=100&output=atom'
        response = urllib.request.urlopen(url)
        index += 1
        tree = ET.parse(response)
        root = tree.getroot()
        
        
        for entry in root.findall('{http://www.w3.org/2005/Atom}entry'):
            #each Entry is a record
            record = []
            
            #title contains multiple fields that needs to be parsed
            #format of title: form_type - entity_name (cik) (action)
            title = entry.find('{http://www.w3.org/2005/Atom}title').text
            parsedTitle = title.split(' - ', 1)
            
            #Only retrieve form 4 data (insider trading)
            if parsedTitle[0] != '4':
                continue
            
            #parsing out cik and action
            temp = (re.findall(r'\((.*?)\)', parsedTitle[1]))
            action = temp[-1]
            cik = temp[-2]
    
            #Only retrieve records labelled (Reporting)
            #There is 1 reporting file and 1 issuer file per transaction (two identical forms)
            #Retrieving both will result in processing two identical forms
            if action != 'Reporting':
                continue
    
            reportingName = parsedTitle[1][:-(len(cik)+len(action)+6)]
            
            
            #quote around name with "," to ensure correct rendering when writing to csv
            reportingName = '"'+reportingName+'"'
            #Append Reporting Name
            record.append(reportingName)
            #get the accNum
            accNum = entry.find('{http://www.w3.org/2005/Atom}id').text
            accNum = accNum.split('accession-number=', 1)[1].replace('-','')
            record.append(accNum)
            
            #entryUrl: url to html page with next XML file's location
            entryUrl = entry.find('{http://www.w3.org/2005/Atom}link').get('href')
            
            
            #getNewXML returns root of next XML file
            r = getNewXML(entryUrl)
            
            #append issuerName
            issuerName = '"'+r.find('issuer').find('issuerName').text+'"'
            record.append(issuerName)
            
            #Reporting person's relationship to issuer
            relation = r.find('reportingOwner').find('reportingOwnerRelationship')
            
            temp = ''
            if not (relation.find('isDirector') == None or relation.find('isDirector').text.lower() == 'false' or relation.find('isDirector').text =='0'):
                temp += 'D '
            if not (relation.find('isOfficer') == None or relation.find('isOfficer').text.lower() == 'false' or relation.find('isOfficer').text == '0'):
                temp += 'O '
            if not (relation.find('isTenPercentOwner') == None or relation.find('isTenPercentOwner').text.lower() == 'false' or relation.find('isTenPercentOwner').text =='0'):
                temp += 'T '
            
            
            #append reportingOwnerRelationship
            #D for director, O for officer, T for Ten Percent Owner
            record.append(temp)
            
            #record now has the basic informatiion of this entry.
            #Each record represents 1 transaction
            #A sigle transaction can either be with Non-Derivative Sec or Devrivative Sec.
            #An entry can have multiple transactions.
            recordList = []
            #print(cik)
            if r.find('nonDerivativeTable') != None:
                for i in handleTable1(record, r.find('nonDerivativeTable')):
                    recordList.append(i)
                    
                
                #Since nonDerivativeTable can hold multiple transactions, it returns a list of records
            if r.find('derivativeTable') != None:
                
                for i in handleTable2(record, r.find('derivativeTable')):
                    recordList.append(i)
                #Since derivativeTable can hold multiple transactions, it returns a list of records
            
                
            for record in recordList:
                datetime = entry.find('{http://www.w3.org/2005/Atom}updated').text
                parse = datetime.split('T', 1)
                date = parse[0].split('-', 2)
                Time = parse[1].split('-', 1)[0]
                for ele in date:
                    record.append(ele)
                record.append(Time)
                
                
                table.append(record)
        #print('page done', index)
    
        

#=================================================================================================================================
# Parallel 
else:
    import multiprocessing

    def processPage(i):
        t = []
        url = 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&CIK=&type=&company=&dateb=&owner=include&start='+str(i*100)+'&count=100&output=atom'
        response = urllib.request.urlopen(url)
        i += 1
        tree = ET.parse(response)
        root = tree.getroot()
        
        
        for entry in root.findall('{http://www.w3.org/2005/Atom}entry'):
            #each Entry is a record
            record = []
            
            #title contains multiple fields that needs to be parsed
            #format of title: form_type - entity_name (cik) (action)
            title = entry.find('{http://www.w3.org/2005/Atom}title').text
            parsedTitle = title.split(' - ', 1)
            
            #Only retrieve form 4 data (insider trading)
            if parsedTitle[0] != '4':
                continue
            
            #parsing out cik and action
            temp = (re.findall(r'\((.*?)\)', parsedTitle[1]))
            action = temp[-1]
            cik = temp[-2]
    
            #Only retrieve records labelled (Reporting)
            #There is 1 reporting file and 1 issuer file per transaction (two identical forms)
            #Retrieving both will result in processing two identical forms
            if action != 'Reporting':
                continue
    
            reportingName = parsedTitle[1][:-(len(cik)+len(action)+6)]
            
            
            #quote around name with "," to ensure correct rendering when writing to csv
            reportingName = '"'+reportingName+'"'
            #Append Reporting Name
            record.append(reportingName)
            #get the accNum
            accNum = entry.find('{http://www.w3.org/2005/Atom}id').text
            accNum = accNum.split('accession-number=', 1)[1].replace('-','')
            record.append(accNum)
            
            #entryUrl: url to html page with next XML file's location
            entryUrl = entry.find('{http://www.w3.org/2005/Atom}link').get('href')
            
            
            #getNewXML returns root of next XML file
            r = getNewXML(entryUrl)
            
            #append issuerName
            issuerName = '"'+r.find('issuer').find('issuerName').text+'"'
            record.append(issuerName)
            
            #Reporting person's relationship to issuer
            relation = r.find('reportingOwner').find('reportingOwnerRelationship')
            
            temp = ''
            if not (relation.find('isDirector') == None or relation.find('isDirector').text.lower() == 'false' or relation.find('isDirector').text =='0'):
                temp += 'D '
            if not (relation.find('isOfficer') == None or relation.find('isOfficer').text.lower() == 'false' or relation.find('isOfficer').text == '0'):
                temp += 'O '
            if not (relation.find('isTenPercentOwner') == None or relation.find('isTenPercentOwner').text.lower() == 'false' or relation.find('isTenPercentOwner').text =='0'):
                temp += 'T '
            
            
            #append reportingOwnerRelationship
            #D for director, O for officer, T for Ten Percent Owner
            record.append(temp)
            
            #record now has the basic informatiion of this entry.
            #Each record represents 1 transaction
            #A sigle transaction can either be with Non-Derivative Sec or Devrivative Sec.
            #An entry can have multiple transactions.
            recordList = []
            #print(cik)
            if r.find('nonDerivativeTable') != None:
                for i in handleTable1(record, r.find('nonDerivativeTable')):
                    recordList.append(i)
                    
                
                #Since nonDerivativeTable can hold multiple transactions, it returns a list of records
            if r.find('derivativeTable') != None:
                
                for i in handleTable2(record, r.find('derivativeTable')):
                    recordList.append(i)
                #Since derivativeTable can hold multiple transactions, it returns a list of records
            
                
            for record in recordList:
                datetime = entry.find('{http://www.w3.org/2005/Atom}updated').text
                parse = datetime.split('T', 1)
                date = parse[0].split('-', 2)
                time = parse[1].split('-', 1)[0]
                for ele in date:
                    record.append(ele)
                record.append(time) 
                t.append(record)
        return t
                


    pRange = range(pageToRun) 
    
    coresCount = multiprocessing.cpu_count()
    #print("Running with "+str(coresCount)+" cores")
    res = []
    res.append(Parallel(n_jobs=coresCount)(delayed(processPage)(i) for i in pRange))
    
    for tableList in res:
        for tables in tableList:
            for records in tables:
                table.append(records)
    
    
#=================================================================================================================================    
f = open('lateest_filing_at_sec.csv', 'w')

for record in table:
    row = ",".join(record) 
    row = row + "\n" 
    f.write(row)

f.close()

#t1 = time.time();
#print(t1-t0)    
