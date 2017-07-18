# -*- coding: utf-8 -*-
"""
Created on Tue Jan 10 14:29:58 2017

v4_3: Added new date format and creation of Result directory for CSV file and report.txt
v4_6: Added soft bounce activity and changed lead clicks activity from #3 to #11

@author: Daniel
"""

import configparser
import requests
import json
from datetime import date
from dateutil.relativedelta import relativedelta
import dateutil.parser as dp
import csv
import time
import os, os.path
import errno

def isodateconverter(date_time):
    
    parsed_dt = dp.parse(date_time)
    protime = parsed_dt.strftime('%Y-%m-%d %H:%M:%S')
    
    return protime

def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else: raise

def safe_open_w(path):
    ''' Open "path" for writing, creating any parent directories as needed.
    '''
    mkdir_p(os.path.dirname(path))
    return open(path, 'w', newline = '', encoding="utf-8")

dir_path = os.path.dirname(os.path.realpath(__file__))

path = dir_path + '/Result/'

#Input function to deal with a response of more than 300 records

#Intialisation

starttime = time.time()

config = configparser.ConfigParser()

config.read('config/config.ini', 'utf-8')

actual = config['credentials']['actual']
client_Id = config['credentials']['client_Id']
client_Secret = config['credentials']['client_Secret']

activity7 = config['activity']['activity7']
activity1 = config['activity']['activity1']
activity11 = config['activity']['activity11']
activity9 = config['activity']['activity9']
activity8 = config['activity']['activity8']
activity10 = config['activity']['activity10']
activity27 = config['activity']['activity27']

url = "https://" + actual + ".mktorest.com"
moreResult = True
res_info_fin = []

loop = True

#Collecting AuthToken and PageToken
   
#debug = open('debug_api_calls.txt', 'w', encoding="utf-8")

#Token Fetcher

print ("Authenticating...")

token = requests.api.get(url + "/identity/oauth/token?grant_type=client_credentials&client_id=" + client_Id + "&client_secret=" + client_Secret, headers={'Connection':'close'})

tokenst = str(token.content, 'utf-8')
tokenst = json.loads(tokenst)
tokenst = "access_token=" + tokenst["access_token"]

print ("Authenticated \n")

#Time Fetcher
monthsago = int(config['time']['monthsago'])
sincetime = str(date.today() - relativedelta(months=+monthsago))

#Paging Token Fetcher

print ("Fetching Page Token...")

ptoken = requests.api.get(url + "/rest/v1/activities/pagingtoken.json?sinceDatetime=" + sincetime + "&" + tokenst, headers={'Connection':'close'})

ptoken = str(ptoken.content, 'utf-8')
ptoken = json.loads(ptoken)
ptoken = "nextPageToken=" + ptoken['nextPageToken']

print ("Fetched \n")

#Calling Full list of Leads

print ("Calling Leads from Sent Emails activity #6...\n")

moreLeads = True

leadId_list = ""
actualid_list = []
actualid_300 = []
senddatelist = []
camplist = []

iterate = 0
iterate_limit = 3

ptoken6 = ptoken

while moreLeads == True:
    
    print ("Batch Number", iterate + 1)
    
    #Calling API for Leads
    
    leadId_list = ""
    
    try:
        
        call = requests.api.get(url + "/rest/v1/activities.json?" + tokenst + "&activityTypeIds=6" + "&" + ptoken6, headers={'Connection':'close'})
        response = str(call.content, 'utf-8')
        res_dict = json.loads(response)
        
    except requests.exceptions.ConnectionError:
        
        print ('Connection Error.')
        pass

    if 'result' not in res_dict:
        
        print ('No Result!')
        break

    for lead in res_dict['result']:
        
        if lead['leadId'] in [elem for sublist in senddatelist for elem in sublist]:
            
            for entry in senddatelist:
                
                if lead['leadId'] == entry[0] and entry[2] < dp.parse(lead['activityDate']):
                    
                    senddatelist.remove(entry)
            
                    actualid_list.append(lead['leadId'])
                    leadId_list = leadId_list + "," + str(lead['leadId']) 
                    
                    #Collecting Activity Date
                    
                    senddatelist.append([lead['leadId'], isodateconverter(lead['activityDate']), dp.parse(lead['activityDate'])])
                
                    #Collecting Campaign IDs
                            
                    camplist.append([lead['leadId'], lead['campaignId']])
                    
        else:
            
            actualid_list.append(lead['leadId'])
            leadId_list = leadId_list + "," + str(lead['leadId']) 
            
            #Collecting Activity Date
            
            senddatelist.append([lead['leadId'], isodateconverter(lead['activityDate']), dp.parse(lead['activityDate'])])
        
            #Collecting Campaign IDs
                    
            camplist.append([lead['leadId'], lead['campaignId']])
    
    leadId_list = leadId_list[1:]
    
    #Collecting PageToken
    if res_dict['moreResult'] == True:
            
        ptoken6 = ""
        ptoken6 = "nextPageToken=" + res_dict['nextPageToken']
        moreLeads = True
        #print (ptoken)

    else:
        
        moreLeads = False
        
    actualid_300.append(leadId_list)
    
    #debugging iteration
    if iterate == iterate_limit:
        
        moreLeads = False
    
    iterate += 1

#Calling info on Lead by LeadId

print ("\nCalling info on Leads...\n")

res_info_list = []
campid_list = []
response = ""
campids = ''
call = ""

iterate = 0

for chunk in actualid_300:
    
    print ("Batch Number", iterate + 1)

    try:
        callleadid = requests.api.get(url + "/rest/v1/leads.json?" + tokenst + "&filterType=Id&filterValues=" + chunk, timeout = 30)
    
        #print ('boop')
    
    except requests.exceptions.ConnectionError:
        
        print ('Connection Error.')
        pass
    
    response = str(callleadid.content, 'utf-8')
    res_info = json.loads(response)
    
    #Add Lead Info To List
    
    for lead in res_info['result']:
        
        res_info_list.append(lead)
        
    #debugging iteration
    if iterate == iterate_limit:
        
        break
    
    iterate += 1
        
#Collecting Campaign Names

for ID in camplist:
    
    if (ID[1] in campid_list) == False:
        
        campid_list.append(ID[1])

#Calling Campaign Info

response = ""
call = ""

call = requests.api.get(url + "/rest/v1/campaigns.json?" + tokenst, headers={'Connection':'close'})
response = str(call.content, 'utf-8')
res_campnme = json.loads(response)

#print (res_campnme)

for camp in res_campnme['result']:
    
    for ID in camplist:
        
        if camp['id'] == ID[1]:
                
            ID.append(camp['programName'])
            
camp_dict_list = res_campnme['result']
            
#Calling Further Information for additional activites

#7: Email Delivered

print ("\nFetching info on Emails Delivered activity #7...\n")

if activity7.lower() == 'on':
    
    moreLeads = True

else:
    
    print ("Skipped")

delivdatelist = []
response = ""
call = ""

iterate = 0

ptoken7 = ptoken

while moreLeads == True:
    
    print ("Batch Number", iterate + 1)
       
    call = requests.api.get(url + "/rest/v1/activities.json?" + tokenst + "&activityTypeIds=7" + "&" + ptoken7, timeout=5)
    response = str(call.content, 'utf-8')
    res_delivered = json.loads(response)
    
    #Collecting Next Page Token
    if 'nextPageToken' in list(res_delivered.keys()):
        
        ptoken7 = "nextPageToken=" + res_delivered['nextPageToken']
        
    else:
        
        moreLeads = False
    
    #print (res_delivered)
    
    #Collecting Activity Date
    
    if 'result' in list(res_delivered.keys()):
    
        for lead in res_delivered['result']:
        
            for comp in senddatelist:
                
                if lead['leadId'] == comp[0] and dp.parse(lead['activityDate']) > comp[2]:
                    
                    #print (lead['leadId'], dp.parse(lead['activityDate']), comp[2])
                    
                    delivdatelist.append([lead['leadId'], isodateconverter(lead['activityDate'])])
            
    else:
        
        moreLeads = False
            
    #debugging iteration
    if iterate == iterate_limit:
        
        moreLeads = False
    
    iterate += 1

#1: Visit Webpage

print ("\nFetching info Visited Webpage activity #1...\n")

if activity1.lower() == 'on':
    
    moreLeads = True

else:
    
    print ("Skipped")
    
call = ""
response = ""
res_webpage = {}

iterate = 0

ptoken1 = ptoken

webpagedatelist = []

while moreLeads == True:
    
    print ("Batch Number", iterate + 1)
            
    call = requests.api.get(url + "/rest/v1/activities.json?" + tokenst + "&activityTypeIds=1" + "&" + ptoken1, headers={'Connection':'close'})
    response = str(call.content, 'utf-8')
    res_webpage = json.loads(response)
    
    #Collecting Next Page Token
    if 'nextPageToken' in list(res_webpage.keys()):
        
        ptoken1 = "nextPageToken=" + res_webpage['nextPageToken']
        
    else:
        
        moreLeads = False
        
    #print (res_webpage)
    
    #Collecting Activity Date
    
    if 'result' in list(res_webpage.keys()):
    
        for lead in res_webpage['result']:
        
            webpagedatelist.append([lead['leadId'], isodateconverter(lead['activityDate'])])
            
    else:
        
        moreLeads = False
            
    #debugging iteration
    if iterate == iterate_limit:
        
        moreLeads = False
    
    iterate += 1
    
#11: Lead clicked on link

print ("\nFetching info Lead Clicks activity #11...\n")

if activity11.lower() == 'on':
    
    moreLeads = True

else:
    
    print ("Skipped")

call = ""
response = ""
res_clicked = {}

iterate = 0

ptoken11 = ptoken

clicklinkdatelist = []

while moreLeads == True:
    
    print ("Batch Number", iterate + 1)
        
    call = requests.api.get(url + "/rest/v1/activities.json?" + tokenst + "&activityTypeIds=11" + "&" + ptoken11, headers={'Connection':'close'})
    response = str(call.content, 'utf-8')
    res_clicked = json.loads(response)
    
    #print (res_clicked)
    
    #Collecting Next Page Token
    if 'nextPageToken' in list(res_clicked.keys()):
        
        ptoken11 = "nextPageToken=" + res_clicked['nextPageToken']
        
    else:
        
        moreLeads = False
    
    #Collecting Activity Date
    if 'result' in list(res_clicked.keys()):
    
        for lead in res_clicked['result']:
        
            clicklinkdatelist.append([lead['leadId'], isodateconverter(lead['activityDate'])])
            
    else:
        
        moreLeads = False
            
    #debugging iteration
    if iterate == iterate_limit:
        
        moreLeads = False
    
    iterate += 1
    
#9: Lead Unsubsrcibes

print ("\nFetching info Lead Unsubscribed activity #9...\n")

if activity9.lower() == 'on':
    
    moreLeads = True

else:
    
    print ("Skipped")

call = ""
response = ""
res_unsub = {}

iterate = 0

ptoken9 = ptoken

unsubdatelist = []

while moreLeads == True:
    
    print ("Batch Number", iterate + 1)
    
    call = requests.api.get(url + "/rest/v1/activities.json?" + tokenst + "&activityTypeIds=9" + "&" + ptoken9, headers={'Connection':'close'})
    response = str(call.content, 'utf-8')
    res_unsub = json.loads(response)
    
    #print (res_unsub)
    
    #Collecting Next Page Token
    if 'nextPageToken' in list(res_unsub.keys()):
        
        ptoken9 = "nextPageToken=" + res_unsub['nextPageToken']
        
    else:
        
        moreLeads = False
    
    #Collecting Activity Date
    if 'result' in list(res_unsub.keys()):
    
        for lead in res_unsub['result']:
        
            unsubdatelist.append([lead['leadId'], isodateconverter(lead['activityDate'])])
            
    else:
        
        moreLeads = False
            
    #debugging iteration
    if iterate == iterate_limit:
        
        moreLeads = False
    
    iterate += 1
            
#8: Email Bounced

print ("\nFetching info Email Bounced activity #8...\n")

if activity8.lower() == 'on':
    
    moreLeads = True

else:
    
    print ("Skipped")

call = ""
response = ""
res_bounce = {}

iterate = 0

ptoken8 = ptoken

bouncedatelist = []

while moreLeads == True:
    
    print ("Batch Number", iterate + 1)
        
    call = requests.api.get(url + "/rest/v1/activities.json?" + tokenst + "&activityTypeIds=8" + "&" + ptoken8, headers={'Connection':'close'})
    response = str(call.content, 'utf-8')
    res_bounce = json.loads(response)
    
    #print (res_bounce)
    
    #Collecting Next Page Token
    if 'nextPageToken' in list(res_bounce.keys()):
        
        ptoken8 = "nextPageToken=" + res_bounce['nextPageToken']
        
    else:
        
        moreLeads = False
    
    #Collecting Activity Date
    if 'result' in list(res_bounce.keys()):
    
        for lead in res_bounce['result']:
            
            for attr in lead['attributes']:
                
                if attr['name'] == 'Details':
                    
                    bouncedatelist.append([lead['leadId'], isodateconverter(lead['activityDate']), attr['value']])
                    
    else:
        
        moreLeads = False
                    
    #debugging iteration
    if iterate == iterate_limit:
        
        moreLeads = False
    
    iterate += 1
    
#27: Email Soft Bounced

print ("\nFetching info Email Soft Bounced activity #27...\n")

if activity27.lower() == 'on':
    
    moreLeads = True

else:
    
    print ("Skipped")

call = ""
response = ""
res_softbounce = {}

iterate = 0

ptoken27 = ptoken

softbouncedatelist = []

while moreLeads == True:
    
    print ("Batch Number", iterate + 1)
        
    call = requests.api.get(url + "/rest/v1/activities.json?" + tokenst + "&activityTypeIds=27" + "&" + ptoken27, headers={'Connection':'close'})
    response = str(call.content, 'utf-8')
    res_softbounce = json.loads(response)
    
    #print (res_softbounce)
    
    #Collecting Next Page Token
    if 'nextPageToken' in list(res_softbounce.keys()):
        
        ptoken27 = "nextPageToken=" + res_softbounce['nextPageToken']
        
    else:
        
        moreLeads = False
    
    #Collecting Activity Date
    if 'result' in list(res_softbounce.keys()):
    
        for lead in res_softbounce['result']:
            
            for attr in lead['attributes']:
                
                if attr['name'] == 'Details':
                    
                    softbouncedatelist.append([lead['leadId'], isodateconverter(lead['activityDate']), attr['value']])
                    
    else:
        
        moreLeads = False
                    
    #debugging iteration
    if iterate == iterate_limit:
        
        moreLeads = False
    
    iterate += 1
    
#10: Email Opened

print ("\nFetching info Email Opened activity #10...\n")

if activity10.lower() == 'on':
    
    moreLeads = True
    
else:
    
    print ("Skipped")

call = ""
response = ""
res_open = {}

iterate = 0

ptoken10 = ptoken

openeddatelist = []

while moreLeads == True:
    
    print ("Batch Number", iterate + 1)
        
    call = requests.api.get(url + "/rest/v1/activities.json?" + tokenst + "&activityTypeIds=10" + "&" + ptoken10, headers={'Connection':'close'})
    response = str(call.content, 'utf-8')
    res_open = json.loads(response)
    
    #print (res_open)
    
    #Collecting Next Page Token
    if 'nextPageToken' in list(res_open.keys()):
        
        ptoken10 = "nextPageToken=" + res_open['nextPageToken']
        
    else:
        
        moreLeads = False
    
    #Collecting Activity Date
    if 'result' in list(res_open.keys()):
    
        for lead in res_open['result']:
        
            openeddatelist.append([lead['leadId'], isodateconverter(lead['activityDate'])])
            
    else:
        
        moreLeads = False
            
    #debugging iteration
    if iterate == iterate_limit:
        
        moreLeads = False
    
    iterate += 1
    
#Attach Extra Fields

print ("\nCreating Lead Entries from data collected...")
        
for lead in res_info_list:
    
    #print (lead['id'])
    
    for ID in camplist:
        
        if ID[0] == lead['id']:
            
            lead['Campaign ID'] = ID[1]

            if len(ID) == 2:
                
                lead['Campaign Name'] = "Not Found"
                
            else:
                
                lead['Campaign Name'] = ID[2]

    for datelist6 in senddatelist:
        
        if datelist6[0] == lead['id']:
            
            lead['Email Sent Date'] = datelist6[1]

    for datelist7 in delivdatelist:
        
        if datelist7[0] == lead['id']:
            
            lead['Email Delivery Date'] = datelist7[1]
            
    #print (delivdatelist)
        
    for datelist1 in webpagedatelist:
        
        if datelist1[0] == lead['id']:
            
            lead['Visited Web Page'] = datelist1[1]
            
    for datelist3 in clicklinkdatelist:
        
        if datelist3[0] == lead['id']:
            
            lead['Clicked on link'] = datelist3[1]

    for datelist9 in unsubdatelist:
        
        if datelist9[0] == lead['id']:
            
            lead['Unsubscribed'] = datelist9[1]

    for datelist8 in bouncedatelist:
        
        if datelist8[0] == lead['id']:
            
            lead['Email Bounced'] = datelist8[1]
            lead['Bounced Reason'] = datelist8[2]

    for datelist27 in softbouncedatelist:
        
        if datelist27[0] == lead['id']:
            
            lead['Email Soft Bounced'] = datelist27[1]
            lead['Soft Bounced Reason'] = datelist27[2]
            
    for datelist10 in openeddatelist:
        
        if datelist10[0] == lead['id']:
            
            lead['Email Opened'] = datelist10[1]
    
    #Filling Fields for null return
    
    if len(delivdatelist) == 0 or ('Email Delivery Date' in lead) == False:
        
        lead['Email Delivery Date'] = "Not Delivered"
        
    if len(webpagedatelist) == 0 or ('Visited Web Page' in lead) == False:
        
        lead['Visited Web Page'] = "Did not visit web page"
        
    if len(clicklinkdatelist) == 0 or ('Clicked on link' in lead) == False:
        
        lead['Clicked on link'] = "Lead not clicked"
        
    if len(unsubdatelist) == 0 or ('Unsubscribed' in lead) == False:
        
        lead['Unsubscribed'] = "Still Subscribed"
        
    if len(bouncedatelist) == 0 or ('Email Bounced' in lead) == False:
        
        lead['Email Bounced'] = "Email Succesfully Sent"
        
    if len(softbouncedatelist) == 0 or ('Email Soft Bounced' in lead) == False:
        
        lead['Email Soft Bounced'] = "Email Succesfully Sent"
        
    if len(openeddatelist) == 0 or ('Email Opened' in lead) == False:
        
        lead['Email Opened'] = "Email has not been opened"
    
    #Tidy Up
        
    lead['createdAt'] = isodateconverter(lead['createdAt'])
    lead['updatedAt'] = isodateconverter(lead['updatedAt'])
        
    lead['Id'] = lead['id']
    del lead['id']
    lead['First Name'] = lead['firstName']
    del lead['firstName']
    lead['Last Name'] = lead['lastName']
    del lead['lastName']
    lead['Email'] = lead['email']
    del lead['email']
    lead['Created At'] = lead['createdAt']
    del lead['createdAt']
    lead['Updated At'] = lead['updatedAt']
    del lead['updatedAt']
    
    #print (lead)
    
    res_info_fin.append(lead)

for camp in camp_dict_list:
    
    camp['createdAt'] = isodateconverter(camp['createdAt'])
    camp['updatedAt'] = isodateconverter(camp['updatedAt'])
        
    camp['Id'] = camp['id']
    del camp['id']
    
    try:
        camp['Programme Id'] = camp['programId']
        del camp['programId']
            
    except:
        pass
    
    camp['Name'] = camp['name']
    del camp['name']
    
    try:
        camp['Description'] = camp['description']
        del camp['description']
            
    except:
        pass
    
    try:
        camp['Programme Name'] = camp['programName']
        del camp['programName']
        
    except:
        pass
    
    camp['Workspace Name'] = camp['workspaceName']
    del camp['workspaceName']
    camp['Active'] = camp['active']
    del camp['active']
    camp['Type'] = camp['type']
    del camp['type']
    
    camp['Created At'] = camp['createdAt']
    del camp['createdAt']
    camp['Updated At'] = camp['updatedAt']
    del camp['updatedAt']

print ("Done")

#Counting dupes (same leadid and same campaign)

dupecount = 0

res_info_dupe = res_info_fin

'''
for lead in res_info_dupe:
    
    res_info_dupe.remove(lead)
    
    if any((d['Email'] == lead['Email']) and (d['Campaign ID'] != lead['Campaign ID']) for d in res_info_dupe):
        
        dupecount += 1

print (dupecount, len(res_info_fin))
'''

#Import into CSV file

print ("\nWriting CSV file...")

timeofrun = str(date.today())
    
with safe_open_w(path + 'Call_result_' + timeofrun + '.csv') as csvfile:
    
    fieldnames = ['Id', 'First Name', 'Last Name', 'Email', 'Updated At', 'Created At', 'Campaign ID', 'Campaign Name', 'Email Sent Date', 'Email Delivery Date', 'Email Opened', 'Email Bounced', 'Bounced Reason', 'Email Soft Bounced', 'Soft Bounced Reason', 'Clicked on link', 'Visited Web Page', 'Unsubscribed']
    writer = csv.DictWriter(csvfile, fieldnames = fieldnames)
    
    writer.writeheader()
    
    for lead in res_info_fin:
        
        writer.writerow(lead)
        
with safe_open_w(path + 'Campaign_result_' + timeofrun + '.csv') as csvfile:
    
    fieldnames = ['Id', 'Programme Id', 'Name', 'Updated At', 'Created At', 'Description', 'Programme Name', 'Workspace Name', 'Type', 'Active']
    writer = csv.DictWriter(csvfile, fieldnames = fieldnames)
    
    writer.writeheader()
    
    for camp in camp_dict_list:
        
        writer.writerow(camp)
        
print ("Done")

#Writing Report
print ("\nWriting Report...")

report = open(path + 'report.txt', 'w', encoding="utf-8")

report.write("[Report]\n\n")
report.write("Total Leads Calls: " + str(len(actualid_list)) + "\n")
report.write("Time Taken: " + str(format(time.time() - starttime, ".2f")) + " seconds")
#report.write("Duplicate count; Same email and different Camp Id: " + str(dupecount))

report.close()

print ("Done\n")

print("Done - ", format(time.time() - starttime, ".2f"), "seconds")
