# marketo-to-csv

Welcome to the Marketo-To-CSV (M2CSV) repository.

This python script takes leads from a Marketo database who have been sent emails and puts details on the email such as delivery status and bounce status and puts them into a CSV file.

Two CSV files will be created with the excecution of the script. One that will hold the lead database and the other holding the campaign database.

A report.txt file will be created holding the amount of leads called and the run time of the script.

A config.ini file will need to be setup and put in the local/config directory of the script to succesfully run.

Currently, the config file holds:

Client ID
Client Secret
Munchkin URL

You can specify the time span your call would search (Only supports months)

You can toggle the activities the by adding or removing on from the activity.

7: Email Delivered
1: Visit Webpage
11: Lead clicked on link
9: Lead Unsubsrcibes
8: Email Bounced
27: Email Soft Bounced
10: Email Opened
