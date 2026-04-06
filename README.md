## JiraAPI ETL Pipeline
A set of Python functions for use with JiraAPI including functions to normalize the data and upload to SQL Server.

Your env file should contain 3 items and be named Jira.env unless you modify the JiraAPI.py file.
1. EMAIL = youremailaddress@domain.com
2. API_KEY = Get this from your Atlassian account.  Your access level may need to be adjusted if you don't have the option
3. SITE = https://company.atlassian.net/

The current scope of this project also includes and SQL Server element that is used to both drive the effective dates of the API as well as series of functions used to upload the data into the SQL Server database.  While useful, this is not a requirement and the core functionality will still allow you to down the data.

**fCheckAPIConnection()** <br/>
This confirms API endpoint is alive and reachable.

**fCredCheck()** <br/>
Use this to confirm your credentials in the env file are being loaded.

**fDateConversion()** <br/>
This function converts dates from MM-DD-YYYY to YYY-MM-DD

***Usage:*** <br/>
It is strongly recommended to use the Jupyter Notebook. <br/>
*Daily:* <br/>
```
# Full download of Jira data for today's date:
effectivedate = date.today() - timedelta(days=1)
effectivedate = effectivedate.strftime('%Y-%m-%d')
dailytickets, comments, changelog, SLA = Jira.fJiraExport(effectivedate=effectivedate)
```
Add if you're using the SQL Server Upload functionality.
```
# Full download of Jira data for today's date:
effectivedate = date.today() - timedelta(days=1)
effectivedate = effectivedate.strftime('%Y-%m-%d')
dailytickets, comments, changelog, SLA = Jira.fJiraExport(effectivedate=effectivedate)
```

*SQL Guided (best for non-daily use):*<br/>
please note this method has the upload process built in already and may need to be removed if not needed.<br/>
```
sql = """SELECT dateadd(dd,1,max(cast(Updated as date))) as STARTDATE, cast(getdate()-1 as date) as ENDDATE FROM IceAutomation_AZR.jira.TicketTable"""
effdates = dbcc.SQL_Call_pandas(sql=sql)
sd = str(effdates.loc[0, 'STARTDATE'])
ed = str(effdates.loc[0, 'ENDDATE'])
date_list = pd.date_range(start=sd, end=ed)
for i in date_list:
    effectivedate = i.strftime('%Y-%m-%d')
    print(effectivedate)
    Jira.fFullRun(effectivedate=effectivedate)   
    sleep(5)
```

*Date Range Pull: (best for non-daily use or bulk loading historical data.* <br/>
```
date_list = pd.date_range(start='2025-12-12', end='2025-12-14')
for i in date_list:
    effectivedate = i.strftime('%Y-%m-%d')
    print(effectivedate)
    Jira.fFullRun(effectivedate=effectivedate)   
    sleep(5)
```


___
**issues_to_dataframe()** <br/>
This function cleans up and normalized the API return.  The current output has Sprint, EpicLink, and StoryPoints commented out as they were not being utilized in the Dev enviroment.  Uncomment them if you need them

**adf_to_text()** <br/>
This is another funciton used to clean and normalize the API return data.

**flatten_sla_cycle()** <br/>
This is another funciton used to clean and normalize the API return data.>
___
*API Calls:* <br/>
**get_issues_by_effective_date()** <br/>
**get_issue_sla_flat()** <br/>
**get_issue_changelog_flat** <br/>
**get_issue_conversation_history** <br/>


___
**SQL Upload Process:**  <br/>
The upload process has 5 steps. Each step loads the data into a Staging table while the final step executes a Stored Procedure.  The SP simply checks if the data already exists in the main table.  IF it already exists, it's deleted.  The final step of the SP moves all the data from the staging tables to the main and TRUNCATES the staging tables.

*SQL Server uploads and sp calls:*  <br/>
**fUploadJiraDataToDB_Tickets** <br/>
**fUploadJiraDataToDB_Comments** <br/>
**fUploadJiraDataToDB_ChangeLog()**  <br/>
**fUploadJiraDataToDB_SLA()**  <br/>
**fRunStoredProcedures()**  <br/>

___


**fSQLGuidedPull()**
This is the full process run used as the main driver when using a packager and scheduling the executable on a scheduler.

If you chose to utilize the SQL Server upload functionality you will need and enc file with the following. 
1. eSERVER
2. eSCHEMA
3. eDATABASE
4. eUSERNAME
5. ePASSWORD

Depending on your security configurations you may need to adjust your approach.