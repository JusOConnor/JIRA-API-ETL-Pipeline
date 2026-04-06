import os
from dotenv import load_dotenv
import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
import dbCall as  dbcc
from time import sleep
import csv
from datetime import date, timedelta, datetime
from pathlib import Path

class DatabaseConfigReporting:
    def __init__(self, env_file=r'Jira.env'):
        base_dir = Path(__file__).resolve().parent
        # env_path = base_dir / (env_file or 'dbcreds_dba_Monitor.env')
        env_path = base_dir / env_file
        load_dotenv(env_path, override=True)

        # Initialize environment variables
        self.email = os.getenv('EMAIL')
        self.api_token = os.getenv('API_KEY')
        self.base_url = os.getenv('SITE')

    def get_config(self):
        # Return the configuration as a dictionary
        return self.email, self.api_token, self.base_url

# Confirms connection via REST API
def fCheckAPIConnection():
    # email, api_token, base_url = fGetENV()
    email, api_token, base_url = DatabaseConfigReporting().get_config()
    url = f'{base_url}/rest/api/3/myself'
    auth = HTTPBasicAuth(email, api_token)
    headers = {
        'Accept': 'application/json'
    }

# Helper function: convert Jira JSON to DataFrame
def issues_to_dataframe(issues):
    # Convert a list of Jira issues (JSON) into a pandas DataFrame including standard and common additional fields.
    df = pd.DataFrame([
        {
            "Key": issue["key"],
            "Project": issue["fields"]["project"]["key"],
            "Summary": issue["fields"]["summary"],
            "Assignee": issue["fields"]["assignee"]["displayName"]
                        if issue["fields"].get("assignee") else None,
            "Reporter": issue["fields"]["reporter"]["displayName"]
                        if issue["fields"].get("reporter") else None,
            "Status": issue["fields"]["status"]["name"],
            "Priority": issue["fields"]["priority"]["name"]
                        if issue["fields"].get("priority") else None,
            "Resolution": issue["fields"]["resolution"]["name"]
                        if issue["fields"].get("resolution") else None,
            "Created": issue["fields"]["created"],
            "Updated": issue["fields"]["updated"],
            "IssueType": issue["fields"]["issuetype"]["name"]
                        if issue["fields"].get("issuetype") else None,
            "Labels": ",".join(issue["fields"].get("labels", [])),
            "Components": ",".join([c["name"] for c in issue["fields"].get("components", [])])
            # "Sprint": ", ".join([s["name"] for s in issue["fields"].get("customfield_10020") or []]),  # Jira Software Sprint field (customField may vary)
            # "EpicLink": issue["fields"].get("customfield_10008"),  # Epic Link (customField may vary)
            # "StoryPoints": issue["fields"].get("customfield_10026")  # Story Points (customField may vary)
        }
        for issue in issues
    ])
    return df

# Supports the 'get_issue_conversation_history' function
# Function: Translates conversation history bodytext from JSON to flat
def adf_to_text(adf_node):
    # Recursively parse Atlassian Document Format (ADF) into plain text. Supports paragraphs, text, links, lists, headings, etc.
    if not adf_node:
        return ""

    node_type = adf_node.get("type")
    content = adf_node.get("content", [])
    text = ""

    # Text node
    if node_type == "text":
        return adf_node.get("text", "")

    # Paragraph
    if node_type == "paragraph":
        return "".join(adf_to_text(c) for c in content) + "\n"

    # Bullet list
    if node_type == "bulletList":
        for c in content:
            text += "• " + adf_to_text(c)
        return text + "\n"

    # Ordered list
    if node_type == "orderedList":
        for i, c in enumerate(content, start=1):
            text += f"{i}. " + adf_to_text(c)
        return text + "\n"

    # List item
    if node_type == "listItem":
        return "".join(adf_to_text(c) for c in content)

    # Heading
    if node_type == "heading":
        return "".join(adf_to_text(c) for c in content) + "\n"

    # Blockquote
    if node_type == "blockquote":
        return "> " + "".join(adf_to_text(c) for c in content) + "\n"

    # Hard break
    if node_type == "hardBreak":
        return "\n"

    # Default: recurse
    if isinstance(content, list):
        return "".join(adf_to_text(c) for c in content)

    return ""

def get_issues_by_effective_date(project_keys, effective_date, jql_extra=""):
    # email, api_token, base_url = fGetENV()
    email, api_token, base_url = DatabaseConfigReporting().get_config()
    # Fetch Jira issues updated on a specific effective date (local Jira server time).

    # Args:
    #     base_url (str): Jira cloud base URL
    #     email (str): Atlassian user email
    #     api_token (str): API token
    #     project_keys (list[str]): List of project keys
    #     effective_date (str): Date in YYYY-MM-DD format
    #     jql_extra (str): Optional extra JQL filters

    # Returns:
    #     pandas.DataFrame

    auth = HTTPBasicAuth(email, api_token)
    headers = {"Accept": "application/json"}

    project_list = ",".join(project_keys)

    # Build date window
    next_day = effective_date + " 23:59"

    jql = (
        f'project in ({project_list}) '
        f'AND updated >= "{effective_date}" '
        f'AND updated < "{effective_date} 23:59" '
    )

    # Allow optional extras
    if jql_extra:
        jql += f" {jql_extra}"

    # ORDER BY to ensure deterministic behavior
    jql += " ORDER BY updated ASC"

    url = f"{base_url}/rest/api/3/search/jql"
    params = {
        "jql": jql,
        "fields": "summary,project,key,assignee,status,created,updated,"
                  "reporter,priority,resolution,issuetype,labels,components,"
                  "customfield_10020,customfield_10008,customfield_10026"
    }

    response = requests.get(url, headers=headers, auth=auth, params=params)
    if response.status_code != 200:
        raise Exception(f"Error {response.status_code}: {response.text}")

    data = response.json()
    issues = data.get("issues", [])
    total = data.get("total", 0)

    print(f"Effective Date: {effective_date}")
    print(f"Returned: {len(issues)} issues, Total Matching: {total}")

    return issues_to_dataframe(issues)

# Daily Tickets Function
def fDailyTickets(effectivedate=None):
    # Gets Ticket Updates by Date
    production = get_issues_by_effective_date(['ICEDESK'], effective_date=effectivedate);sleep(1)
    enhancements = get_issues_by_effective_date(['ENH'], effective_date=effectivedate);sleep(1)
    dailytickets = pd.concat([production, enhancements])
    dailytickets.fillna('', inplace=True)
    return dailytickets

# Function: Get coversation history
def get_issue_conversation_history(issue_key):
    # email, api_token, base_url = fGetENV()
    email, api_token, base_url = DatabaseConfigReporting().get_config()
    url = f"{base_url}/rest/api/3/issue/{issue_key}/comment?maxResults=1000"
    auth = HTTPBasicAuth(email, api_token)
    headers = {"Accept": "application/json"}

    response = requests.get(url, headers=headers, auth=auth)
    if response.status_code != 200:
        raise Exception(f"Error {response.status_code}: {response.text}")

    data = response.json()
    comments = data.get("comments", [])

    rows = []
    for c in comments:
        body_adf = c.get("body", {})
        rows.append({
            "IssueKey": issue_key,
            "CommentID": c["id"],
            "Author": c["author"]["displayName"],
            "Created": c["created"],
            "Updated": c["updated"],
            "Is_Internal": c.get("visibility", {}).get("value", "Public"),
            "BodyText": adf_to_text(body_adf).strip()
        })

    return pd.DataFrame(rows)

# Function: Gets Issue change log
def get_issue_changelog_flat(issue_key):
    # email, api_token, base_url = fGetENV()
    email, api_token, base_url = DatabaseConfigReporting().get_config()
    # Fetches the FULL changelog for an issue and flattens it into a row-per-change table.
    # Returns a pandas DataFrame with: IssueKey | When | Author | Field | From | To | FieldType
    auth = HTTPBasicAuth(email, api_token)
    headers = {"Accept": "application/json"}

    changelog_rows = []
    start_at = 0
    max_results = 100

    while True:
        url = f"{base_url}/rest/api/3/issue/{issue_key}"
        params = {
            "expand": "changelog",
            "startAt": start_at,
            "maxResults": max_results
        }

        response = requests.get(url, headers=headers, auth=auth, params=params)
        response.raise_for_status()
        data = response.json()

        histories = data.get("changelog", {}).get("histories", [])
        for h in histories:
            created = h.get("created")
            author = h.get("author", {}).get("displayName")
            for item in h.get("items", []):
                changelog_rows.append({
                    "IssueKey": issue_key,
                    "When": created,
                    "Author": author,
                    "Field": item.get("field"),
                    "From": item.get("fromString"),
                    "To": item.get("toString"),
                    "FieldType": item.get("fieldtype")
                })

        total = data.get("changelog", {}).get("total", 0)
        start_at += max_results
        if start_at >= total:
            break

    df = pd.DataFrame(changelog_rows)
    return df

# Ticket Details Function
def fTicketInfo(dataframe=None):
    firstDF = 1
    if len(dataframe) == 0:
        comments = pd.DataFrame()
        changelog = pd.DataFrame()
        SLA = pd.DataFrame()
        return comments, changelog, SLA
    for i in dataframe['Key']:
        print(i)
        if firstDF == 1:
            comments = get_issue_conversation_history(issue_key=i);sleep(1)
            changelog = get_issue_changelog_flat(issue_key=i);sleep(1)
            SLA = get_issue_sla_flat(issue_key=i);sleep(1)
            firstDF = 0
        else:
            comments2 = get_issue_conversation_history(issue_key=i);sleep(1)
            changelog2 = get_issue_changelog_flat(issue_key=i);sleep(1)
            SLA2 = get_issue_sla_flat(issue_key=i);sleep(1)
            comments = pd.concat([comments, comments2])
            changelog = pd.concat([changelog, changelog2])
            SLA = pd.concat([SLA, SLA2])
    comments.fillna('', inplace=True)
    changelog.fillna('', inplace=True)
    SLA.fillna('', inplace=True)
    return comments, changelog, SLA

# Runs all extract functions by effective date
def fJiraExport(effectivedate=None):
    dailytickets = fDailyTickets(effectivedate=effectivedate);print('Fetched Daily Tickets')
    comments, changelog, SLA = fTicketInfo(dataframe=dailytickets);print('Fetched Ticket Info')
    return dailytickets, comments, changelog, SLA

# Uploads the Ticket data to the JIRA.TicketTable_STG:
def fUploadJiraDataToDB_Tickets(dailytickets=None):
    for index, row in dailytickets.iterrows():
        sql = fr"""
        INSERT INTO JIRA.TicketTable_STG 
        SELECT 
        '{row["Key"]}',
        '{row["Project"]}',
        '{row["Summary"].replace("'", "''")}',
        '{row["Assignee"].replace("'", "''")}',
        '{row["Reporter"].replace("'", "''")}',
        '{row["Status"].replace("'", "''")}',
        '{row["Priority"].replace("'", "''")}',
        '{row["Resolution"].replace("'", "''")}',
        cast(left('{row["Created"]}', 23) as datetime),
        cast(left('{row["Updated"]}', 23) as datetime),
        '{row["IssueType"].replace("'", "''")}',
        '{row["Labels"].replace("'", "''")}',
        '{row["Components"].replace("'", "''")}'
        """
        # print(sql)
        dbcc.SQL_Call_pyodbc(sql=sql,server='Ice-AZRAXSQL01', database='IceAutomation_AZR')
    print(f'Uploaded {len(dailytickets)} tickets')

# Uploads the Ticket data to the JIRA.TicketComments_STG:
def fUploadJiraDataToDB_Comments(comments=None):
    for index, row in comments.iterrows():
        sql = fr"""
        INSERT INTO JIRA.TicketComments_STG 
        SELECT 
        '{row["IssueKey"]}',
        '{row["CommentID"]}',
        '{row["Author"].replace("'", "''")}',
        cast(left('{row["Created"]}', 23) as datetime),
        cast(left('{row["Updated"]}', 23) as datetime),
        '{row["Is_Internal"].replace("'", "''")}',
        '{row["BodyText"].replace("'", "''")}'
        """
        # print(sql)
        dbcc.SQL_Call_pyodbc(sql=sql,server='Ice-AZRAXSQL01', database='IceAutomation_AZR')
    print(f'Uploaded {len(comments)} comments')

# Uploads the Ticket data to the JIRA.TicketChangeLog_STG:
def fUploadJiraDataToDB_ChangeLog(changelog=None):
    for index, row in changelog.iterrows():
        sql = fr"""
        INSERT INTO JIRA.TicketChangeLog_STG 
        SELECT 
        '{row["IssueKey"]}',
        cast(left('{row["When"]}', 23) as datetime),
        '{row["Author"].replace("'", "''")}',
        '{row["Field"].replace("'", "''")}',
        '{row["From"].replace("'", "''")}',
        '{row["To"].replace("'", "''")}',
        '{row["FieldType"].replace("'", "''")}'
        """
        # print(sql)
        dbcc.SQL_Call_pyodbc(sql=sql,server='Ice-AZRAXSQL01', database='IceAutomation_AZR')
    print(f'Uploaded {len(changelog)} changelog entries')

# Runs Stored Procedures on Server
def fRunStoredProcedures():
    sql = "EXEC JIRA.spUPDATEDATATABLES"
    dbcc.SQL_Call_pyodbc(sql=sql,server='Ice-AZRAXSQL01', database='IceAutomation_AZR')
    print('Executed Stored Procedures')

# Full run with upload:
def fFullRun(effectivedate=None):
    dailytickets, comments, changelog, SLA = fJiraExport(effectivedate=effectivedate)
    fUploadJiraDataToDB_Tickets(dailytickets=dailytickets)
    fUploadJiraDataToDB_Comments(comments=comments)
    fUploadJiraDataToDB_ChangeLog(changelog=changelog)
    fUploadJiraDataToDB_SLA(sla=SLA)
    fRunStoredProcedures()

# Converts a date from MM-DD-YYYY to YYYY-MM-DD format:
def fDateConversion(ddate=None):
    converted = datetime.strptime(ddate, '%m-%d-%Y').strftime('%Y-%m-%d')
    return converted

# Function: Flattens SLA cycle data into a row
def flatten_sla_cycle(issue_key, sla_name, cycle, cycle_type):
    # Flatten a single SLA cycle (ongoing or completed) into a dict. Cycle_type = "ongoing" or "completed"
    breached = cycle.get("breached")
    start = cycle.get("startTime", {})
    stop = cycle.get("stopTime", {})
    breach = cycle.get("breachTime", {})
    goal_hours = cycle.get("goalDuration", {}).get("millis", 0) / 3600000
    elapsed_hours = cycle.get("elapsedTime", {}).get("millis", 0) / 3600000
    remaining_hours = cycle.get("remainingTime", {}).get("millis", 0) / 3600000


    return {
        "IssueKey": issue_key,
        "SLA_Name": sla_name,
        "CycleType": cycle_type,  # completed / ongoing

        # Times
        "StartTime": start.get("iso8601"),
        "StopTime": stop.get("iso8601"),
        "TargetTime": breach.get("iso8601"),

        # SLA outcome
        "Breached": breached,
        "WithinGoal": False if breached else True,
        "Paused": cycle.get("paused"),
        "WithinCalendarHours": cycle.get("withinCalendarHours"),

        # Durations
        "goal_hours": goal_hours,
        "elapsed_hours": elapsed_hours,
        "remaining_hours": remaining_hours,
    }

# Function: Get issue SLA flat
def get_issue_sla_flat(issue_key):
    # Pulls full SLA history for a single Jira issue (completed + ongoing cycles). Returns a flat pandas DataFrame.

    import requests
    from requests.auth import HTTPBasicAuth
    import pandas as pd

    # Secure environment loader
    # email, api_token, base_url = fGetENV()
    email, api_token, base_url = DatabaseConfigReporting().get_config()

    url = f"{base_url}/rest/servicedeskapi/request/{issue_key}/sla"
    headers = {"Accept": "application/json"}
    auth = HTTPBasicAuth(email, api_token)

    response = requests.get(url, headers=headers, auth=auth)

    if response.status_code != 200:
        raise Exception(
            f"SLA API error {response.status_code}: {response.text}"
        )

    data = response.json()
    metrics = data.get("values", [])

    flat_rows = []

    for sla in metrics:
        sla_name = sla.get("name")

        # Completed cycles
        for cycle in sla.get("completedCycles", []):
            flat_rows.append(
                flatten_sla_cycle(issue_key, sla_name, cycle, "completed")
            )

        # Ongoing cycle
        ongoing = sla.get("ongoingCycle")
        if ongoing:
            flat_rows.append(
                flatten_sla_cycle(issue_key, sla_name, ongoing, "ongoing")
            )

    return pd.DataFrame(flat_rows)

def fUploadJiraDataToDB_SLA(sla=None):
    for index, row in sla.iterrows():
        sql = fr"""
        INSERT INTO JIRA.TicketSLA_STG 
        SELECT 
        '{row["IssueKey"]}',
        '{row["SLA_Name"]}',
        '{row["CycleType"].replace("'", "''")}',
        cast(left('{row["StartTime"]}', 19) as datetime),
        --nullif(cast(left('{row["StopTime"]}', 19) as datetime), ''),
        cast(left('{row["StopTime"]}', 19) as datetime),
        --'{row["TargetTime"].replace("'", "''")}',
        cast(left('{row["TargetTime"]}', 19) as datetime),
        '{row["Breached"]}',
        '{row["WithinGoal"]}',
        '{row["Paused"]}',
        '{row["WithinCalendarHours"]}',
        '{row["goal_hours"]}',
        '{row["elapsed_hours"]}',
        '{row["remaining_hours"]}'
        """
        dbcc.SQL_Call_pyodbc(sql=sql,server='Ice-AZRAXSQL01', database='IceAutomation_AZR')
    print(f'Uploaded {len(sla)} SLA records')


def fCredCheck():
    email, api_token, base_url = DatabaseConfigReporting().get_config()
    print(f'Email: {email}\nAPI Token: {api_token}\nBase URL: {base_url}')

def fSQLGuidedPull():
    print('Process Started\nQuerying database')
    sql = """SELECT dateadd(dd,1,max(cast(Updated as date))) as STARTDATE, cast(getdate()-1 as date) as ENDDATE FROM jira.TicketTable"""
    effdates = dbcc.SQL_Call_pandas(sql=sql)
    sd = str(effdates.loc[0, 'STARTDATE'])
    ed = str(effdates.loc[0, 'ENDDATE'])
    date_list = pd.date_range(start=sd, end=ed)
    print('Starting API requests')
    for i in date_list:
        effectivedate = i.strftime('%Y-%m-%d')
        print(effectivedate)
        fFullRun(effectivedate=effectivedate)   
        sleep(5)
    print('API requests completed and uploaded to server.\nThe dashboard can now be refreshed')

if __name__ == "__main__":
    fSQLGuidedPull()
