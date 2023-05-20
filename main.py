import pandas as pd
import datetime
from urllib.request import Request, urlopen
import certifi
import ssl
import json
import threading
import time
#=============================================== FETCHING DATA ========================================================================
def fetch_data():
    df = pd.read_excel('./Dataset/covid-19-state-level-data.xlsx') # file path needs to be updated based on dataset location
    state_list=set(df['state'])
    count_df=pd.DataFrame(0, index =list(state_list), 
                          columns =['March', 'April','May','June'])

    for state in state_list:    
        count_df.loc[state]['March'] = df[(df['date'] >= '2020-03-01') & 
                                          (df['date'] <= '2020-03-31') & 
                                          (df['state'] == state)]['deaths'].sum()
        count_df.loc[state]['April'] = df[(df['date'] >= '2020-04-01') & 
                                          (df['date'] <= '2020-04-30') & 
                                          (df['state'] == state)]['deaths'].sum()
        count_df.loc[state]['May'] = df[(df['date'] >= '2020-05-01') & 
                                        (df['date'] <= '2020-05-31') & 
                                        (df['state'] == state)]['deaths'].sum()
        count_df.loc[state]['June'] = df[(df['date'] >= '2020-06-01') & 
                                         (df['date'] <= '2020-06-30') & 
                                         (df['state'] == state)]['deaths'].sum()
    return count_df

#=============================================== SENDING MESSAGE TO SLACK CHANNEL ====================================================
def slack_notifier(count_df):
    top_state_March = count_df['March'].nlargest(n=3)
    top_state_April = count_df['April'].nlargest(n=3)
    top_state_May = count_df['May'].nlargest(n=3)
    top_state_June = count_df['June'].nlargest(n=3)
    March_total=count_df['March'].sum()
    April_total=count_df['April'].sum()
    May_total=count_df['May'].sum()
    June_total=count_df['June'].sum()
    
 # Message to be sent
    _message = {
            
            'attachments': [
                {
                    'title': "COVID-19 Deaths Month Wise Analysis",
                    'ts': datetime.datetime.now().timestamp(),
                    
                    'fields': [
                        {
                            "title": "Top 3 states with the higest number of covid deaths for the month of March",
                            "value": "\n Month - March" +
                            "\n" +str(top_state_March.index[0]) +
                            " - " + str(top_state_March[0]) + 
                            " no. of deaths," + str(round(((top_state_March[0]/March_total)*100),2)) +
                            " % of total US deaths" +
                            "\n" +str(top_state_March.index[1]) +
                            " - " + str(top_state_March[1]) + 
                            " no. of deaths," + str(round(((top_state_March[1]/March_total)*100),2)) +
                            " % of total US deaths"+ 
                            "\n" +str(top_state_March.index[2]) +
                            " - " + str(top_state_March[2]) + 
                            " no. of deaths," + str(round(((top_state_March[2]/March_total)*100),2)) +
                            " % of total US deaths" ,
                            "short": False
                        },
                        {
                            "title": "Top 3 states with the higest number of covid deaths for the month of April",
                            "value": "\n Month - April" +
                            "\n" +str(top_state_April.index[0]) +
                            " - " + str(top_state_April[0]) + 
                            " no. of deaths," + str(round(((top_state_April[0]/April_total)*100),2)) +
                            " % of total US deaths" +
                            "\n" +str(top_state_April.index[1]) +
                            " - " + str(top_state_April[1]) + 
                            " no. of deaths," + str(round(((top_state_April[1]/April_total)*100),2)) +
                            " % of total US deaths"+ 
                            "\n" +str(top_state_April.index[2]) +
                            " - " + str(top_state_April[2]) + 
                            " no. of deaths," + str(round(((top_state_April[2]/April_total)*100),2)) +
                            " % of total US deaths" ,
                            "short": False
                        },
                        {
                            "title": "Top 3 states with the higest number of covid deaths for the month of May",
                            "value": "\n Month - May" +
                            "\n" +str(top_state_May.index[0]) +
                            " - " + str(top_state_May[0]) + 
                            " no. of deaths," + str(round(((top_state_May[0]/May_total)*100),2)) +
                            " % of total US deaths" +
                            "\n" +str(top_state_May.index[1]) +
                            " - " + str(top_state_May[1]) + 
                            " no. of deaths," + str(round(((top_state_May[1]/May_total)*100),2)) +
                            " % of total US deaths"+ 
                            "\n" +str(top_state_May.index[2]) +
                            " - " + str(top_state_May[2]) + 
                            " no. of deaths," + str(round(((top_state_May[2]/May_total)*100),2)) +
                            " % of total US deaths" ,
                            "short": False
                        },
                        {
                            "title": "Top 3 states with the higest number of covid deaths for the month of June",
                            "value": "\n Month - June " +
                            "\n" +str(top_state_June.index[0]) +
                            " - " + str(top_state_June[0]) + 
                            " no. of deaths," + str(round(((top_state_June[0]/June_total)*100),2)) +
                            " % of total US deaths" +
                            "\n" +str(top_state_June.index[1]) +
                            " - " + str(top_state_June[1]) + 
                            " no. of deaths," + str(round(((top_state_June[1]/June_total)*100),2)) +
                            " % of total US deaths"+ 
                            "\n" +str(top_state_June.index[2]) +
                            " - " + str(top_state_June[2]) + 
                            " no. of deaths," + str(round(((top_state_June[2]/June_total)*100),2)) +
                            " % of total US deaths" ,
                            "short": False
                        }
                        
                   
                       
                    ]
                }
            ]
        }
    webhook_url = 'https://hooks.slack.com/services/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'  # Slack Channel Web Hook
    slack_data = _message 
    slack_data["channel"] = 'cloudwatch_alert' # Channel name
    
    request = Request(
        webhook_url, 
        data=json.dumps(slack_data).encode(),
        headers={'Content-Type': 'application/json'}
        )
    response = urlopen(request,context=ssl.create_default_context(cafile=certifi.where()))
    print(response.getcode())
    print(response.read().decode())
if __name__ == "__main__":
    data = fetch_data()
    while True:
        thr = threading.Thread(target=slack_notifier, args=(data, ),)
        thr.start()
        time.sleep(3600) # Sleeps for 1 hour