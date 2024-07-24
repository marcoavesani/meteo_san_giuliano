import numpy as np
import pandas as pd
from datetime import datetime
import requests
from datetime import datetime, timedelta
import os
import re
import json
import requests

def get_wind_data_today(debug=False):

  url = 'https://meteo-venezia.net/'

  html_data = requests.get(url).text
  #data = re.search(r'dataPoints\s*=\s*\[(.*?)\]', html_data)

  data = re.search(r'dataPoints=\[(.*?) \]', html_data)
  print(data)


  datapoints_loc=html_data.find("dataPoints=")
  init_sq=html_data[datapoints_loc:].find("[")
  close_sq=html_data[datapoints_loc:].find("]")

  extracted=html_data[datapoints_loc+init_sq:datapoints_loc+close_sq+1].replace("x:","\"x\":").replace("y:","\"y\":").replace("ygust:","\"ygust\":").replace("dir:","\"dir\":").replace("dirdegree:","\"dirdegree\":").replace("xtmp:","\"xtmp\":").replace("ytmp:","\"ytmp\":").replace("ygusttmp:","\"ygusttmp\":").replace(",t",",\"t\"")
  extracted_split=extracted.split("},{")
  extracted_split[0]=extracted_split[0][8:]
  extracted_split[-1]=extracted_split[-1][:-19]
  print(extracted_split[-1])
  array_dict=[json.loads("{"+i+"}") for i in  extracted_split  ]

  df=pd.DataFrame(array_dict)
  return(df)

def process_wind_data(df):

  df = df.drop(columns=['xtmp','ytmp','ygusttmp','x'])

  #Get current date in the desired format
  current_date = datetime.now().strftime('%Y-%m-%d')
  # Concatenate current date with time column
  df['t'] = current_date + ' ' + df['t']

  # Convert 't' column to datetime dtype
  df['t'] = pd.to_datetime(df['t'],format="%Y-%m-%d %H:%M:%S")
  df['dir']=df['dir'].astype("string")
  df['dirdegree']=df['dirdegree'].astype(np.int64)

  df = df.rename(columns={"t": "time_measured"})
  df = df.rename(columns={"y": "wind_speed_measured"})
  df = df.rename(columns={"ygust": "wind_gust_measured"})
  df = df.rename(columns={"dir": "wind_direction_measured"})
  df = df.rename(columns={"dirdegree": "wind_direction_degree_measured"})

  # Specify the desired column order
  desired_columns = ['time_measured','wind_speed_measured', 'wind_gust_measured', 'wind_direction_measured', 'wind_direction_degree_measured']

  # Reorder columns in DataFrame
  df = df[desired_columns]
  print(df)
  return(df)

def get_old_data_github():
  try:
    df_old=pd.read_csv(f"https://raw.githubusercontent.com/marcoavesani/meteo_san_giuliano/master/data/measurements/measured_wind_venice_{datetime.now().month}_{datetime.now().year}.csv",index_col=[0],parse_dates=True,date_format="%Y-%m-%d %H:%M:%S")
    print(f"Found old file at path https://raw.githubusercontent.com/marcoavesani/meteo_san_giuliano/master/data/measurements/measured_wind_venice_{datetime.now().month}_{datetime.now().year}.csv, opening")
    df_old['time_measured'] = pd.to_datetime(df_old['time_measured'],format="%Y-%m-%d %H:%M:%S")
    df_old['wind_direction_measured']=df_old['wind_direction_measured'].astype("string")
  except:
    print("File not found creating a new one")
    df_old=pd.DataFrame()
  return(df_old)

if __name__ == "__main__":
  df=get_wind_data_today()
  df=process_wind_data(df)
  df_old=get_old_data_github()
  df_merged=pd.concat([df_old,df]).drop_duplicates().reset_index(drop=True)
  
  directory_path = "./data/measurements"

  if not os.path.exists(directory_path):
    os.makedirs(directory_path)
  
  df_merged.to_csv(f"./data/measurements/measured_wind_venice_{datetime.now().month}_{datetime.now().year}.csv")
