import numpy as np
import pandas as pd
from datetime import datetime
import requests
from datetime import datetime, timedelta
import os
import re
import json
import requests


def get_wind_data_today_WG(debug=False):
  headers = {
    'Accept': '*/*',
    'Accept-Language': 'it-IT,it;q=0.5',
    'Connection': 'keep-alive',
    # 'Cookie': 'langc=it-; deviceid=f657431976014be7eb07c1c865a87e33; session=200a813a4c4272ba40ccf6c6521d8c67; wgcookie=2|||||||||536155||||0|_|0|||||||||',
    'If-Modified-Since': 'Fri, 19 Jul 2024 17:54:20 GMT',
    'Referer': 'https://www.windguru.cz/536155',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-GPC': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Brave";v="126"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
  }

  params = {
      'q': 'forecast_spot',
      'id_spot': '536155',
  }

  response = requests.get('https://www.windguru.cz/int/iapi.php', params=params, headers=headers)

  id_model_arr = response.json()["tabs"][0]["id_model_arr"]
  if(debug):
    print(response.json())
    print(id_model_arr)


  headers = {
    'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Brave";v="126"',
    'Referer': 'https://www.windguru.cz/',
    'sec-ch-ua-mobile': '?0',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
    'sec-ch-ua-platform': '"Windows"',
  }

  wind_data_per_model = []

  for i in id_model_arr:

    params = {
        'q': 'forecast',
        'id_model': i["id_model"],
        'rundef': i["rundef"],
        'initstr': i["initstr"],
        'id_spot': '536155',
        #'WGCACHEABLE': '21600',
        'cachefix': i["cachefix"],
    }

    response = requests.get('https://www.windguru.net/int/iapi.php', params=params, headers=headers)
    wind_data_per_model.append(response.json())
    if(debug):
      print(response.json())


  return(wind_data_per_model)


def get_old_data_github_WG(id):
  try:
    df_old=pd.read_csv(f"https://raw.githubusercontent.com/marcoavesani/meteo_san_giuliano/master/data/predictions/predicted_wind_venice_{datetime.now().month}_{datetime.now().year}_{str(id)}.csv",index_col=[0],parse_dates=True,date_format="%Y-%m-%d %H:%M:%S")
    df_old['timestamp'] = pd.to_datetime(df_old['timestamp'],format="%Y-%m-%d %H:%M:%S")
    df_old['model_name'] = df_old['model_name'].astype("string")
  except:
    print(f"File https://raw.githubusercontent.com/marcoavesani/meteo_san_giuliano/master/data/predictions/predicted_wind_venice_{datetime.now().month}_{datetime.now().year}_{str(id)}.csv not found creating a new one")
    df_old=pd.DataFrame()
  return(df_old)



if __name__ == "__main__":

  model_data=get_wind_data_today_WG()

  tomorrow = datetime.now() + timedelta(days=1)
  desired_keys = ['WINDSPD','GUST', 'WINDDIR', 'SLP','TMP','TMPE' 'FLHGT',  'RH', 'TCDC', 'APCP', 'APCP1', 'HCDC', 'MCDC', 'LCDC', 'SLHGT', 'PCPT']


  #dfs = []

  for i in range(0,len(model_data)):

    time_local_init=datetime.strptime( model_data[i]["fcst"]["initdate"],"%Y-%m-%d %H:%M:%S")
    # Convert the string into a datetime object
    time_model=[ time_local_init + timedelta(hours=i) for i in model_data[i]["fcst"]["hours"] ]
    filtered_dates = [dt for dt in time_model if dt.date() == tomorrow.date()]
    filtered_indices = np.array([i for i, dt in enumerate(time_model) if dt.date() == tomorrow.date()])

    # Take only the data of the forecast of tomorrow
    model_df = pd.DataFrame({"timestamp":filtered_dates })

    for key in desired_keys:
        if key in model_data[i]['fcst']:
            model_df[key] = np.array(model_data[i]['fcst'][key])[filtered_indices]
    model_df["model_name"]=model_data[i]["wgmodel"]["model_longname"]
    model_df["model_id"]=model_data[i]["wgmodel"]["id_model"]
    model_df["model_name"]=model_df["model_name"].astype("string")  
    
    id=model_data[i]["wgmodel"]["id_model"]
    
    
    df_old=get_old_data_github_WG(int(id))
    df_merged=pd.concat([df_old,model_df]).drop_duplicates().reset_index(drop=True)
    


    directory_path = "./data/predictions"

    if not os.path.exists(directory_path):
      os.makedirs(directory_path)
    
    df_merged.to_csv(f"./data/predictions/predicted_wind_venice_{datetime.now().month}_{datetime.now().year}_{id}.csv")
    #dfs.append(model_df)

  #combined_df = pd.concat(dfs, ignore_index=True)

