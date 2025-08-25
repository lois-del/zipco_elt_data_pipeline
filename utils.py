import requests
import pandas as pd
import os

url = os.getenv("API_URL")
api_key = os.getenv("API_KEY")

headers = {
    "accept": "application/json",
    "X-Api-Key": api_key
}

#function to extract properties data from API
def get_data(url):
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json
        df = pd.json_normalize(data)
        return df         
    else:
        raise Exception(f"Error fetching data: {response.status.code}")
    
#function to clean the headers for the properties dataset
def clean_headers(df):
    df.columns = (
        df.columns.str.lower()
        .str.replace(' ', '_')
        .str.replace(r'[^\w_]', '', regex=True)
    )
    return df