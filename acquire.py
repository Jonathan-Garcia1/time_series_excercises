import requests
import pandas as pd



def get_people():
    
    # new website:
    sw_page = 'https://swapi.dev/api'
    # lets get a new response:
    response = requests.get(sw_page)
    # first payload:
    home_page = response.json()
    
    # Initialize an empty DataFrame to store the results
    all_data_people = pd.DataFrame()

    # Start with the first page
    next_page_url = home_page['people']

    while next_page_url is not None:
        # Fetch data from the current page
        people_payload = requests.get(next_page_url).json()
        page_data = pd.DataFrame(people_payload['results'])
        
        # Concatenate the current page's data to the main DataFrame
        all_data_people = pd.concat([all_data_people, page_data], ignore_index=True)
        
        # Check if there's a next page
        next_page_url = people_payload.get('next')
        
    return all_data_people


def get_data(custom_endpoint):
    # Construct the URL with the custom endpoint
    base_url = 'https://swapi.dev/api/'
    url = f'{base_url}{custom_endpoint}/'
    
    # Get the response from the URL
    response = requests.get(url)
    
    # Check if the response was successful
    if response.status_code == 200:
        data = response.json()
        
        # Initialize an empty DataFrame with a name reflecting the custom endpoint
        dataframe_name = f'{custom_endpoint}_data'
        all_data = pd.DataFrame()

        # Start with the first page
        next_page_url = data['next']

        while next_page_url is not None:
            # Fetch data from the current page
            payload = requests.get(next_page_url).json()
            page_data = pd.DataFrame(payload['results'])

            # Concatenate the current page's data to the main DataFrame
            all_data = pd.concat([all_data, page_data], ignore_index=True)

            # Check if there's a next page
            next_page_url = payload.get('next')

        # Return the DataFrame with the custom name
        globals()[dataframe_name] = all_data
        return all_data
    else:
        print(f"Failed to fetch data from {url}. Status code: {response.status_code}")
        return None

def acquire_data():
    people = get_data('people')
    planets = get_data('planets')
    starships = get_data('starships')
