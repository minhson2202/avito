#!/usr/bin/env python
# coding: utf-8

import requests
import time
import pandas as pd
import xml.etree.ElementTree as ET
import multiprocessing as mp
import os
from multiprocessing import Semaphore
import math

# Define the list of proxies
proxies = [f'http://92.63.77.{ip}:3139' for ip in range(130, 230)]

# Initialize a Semaphore to limit concurrent requests
request_semaphore = Semaphore(2)  # 2 concurrent requests to match 120 req/min

def check_rate_limit(response):
    remaining = int(response.headers.get('X-RateLimit-Remaining', 1))
    reset_time = response.headers.get('X-RateLimit-Reset')
    
    if reset_time:
        reset_time = int(reset_time)
    else:
        reset_time = None
    
    if remaining == 0:
        if reset_time:
            wait_time = reset_time - int(time.time()) + 1
            print(f"Rate limit reached. Sleeping for {wait_time} seconds...")
            time.sleep(wait_time)
        else:
            print("Rate limit reached but reset time is unknown. Sleeping for 60 seconds as a fallback.")
            time.sleep(60)
    elif remaining < 5:
        print("Approaching rate limit. Sleeping for 2 seconds to throttle requests.")
        time.sleep(2)

def fetch_vacancies_with_session(session, url, headers, params, proxy=None, max_retries=5):

    attempt = 0
    backoff_time = 1  # Start with 1 second
    
    while attempt < max_retries:
        print("Calling API request...")
        proxy_dict = {"http": proxy, "https": proxy, "ftp": proxy} if proxy else None

        try:
            response = session.get(url, headers=headers, params=params, proxies=proxy_dict, timeout=10)
            check_rate_limit(response)
            
            if response.status_code == 200:
                print(f"Request succeeded with proxy {proxy}.")
                return response.json()
            elif response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", backoff_time))
                print(f"Rate limit exceeded (429). Retrying after {retry_after} seconds...")
                time.sleep(retry_after)
                attempt += 1
                backoff_time *= 2  # Exponential backoff
            else:
                print(f"Request failed with status code {response.status_code} and proxy {proxy}.")
                print("Response:", response.text)
                return None
        except requests.exceptions.RequestException as e:
            print(f"Error during API request with proxy {proxy}: {e}")
            time.sleep(backoff_time)
            attempt += 1
            backoff_time *= 2 
    print(f"Max retries reached for proxy {proxy}. Skipping this request.")
    return None

def export_vacancies_to_csv(vacancies, filename="dv.csv"):
    df = pd.DataFrame(vacancies)

    file_exists = os.path.isfile(filename)

    df.to_csv(
        filename, 
        mode='a',  # Append mode
        header=not file_exists,  # Write header only if file does not exist
        index=False,  # Do not write row indices
        encoding='utf-8'  # Use UTF-8 encoding
    )

    print(f"Exported {len(vacancies)} vacancies to {filename}")

def fetch_vacancies_worker(task):
    access_token, location, business_area, url, headers, proxy = task
    current_page = 1  
    all_vacancies = []  

    params = {
        "locations": location["id"],
        "business_area": business_area["id"],
        "per_page": 50,
        "page": current_page,
    }

    with request_semaphore:  
        with requests.Session() as session:
            try:
                while True:  
                    data = fetch_vacancies_with_session(session, url, headers, params, proxy)
                    if data:
                        vacancies = data.get("vacancies", [])
                        all_vacancies.extend(vacancies)  # Collect the fetched vacancies
                        export_vacancies_to_csv(vacancies, filename="dv.csv")  # Export to CSV

                        # Check meta data for pagination information
                        meta = data.get("meta", {})
                        total_pages = meta.get("pages", 1)

                        if current_page >= total_pages:
                            break

                        current_page += 1
                        params["page"] = current_page  

            except Exception as e:
                print(f"Error fetching vacancies for location {location['name']} and business area {business_area['name']} with proxy {proxy}: {e}")
    
    return all_vacancies


def process_vacancies_multiprocessing(access_token, locations, business_areas, url, headers, processes=2):
    # Use a set to track unique combinations of location and business area
    unique_tasks = set()
    tasks = []  # Initialize the tasks list

    # Generate tasks ensuring uniqueness
    for location in locations:
        for business_area in business_areas:
            task_key = (location['id'], business_area['id'])  # Unique identifier for the task
            if task_key not in unique_tasks:
                unique_tasks.add(task_key)
                tasks.append((access_token, location, business_area, url, headers, proxies[len(unique_tasks) % len(proxies)]))

    all_vacancies = []

    # Use multiprocessing pool to process tasks
    with mp.Pool(processes=processes) as pool:
        results = pool.map(fetch_vacancies_worker, tasks)

    # Collect all results from the workers
    for result in results:
        if result:
            all_vacancies.extend(result)

    print(f"Total vacancies fetched: {len(all_vacancies)}")
    return all_vacancies

def get_access_token():
    url = "https://api.avito.ru/token"
    params = {
        "client_id": "oNwJeKq7XxKdbMisWAw7",
        "client_secret": "wsFicRL8q2lmfPnYMcevaVyf9kwnAV7QNdU-Jjtd",
        "grant_type": "client_credentials"
    }
    
    try:
        response = requests.post(url, params=params)
        if response.status_code == 200:
            token_data = response.json()
            access_token = token_data.get("access_token")
            print(f"Access Token: {access_token}")
            return access_token
        else:
            print(f"Failed to get access token. Status Code: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None

def load_locations_from_xml(xml_file):
    locations = []
    seen_ids = set()  

    try: 
        # Parse the XML
        tree = ET.parse(xml_file)
        root = tree.getroot()

        # Iterate over each region in the XML
        for region in root.findall(".//Region"):
            region_id = region.get("Id")
            region_name = region.get("Name")
            
            # Ensure the region has a unique ID
            if region_id and region_name and region_id not in seen_ids:
                locations.append({"id": region_id, "name": region_name, "type": "Region"})
                seen_ids.add(region_id)

                # Process Districts in the region
                for district in region.findall(".//District"):
                    district_id = district.get("Id")
                    district_name = district.get("Name")
                    if district_id and district_name and district_id not in seen_ids:
                        locations.append({
                            "id": district_id, 
                            "name": f"{district_name} ({region_name})", 
                            "type": "District"
                        })
                        seen_ids.add(district_id)

                # Process Subways in the region
                for subway in region.findall(".//Subway"):
                    subway_id = subway.get("Id")
                    subway_name = subway.get("Name")
                    if subway_id and subway_name and subway_id not in seen_ids:
                        locations.append({
                            "id": subway_id, 
                            "name": f"{subway_name} ({region_name})", 
                            "type": "Subway"
                        })
                        seen_ids.add(subway_id)

                for city in region.findall(".//City"):
                    city_id = city.get("Id")
                    city_name = city.get("Name")
                    if city_id and city_name and city_id not in seen_ids:
                        locations.append({
                            "id": city_id, 
                            "name": f"{city_name} ({region_name})", 
                            "type": "City"
                        })
                        seen_ids.add(city_id)

                        for district in city.findall(".//District"):
                            district_id = district.get("Id")
                            district_name = district.get("Name")
                            if district_id and district_name and district_id not in seen_ids:
                                locations.append({
                                    "id": district_id, 
                                    "name": f"{district_name} ({city_name})", 
                                    "type": "District"
                                })
                                seen_ids.add(district_id)

                        for subway in city.findall(".//Subway"):
                            subway_id = subway.get("Id")
                            subway_name = subway.get("Name")
                            if subway_id and subway_name and subway_id not in seen_ids:
                                locations.append({
                                    "id": subway_id, 
                                    "name": f"{subway_name} ({city_name})", 
                                    "type": "Subway"
                                })
                                seen_ids.add(subway_id)

        print(f"Loaded {len(locations)} unique locations from XML.")
        return locations
    except ET.ParseError as e:
        print(f"Error parsing XML file: {e}")
        return []
    except FileNotFoundError:
        print(f"File not found: {xml_file}")
        return []
    except Exception as e:
        print(f"An error occurred while loading locations from XML: {e}")
        return []


def load_business_areas_from_xml(xml_file):
    business_areas = []  
    try:
        tree = ET.parse(xml_file)  
        root = tree.getroot()      
        for business_area in root.findall(".//BusinessArea"):
            area_id_elem = business_area.find("id")  
            area_name_elem = business_area.find("name")  

            if area_id_elem is not None and area_name_elem is not None:
                area_id = area_id_elem.text
                area_name = area_name_elem.text
                business_areas.append({"id": area_id, "name": area_name})

        print(f"Loaded {len(business_areas)} business areas from XML.")
        return business_areas
    except ET.ParseError as e:
        print(f"Error parsing XML file: {e}")
        return []
    except FileNotFoundError:
        print(f"File not found: {xml_file}")
        return []
    except Exception as e:
        print(f"An error occurred while loading business areas from XML: {e}")
        return []

if __name__ == "__main__":
    access_token = get_access_token()
    
    if access_token:
        locations = load_locations_from_xml("catalog-location.xml")
        business_areas = load_business_areas_from_xml("catalog-business-area.xml")

        url = "https://api.avito.ru/job/v2/vacancies"
        headers = {
            "Authorization": f"Bearer {access_token}"
        }

        all_vacancies = process_vacancies_multiprocessing(
            access_token,
            locations=locations,
            business_areas=business_areas,
            url=url,
            headers=headers,
            processes=2  
        )

        print(f"Total {len(all_vacancies)} vacancies fetched.")
