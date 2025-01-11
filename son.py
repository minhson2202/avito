import requests
import time
import pandas as pd
import xml.etree.ElementTree as ET
import multiprocessing as mp
import os

proxies = [f'http://92.63.77.{ip}:3139' for ip in range(130, 230)]

def check_rate_limit(response):
    remaining = int(response.headers.get('X-RateLimit-Remaining', 1))
    reset_time = int(response.headers.get('X-RateLimit-Reset', time.time()))
    
    if remaining == 0:
        wait_time = reset_time - time.time() + 1
        print(f"Rate limit reached. Sleeping for {wait_time} seconds...")
        time.sleep(wait_time)

def fetch_vacancies_with_session(session, url, headers, params, proxy=None):
    print("Making API request...")
    proxy_dict = {"http": proxy, "https": proxy} if proxy else None
    response = session.get(url, headers=headers, params=params, proxies=proxy_dict, timeout=10)
    check_rate_limit(response)

    if response.status_code == 200:
        return response.json()
    elif response.status_code == 429:
        # Rate limit hit: Handle retry-after
        retry_after = int(response.headers.get("Retry-After", 5))  # Default wait 5 seconds if header is missing
        print(f"Rate limit exceeded (429). Retrying after {retry_after} seconds...")
        time.sleep(retry_after)
    else:
        print(f"Failed request. Status Code: {response.status_code}")
        return None


def export_vacancies_to_csv(vacancies, filename="data2.csv"):
    df = pd.DataFrame(vacancies)

    file_exists = os.path.isfile(filename)

    df.to_csv(
        filename,
        mode='a',  
        header=not file_exists,  
        index=False,  
        encoding='utf-8'  
    )

    print(f"Exported {len(vacancies)} vacancies to {filename}")

def fetch_vacancies_worker(task):
    access_token, location, business_area, url, headers, proxy = task
    params = {
        "locations": location["id"],
        "business_area": business_area["id"],
        "per_page": 50,
    }
    
    with requests.Session() as session:
        try:
            data = fetch_vacancies_with_session(session, url, headers, params, proxy)
            if data:
                vacancies = data.get("vacancies", [])
                export_vacancies_to_csv(vacancies, filename="data2.csv")
                return vacancies
        except Exception as e:
            print(f"Error fetching vacancies for location {location['name']} and business area {business_area['name']} with proxy {proxy}: {e}")
    return []

def process_vacancies_multiprocessing(access_token, locations, business_areas, url, headers, processes=50):

    tasks = [
        (access_token, location, business_area, url, headers, proxies[i % len(proxies)])
        for i, (location, business_area) in enumerate(
            [(location, business_area) for location in locations for business_area in business_areas]
        )
    ]

    all_vacancies = []

    with mp.Pool(processes=processes) as pool:
        results = pool.map(fetch_vacancies_worker, tasks)

    for result in results:
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
    try: 
        tree = ET.parse(xml_file)
        root = tree.getroot()
    
        for region in root.findall(".//Region"):
            location_id = region.get("Id")
            location_name = region.get("Name")
            if location_id and location_name:
                locations.append({"id": location_id, "name": location_name})

                for district in region.findall(".//District"):
                    district_id = district.get("Id")
                    district_name = district.get("Name")
                    if district_id and district_name:
                        locations.append({"id": district_id, "name": f"{district_name} ({location_name})", "type": "District"})

                for subway in region.findall(".//Subway"):
                    subway_id = subway.get("Id")
                    subway_name = subway.get("Name")
                    if subway_id and subway_name:
                        locations.append({"id": subway_id, "name": f"{subway_name} ({location_name})", "type": "Subway"})

                for city in region.findall(".//City"):
                    city_id = city.get("Id")
                    city_name = city.get("Name")
                    if city_id and city_name:
                        locations.append({"id": city_id, "name": f"{city_name} ({location_name})", "type": "City"})

                        for district in city.findall(".//District"):
                            district_id = district.get("Id")
                            district_name = district.get("Name")
                            if district_id and district_name:
                                locations.append({"id": district_id, "name": f"{district_name} ({city_name})", "type": "District"})

                        for subway in city.findall(".//Subway"):
                            subway_id = subway.get("Id")
                            subway_name = subway.get("Name")
                            if subway_id and subway_name:
                                locations.append({"id": subway_id, "name": f"{subway_name} ({city_name})", "type": "Subway"})
                                
        print(f"Loaded {len(locations)} locations from XML.")
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
            processes=10
        )

        print(f"Total {len(all_vacancies)} vacancies fetched.")
    
