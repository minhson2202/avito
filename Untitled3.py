import asyncio
import aiohttp
import time
import xml.etree.ElementTree as ET
import csv

# Generate proxy list
proxies = [f'http://92.63.77.{ip}:3139' for ip in range(130, 230)]

# Function to fetch vacancies
async def fetch_vacancies(session, url, headers, params, proxy=None):
    proxy_dict = {"http": proxy, "https": proxy} if proxy else None
    try:
        async with session.get(url, headers=headers, params=params, proxy=proxy_dict, timeout=10) as response:
            if response.status == 200:
                return await response.json()
            elif response.status == 429:  # Rate limit exceeded
                retry_after = int(response.headers.get("Retry-After", 5))
                print(f"Rate limit exceeded. Retrying after {retry_after} seconds...")
                await asyncio.sleep(retry_after)
                return await fetch_vacancies(session, url, headers, params, proxy)
            else:
                print(f"Failed request. Status Code: {response.status}")
                return None
    except Exception as e:
        print(f"Error during API request with proxy {proxy}: {e}")
        return None

# Function to save vacancies to CSV
def export_vacancies_to_csv(vacancies, filename="data2.csv"):
    csv_header = ["id", "Title", "Profession", "Company", "City", "Address", "Link"]
    with open(filename, mode="w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(csv_header)
        
        for index, vacancy in enumerate(vacancies, start=1):
            title = vacancy.get("title", "N/A")
            profession = vacancy.get("profession", "N/A")
            company = vacancy.get("companyName", "N/A")
            city = vacancy.get("addressDetails", {}).get("city", "N/A")
            address = vacancy.get("addressDetails", {}).get("address", "N/A")
            link = vacancy.get("link", "N/A")
            writer.writerow([index, title, profession, company, city, address, link])

# Asynchronous function to fetch all vacancies
async def process_vacancies(access_token, locations, business_areas, url, headers):
    tasks = []
    vacancies = []

    async with aiohttp.ClientSession() as session:
        for location in locations:
            for business_area in business_areas:
                for proxy in proxies:
                    params = {
                        "locations": location["id"],
                        "business_area": business_area["id"],
                        "per_page": 50,
                    }
                    tasks.append(fetch_vacancies(session, url, headers, params, proxy))

        # Run all tasks concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Collect results
        for result in results:
            if isinstance(result, dict):  # Successful response
                vacancies.extend(result.get("vacancies", []))

    print(f"Total vacancies fetched: {len(vacancies)}")
    return vacancies

# Function to get access token
async def get_access_token():
    url = "https://api.avito.ru/token"
    params = {
        "client_id": "oNwJeKq7XxKdbMisWAw7",
        "client_secret": "wsFicRL8q2lmfPnYMcevaVyf9kwnAV7QNdU-Jjtd",
        "grant_type": "client_credentials",
    }
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(url, params=params) as response:
                if response.status == 200:
                    token_data = await response.json()
                    access_token = token_data.get("access_token")
                    print(f"Access Token: {access_token}")
                    return access_token
                else:
                    print(f"Failed to get access token. Status Code: {response.status}")
                    return None
        except Exception as e:
            print(f"Error fetching access token: {e}")
            return None

# Function to load locations from XML
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
        print(f"Loaded {len(locations)} locations from XML.")
        return locations
    except Exception as e:
        print(f"Error loading locations from XML: {e}")
        return []

# Function to load business areas from XML
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
    except Exception as e:
        print(f"Error loading business areas from XML: {e}")
        return []

# Main entry point
async def main():
    access_token = await get_access_token()
    
    if access_token:
        locations = load_locations_from_xml("catalog-location.xml")
        business_areas = load_business_areas_from_xml("catalog-business-area.xml")

        url = "https://api.avito.ru/job/v2/vacancies"
        headers = {
            "Authorization": f"Bearer {access_token}"
        }

        all_vacancies = await process_vacancies(
            access_token,
            locations=locations,
            business_areas=business_areas,
            url=url,
            headers=headers
        )

        export_vacancies_to_csv(all_vacancies)

# Run the async main function
if __name__ == "__main__":
    asyncio.run(main())
