import asyncio
import aiohttp
import math
import os
import pandas as pd

API_KEY = os.environ.get('API_KEY')
PILOTERR_API_URL = 'https://piloterr.com/api/v2/linkedin/profile/info'
RATE_LIMIT = 7  # requests per second
REQUEST_INTERVAL = 1 / RATE_LIMIT  # interval between requests

async def fetch_profile_data(session, url, semaphore):
    headers = {
        'Content-Type': 'application/json',
        'x-api-key': API_KEY
    }
    params = {'query': url}

    # Check if the URL is valid
    if url is None or isinstance(url, float) and math.isnan(url) or url.strip() == "" or url == "nan":
        return {'error': 'URL is blank or invalid', 'url': url}
    print('url:', url)
    async with semaphore:
        await asyncio.sleep(REQUEST_INTERVAL)  # Ensure the delay between requests
        for attempt in range(3):  # Retry logic
            try:
                async with session.get(PILOTERR_API_URL, headers=headers, params=params) as response:
                    if response.status == 200:
                        if response.content_type == 'application/json':
                            return await response.json()
                        else:
                            print(f"Unexpected content type: {response.content_type}")
                            return None
                    elif response.status == 502:
                        print(f"502 Bad Gateway for URL {url}")
                        return {'error': '502 Bad Gateway', 'url': url}
                    elif response.status == 404:
                        print(f"Profile not found for URL {url}")
                        return {'error': 'Profile not found', 'url': url}
                    else:
                        print(f"Error fetching profile data: HTTP {response.status} for URL {url}")
                        return {'error': 'HTTP error', 'status': response.status, 'url': url}
            except Exception as e:
                print(f"Exception during fetch (attempt {attempt + 1}): {e}")
            await asyncio.sleep(2 ** attempt)  # Exponential backoff
        return None
   
async def process_profiles(file_path):
    # Read the Excel file, skipping the first row if it's meant to be data
    data = pd.read_excel(file_path, header=None)
    print("First few rows of data (including header row):", data.head())

    # Assume the actual headers are in the first row of the data
    data.columns = data.iloc[0]
    data = data[1:]

    # Make column names unique if necessary
    data.columns = list(make_unique_columns(data.columns))
    print("Columns after setting unique names:", data.columns)

    # Ensure all columns have consistent types
    data = data.astype(str).fillna('')
    print("Data after conversion to string and filling NaN:", data.head())

    # Extract "Person LinkedIn" column values
    linkedin_column = "Person Linkedin"
    if linkedin_column in data.columns:
        linkedin_values = data[linkedin_column].tolist()
    else:
        linkedin_values = []
    semaphore = asyncio.Semaphore(RATE_LIMIT)  # Semaphore to control the rate limit

    async with aiohttp.ClientSession() as session:
        tasks = [(url, fetch_profile_data(session, url, semaphore)) for url in linkedin_values]
        results = await asyncio.gather(*[task[1] for task in tasks])
        profiles_with_scores = []
        for url, profile_data in zip(linkedin_values, results):
            if 'error' not in profile_data:
                score = calculate_score(profile_data)
                profile_data['score'] = score
                profile_data['url'] = url  # Attach URL to each profile
                profiles_with_scores.append(profile_data)
            else:
                profile_data['score'] = profile_data["error"]
                profiles_with_scores.append(profile_data)

                # Log the error or handle it according to your application's needs
                print(f"Error retrieving profile: {profile_data['error']} for URL {profile_data.get('url', 'Unknown')}")
    return profiles_with_scores

def calculate_score(profile):
    score = 0
    if profile.get('photo_url'):
        score += 10
    if profile.get('background_url'):
        score += 10
    if profile.get('headline'):
        score += 10
    if profile.get('summary'):
        score += 10
    if profile.get('articles'):
        score += min(len(profile['articles']) * 4, 20)
    score += min(profile.get('follower_count', 0) // 10000, 30)
    if profile.get('connection_count'):
        connection_count = profile['connection_count']
        if connection_count < 200:
            connection_score = 10
        elif connection_count < 500:
            connection_score = 20
        else:
            connection_score = 40 + ((connection_count - 500) // 100)
        score += min(connection_score, 50)
    return min(score, 100)

def make_unique_columns(columns):
    seen = {}
    for item in columns:
        if item in seen:
            seen[item] += 1
            yield f"{item}_{seen[item]}"
        else:
            seen[item] = 0
            yield item