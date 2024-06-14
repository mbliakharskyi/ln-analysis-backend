import asyncio
import aiohttp
import math
import os
import pandas as pd
import json  # Import json module

API_KEY = os.environ.get('API_KEY')
PILOTERR_API_URL = 'https://piloterr.com/api/v2/linkedin/profile/info'
RATE_LIMIT = 7  # requests per second
REQUEST_INTERVAL = 1 / RATE_LIMIT  # interval between requests
GOOGLE_SHEETS_WEBHOOK_URL = os.environ.get('GOOGLE_SHEETS_WEBHOOK_URL')

async def fetch_profile_data(session, url, semaphore):
    print("url:", url)
    headers = {
        'Content-Type': 'application/json',
        'x-api-key': API_KEY
    }
    params = {'query': url}

    if url is None or isinstance(url, float) and math.isnan(url) or url.strip() == "" or url == "nan":
        return {'error': 'URL is blank or invalid', 'url': url}
    async with semaphore:
        await asyncio.sleep(REQUEST_INTERVAL)  # Ensure the delay between requests
        for attempt in range(3):  # Retry logic
            try:
                async with session.get(PILOTERR_API_URL, headers=headers, params=params) as response:
                    if response.status == 200:
                        if response.content_type == 'application/json':
                            profile_data = await response.json()
                            profile_data['url'] = url  # Attach URL to the profile data
                            await send_to_google_sheets(profile_data)  # Send formatted profile data to Google Sheets
                            return profile_data
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

async def process_profiles(file_path):
    data = pd.read_excel(file_path, header=None)
    print("First few rows of data (including header row):", data.head())

    data.columns = data.iloc[0]
    data = data[1:]

    data.columns = list(make_unique_columns(data.columns))
    print("Columns after setting unique names:", data.columns)

    data = data.astype(str).fillna('')
    print("Data after conversion to string and filling NaN:", data.head())

    linkedin_column = "Person Linkedin"
    if linkedin_column in data.columns:
        linkedin_values = data[linkedin_column].tolist()
    else:
        linkedin_values = []
    semaphore = asyncio.Semaphore(RATE_LIMIT)

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
                print(f"Error retrieving profile: {profile_data['error']} for URL {profile_data.get('url', 'Unknown')}")
    return profiles_with_scores

async def send_to_google_sheets(profile_data):
    if 'error' not in profile_data:
        score = calculate_score(profile_data)
        profile_data['score'] = score
    else:
        profile_data['score'] = profile_data["error"]
        print(f"Error retrieving profile: {profile_data['error']} for URL {profile_data.get('url', 'Unknown')}")
    
    send_data = {
        "FullName": profile_data.get('full_name', 'N/A'),
        "LinkedIn URL": profile_data['url'],
        "Score": profile_data['score'],
    }
    
    async with aiohttp.ClientSession() as session:
        headers = {
            'Content-Type': 'application/json'
        }
        # Use json.dumps to ensure proper JSON formatting
        async with session.post(GOOGLE_SHEETS_WEBHOOK_URL, data=json.dumps(send_data), headers=headers) as response:
            if response.status != 200:
                print(f"Failed to send data to Google Sheets: {response.status}")
            else:
                print(f"Successfully sent data to Google Sheets for URL: {send_data['LinkedIn URL']}")
                print("send_data:", json.dumps(send_data))


def make_unique_columns(columns):
    seen = {}
    for item in columns:
        if item in seen:
            seen[item] += 1
            yield f"{item}_{seen[item]}"
        else:
            seen[item] = 0
            yield item