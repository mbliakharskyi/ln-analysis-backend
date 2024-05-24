# app.py
from flask import Flask, request, jsonify, send_file, after_this_request
from flask_cors import CORS
import pandas as pd
import requests
import os
import aiohttp
import asyncio
from io import BytesIO
import tempfile

UPLOAD_FOLDER = 'uploads'
PILOTERR_API_URL = 'https://piloterr.com/api/v2/linkedin/profile/info'
PILOTERR_API_KEY = os.environ.get('API_KEY')
app = Flask(__name__)
CORS(app)

if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

async def fetch_profile_data(session, url):
    headers = {
        'Content-Type': 'application/json',
        'x-api-key': PILOTERR_API_KEY
    }
    params = {'query': url}
    async with session.get(PILOTERR_API_URL, headers=headers, params=params) as response:
        return await response.json()
    
def calculate_score(profile):
    score = 0
    # Example scoring logic
    if profile.get('photo_url'):
        score += 5
    if profile.get('background_url'):
        score += 5
    if profile.get('headline'):
        score += 5
    if profile.get('summary'):
        score += 5
    if profile.get('experiences'):
        score += 5
    if profile.get('educations'):
        score += 5
    if profile.get('languages'):
        score += 5
    if profile.get('activities'):
        score += 5
    if profile.get('articles'):
        score += min(len(profile['articles']), 20)
    score += min(profile.get('follower_count', 0) // 10000, 20)
    score += min(len(profile.get('recommendations', [])) * 2, 10)
    return min(score, 100)

async def process_profiles(file_path):
    # Read the Excel file
    data = pd.read_excel(file_path)
    # Rename columns
    data.columns = list(make_unique_columns(data.iloc[0]))
    data = data[1:]
    # Ensure all columns have consistent types
    data = data.astype(str).fillna('')
    # Extract "Person LinkedIn" column values
    linkedin_column = "Person LinkedIn"
    if linkedin_column in data.columns:
        linkedin_values = data[linkedin_column].tolist()
    else:
        linkedin_values = []
    linkedin_values = ["https://www.linkedin.com/in/weprice",]

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_profile_data(session, url) for url in linkedin_values]
        profiles = await asyncio.gather(*tasks)
        profiles_with_scores = []
        for profile_data in profiles:
            score = calculate_score(profile_data)
            profile_data['score'] = score
            profiles_with_scores.append(profile_data)
    return profiles_with_scores

def make_unique_columns(columns):
    seen = {}
    for item in columns:
        if item in seen:
            seen[item] += 1
            yield f"{item}_{seen[item]}"
        else:
            seen[item] = 0
            yield item
@app.route('/')
def home():
    return 'LinkedIn Analysis Tool Backend'

@app.route('/upload', methods=['POST'])
async def upload_file():
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400
    if file:
        file_path = os.path.join(UPLOAD_FOLDER, file.filename)
        file.save(file_path)
        try:
            profiles_with_scores = await process_profiles(file_path)

            # Create an Excel file in memory
            df = pd.DataFrame(profiles_with_scores)
            output = BytesIO()
            df.to_excel(output, index=False, engine='openpyxl')
            output.seek(0)

            # Store the Excel file in a temporary file
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx')
            with open(temp_file.name, 'wb') as f:
                f.write(output.getbuffer())

            return jsonify({
                'json_data': profiles_with_scores,
                'file_path': temp_file.name
            }), 200
        except Exception as e:
            print(f"Error processing file: {e}")
            return jsonify({'error': 'Error processing file', 'message': str(e)}), 500

@app.route('/download', methods=['GET'])
async def download_file():
    file_path = request.args.get('file_path')
    if not file_path or not os.path.exists(file_path):
        return jsonify({'error': 'File not found'}), 404

    @after_this_request
    def remove_file(response):
        try:
            os.remove(file_path)
        except Exception as e:
            print(f"Error removing file: {e}")
        return response

    return send_file(file_path, download_name='profiles_with_scores.xlsx', as_attachment=True)

if __name__ == '__main__':
    app.run(host="0.0.0.0", debug=True)