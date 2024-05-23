# app.py
from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
import requests

app = Flask(__name__)
CORS(app)

def fetch_profile(url):
    # Assuming piloterr API provides an endpoint to fetch profile by URL
    api_url = "https://api.piloterr.com/profile"
    response = requests.post(api_url, json={"url": url})
    return response.json()

def calculate_score(profile):
    score = 0
    if profile.get('profile_picture'):
        score += 10
    if profile.get('headline'):
        score += 5
    if profile.get('summary'):
        score += 5
    if profile.get('work_experience'):
        score += 5
    if profile.get('education'):
        score += 5
    if profile.get('skills'):
        score += 5
    if profile.get('endorsements'):
        score += 5

    if profile.get('recent_posts'):
        score += min(len(profile['recent_posts']), 20)

    score += min(profile.get('followers', 0) // 10, 20)
    score += min(profile.get('recommendations', 0) * 2, 10)

    return min(score, 100)

@app.route('/')
def home():
    return 'LinkedIn Analysis Tool Backend'

@app.route('/score-profile', methods=['POST'])
def score_profile():
    profiles = request.json.get('profiles', [])
    scores = []
    for profile in profiles:
        score = calculate_score(profile)
        scores.append(score)
    return jsonify({'scores': scores})

if __name__ == '__main__':
    app.run(host="0.0.0.0", debug=False)