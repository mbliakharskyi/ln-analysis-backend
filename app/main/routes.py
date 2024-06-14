from flask import request, jsonify, send_file, after_this_request, current_app as app
import pandas as pd
import os
import aiohttp
import asyncio
from io import BytesIO
import tempfile
import math
from .utils import fetch_profile_data, process_profiles

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
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
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
