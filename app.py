from flask import Flask, render_template, request, send_file
import pandas as pd
import os

app = Flask(__name__)
UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/process', methods=['POST'])
def process():
    total_file = request.files['total_file']
    present_file = request.files['present_file']

    total_path = os.path.join(UPLOAD_FOLDER, 'total.csv')
    present_path = os.path.join(UPLOAD_FOLDER, 'present.csv')

    total_file.save(total_path)
    present_file.save(present_path)

    # Load CSVs
    total_df = pd.read_csv(total_path)
    present_df = pd.read_csv(present_path)

    # Normalize names
    total_df['StudentName'] = total_df['StudentName'].str.strip().str.lower()
    present_df['StudentName'] = present_df['StudentName'].str.strip().str.lower()

    # Get absentees
    absentees_df = total_df[~total_df['StudentName'].isin(present_df['StudentName'])]
    absentees_df.to_csv('absentees.csv', index=False)

    return render_template('index.html', absentees=True)

@app.route('/download')
def download():
    return send_file('absentees.csv', as_attachment=True)

if __name__ == '__main__':
    app.run(debug=True)
