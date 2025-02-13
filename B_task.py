import json
import requests
import git
import sqlite3
import duckdb
from bs4 import BeautifulSoup
from PIL import Image
import speech_recognition as sr
import markdown
import csv
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

# B3. Fetch data from an API and save it
def fetch_and_save_api_data(api_url: str, output_file: str):
    response = requests.get(api_url)
    response.raise_for_status()
    with open(output_file, 'w') as f:
        json.dump(response.json(), f)
    return f"B3 Completed: API data saved to {output_file}"

# B4. Clone a git repo and make a commit
def clone_and_commit(repo_url: str, commit_message: str):
    repo_name = repo_url.split('/')[-1].split('.')[0]
    repo = git.Repo.clone_from(repo_url, repo_name)
    with open(f"{repo_name}/README.md", 'a') as f:
        f.write("\nUpdated by script")
    repo.git.add(update=True)
    repo.index.commit(commit_message)
    return f"B4 Completed: Cloned and committed to {repo_name}"

# B5. Run a SQL query on a SQLite or DuckDB database
def run_sql_query(db_file: str, query: str, db_type: str = 'sqlite'):
    if db_type == 'sqlite':
        conn = sqlite3.connect(db_file)
    elif db_type == 'duckdb':
        conn = duckdb.connect(db_file)
    else:
        raise ValueError("Unsupported database type")
    
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    conn.close()
    return f"B5 Completed: Query executed, {len(results)} rows returned"

# B6. Extract data from (i.e. scrape) a website
def scrape_website(url: str, output_file: str):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    data = {'title': soup.title.string, 'paragraphs': [p.text for p in soup.find_all('p')]}
    with open(output_file, 'w') as f:
        json.dump(data, f)
    return f"B6 Completed: Website data scraped to {output_file}"

# B7. Compress or resize an image
def process_image(input_file: str, output_file: str, action: str = 'compress', quality: int = 85, size: tuple = (800, 600)):
    with Image.open(input_file) as img:
        if action == 'compress':
            img.save(output_file, optimize=True, quality=quality)
        elif action == 'resize':
            img.thumbnail(size)
            img.save(output_file)
    return f"B7 Completed: Image {action}ed and saved to {output_file}"

# B8. Transcribe audio from an MP3 file
def transcribe_audio(audio_file: str, output_file: str):
    r = sr.Recognizer()
    with sr.AudioFile(audio_file) as source:
        audio = r.record(source)
    text = r.recognize_google(audio)
    with open(output_file, 'w') as f:
        f.write(text)
    return f"B8 Completed: Audio transcribed to {output_file}"

# B9. Convert Markdown to HTML
def markdown_to_html(input_file: str, output_file: str):
    with open(input_file, 'r') as f:
        md_content = f.read()
    html_content = markdown.markdown(md_content)
    with open(output_file, 'w') as f:
        f.write(html_content)
    return f"B9 Completed: Markdown converted to HTML in {output_file}"

