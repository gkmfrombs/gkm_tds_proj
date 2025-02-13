import os
import json
import subprocess
from urllib.parse import urlparse
import requests
import sqlite3
from PIL import Image
import os
from pathlib import Path
import cv2
import numpy as np
import pytesseract
import requests
from pathlib import Path
from datetime import datetime
from typing import List
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import PlainTextResponse
import numpy as np
from llm_utils import chat_completion, generate_embeddings  # Make sure that llm_utils defines chat_completion

app = FastAPI()

# Define a constant for the data directory used in some tasks.
DATA_DIR = Path("/data")



# Enable CORS
from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allow all methods
    allow_headers=["*"],  # Allow all headers
)







# ----- Task Function Definitions -----
##############################################




DATA_DIR = f"./data"  # Example data directory

def install_uv_and_run_datagen(user_email: str = None,url: str = None):
    """
    Install 'uv', download and execute datagen.py.
    """
    print(f"User email: {user_email}")
    if not user_email:
        user_email = "22f3001315@ds.study.iitm.ac.in"
    print(f"URL: {url}")
    if not url:
        url = "https://raw.githubusercontent.com/sanand0/tools-in-data-science-public/tds-2025-01/project-1/datagen.py"
    
    file_name = os.path.basename(urlparse(url).path)
    file_path = os.path.join(file_name)

    # Ensure DATA_DIR exists

    Path(DATA_DIR).mkdir(parents=True, exist_ok=True)

    # Install 'uv' if not already installed
    try:
        subprocess.run(["uv", "--version"], check=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        subprocess.run(["python", "-m", "pip", "install", "uv"], check=True)

    # Install 'requests' if needed
    # subprocess.run(["python", "-m", "pip", "install", "requests"], check=True)

    # Download datagen.py using requests
    # url = "https://raw.githubusercontent.com/sanand0/tools-in-data-science-public/tds-2025-01/project-1/datagen.py"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        file_path = file_path
        print(f"Downloaded datagen.py to {file_path}")
        with open(file_path, "wb") as file:
            file.write(response.content)
        print(f"datagen.py successfully downloaded to {file_path}")
    except requests.RequestException as e:
        raise ValueError(f"Failed to download datagen.py: {e}")

    # Run datagen.py using uv
    try:
        subprocess.run(["uv", "run", str(file_path), user_email, "--root", DATA_DIR], check=True)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Failed to execute datagen.py: {e}")

    return f"A1 Completed: datagen.py executed with email {user_email}"




def format_markdown_file(file_path: str , version: str = "3.4.2"):
    """
    Format the specified Markdown file using Prettier and handle
    additional manual cleanup for whitespace and indentation issues.
    """
    file_path = f".{file_path}"
    # file_path = Path(file_path)
    version = version
    print("version==", version)
    if not Path(file_path).exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    # Ensure Prettier is installed
    try:
        subprocess.run(["npx", "prettier", "--version"], check=True, shell=True)
    except subprocess.CalledProcessError:
        subprocess.run(["npm", "install", "-g", f"prettier@{version}"], check=True, shell=True)

    # Read the file content before formatting
    try:
        with open(file_path, "r") as file:
            content = file.read()
    except UnicodeDecodeError:
        raise ValueError("Failed to read the file. It might not be a valid text file.")
    

    # # Perform additional manual cleanup
    # cleaned_content = "\n".join(
    #     line.rstrip()  # Remove trailing spaces
    #     for line in content.strip().split("\n")
    # )

    # Format using Prettier
    try:
        subprocess.run(
            ["npx", "prettier", "--write", Path(file_path)],
            input=content,
            capture_output=True,
            text=True,
            check=True,
            shell=True,
        )
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Failed to format the file: {e}")
    # Write back the formatted result
    
    return f"A2 Completed: {file_path} formatted successfully."
    



def count_days(input_file: str, output_file: str, weekday_name: str):
    weekday_name = weekday_name
    input_file = f".{input_file}"
    output_file = f".{output_file}"
    # Mapping weekday names to weekday numbers
    weekday_map = {
        "Monday": 0, "Tuesday": 1, "Wednesday": 2,
        "Thursday": 3, "Friday": 4, "Saturday": 5, "Sunday": 6
    }

    if weekday_name not in weekday_map:
        raise ValueError(f"Invalid weekday name: {weekday_name}")

    target_weekday = weekday_map[weekday_name]

    date_formats = [
        "%b %d, %Y",          # Jun 02, 2000
        "%d-%b-%Y",           # 18-Jan-2019
        "%Y-%m-%d",           # 2019-01-18
        "%d/%m/%Y",           # 18/01/2019
        "%m/%d/%Y",           # 01/18/2019
        "%Y/%m/%d %H:%M:%S"   # 2013/08/29 14:34:40
    ]
    
    print("input_file===",type(input_file),input_file)
    if not os.path.exists(input_file):
        raise ValueError(f"File {input_file} does not exist.")

    count = 0
    with open(input_file, "r") as f:
        
        for line in f:
            line = line.strip()
            if not line:
                continue

            for date_format in date_formats:
                try:
                    date = datetime.strptime(line, date_format)
                    if date.weekday() == target_weekday:
                        count += 1
                    break
                except ValueError:
                    continue
    
    # Write the count to the output file
    
    print("output_file===",output_file)
    with open(output_file, "w") as f:
        json.dump(count, f)

    return f"A3 Completed: {count} occurrences of {weekday_name} written to {output_file}"


def sort_contacts(input_file: str, output_file: str):
    """
    A4. Reads contacts from a JSON file, sorts them by last_name then first_name,
    and writes the sorted list to output_file.
    """
    input_file = f".{input_file}"
    if not os.path.exists(input_file):
        raise ValueError(f"File {input_file} does not exist.")
    with open(input_file, "r") as f:
        contacts = json.load(f)
    sorted_contacts = sorted(contacts, key=lambda c: (c.get("last_name", ""), c.get("first_name", "")))
    output_file=output_file[5:]
    with open(f"./data/{output_file}", "w") as f:
        json.dump(sorted_contacts, f)
    return f"A4 Completed: Sorted contacts stored in {output_file}"

def write_recent_logs(input_dir: str, output_file: str ):
    """
    A5. Write the first line of the 10 most recent .log files from a given directory
    to a specified output file, most recent first.
    """
    logs_dir = Path(f".{input_dir}")
    output_path = Path(f".{output_file}")

    if not logs_dir.exists() or not logs_dir.is_dir():
        raise ValueError(f"Invalid directory path: {logs_dir}")

    # Get all .log files sorted by modification time (most recent first)
    log_files = sorted(
        logs_dir.glob("*.log"),
        key=lambda f: f.stat().st_mtime,
        reverse=True
    )[:10]  # Get the 10 most recent files

    recent_lines = []
    for log_file in log_files:
        with log_file.open("r") as f:
            first_line = f.readline().strip()  # Read the first line of the file
            if first_line:  # Only add if the line is not empty
                recent_lines.append(first_line)

    # Ensure the parent directory for the output file exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Write the collected lines to the output file
    with output_path.open("w") as f:
        f.write("\n".join(recent_lines) + "\n")

    return f"A5 Completed: First lines of 10 most recent log files written to {output_file}"

def create_markdown_index(input_dir: str, output_file: str):
    """
    A6. Find all Markdown (.md) files in the given directory. For each file, 
    extract the first occurrence of an H1 title and create an index JSON file.

    Args:
        input_dir (str): Path to the directory containing Markdown files.
        output_file (str): Path to the output JSON file to store the index.
    """
    docs_dir = f".{input_dir}" # Make sure docs_dir is a Path object
    output_path = f".{output_file}"  # Ensure output_path is a Path object
    print(output_path)

    if not Path(docs_dir).exists():
        raise ValueError(f"Invalid directory path: {docs_dir}")

    index_data = {}

    # Traverse all .md files in the directory and subdirectories
    for md_file in Path(docs_dir).rglob("*.md"):
        with md_file.open("r", encoding="utf-8") as file:
            for line in file:
                # Extract the first H1 line starting with "# "
                if line.strip().startswith("# "):
                    title = line.strip()[2:].strip()  # Remove "# " and strip spaces
                    # Normalize the file path to match expected format (replace backslashes with forward slashes)
                    relative_filename = str(md_file.relative_to(docs_dir)).replace("\\", "/")
                    # Assign title to the file in the index
                    index_data[relative_filename] = title
                    break  # Only consider the first H1

   

    # Write index to JSON file
    with open(output_path,"w") as f:
        json.dump(index_data, f)

    return f"A6 Completed: Index file created at {output_file}"



def extract_email_sender(input_file: str, output_file: str):
    input_path = Path(f".{input_file}")
    output_path = Path(f".{output_file}")

    if not input_path.exists():
        raise ValueError(f"File {input_path} does not exist.")

    # Read email content
    with input_path.open("r", encoding="utf-8") as f:
        email_content = f.read()

    # Prompt LLM to extract the sender's email address
    prompt = f"Extract the sender's email address from the following message and nothing else:\n\n{email_content}"
    response = chat_completion(prompt)
    sender_email = response.get("choices", [])[0].get("message", {}).get("content", "").strip()

    # Write the email address to output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        f.write(sender_email)

    return f"A7 Completed: Email address extracted to {output_file}"









pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe" # 

def preprocess_image(input_path):
    """Enhance image for better OCR accuracy."""
    img = cv2.imread(str(input_path), cv2.IMREAD_GRAYSCALE)

    # Resize for clarity
    img = cv2.resize(img, None, fx=2, fy=2, interpolation=cv2.INTER_CUBIC)

    # Apply Gaussian blur
    img = cv2.GaussianBlur(img, (5, 5), 0)

    # Apply adaptive threshold
    img = cv2.adaptiveThreshold(img, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 11, 2)

    # Save processed image
    processed_path = str(input_path).replace(".png", "_processed.png")
    cv2.imwrite(processed_path, img)
    return processed_path

def extract_credit_card_number(input_image: str, output_file: str):
    """
    A8. Extract the credit card number from an image using OCR and refine it using the LLM.
    
    Args:
        input_image (str): Path to the input image file.
        output_file (str): Path to the output text file.
    """
    input_path = Path(f".{input_image}")
    output_path = Path(f".{output_file}")

    if not input_path.exists():
        raise ValueError(f"Image file {input_path} does not exist.")

    # Step 1: Preprocess Image
    processed_image = preprocess_image(input_path)
    # Step 2: Extract Text Using Tesseract
    raw_text = pytesseract.image_to_string(processed_image, config="--psm 6 -c tessedit_char_whitelist=0123456789")
    # Extract only digits
    extracted_number = "".join(filter(str.isdigit, raw_text))
    extracted_number=extracted_number[:16]
    try:
        # Write the card number to the output file
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            f.write(extracted_number)

        return f"A8 Completed: Credit card number extracted and written to {output_file}"
    except Exception as e:
        raise ValueError(f"Failed to process text with LLM: {str(e)}")

def find_most_similar_comments(input_file: str, output_file: str):
    """
    Given a file with a list of comments, find the most similar pair using embeddings and write them to an output file.
    """
    input_path = f".{input_file}"
    output_path = f".{output_file}"

    with open(input_path, "r") as f:
        comments = f.readlines()

    # Generate embeddings for the comments
    embeddings_response = generate_embeddings(comments)
    embeddings = [emb["embedding"] for emb in embeddings_response]

    # Compute the cosine similarity between all pairs of comments
    similarity_matrix = np.dot(embeddings, np.array(embeddings).T)

    # Mask the diagonal (self-similarity) to avoid selecting the same comment pair
    np.fill_diagonal(similarity_matrix, -np.inf)

    # Find the indices of the most similar pair of comments
    i, j = np.unravel_index(np.argmax(similarity_matrix), similarity_matrix.shape)

    # Get the most similar comments
    most_similar_comments = [comments[i].strip(), comments[j].strip()]

    # Write the most similar comments to the output file
    with open(output_path, "w") as f:
        f.write("\n".join(sorted(most_similar_comments)))
    return f"A9 Completed: Most similar comments written to {output_file}"



def calculate_gold_ticket_sales(db_file: str, output_file: str):
    db_path = f".{db_file}"
    output_path = f".{output_file}"

    if not Path(db_path).exists():
        raise ValueError(f"Database file {db_path} does not exist.")

    # Connect to SQLite database
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        query = """
        SELECT SUM(units * price) AS total_sales
        FROM tickets
        WHERE TRIM(LOWER(type)) = 'gold';

        """
        cursor.execute(query)
        result = cursor.fetchone()[0] or 0

    # Write the result to the output file
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open( output_path, "w", encoding="utf-8") as f:
        f.write(str(result))

    return f"A10 Completed: Total sales of 'Gold' tickets written to {output_file}"

######################################################################################


from B_task import *


#####################################################################################


# Mapping function names to actual functions.
task_functions = {
    "install_uv_and_run_datagen": install_uv_and_run_datagen,
    "format_markdown_file": format_markdown_file,
    "count_days": count_days,
    "sort_contacts": sort_contacts,
    "write_recent_logs": write_recent_logs,
    "create_markdown_index": create_markdown_index,
    "extract_email_sender": extract_email_sender,
    "extract_credit_card_number": extract_credit_card_number,
    "find_most_similar_comments": find_most_similar_comments,
    "calculate_gold_ticket_sales": calculate_gold_ticket_sales,
    # B_task functions ###############
    "fetch_and_save_api_data": fetch_and_save_api_data,
    "clone_and_commit": clone_and_commit,
    "run_sql_query": run_sql_query,
    "scrape_website": scrape_website,
    "process_image": process_image,
    "transcribe_audio": transcribe_audio,
    "markdown_to_html": markdown_to_html,

}

# ----- LLM Task Determination -----
#####################################################

def determine_task(task_description: str) -> dict:
    print(f"Received task description: {task_description}")
    
    prompt = f"""
You are an assistant that maps user instructions to system function calls.
Available functions:
1. install_uv_and_run_datagen(user_email, url): Install 'uv' and run datagen.py.
2. format_markdown_file(file_path , version): Format a markdown file using Prettier.
3. count_days(input_file, output_file ,weekday_name): Count the number of Weekdays from a date file and write the count.
4. sort_contacts(input_file, output_file): Sort a contacts JSON file by last name then first name.
5. write_recent_logs(input_dir, output_file): Write the last 10 lines of a log file to an output file.
6. create_markdown_index(input_dir, output_file): Create an index of Markdown files.
7. extract_email_sender(input_file, output_file): Extract the email sender from a file.
8. extract_credit_card_number(input_image, output_file): Extract the credit card number from an image.
9. find_most_similar_comments(input_file, output_file): Find the most similar comments in a file.
10. calculate_gold_ticket_sales(db_file, output_file): Calculate the total sales of 'Gold' tickets from a database.
11. fetch_and_save_api_data(api_url, output_file): Fetch data from an API and save it to a file.
12. clone_and_commit(repo_url, commit_message): Clone a repository and commit changes.
13. run_sql_query(db_file, query, output_file): Run an SQL query on a database and save the results.
14. scrape_website(url, output_file): Scrape a website and save the content.
15. process_image(input_image, output_file): Process an image and save the result.
16. transcribe_audio(input_audio, output_file): Transcribe an audio file.
17. markdown_to_html(input_file, output_file): Convert a Markdown file to HTML.


Given the following instruction: "{task_description}"

### Important Instructions:
1. Return **only** a valid JSON object with these keys:
   - `"function"`: Name of the function.
   - `"params"`: Function parameters.
2. No explanations, instructions, or markdown formatting.
"""
    
    response = chat_completion(prompt)
    content = response.get("choices", [])[0].get("message", {}).get("content", "").strip()
    
    # Clean content for potential formatting markers
    if content.startswith("```json"):
        content = content.lstrip("```json").rstrip("```").strip()

    try:
        return json.loads(content)
    except json.JSONDecodeError as e:
        print(f"LLM response parsing failed: {content}")
        raise ValueError(f"Failed to parse LLM response: {content}. Error: {str(e)}")
    



# ----- API Endpoints -----
######################################################

@app.post("/run")
def run_task(task: str = Query(..., description="Task description in plain English")):
    """
    Receives a task description and passes it to the LLM to determine
    which function to run and with what parameters.
    If the LLM does not return a proper mapping or if the mapped function fails,
    an appropriate HTTP error is raised.
    """
    try:
        task_info = determine_task(task)
        print("task_info==", task_info)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"LLM processing error: {str(e)}")
    
    func_name = task_info.get("function")
    params = task_info.get("params", {})
    # Ensure params is a dictionary
    if not isinstance(params, dict):
        params = {"user_email": params}
    if func_name not in task_functions:
        raise HTTPException(status_code=400, detail=f"Function '{func_name}' not recognized.")

    try:
        result = task_functions[func_name](**params)
        return {"status": "success", "result": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Task execution error: {str(e)}")

@app.get("/read", response_class=PlainTextResponse)
def read_file(path: str = Query(..., description="Path to the file to read")):
    """
    Returns the content of the specified file.
    If the file does not exist, a 404 error is raised.
    """
    # B1:Data outside /data is never accessed
    if not path.startswith("/data"):
        raise HTTPException(status_code=403, detail="Access to external files is not allowed")
    path = f".{path}"
    print(f"Reading file: {path}")
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="File not found")
    try:
        with open(path, "r") as f:
            content = f.read()
        return content
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)