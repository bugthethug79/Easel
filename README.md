# Easel

Easel is a Python script that pulls all of your Canvas coursework into a single JSON file. It shows what's due, what's missing, and what your teachers actually asked for, all in one place. You can read the file yourself or give it to an AI assistant to help plan your week and study.

## Requirements

- Python 3
- A Canvas account that lets students create API tokens (some districts turn this off)

## Setup

1. Download or clone this repository.
2. Install the dependencies:
   `pip install -r requirements.txt`
3. In Canvas, go to **Account → Settings → Approved Integrations** and click **+ New Access Token**. Copy the token somewhere safe, because Canvas only shows it once.
4. Open `.env` and fill in your values:
   CANVAS_TOKEN=your_token_here
   CANVAS_URL=https://yourschool.instructure.com
5. Run the script: `python main.py`
6. Easel writes `canvas_data.json` (and saves attachments to `attachments/`) in the project folder.

## What It Tracks

- Overdue, undated, and recently completed assignments, grouped by course
- Due dates and lock dates, converted to your local time
- Submission status, scores, late flags, and teacher comments
- Assignment descriptions as plain text
- Rubrics with criteria and point values
- Attached files, with text extracted from PDFs
- Module structure and which modules are unlocked
- Course grades where your teacher has posted them

## Security

Your Canvas token gives full access to your account, so treat it like a password.

- Never reveal your `.env`. Make sure it stays on your machine only.
- `canvas_data.json` includes your grades and teacher comments. Don't post it publicly.
- If a token leaks, delete it in Canvas right away and create a new one.
