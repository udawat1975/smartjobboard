import os
import json
import logging
import requests
import pyodbc
import azure.functions as func
from datetime import datetime

def main(mytimer: func.TimerRequest) -> None:
    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info("Job fetch function started.")

    # Load environment variables
    RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY")
    DB_SERVER = os.getenv("DB_SERVER")
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")

    conn_str = f"Driver={{ODBC Driver 17 for SQL Server}};Server=tcp:{DB_SERVER},1433;Database={DB_NAME};Uid={DB_USER};Pwd={DB_PASSWORD};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id, query, page, num_pages, date_posted FROM job_queries")
            search_queries = cursor.fetchall()

            for row in search_queries:
                query_id, query, page, num_pages, date_posted = row
                logging.info(f"🔍 Fetching: {query}, Page {page}, {num_pages} page(s)")

                params = {
                    "query": query,
                    "page": str(page),
                    "num_pages": str(num_pages),
                    "country": "us",
                    "date_posted": date_posted
                }

                headers = {
                    "x-rapidapi-host": "jsearch.p.rapidapi.com",
                    "x-rapidapi-key": RAPIDAPI_KEY
                }

                response = requests.get("https://jsearch.p.rapidapi.com/search", headers=headers, params=params)
                if response.status_code != 200:
                    logging.error(f"⚠️ Failed to fetch API for query: {query}")
                    continue

                try:
                    response_json = response.json()
                    logging.info(f"📦 Raw response: {json.dumps(response_json)[:1000]}")
                    jobs = response_json.get("data", [])
                    if not isinstance(jobs, list):
                        logging.error("⚠️ 'data' field is missing or not a list in API response")
                        continue
                except Exception as e:
                    logging.error(f"❌ Error parsing response: {str(e)}")
                    continue

                for job in jobs:
                    job_id = job.get("job_id")
                    if not job_id:
                        continue

                    cursor.execute("SELECT 1 FROM jobs WHERE job_id = ?", job_id)
                    if cursor.fetchone():
                        continue  # Skip duplicate

                    cursor.execute("""
                        INSERT INTO jobs (
                            job_id, job_title, employer_name, employer_logo, employer_website, job_publisher,
                            job_employment_type, job_apply_link, job_is_remote, job_posted_at, job_location,
                            job_city, job_state, job_country, job_latitude, job_longitude,
                            job_description, job_google_link, job_min_salary, job_max_salary,
                            job_salary_period, job_onet_soc, job_onet_job_zone
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, job_id,
                        job.get("job_title"),
                        job.get("employer_name"),
                        job.get("employer_logo"),
                        job.get("employer_website"),
                        job.get("job_publisher"),
                        job.get("job_employment_type"),
                        job.get("job_apply_link"),
                        1 if job.get("job_is_remote") else 0,
                        parse_date(job.get("job_posted_at_datetime_utc")),
                        job.get("job_location"),
                        job.get("job_city"),
                        job.get("job_state"),
                        job.get("job_country"),
                        job.get("job_latitude"),
                        job.get("job_longitude"),
                        job.get("job_description"),
                        job.get("job_google_link"),
                        job.get("job_min_salary"),
                        job.get("job_max_salary"),
                        job.get("job_salary_period"),
                        job.get("job_onet_soc"),
                        job.get("job_onet_job_zone")
                    )

                    # Insert benefits
                    benefits = job.get("job_benefits")
                    if isinstance(benefits, list):
                        for benefit in benefits:
                            cursor.execute("INSERT INTO job_benefits (job_id, benefit) VALUES (?, ?)", job_id, benefit)

                    # Insert apply options
                    apply_options = job.get("job_apply_options")
                    if isinstance(apply_options, list):
                        for option in apply_options:
                            cursor.execute("""
                                INSERT INTO job_apply_options (job_id, publisher, apply_link, is_direct)
                                VALUES (?, ?, ?, ?)
                            """, job_id,
                                option.get("publisher"),
                                option.get("apply_link"),
                                1 if option.get("is_direct") else 0
                            )

                    # Insert highlights
                    highlights = job.get("job_highlights")
                    if isinstance(highlights, dict):
                        for section_type, items in highlights.items():
                            if isinstance(items, list):
                                for content in items:
                                    cursor.execute("""
                                        INSERT INTO job_highlights (job_id, type, content)
                                        VALUES (?, ?, ?)
                                    """, job_id, section_type, content)

                conn.commit()
                logging.info(f"✅ Inserted {len(jobs)} job(s) for query: {query}")

    except Exception as e:
        logging.error(f"❌ Error: {str(e)}")


def parse_date(utc_string):
    try:
        return datetime.fromisoformat(utc_string.replace("Z", "+00:00"))
    except Exception:
        return None
    