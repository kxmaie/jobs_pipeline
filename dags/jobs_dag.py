from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import pandas as pd
import os
from airflow import DAG
from datetime import datetime, timedelta
import pendulum
from airflow.operators.python import PythonOperator
import snowflake.connector
from airflow.operators.bash import BashOperator

project_path='/usr/local/airflow/dags/dbt_wuzzuf'
def fetch_data():
    start = 0
    job_links = []     # هنا هخزن بيانات الوظايف الأساسية
    job_data = []      # هنا هخزن البيانات النهائية بعد فتح اللينك
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_experimental_option('excludeSwitches', ['enable-logging'])

    driver = webdriver.Remote(
    command_executor="http://selenium-grid:4444/wd/hub", 
    options=options
  
)

    # --- المرحلة الأولى: تجميع اللينكات والبيانات الأساسية ---
    while True:
        if start > 1:   # عدد الصفحات اللي يجيبها
            print("It's done collecting links..........")
            break

        url = f"https://wuzzuf.net/search/jobs/?q=Data+Engineer&start={start}"
        driver.get(url)
        time.sleep(3)

        job_titles = driver.find_elements(By.CLASS_NAME, "css-pkv5jc")

        for job in job_titles:
            try:
                name_of_job = job.find_element(By.CLASS_NAME, "css-193uk2c").text
            except:
                name_of_job = None

            try:
                name_of_company = job.find_element(By.CLASS_NAME, "css-ipsyv7").text
            except:
                name_of_company = None

            try:
                location = job.find_element(By.CLASS_NAME, "css-16x61xq").text
            except:
                location = None

            # وقت الإعلان
            try:
                job_announcement_time = job.find_element(By.CLASS_NAME, "css-eg55jf").text
            except:
                try:
                    job_announcement_time = job.find_element(By.CLASS_NAME, "css-1jldrig").text
                except:
                    job_announcement_time = None

            # نوع الوظيفة وطريقة العمل
            job_type, work_mode = None, None
            info_for_job = job.find_elements(By.CLASS_NAME, "css-5jhz9n")
            for info in info_for_job:
                try:
                    job_type = info.find_element(By.CSS_SELECTOR, ".css-uc9rga.eoyjyou0").text
                except:
                    pass
                try:
                    work_mode = info.find_element(By.CSS_SELECTOR, ".css-uofntu.eoyjyou0").text
                except:
                    pass

            try:
                skills = job.find_element(By.CLASS_NAME, "css-1rhj4yg").text
            except:
                skills = None

            # لينك الوظيفة
            job_link_element = job.find_element(By.CLASS_NAME, "css-193uk2c")
            link_element = job_link_element.find_element(By.TAG_NAME, "a")
            job_link = link_element.get_attribute("href")

            job_links.append({
                "name": name_of_job,
                "company": name_of_company,
                "location": location,
                "announcement_time": job_announcement_time,
                "jobtype": job_type,
                "workmode": work_mode,
                "skills": skills,
                "link": job_link
            })

        start += 1

    print(f"Collected {len(job_links)} jobs links.")

    # --- المرحلة الثانية: الدخول على كل لينك وجمع التفاصيل ---
    for job in job_links:
        driver.get(job["link"])
        time.sleep(2)

        # مرتب الوظيفة
        details = driver.find_elements(By.CLASS_NAME, "css-1ajx53j")
        job_salary = None
        for detail in details:
            text = detail.text.strip()
            if "Salary:" in text:
                job_salary = text.split("Salary:")[1].strip()

        try:
            job_skills = driver.find_element(By.CLASS_NAME, "css-qe7mba").text
        except:
            job_skills = None

        try:
            job_description = driver.find_element(By.CLASS_NAME, "css-n7fcne").text
        except:
            job_description = None

        try:
            job_requirements = driver.find_element(By.CLASS_NAME, "css-1lqavbg").text
        except:
            job_requirements = None

        job_data.append({
            "nameofjob": job["name"],
            "nameofcompany": job["company"],
            "location": job["location"],
            "jobannouncementtime": job["announcement_time"],
            "jobtype": job["jobtype"],
            "workmode": job["workmode"],
            "skills": job["skills"],
            "joblink": job["link"],
            "Salary": job_salary,
            "job_skills": job_skills,
            "description": job_description,
            "requirements": job_requirements
        })

    # --- تخزين البيانات في CSV ---
    file_path = '/usr/local/airflow/include/jobs.csv'
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    new_data = pd.DataFrame(job_data)

    if os.path.exists(file_path):
        old_data = pd.read_csv(file_path)
        combine_data = pd.concat([old_data, new_data], ignore_index=True)
    else:
        combine_data = new_data

    combine_data.to_csv(file_path, encoding='utf-8-sig', index=False)
    print(f"Saved {len(combine_data)} jobs to {file_path}")

    driver.quit()

# الاتصال ب snowflake
def load_data_to_snowflake():
    conn=snowflake.connector.connect(
        user="momen",
        password="AsAmomen12345@@",
        account="bc45973.me-central2.gcp",
        warehouse="JOBS_WH",
        database="JOBS_DB",
        schema="RAW",
        role="ACCOUNTADMIN"
    )
    css=conn.cursor()
    # رفع الملف على الاستيدج فى snowflake
    try:
        css.execute("PUT file:///usr/local/airflow/include/jobs.csv @JOBS_STG OVERWRITE=TRUE")
        # نقل البيانات من الملف الى الجدول 
        css.execute(''' COPY INTO RAW.JOBS 
                        FROM @JOBS_STG/jobs.csv 
                        FILE_FORMAT=(TYPE=CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1) ''')
    except Exception as e:
        print("حدث خطا اثناء نقل البيانات الى snowflake",e)
    finally:
        css.close()
        conn.close()




local_tz = 'Africa/Cairo'


with DAG(
    dag_id="wazzuf_dag",
    description="this dag is to fetch data of job and load in snowflake",
    start_date=pendulum.datetime(2025, 8, 15, tz=local_tz),
    schedule="0 0 * * *",
    catchup=False,
    dagrun_timeout=timedelta(minutes=45)
) as dag:

    task_of_fetch_data = PythonOperator(
        task_id="fetch_data",
        python_callable=fetch_data
    )

    task_of_load_data_to_snowflake=PythonOperator(
        task_id="load_data_to_snowflake",
        python_callable=load_data_to_snowflake
    )

    task_of_run_dbt=BashOperator(
        task_id="run_dbt",
        bash_command=f"cd {project_path} && dbt run --profiles-dir {project_path}"
    )

task_of_fetch_data>>task_of_load_data_to_snowflake>>task_of_run_dbt
