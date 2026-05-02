from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import yfinance as yf
import feedparser
import psycopg2
import pandas as pd
from email.utils import parsedate_to_datetime

DB_CONFIG = {
    "host": "postgres",
    "database": "projekipbd_db",
    "user": "kelompok6",
    "password": "ipbdkelompok6"
}

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

# ETL DATA SAHAM 
def etl_harga_saham():
    saham_pilihan = ['BBCA.JK', 'BBRI.JK', 'BREN.JK', 'TLKM.JK', 'ASII.JK']
    conn = get_db_connection()
    cursor = conn.cursor()

    for simbol in saham_pilihan:
        print(f"Mengambil data saham: {simbol}")
        saham = yf.Ticker(simbol)
        df = saham.history(period="1d")
        
        if df.empty:
            print(f"Data {simbol} kosong, mungkin bursa libur.")
            continue 
            
        df = df.reset_index()
        for index, row in df.iterrows():
            tanggal = row['Date'].strftime('%Y-%m-%d')
            
            harga_buka = round(float(row['Open']), 2)
            harga_tertinggi = round(float(row['High']), 2)
            harga_terendah = round(float(row['Low']), 2)
            harga_tutup = round(float(row['Close']), 2)
            volume = int(row['Volume'])

            query = """
                INSERT INTO harga_saham (simbol_saham, tanggal, harga_buka, harga_tertinggi, harga_terendah, harga_tutup, volume) 
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (simbol_saham, tanggal) DO NOTHING;
            """
            cursor.execute(query, (simbol, tanggal, harga_buka, harga_tertinggi, harga_terendah, harga_tutup, volume))
            
    conn.commit()
    cursor.close()
    conn.close()
    print("ETL Saham Selesai.")

# ETL BERITA EKONOMI 
def etl_berita_keuangan():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    rss_url = 'https://news.google.com/rss/search?q="IHSG"+OR+"Saham"&hl=id&gl=ID&ceid=ID:id'
    feed = feedparser.parse(rss_url)
    
    print(f"Mengambil berita dari RSS. Ditemukan: {len(feed.entries)} berita.")

    for entry in feed.entries[:15]:
        judul = entry.title
        link = entry.link
        tanggal_publish = parsedate_to_datetime(entry.published)
        
        query = """
            INSERT INTO berita_keuangan (tanggal_publish, judul, link_url, sumber) 
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (link_url) DO NOTHING;
        """
        cursor.execute(query, (tanggal_publish, judul, link, "Google News"))
        
    conn.commit()
    cursor.close()
    conn.close()
    print("ETL Berita Selesai.")

default_args = {
    'owner': 'kelompok6',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dag_finance_kelompok6',
    default_args=default_args,
    start_date=datetime(2024, 1, 1), 
    schedule_interval='0 2 * * *',   
    catchup=False,
    tags=['projek_ipbd', 'kelompok6']
) as dag:

    task_saham = PythonOperator(
        task_id='etl_saham_yfinance',
        python_callable=etl_harga_saham
    )

    task_berita = PythonOperator(
        task_id='etl_berita_rss',
        python_callable=etl_berita_keuangan
    )

    task_saham >> task_berita