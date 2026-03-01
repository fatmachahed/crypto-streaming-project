import sqlite3
import os 

BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "..", "output")

def save_to_db(pdf_clean, agg_pdf, pred_df, anomaly_df):
    db_path = os.path.join(OUTPUT_DIR, "crypto.db")
    conn = sqlite3.connect(db_path)
    pdf_clean.to_sql("crypto_clean",     conn, if_exists="append", index=False)
    agg_pdf.toPandas().to_sql("crypto_agg", conn, if_exists="append", index=False)
    if not pred_df.empty:
        pred_df.to_sql("crypto_predictions", conn, if_exists="append", index=False)
    if not anomaly_df.empty:
        anomaly_df.to_sql("crypto_anomalies",  conn, if_exists="append", index=False)
    conn.close()
        

    