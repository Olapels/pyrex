import time
import json
from edgar import set_identity, get_filings
from kafka import KafkaProducer

# 1. Identity is required by SEC
set_identity("DataPipelineSupport support@example.com")

producer = KafkaProducer(
    bootstrap_servers=['pyrex-redpanda-1:29092'], # Use the service name and INTERNAL port
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_sec_data():
    # Fetch latest 10-K and 10-Q filings
    filings = get_filings(form=["10-K", "10-Q"]).head(10)
    
    for filing in filings:
        try:
            xbrl = filing.xbrl()
            if not xbrl: continue
            
            # Extract standard facts using edgartools high-level API
            income_stmt = xbrl.income_statement()
            revenue = income_stmt.get_concept_value("Revenues") or 0.0
            net_profit = income_stmt.get_concept_value("NetIncomeLoss") or 0.0

            payload = {
                "acc_num": filing.accession_number,
                "company": filing.company,
                "filed_date": filing.filing_date,
                "revenue": float(revenue),
                "profit": float(net_profit)
            }
            producer.send('sec_raw', payload)
            print(f"Produced: {filing.company}")
        except Exception as e:
            print(f"Skipping {filing.company}: {e}")

if __name__ == "__main__":
    while True:
        fetch_sec_data()
        time.sleep(300)