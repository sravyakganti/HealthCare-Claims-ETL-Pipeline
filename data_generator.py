import pandas as pd
from faker import Faker
import random
import os

fake = Faker()
DATA_DIR = 'data'
os.makedirs(DATA_DIR, exist_ok=True)

def generate_claims_data(num_records=5000):
    """Generates mock HIPAA-compliant healthcare data for testing."""
    data = []
    for _ in range(num_records):
        record = {
            'claim_id': fake.uuid4(),
            'patient_id': fake.random_int(min=1000, max=9999),
            'provider_name': fake.company(),
            'diagnosis_code': random.choice(['E11.9', 'I10', 'J45', None, 'M54.5']),
            'claim_amount': round(random.uniform(50.0, 5000.0), 2),
            'date_of_service': fake.date_between(start_date='-2y', end_date='today').strftime('%Y-%m-%d'),
            'status': random.choice(['PAID', 'DENIED', 'PENDING', 'ERROR'])
        }
        data.append(record)
    
    df = pd.DataFrame(data)
    df.to_csv(os.path.join(DATA_DIR, 'raw_claims_data.csv'), index=False)
    print("Mock data generated successfully.")

if __name__ == "__main__":
    generate_claims_data()