import sys
import os
import psycopg2
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
import joblib

def get_connection():
    # Attempt to connect to the database (defaults to docker-compose standard)
    db_url = os.getenv("DATABASE_URL", "postgresql://jobmarket:your_password_here@postgres:5432/job_market")
    # For local execution without Docker, use localhost if needed
    if "postgres:5432" in db_url:
        try:
            conn = psycopg2.connect(db_url)
        except Exception:
            db_url = db_url.replace("postgres:5432", "localhost:5432")
            conn = psycopg2.connect(db_url)
    else:
        conn = psycopg2.connect(db_url)
    return conn

def train_and_predict():
    print("🤖 Starting Salary Prediction ML Pipeline...")
    conn = get_connection()
    
    # 1. Fetch training data (jobs with known salaries)
    query_train = """
        SELECT job_key, job_category, seniority_level, location_raw, salary_midpoint 
        FROM marts.fct_job_postings 
        WHERE salary_midpoint IS NOT NULL
    """
    df_train = pd.read_sql(query_train, conn)
    
    if len(df_train) < 50:
        print("Not enough training data to build a reliable ML model (need 50+ records). Skipping predictions.")
        return

    print(f"📈 Training model on {len(df_train)} records...")
    
    # Define features and target
    X_train = df_train[['job_category', 'seniority_level', 'location_raw']]
    y_train = df_train['salary_midpoint']
    
    # 2. Build ML Pipeline
    # Convert categorical strings to numbers (One-Hot Encoding)
    preprocessor = ColumnTransformer(
        transformers=[
            ('cat', OneHotEncoder(handle_unknown='ignore'), ['job_category', 'seniority_level', 'location_raw'])
        ])
    
    # Create the Random Forest pipeline
    model_pipeline = Pipeline(steps=[
        ('preprocessor', preprocessor),
        ('regressor', RandomForestRegressor(n_estimators=100, random_state=42))
    ])
    
    # Train the model
    model_pipeline.fit(X_train, y_train)
    
    # 3. Fetch prediction data (jobs missing salaries)
    query_predict = """
        SELECT job_key, job_category, seniority_level, location_raw
        FROM marts.fct_job_postings 
        WHERE salary_midpoint IS NULL
    """
    df_predict = pd.read_sql(query_predict, conn)
    
    if len(df_predict) == 0:
        print("✅ No jobs missing salaries! Pipeline complete.")
        return
        
    print(f"🔮 Predicting salaries for {len(df_predict)} jobs...")
    
    X_predict = df_predict[['job_category', 'seniority_level', 'location_raw']]
    predictions = model_pipeline.predict(X_predict)
    
    df_predict['predicted_salary'] = predictions
    
    # 4. Save predictions back to the database
    print("💾 Saving predictions to Data Warehouse (marts.ml_salary_predictions)...")
    cur = conn.cursor()
    
    # Create the table if it doesn't exist
    cur.execute("""
        CREATE TABLE IF NOT EXISTS marts.ml_salary_predictions (
            job_key VARCHAR PRIMARY KEY,
            predicted_salary NUMERIC,
            prediction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Insert predictions
    insert_query = """
        INSERT INTO marts.ml_salary_predictions (job_key, predicted_salary, prediction_date)
        VALUES (%s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (job_key) DO UPDATE SET 
            predicted_salary = EXCLUDED.predicted_salary,
            prediction_date = EXCLUDED.prediction_date
    """
    
    # Batch execute
    records = list(zip(df_predict['job_key'], df_predict['predicted_salary']))
    psycopg2.extras.execute_batch(cur, insert_query, records)
    
    conn.commit()
    cur.close()
    conn.close()
    
    print("✅ ML Pipeline Complete! Missing salaries successfully estimated.")

if __name__ == "__main__":
    train_and_predict()
