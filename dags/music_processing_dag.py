from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
import pandas as pd
import os
import numpy as np

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define paths
DATA_DIR = '/opt/airflow/data'
INTERMEDIATE_DIR = os.path.join(DATA_DIR, 'intermediate')

def read_music_data():
    """Read the music dataset and perform initial cleaning"""
    data_path = os.path.join(os.path.dirname(__file__), '../data/music_dataset.csv')
    df = pd.read_csv(data_path)
    
    # Basic cleaning
    df['Release Year'] = pd.to_numeric(df['Release Year'], errors='coerce')
    df['Streams'] = pd.to_numeric(df['Streams'], errors='coerce')
    df['Daily Streams'] = pd.to_numeric(df['Daily Streams'], errors='coerce')
    
    # Save intermediate result
    df.to_csv(os.path.join(INTERMEDIATE_DIR, 'cleaned_music_data.csv'), index=False)
    return "Data read and cleaned successfully"

def analyze_genre_performance():
    """Analyze performance metrics by genre"""
    df = pd.read_csv(os.path.join(INTERMEDIATE_DIR, 'cleaned_music_data.csv'))
    
    # Calculate genre statistics
    genre_stats = df.groupby('Genre').agg({
        'Streams': ['mean', 'sum', 'count'],
        'Peak Position': 'min',
        'Weeks on Chart': 'mean',
        'TikTok Virality': 'mean'
    }).round(2)
    
    # Rename columns for clarity
    genre_stats.columns = ['Avg_Streams', 'Total_Streams', 'Song_Count', 
                          'Best_Peak_Position', 'Avg_Weeks_on_Chart', 
                          'Avg_TikTok_Virality']
    
    # Save results
    genre_stats.to_csv(os.path.join(INTERMEDIATE_DIR, 'genre_analysis.csv'))
    return "Genre analysis completed"

def analyze_artist_success():
    """Analyze artist success metrics"""
    df = pd.read_csv(os.path.join(INTERMEDIATE_DIR, 'cleaned_music_data.csv'))
    
    # Calculate artist success metrics
    artist_stats = df.groupby('Artist').agg({
        'Streams': ['sum', 'mean'],
        'Peak Position': 'min',
        'Weeks on Chart': 'sum',
        'TikTok Virality': 'mean'
    }).round(2)
    
    # Rename columns
    artist_stats.columns = ['Total_Streams', 'Avg_Streams', 'Best_Peak_Position',
                           'Total_Weeks_on_Chart', 'Avg_TikTok_Virality']
    
    # Sort by total streams
    artist_stats = artist_stats.sort_values('Total_Streams', ascending=False)
    
    # Save results
    artist_stats.to_csv(os.path.join(INTERMEDIATE_DIR, 'artist_analysis.csv'))
    return "Artist analysis completed"

def analyze_musical_characteristics():
    """Analyze musical characteristics and their relationship with success"""
    df = pd.read_csv(os.path.join(INTERMEDIATE_DIR, 'cleaned_music_data.csv'))
    
    # Calculate correlations with streams
    correlations = df[['Streams', 'Danceability', 'Acousticness', 'Energy', 
                      'Lyrics Sentiment', 'TikTok Virality']].corr()
    
    # Calculate average characteristics by genre
    genre_characteristics = df.groupby('Genre')[['Danceability', 'Acousticness', 
                                                'Energy', 'Lyrics Sentiment']].mean().round(2)
    
    # Save results
    correlations.to_csv(os.path.join(INTERMEDIATE_DIR, 'musical_correlations.csv'))
    genre_characteristics.to_csv(os.path.join(INTERMEDIATE_DIR, 'genre_characteristics.csv'))
    
    return "Musical characteristics analysis completed"

# Define the DAG
with DAG(
    'music_processing_dag',
    default_args=default_args,
    description='A DAG for processing and analyzing music data',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    # Create TaskGroup for all tasks
    with TaskGroup(group_id='music_processing_tasks') as music_processing_tasks:
        # Read task
        read_task = PythonOperator(
            task_id='read_music_data',
            python_callable=read_music_data,
        )

        # Transform 1: Genre Analysis
        genre_analysis_task = PythonOperator(
            task_id='analyze_genre_performance',
            python_callable=analyze_genre_performance,
        )

        # Transform 2: Artist Analysis
        artist_analysis_task = PythonOperator(
            task_id='analyze_artist_success',
            python_callable=analyze_artist_success,
        )

        # Transform 3: Musical Characteristics Analysis
        characteristics_analysis_task = PythonOperator(
            task_id='analyze_musical_characteristics',
            python_callable=analyze_musical_characteristics,
        )

        # Set sequential dependencies
        read_task >> genre_analysis_task >> artist_analysis_task >> characteristics_analysis_task 