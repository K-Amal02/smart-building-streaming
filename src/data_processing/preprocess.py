import pandas as pd
import numpy as np
from datetime import datetime
import os

def load_and_clean_data(input_path):
    """Charge et nettoie les données brutes"""
    df = pd.read_csv(input_path)
    
    # Conversion timestamp si nécessaire
    if 'Date' in df.columns and 'Time' in df.columns:
        df['timestamp'] = pd.to_datetime(df['Date'] + ' ' + df['Time'])
        df = df.drop(['Date', 'Time'], axis=1)
    
    # Suppression des doublons
    df = df.drop_duplicates()
    
    # Gestion des valeurs manquantes
    numeric_columns = df.select_dtypes(include=[np.number]).columns
    df[numeric_columns] = df[numeric_columns].fillna(method='ffill')
    
    return df

def add_features(df):
    """Ajoute des features temporelles"""
    df['hour'] = df['timestamp'].dt.hour
    df['day_of_week'] = df['timestamp'].dt.dayofweek
    df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)
    
    return df

def main():
    input_path = "../../data/raw/building_data.csv"
    output_path = "../../data/processed/cleaned_building_data.csv"
    
    # Pipeline de traitement
    df = load_and_clean_data(input_path)
    df = add_features(df)
    
    # Sauvegarde
    df.to_csv(output_path, index=False)
    print(f"✅ Données nettoyées sauvegardées dans : {output_path}")
    print(f"📊 Shape final : {df.shape}")

if __name__ == "__main__":
    main()