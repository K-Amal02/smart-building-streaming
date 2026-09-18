"""
preprocess.py - Prétraitement du dataset Smart Building
Traite TOUS les fichiers CSV de toutes les pièces
"""
import pandas as pd
import numpy as np
import os
from datetime import datetime
import glob

def load_all_sensor_data(base_path):
    """
    Charge tous les fichiers CSV de toutes les pièces
    """
    all_data = []
    
    print(f"🔍 Recherche dans: {os.path.abspath(base_path)}")
    
    # Vérifier que le dossier existe
    if not os.path.exists(base_path):
        print(f"❌ Chemin introuvable: {base_path}")
        return None
    
    # Trouver tous les dossiers de pièces
    room_dirs = [d for d in os.listdir(base_path) 
                if os.path.isdir(os.path.join(base_path, d))]
    
    if not room_dirs:
        print("❌ Aucune pièce trouvée")
        return None
    
    print(f"🏢 {len(room_dirs)} pièces détectées")
    
    # Types de capteurs
    sensor_files = ['co2.csv', 'humidity.csv', 'temperature.csv', 'pir.csv', 'light.csv']
    
    total_files = 0
    
    for room in room_dirs[:5]:  # Limiter à 5 pièces pour le test
        room_path = os.path.join(base_path, room)
        
        for sensor_file in sensor_files:
            file_path = os.path.join(room_path, sensor_file)
            
            if os.path.exists(file_path):
                try:
                    # Lire le fichier CSV
                    df = pd.read_csv(file_path)
                    total_files += 1
                    
                    # Vérifier la structure (normalement 2 colonnes)
                    if len(df.columns) >= 2:
                        # Renommer les colonnes
                        df = df.rename(columns={
                            df.columns[0]: 'timestamp_unix',
                            df.columns[1]: 'value'
                        })
                        
                        # Ajouter métadonnées
                        df['room_id'] = room
                        df['sensor_type'] = sensor_file.replace('.csv', '')
                        df['building_id'] = 'SDH_Building'
                        
                        # Convertir timestamp Unix
                        df['timestamp'] = pd.to_datetime(df['timestamp_unix'], unit='s')
                        
                        all_data.append(df)
                        
                        print(f"  ✓ {room}/{sensor_file}: {len(df)} lignes")
                
                except Exception as e:
                    print(f"  ✗ Erreur {room}/{sensor_file}: {e}")
    
    if all_data:
        # Combiner tous les DataFrames
        combined_df = pd.concat(all_data, ignore_index=True)
        print(f"\n🎉 {total_files} fichiers chargés")
        print(f"📊 Total: {len(combined_df):,} lignes")
        return combined_df
    
    return None

def clean_data(df):
    """Nettoie les données combinées"""
    
    # 1. Supprimer les doublons
    initial_count = len(df)
    df = df.drop_duplicates(subset=['timestamp_unix', 'room_id', 'sensor_type'])
    print(f"📉 Doublons supprimés: {initial_count - len(df)}")
    
    # 2. Vérifier les valeurs aberrantes (par capteur)
    sensor_stats = {}
    
    for sensor in df['sensor_type'].unique():
        sensor_df = df[df['sensor_type'] == sensor]
        
        # Statistiques
        q1 = sensor_df['value'].quantile(0.01)
        q3 = sensor_df['value'].quantile(0.99)
        
        # Filtrer les valeurs extrêmes (1% et 99%)
        mask = (sensor_df['value'] >= q1) & (sensor_df['value'] <= q3)
        df.loc[sensor_df.index, 'is_outlier'] = ~mask
        
        sensor_stats[sensor] = {
            'min': sensor_df['value'].min(),
            'max': sensor_df['value'].max(),
            'mean': sensor_df['value'].mean(),
            'std': sensor_df['value'].std(),
            'outliers': (~mask).sum()
        }
    
    print("\n📊 Statistiques par capteur:")
    for sensor, stats in sensor_stats.items():
        print(f"  {sensor}: {stats['outliers']} outliers")
    
    return df

def add_features(df):
    """Ajoute des features temporelles"""
    
    # Features temporelles
    df['hour'] = df['timestamp'].dt.hour
    df['day_of_week'] = df['timestamp'].dt.dayofweek
    df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)
    df['month'] = df['timestamp'].dt.month
    df['day'] = df['timestamp'].dt.day
    
    # Rolling features (moyenne mobile par pièce/capteur)
    df = df.sort_values(['room_id', 'sensor_type', 'timestamp'])
    
    # Pour les données numériques
    numeric_sensors = ['temperature', 'co2', 'humidity', 'light']
    
    for sensor in numeric_sensors:
        sensor_mask = df['sensor_type'] == sensor
        df.loc[sensor_mask, f'{sensor}_rolling_mean'] = (
            df.loc[sensor_mask].groupby('room_id')['value']
            .transform(lambda x: x.rolling(10, min_periods=1).mean())
        )
    
    return df

def save_processed_data(df, output_dir):
    """Sauvegarde les données traitées"""
    
    # Créer le dossier si nécessaire
    os.makedirs(output_dir, exist_ok=True)
    
    # 1. Fichier complet
    full_path = os.path.join(output_dir, 'smart_building_processed.csv')
    df.to_csv(full_path, index=False)
    print(f"\n💾 Fichier complet sauvegardé: {full_path}")
    
    # 2. Fichier par capteur (pour ML)
    for sensor in df['sensor_type'].unique():
        sensor_df = df[df['sensor_type'] == sensor]
        
        if not sensor_df.empty:
            sensor_path = os.path.join(output_dir, f'{sensor}_processed.csv')
            sensor_df.to_csv(sensor_path, index=False)
            print(f"  - {sensor}: {len(sensor_df):,} lignes")
    
    # 3. Statistiques
    stats_path = os.path.join(output_dir, 'processing_stats.txt')
    with open(stats_path, 'w') as f:
        f.write(f"Dataset Smart Building - Traitement terminé\n")
        f.write(f"Date: {datetime.now()}\n\n")
        
        f.write(" Statistiques globales:\n")
        f.write(f"  Lignes totales: {len(df):,}\n")
        f.write(f"  Pièces: {df['room_id'].nunique()}\n")
        f.write(f"  Capteurs: {df['sensor_type'].nunique()}\n")
        f.write(f"  Période: {df['timestamp'].min()} à {df['timestamp'].max()}\n\n")
        
        f.write(" Par capteur:\n")
        for sensor in sorted(df['sensor_type'].unique()):
            sensor_df = df[df['sensor_type'] == sensor]
            f.write(f"  - {sensor}: {len(sensor_df):,} lignes | "
                   f"Moyenne: {sensor_df['value'].mean():.2f} | "
                   f"Pièces: {sensor_df['room_id'].nunique()}\n")

def main():
    """Fonction principale"""
    print("=" * 60)
    print("🔧 PRÉTRAITEMENT - Dataset Smart Building")
    print("=" * 60)
    
    # Chemins
    input_path = "../../data/raw/smart-building-system"
    output_dir = "../../data/processed"
    
    # 1. Charger toutes les données
    print("\n📥 Étape 1: Chargement des données...")
    df = load_all_sensor_data(input_path)
    
    if df is None or df.empty:
        print("❌ Aucune donnée chargée")
        return
    
    # 2. Nettoyage
    print("\n🧹 Étape 2: Nettoyage...")
    df = clean_data(df)
    
    # 3. Ajout de features
    print("\n✨ Étape 3: Ajout de features...")
    df = add_features(df)
    
    # 4. Sauvegarde
    print("\n💾 Étape 4: Sauvegarde...")
    save_processed_data(df, output_dir)
    
    # Résumé
    print("\n" + "=" * 60)
    print("✅ PRÉTRAITEMENT TERMINÉ !")
    print(f"📊 Données finales: {len(df):,} lignes")
    print(f"🏢 Pièces: {df['room_id'].nunique()}")
    print(f"📟 Capteurs: {', '.join(df['sensor_type'].unique())}")
    print(f"🕒 Période: {df['timestamp'].min().date()} au {df['timestamp'].max().date()}")
    print("=" * 60)

if __name__ == "__main__":
    main()