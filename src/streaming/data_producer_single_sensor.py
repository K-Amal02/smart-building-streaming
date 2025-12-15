"""
data_producer_single_sensor.py - Producteur Kafka pour UN SEUL capteur.
Stream uniquement les données d'un type de capteur spécifique.
VERSION RAPIDE - Limité à 50,000 messages maximum.
"""
import pandas as pd
import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
import os
import glob

class SingleSensorDataProducer:
    def __init__(self, bootstrap_servers='localhost:29095'):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self.sensor_data = None
        
    def connect_kafka(self):
        """Établit la connexion au cluster Kafka."""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                batch_size=16384,
                linger_ms=10,
                max_block_ms=10000
            )
            print(f"✅ Connecté à Kafka sur {self.bootstrap_servers}")
            return True
        except Exception as e:
            print(f"❌ Erreur de connexion à Kafka: {e}")
            return False
    
    def load_single_sensor_data(self, base_path="../../data/raw/smart-building-system", 
                                sensor_type="temperature"):
        """
        Charge UNIQUEMENT les données d'un type de capteur spécifique.
        
        Args:
            base_path: Chemin du dataset
            sensor_type: Type de capteur ('temperature', 'co2', 'humidity', 'occupancy', 'light')
        """
        try:
            print(f"🔍 Chargement des données pour le capteur: {sensor_type}")
            print(f"📂 Chemin: {os.path.abspath(base_path)}")
            
            if not os.path.exists(base_path):
                print(f"❌ Le chemin du dataset n'existe pas: {base_path}")
                return False
            
            # Mapping des types de capteurs vers les fichiers
            sensor_to_file = {
                'temperature': 'temperature.csv',
                'co2': 'co2.csv',
                'humidity': 'humidity.csv',
                'occupancy': 'pir.csv',
                'light': 'light.csv'
            }
            
            if sensor_type not in sensor_to_file:
                print(f"❌ Type de capteur invalide. Choisissez parmi: {list(sensor_to_file.keys())}")
                return False
            
            target_file = sensor_to_file[sensor_type]
            
            # Trouver tous les dossiers de pièces
            room_dirs = [d for d in os.listdir(base_path) 
                        if os.path.isdir(os.path.join(base_path, d))]
            
            if not room_dirs:
                print("❌ Aucun dossier de pièce trouvé")
                return False
            
            print(f"🏢 {len(room_dirs)} pièces détectées")
            
            all_sensor_data = []
            total_records = 0
            
            # Parcourir chaque pièce et charger UNIQUEMENT le fichier cible
            for room_dir in room_dirs:
                room_path = os.path.join(base_path, room_dir)
                file_path = os.path.join(room_path, target_file)
                
                if os.path.exists(file_path):
                    try:
                        df = pd.read_csv(file_path)
                        
                        if len(df.columns) >= 2:
                            # Renommer les colonnes
                            df = df.rename(columns={
                                df.columns[0]: 'timestamp',
                                df.columns[1]: 'value'
                            })
                            
                            # Ajouter les métadonnées
                            df['room_id'] = room_dir
                            df['sensor_type'] = sensor_type
                            df['building_id'] = 'SDH_Building'
                            df['human_time'] = pd.to_datetime(df['timestamp'], unit='s')
                            
                            # Définir l'unité
                            unit_map = {
                                'co2': 'ppm',
                                'humidity': '%',
                                'temperature': 'celsius',
                                'occupancy': 'binary',
                                'light': 'lux'
                            }
                            df['unit'] = unit_map.get(sensor_type, 'unknown')
                            
                            all_sensor_data.append(df)
                            total_records += len(df)
                            
                            if len(room_dirs) <= 10:  # Afficher seulement si peu de pièces
                                print(f"   ✓ {room_dir}/{target_file}: {len(df)} enregistrements")
                                
                    except Exception as e:
                        print(f"   ✗ Erreur lecture {room_dir}/{target_file}: {e}")
                else:
                    print(f"   ⚠️ Fichier manquant: {room_dir}/{target_file}")
            
            if total_records > 0:
                # Fusionner toutes les données
                self.sensor_data = pd.concat(all_sensor_data, ignore_index=True)
                
                # Statistiques
                print(f"\n🎉 Données du capteur '{sensor_type}' chargées!")
                print(f"📊 Total: {total_records:,} enregistrements")
                print(f"🏢 Bâtiment: SDH_Building")
                print(f"📟 Capteur unique: {sensor_type}")
                print(f"🏠 Nombre de pièces: {len(room_dirs)}")
                print(f"📈 Plage de valeurs: {self.sensor_data['value'].min():.2f} à {self.sensor_data['value'].max():.2f}")
                print(f"📊 Valeur moyenne: {self.sensor_data['value'].mean():.2f}")
                
                return True
            else:
                print("❌ Aucune donnée valide chargée")
                return False
                
        except Exception as e:
            print(f"❌ Erreur critique lors du chargement: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def stream_single_sensor_data(self, topic_name='building-sensors', speed_factor=100, max_messages=50000):
        """
        Stream les données d'un seul capteur en temps accéléré.
        VERSION RAPIDE - Limité à un nombre maximum de messages.
        
        Args:
            topic_name: Nom du topic Kafka
            speed_factor: Facteur d'accélération
            max_messages: Nombre maximum de messages à streamer
        """
        if self.sensor_data is None or self.sensor_data.empty:
            print("❌ Aucune donnée chargée")
            return
        
        sensor_type = self.sensor_data['sensor_type'].iloc[0]
        
        # Trier par timestamp
        sorted_df = self.sensor_data.sort_values('human_time').reset_index(drop=True)
        
        # ==================== MODIFICATION CRITIQUE ====================
        # LIMITER le nombre de messages à streamer
        if len(sorted_df) > max_messages:
            print(f"🚀 LIMITATION ACTIVÉE: Streaming {max_messages:,} messages au lieu de {len(sorted_df):,}")
            sorted_df = sorted_df.head(max_messages)
        # ==============================================================
        
        print(f"\n🚀 DÉMARRAGE DU STREAMING RAPIDE - Capteur: {sensor_type}")
        print(f"📤 Topic Kafka: '{topic_name}'")
        print(f"⚡ Vitesse: {speed_factor}x temps réel")
        print(f"📈 Total à streamer: {len(sorted_df):,} messages")
        print(f"⏱️ Estimation: < 1 minute")
        print("=" * 60)
        
        record_count = 0
        start_time = time.time()
        batch_size = 5000  # Afficher progression tous les 5000 messages
        
        for idx, row in sorted_df.iterrows():
            # Message Kafka
            message = {
                'timestamp': row['human_time'].isoformat(),
                'kafka_timestamp': datetime.now().isoformat(),
                'building_id': row['building_id'],
                'room_id': row['room_id'],
                'sensor_type': row['sensor_type'],
                'value': float(row['value']),
                'unit': row['unit'],
                'source': 'single_sensor_historical'
            }
            
            # Envoyer à Kafka
            try:
                self.producer.send(topic_name, message)
                record_count += 1
                
                # Afficher la progression
                if record_count % batch_size == 0:
                    elapsed = time.time() - start_time
                    percentage = (idx + 1) / len(sorted_df) * 100
                    
                    if elapsed > 0:
                        rate = record_count / elapsed
                        remaining = (len(sorted_df) - record_count) / rate if rate > 0 else 0
                        
                        print(f"📊 {percentage:.1f}% | "
                              f"Envoyés: {record_count:,} | "
                              f"Débit: {rate:.0f} msg/s | "
                              f"Restant: {remaining:.0f}s")
                
                # ==================== MODIFICATION CRITIQUE ====================
                # CONTRÔLE DE VITESSE OPTIMISÉ - Presque pas de délai
                if idx < len(sorted_df) - 1:
                    next_time = sorted_df.loc[idx + 1, 'human_time']
                    current_time = row['human_time']
                    time_diff = (next_time - current_time).total_seconds()
                    
                    if time_diff > 0:
                        # ACCÉLÉRATION EXTRÊME : facteur supplémentaire de 1000
                        sleep_time = time_diff / (speed_factor * 1000)
                        # Dormir seulement si vraiment nécessaire, et très peu
                        if sleep_time > 0.0001:
                            time.sleep(0.00001)  # Délai minuscule
                # ==============================================================
                        
            except Exception as e:
                print(f"❌ Erreur envoi message {idx}: {e}")
        
        # Finaliser
        self.producer.flush()
        total_time = time.time() - start_time
        
        print("=" * 60)
        print(f"🎉 STREAMING TERMINÉ EN {total_time:.1f} SECONDES!")
        print(f"✅ Messages envoyés: {record_count:,}")
        print(f"📈 Débit moyen: {record_count/total_time:.0f} messages/sec")
        print(f"🎯 Capteur: {sensor_type}")
        print(f"⏱️ Temps total: {total_time:.1f}s ({total_time/60:.1f} minutes)")
    
    def stream_live_single_sensor(self, topic_name='building-sensors', duration=60, 
                                  sensor_type='temperature'):
        """
        Simulation temps réel pour un seul capteur.
        
        Args:
            topic_name: Nom du topic Kafka
            duration: Durée en secondes
            sensor_type: Type de capteur à simuler
        """
        print(f"\n🚀 SIMULATION TEMPS RÉEL - Capteur: {sensor_type}")
        print(f"📤 Topic Kafka: '{topic_name}'")
        print(f"⏱️ Durée: {duration} secondes")
        print("=" * 60)
        
        # Configuration par type de capteur
        sensor_configs = {
            'temperature': {
                'unit': 'celsius',
                'normal_range': (18, 26),
                'variation': 0.5,
                'alert_threshold': 28
            },
            'co2': {
                'unit': 'ppm',
                'normal_range': (400, 1000),
                'variation': 50,
                'alert_threshold': 1200
            },
            'humidity': {
                'unit': '%',
                'normal_range': (30, 70),
                'variation': 5,
                'alert_threshold': 80
            },
            'occupancy': {
                'unit': 'binary',
                'normal_range': (0, 1),
                'variation': 0,
                'alert_threshold': None
            },
            'light': {
                'unit': 'lux',
                'normal_range': (100, 500),
                'variation': 50,
                'alert_threshold': 800
            }
        }
        
        if sensor_type not in sensor_configs:
            print(f"❌ Type de capteur invalide: {sensor_type}")
            return
        
        config = sensor_configs[sensor_type]
        rooms = ['413', '415', '417', '419', '421']  # 5 pièces pour la simulation
        current_value = sum(config['normal_range']) / 2  # Valeur initiale moyenne
        
        print(f"🏢 Bâtiment: SDH_Building")
        print(f"🏠 Pièces: {len(rooms)}")
        print(f"📊 Plage normale: {config['normal_range'][0]} à {config['normal_range'][1]} {config['unit']}")
        
        record_count = 0
        start_time = time.time()
        last_display = start_time
        
        try:
            while (time.time() - start_time) < duration:
                current_time = time.time()
                room = random.choice(rooms)
                
                # Appliquer variation
                if config['variation'] > 0:
                    variation = random.uniform(-config['variation'], config['variation'])
                    current_value += variation
                    
                    # Garder dans la plage
                    low, high = config['normal_range']
                    current_value = max(low * 0.9, min(high * 1.1, current_value))
                
                # Pour occupancy: 94% vide, 6% occupé
                if sensor_type == 'occupancy':
                    current_value = 1 if random.random() < 0.06 else 0
                
                # Créer message
                message = {
                    'timestamp': datetime.now().isoformat(),
                    'building_id': 'SDH_Building',
                    'room_id': room,
                    'sensor_type': sensor_type,
                    'value': round(current_value, 2),
                    'unit': config['unit'],
                    'source': 'live_single_sensor'
                }
                
                # Vérifier alerte
                if config['alert_threshold'] and current_value > config['alert_threshold']:
                    message['alert'] = True
                    message['alert_type'] = f'high_{sensor_type}'
                
                # Envoyer
                self.producer.send(topic_name, message)
                record_count += 1
                
                # Afficher progression
                if current_time - last_display > 5:
                    elapsed = current_time - start_time
                    print(f"📨 {record_count} messages | "
                          f"Temps: {elapsed:.0f}s/{duration}s | "
                          f"Valeur actuelle: {current_value:.2f} {config['unit']}")
                    last_display = current_time
                
                # Intervalle réaliste (accéléré pour la simulation)
                if sensor_type == 'occupancy':
                    time.sleep(0.5)  # Plus rapide que le réel
                else:
                    time.sleep(0.2)  # Plus rapide que le réel
            
            # Finaliser
            self.producer.flush()
            total_time = time.time() - start_time
            
            print("=" * 60)
            print(f"🎉 SIMULATION TERMINÉE!")
            print(f"✅ Messages générés: {record_count}")
            print(f"⏱️ Durée: {total_time:.1f}s")
            
        except KeyboardInterrupt:
            print(f"\n⏹️ Simulation interrompue - {record_count} messages envoyés")

def main():
    """Fonction principale."""
    print("=" * 60)
    print("🏢 SMART BUILDING - PRODUCER SINGLE SENSOR (VERSION RAPIDE)")
    print("📡 Stream UNIQUEMENT les données d'un capteur spécifique")
    print("⚡ OPTIMISÉ: Limité à 50,000 messages max pour rapidité")
    print("=" * 60)
    
    # Initialisation
    producer = SingleSensorDataProducer(bootstrap_servers='localhost:29095')
    
    if not producer.connect_kafka():
        return
    
    # Menu
    print("\n🎛️  CHOIX DU CAPTEUR")
    print("1. 🌡️  Temperature (recommandé)")
    print("2. 💨 CO2")
    print("3. 💧 Humidity")
    print("4. 👤 Occupancy (PIR)")
    print("5. 💡 Light")
    
    sensor_choice = input("\nChoisissez le capteur (1-5) [défaut: 1]: ").strip()
    
    sensor_map = {
        '1': 'temperature',
        '2': 'co2', 
        '3': 'humidity',
        '4': 'occupancy',
        '5': 'light'
    }
    
    sensor_type = sensor_map.get(sensor_choice, 'temperature')
    
    print("\n🎛️  MODE DE STREAMING")
    print("1. 📜 Données historiques du dataset (limité à 50K messages)")
    print("2. ⚡ Simulation temps réel (60 secondes)")
    
    mode_choice = input("\nChoisissez le mode (1 ou 2) [défaut: 1]: ").strip()
    
    if mode_choice == "2":
        # Mode simulation
        duration = input("Durée en secondes [défaut: 60]: ").strip()
        duration = int(duration) if duration else 60
        
        producer.stream_live_single_sensor(
            sensor_type=sensor_type,
            duration=duration
        )
    else:
        # Mode historique
        base_path = input("Chemin du dataset [défaut: ../../data/raw/smart-building-system]: ").strip()
        base_path = base_path if base_path else "../../data/raw/smart-building-system"
        
        if producer.load_single_sensor_data(base_path=base_path, sensor_type=sensor_type):
            # Demander le nombre maximum de messages
            max_msg = input("Nombre max de messages [défaut: 50000, min: 1000, max: 1000000]: ").strip()
            max_messages = int(max_msg) if max_msg else 50000
            max_messages = max(1000, min(1000000, max_messages))  # Entre 1000 et 1000000
            
            speed = input("Facteur d'accélération [défaut: 100]: ").strip()
            speed_factor = int(speed) if speed else 100
            
            # Ajouter une confirmation
            print(f"\n⚠️  CONFIGURATION FINALE:")
            print(f"   Capteur: {sensor_type}")
            print(f"   Messages: {max_messages:,} (limité)")
            print(f"   Accélération: {speed_factor}x")
            print(f"   Durée estimée: < 1 minute")
            
            confirm = input("\nDémarrer le streaming? (O/N) [O]: ").strip().upper()
            
            if confirm != 'N':
                producer.stream_single_sensor_data(
                    speed_factor=speed_factor,
                    max_messages=max_messages
                )
            else:
                print("🚫 Streaming annulé")
    
    # Nettoyage
    if producer.producer:
        producer.producer.close()
        print("\n🔌 Producteur Kafka fermé")

if __name__ == "__main__":
    main()