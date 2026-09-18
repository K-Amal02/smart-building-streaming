"""
data_producer.py - Producteur Kafka pour le projet Smart Building.
Charge et stream l'INTÉGRALITÉ du dataset historique (Mode 1) ou génère des simulations réalistes (Mode 2).
"""
import pandas as pd
import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
import os
import glob

class BuildingDataProducer:
    def __init__(self, bootstrap_servers='localhost:29095'):
        """Initialise le producteur Kafka."""
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self.sensor_data_frames = []  # Liste pour stocker les DataFrames de tous les capteurs
        
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
            print("Vérifiez que: 1) Kafka est démarré, 2) Le port est correct")
            return False
    
    def load_complete_dataset(self, base_path="../../data/raw/smart-building-system"):
        """
        Charge l'INTÉGRALITÉ du dataset Smart Building System.
        Structure attendue: base_path/room_number/[co2, humidity, temperature, pir, luminance].csv
        """
        try:
            print(f"🔍 Recherche du dataset dans: {os.path.abspath(base_path)}")
            
            if not os.path.exists(base_path):
                print(f"❌ Le chemin du dataset n'existe pas: {base_path}")
                return False
            
            # Trouver tous les dossiers de pièces (rooms)
            room_dirs = [d for d in os.listdir(base_path) 
                        if os.path.isdir(os.path.join(base_path, d))]
            
            if not room_dirs:
                print("❌ Aucun dossier de pièce trouvé dans le dataset")
                return False
            
            print(f"🏢 {len(room_dirs)} pièces détectées")
            
            # Mapper les noms de fichiers aux types de capteurs
            sensor_file_map = {
                'co2.csv': 'co2',
                'humidity.csv': 'humidity', 
                'temperature.csv': 'temperature',
                'pir.csv': 'occupancy',  # PIR = Passive Infrared = Occupancy
                'light.csv': 'light'
            }
            
            total_records = 0
            
            # Parcourir chaque pièce et charger TOUS les fichiers de capteurs
            for room_dir in room_dirs:
                room_path = os.path.join(base_path, room_dir)
                
                for sensor_file, sensor_type in sensor_file_map.items():
                    file_path = os.path.join(room_path, sensor_file)
                    
                    if os.path.exists(file_path):
                        try:
                            # Lire le fichier CSV
                            df = pd.read_csv(file_path)
                            
                            # Vérifier la structure des colonnes
                            if len(df.columns) >= 2:
                                # Les fichiers ont généralement: timestamp, value
                                timestamp_col = df.columns[0]
                                value_col = df.columns[1]
                                
                                # Renommer les colonnes pour uniformité
                                df = df.rename(columns={
                                    timestamp_col: 'timestamp',
                                    value_col: 'value'
                                })
                                
                                # Ajouter les métadonnées
                                df['room_id'] = room_dir
                                df['sensor_type'] = sensor_type
                                df['building_id'] = 'SDH_Building'  # Sutardja Dai Hall
                                
                                # Convertir le timestamp Unix en datetime lisible
                                df['human_time'] = pd.to_datetime(df['timestamp'], unit='s')
                                
                                # Définir les unités selon le type de capteur
                                unit_map = {
                                    'co2': 'ppm',
                                    'humidity': '%',
                                    'temperature': 'celsius',
                                    'occupancy': 'binary',
                                    'light': 'lux'
                                }
                                df['unit'] = unit_map.get(sensor_type, 'unknown')
                                
                                self.sensor_data_frames.append(df)
                                total_records += len(df)
                                
                                print(f"   ✓ {room_dir}/{sensor_file}: {len(df)} enregistrements")
                                
                        except Exception as e:
                            print(f"   ✗ Erreur lecture {room_dir}/{sensor_file}: {e}")
                    else:
                        print(f"   ⚠️ Fichier manquant: {room_dir}/{sensor_file}")
            
            if total_records > 0:
                # Fusionner tous les DataFrames
                self.complete_dataset = pd.concat(self.sensor_data_frames, ignore_index=True)
                print(f"\n🎉 Dataset COMPLET chargé avec succès!")
                print(f"📊 Total: {total_records:,} enregistrements")
                print(f"🏢 Bâtiment: SDH_Building")
                print(f"📟 Capteurs: CO2, Humidity, Temperature, Occupancy(PIR), Light")
                print(f"🕒 Période: Août 2013 (1 semaine de données)")
                return True
            else:
                print("❌ Aucune donnée valide chargée")
                return False
                
        except Exception as e:
            print(f"❌ Erreur critique lors du chargement: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def stream_historical_data(self, topic_name='building-sensors', speed_factor=50):
        """
        Stream TOUTES les données historiques du dataset en temps accéléré.
        
        Args:
            topic_name: Nom du topic Kafka
            speed_factor: Facteur d'accélération (50x = 50 fois plus rapide que le temps réel)
        """
        if not hasattr(self, 'complete_dataset') or self.complete_dataset.empty:
            print("❌ Aucune donnée historique chargée")
            return
        
        print(f"\n🚀 DÉMARRAGE DU STREAMING HISTORIQUE COMPLET")
        print(f"📤 Topic Kafka: '{topic_name}'")
        print(f"⚡ Vitesse: {speed_factor}x temps réel")
        print(f"📈 Total à streamer: {len(self.complete_dataset):,} messages")
        print("=" * 60)
        
        # Trier par timestamp pour un flux chronologique
        sorted_df = self.complete_dataset.sort_values('human_time').reset_index(drop=True)
        
        record_count = 0
        start_time = time.time()
        batch_start = time.time()
        
        # Variables pour le suivi de progression
        last_percentage = 0
        last_print_time = time.time()
        
        for idx, row in sorted_df.iterrows():
            # Construire le message Kafka avec TOUTES les informations
            message = {
                'timestamp': row['human_time'].isoformat(),  # Timestamp original
                'kafka_timestamp': datetime.now().isoformat(),  # Timestamp d'envoi
                'building_id': row['building_id'],
                'room_id': row['room_id'],
                'sensor_type': row['sensor_type'],
                'value': float(row['value']),
                'unit': row['unit'],
                'source': 'historical_dataset',
                'original_timestamp': int(row['timestamp'])  # Timestamp Unix original
            }
            
            # Envoyer à Kafka
            try:
                self.producer.send(topic_name, message)
                record_count += 1
                
                # Afficher la progression
                current_percentage = int((idx + 1) / len(sorted_df) * 100)
                current_time = time.time()
                
                # Afficher tous les 5% de progression ou toutes les 10 secondes
                if (current_percentage > last_percentage and current_percentage % 5 == 0) or \
                   (current_time - last_print_time > 10):
                    
                    elapsed = current_time - start_time
                    rate = record_count / elapsed if elapsed > 0 else 0
                    
                    print(f"📊 {current_percentage}% | "
                          f"Envoyés: {record_count:,} | "
                          f"Durée: {elapsed:.1f}s | "
                          f"Débit: {rate:.1f} msg/s | "
                          f"Capteur: {row['sensor_type']}")
                    
                    last_percentage = current_percentage
                    last_print_time = current_time
                
                # Envoyer un batch toutes les 1000 messages
                if record_count % 1000 == 0:
                    self.producer.flush()
                    
            except Exception as e:
                print(f"❌ Erreur envoi message {idx}: {e}")
            
            # Contrôle de vitesse: simuler l'écoulement du temps accéléré
            # Calculer le délai basé sur l'intervalle réel entre les timestamps
            if idx < len(sorted_df) - 1:
                next_time = sorted_df.loc[idx + 1, 'human_time']
                current_time = row['human_time']
                time_diff = (next_time - current_time).total_seconds()
                
                # Accélérer selon le facteur spécifié
                if time_diff > 0:
                    sleep_time = time_diff / speed_factor
                    time.sleep(max(0, sleep_time - 0.001))  # Minimum 0
        
        # Finaliser
        self.producer.flush()
        total_time = time.time() - start_time
        
        print("=" * 60)
        print(f"🎉 STREAMING HISTORIQUE TERMINÉ!")
        print(f"✅ Messages envoyés: {record_count:,}")
        print(f"⏱️ Temps total: {total_time:.1f} secondes")
        print(f"📈 Débit moyen: {record_count/total_time:.1f} messages/sec")
        print(f"🏭 Topic Kafka: {topic_name}")
    
    def stream_live_simulation(self, topic_name='building-sensors', duration=120):
        """
        Génère une simulation TEMPS RÉEL réaliste basée sur les statistiques du dataset.
        
        Args:
            topic_name: Nom du topic Kafka
            duration: Durée de simulation en secondes
        """
        print(f"\n🚀 DÉMARRAGE SIMULATION TEMPS RÉEL")
        print(f"📤 Topic Kafka: '{topic_name}'")
        print(f"⏱️ Durée: {duration} secondes")
        print("=" * 60)
        
        # Statistiques réalistes basées sur le dataset réel
        buildings = ['SDH_Building']
        rooms = ['413', '415', '417', '419', '421', '422']  # Sous-ensemble de pièces
        sensor_configs = {
            'co2': {
                'unit': 'ppm',
                'normal_range': (400, 1000),
                'alert_threshold': 1200,
                'variation': 50
            },
            'humidity': {
                'unit': '%',
                'normal_range': (30, 70),
                'alert_threshold': 80,
                'variation': 5
            },
            'temperature': {
                'unit': 'celsius',
                'normal_range': (18, 26),
                'alert_threshold': 28,
                'variation': 0.5
            },
            'occupancy': {
                'unit': 'binary',
                'normal_range': (0, 1),
                'alert_threshold': None,
                'variation': 0
            },
            'light': {
                'unit': 'lux',
                'normal_range': (100, 500),
                'alert_threshold': 800,
                'variation': 50
            }
        }
        
        # États initiaux pour chaque capteur/pièce
        sensor_states = {}
        for room in rooms:
            for sensor_type, config in sensor_configs.items():
                key = f"{room}_{sensor_type}"
                low, high = config['normal_range']
                sensor_states[key] = random.uniform(low, high)
        
        record_count = 0
        start_time = time.time()
        last_display = start_time
        
        print("📡 Génération de données temps réel...")
        print("🏢 Bâtiment: SDH_Building")
        print(f"🏠 Pièces: {len(rooms)}")
        print(f"📟 Capteurs: {', '.join(sensor_configs.keys())}")
        
        try:
            while (time.time() - start_time) < duration:
                current_time = time.time()
                
                # Générer un message pour une combinaison aléatoire pièce/capteur
                room = random.choice(rooms)
                sensor_type = random.choice(list(sensor_configs.keys()))
                config = sensor_configs[sensor_type]
                
                key = f"{room}_{sensor_type}"
                current_value = sensor_states[key]
                
                # Appliquer une variation réaliste
                if config['variation'] > 0:
                    variation = random.uniform(-config['variation'], config['variation'])
                    new_value = current_value + variation
                    
                    # Garder dans la plage normale (avec un peu de marge)
                    low, high = config['normal_range']
                    new_value = max(low * 0.9, min(high * 1.1, new_value))
                    
                    sensor_states[key] = new_value
                    current_value = new_value
                
                # Pour occupancy, générer des états binaires réalistes (94% vide, 6% occupé)
                if sensor_type == 'occupancy':
                    current_value = 1 if random.random() < 0.06 else 0
                
                # Créer le message
                message = {
                    'timestamp': datetime.now().isoformat(),
                    'building_id': 'SDH_Building',
                    'room_id': room,
                    'sensor_type': sensor_type,
                    'value': round(current_value, 2),
                    'unit': config['unit'],
                    'source': 'live_simulation',
                    'simulation_time': int(current_time - start_time)
                }
                
                # Vérifier les alertes
                if config['alert_threshold'] and current_value > config['alert_threshold']:
                    message['alert'] = True
                    message['alert_type'] = f'high_{sensor_type}'
                    message['alert_value'] = current_value
                    message['alert_threshold'] = config['alert_threshold']
                
                # Envoyer à Kafka
                self.producer.send(topic_name, message)
                record_count += 1
                
                # Afficher la progression toutes les 10 secondes
                if current_time - last_display > 10:
                    elapsed = current_time - start_time
                    remaining = max(0, duration - elapsed)
                    print(f"📨 {record_count:,} messages | "
                          f"Temps: {elapsed:.0f}s/{duration}s | "
                          f"Restant: {remaining:.0f}s")
                    last_display = current_time
                
                # Intervalle entre les messages (simule l'échantillonnage réel)
                # Dans le dataset réel: 5s pour la plupart, 10s pour PIR
                if sensor_type == 'occupancy':
                    time.sleep(0.1)  # Plus rapide pour la simulation
                else:
                    time.sleep(0.05)  # Simulation accélérée
            
            # Finaliser
            self.producer.flush()
            total_time = time.time() - start_time
            
            print("=" * 60)
            print(f"🎉 SIMULATION TEMPS RÉEL TERMINÉE!")
            print(f"✅ Messages générés: {record_count:,}")
            print(f"⏱️ Durée réelle: {total_time:.1f}s")
            print(f"📈 Débit: {record_count/total_time:.1f} msg/s")
            
        except KeyboardInterrupt:
            print("\n⏹️ Simulation interrompue par l'utilisateur")
            self.producer.flush()
            print(f"📤 {record_count} messages envoyés avant interruption")

def main():
    """Fonction principale avec interface utilisateur."""
    print("=" * 70)
    print("🏢 SMART BUILDING - KAFKA PRODUCER FINAL")
    print("📊 Charge le DATASET COMPLET (Mode 1) ou simule temps réel (Mode 2)")
    print("=" * 70)
    
    # Initialisation
    producer = BuildingDataProducer(bootstrap_servers='localhost:29095')
    
    # Connexion à Kafka
    if not producer.connect_kafka():
        print("❌ Impossible de continuer sans connexion Kafka")
        return
    
    # Menu principal
    print("\n🎛️  MENU PRINCIPAL - CHOIX DU MODE")
    print("1. 📜 Mode HISTORIQUE: Stream TOUTES les données du dataset réel")
    print("2. ⚡ Mode TEMPS RÉEL: Simulation réaliste basée sur le dataset")
    print("3. 🔍 Mode TEST: Envoi de quelques messages de test")
    
    try:
        choice = input("\nEntrez votre choix (1, 2 ou 3): ").strip()
        
        if choice == "1":
            # Mode Historique: Charger et streamer le dataset complet
            print("\n" + "=" * 60)
            print("MODE HISTORIQUE - CHARGEMENT DU DATASET")
            print("=" * 60)
            
            # Demander le chemin du dataset
            default_path = "../../data/raw/smart-building-system"
            dataset_path = input(f"Chemin du dataset [défaut: {default_path}]: ").strip()
            dataset_path = dataset_path if dataset_path else default_path
            
            # Charger le dataset
            if producer.load_complete_dataset(dataset_path):
                # Options de streaming
                print("\n⚙️  PARAMÈTRES DE STREAMING")
                speed = input("Facteur d'accélération [défaut: 50]: ").strip()
                speed_factor = int(speed) if speed else 50
                
                topic = input("Topic Kafka [défaut: building-sensors]: ").strip()
                topic_name = topic if topic else 'building-sensors'
                
                # Démarrer le streaming
                producer.stream_historical_data(
                    topic_name=topic_name,
                    speed_factor=speed_factor
                )
            else:
                print("❌ Échec du chargement du dataset")
        
        elif choice == "2":
            # Mode Temps Réel: Simulation
            print("\n" + "=" * 60)
            print("MODE SIMULATION TEMPS RÉEL")
            print("=" * 60)
            
            duration = input("Durée en secondes [défaut: 120]: ").strip()
            duration = int(duration) if duration else 120
            
            topic = input("Topic Kafka [défaut: building-sensors]: ").strip()
            topic_name = topic if topic else 'building-sensors'
            
            producer.stream_live_simulation(
                topic_name=topic_name,
                duration=duration
            )
        
        elif choice == "3":
            # Mode Test: Envoi de quelques messages
            print("\n🧪 MODE TEST")
            test_messages = [
                {
                    'timestamp': datetime.now().isoformat(),
                    'building_id': 'TEST_Building',
                    'room_id': 'TEST_Room',
                    'sensor_type': 'temperature',
                    'value': 22.5,
                    'unit': 'celsius',
                    'source': 'test'
                }
            ]
            
            for i, msg in enumerate(test_messages):
                producer.producer.send('building-sensors', msg)
                print(f"✅ Message test {i+1} envoyé: {msg}")
            
            producer.producer.flush()
            print("🎉 Test terminé - Vérifiez votre consumer Kafka")
        
        else:
            print("❌ Choix invalide. Veuillez choisir 1, 2 ou 3.")
    
    except KeyboardInterrupt:
        print("\n\n👋 Programme interrompu par l'utilisateur")
    except Exception as e:
        print(f"\n❌ Erreur: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Nettoyage
        if producer.producer:
            producer.producer.close()
            print("\n🔌 Producteur Kafka fermé")

if __name__ == "__main__":
    main()