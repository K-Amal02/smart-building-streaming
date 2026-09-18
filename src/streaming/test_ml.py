# test_ml.py
from ml_models import SmartBuildingML
import numpy as np

print("🧪 TEST DU MODÈLE ML")
print("=" * 50)

# 1. Initialiser le modèle
ml = SmartBuildingML()

# 2. Tester la détection d'anomalie
print("\n🔍 Test Détection d'Anomalie:")

# Valeurs normales
normal_values = [22.0, 22.1, 22.3, 22.5, 22.8, 23.0, 23.2, 23.5, 23.7]

for val in normal_values:
    is_anomaly = ml.detect_anomaly('temperature', val)
    print(f"  Température {val}°C → Anomalie: {is_anomaly}")

# Valeur anormale (trop haute)
print("\n🚨 Test avec valeur anormale:")
anomaly_value = 30.0
is_anomaly = ml.detect_anomaly('temperature', anomaly_value)
print(f"  Température {anomaly_value}°C → Anomalie: {is_anomaly}")

# 3. Tester la prédiction
print("\n📈 Test Prédiction:")
for i in range(10):
    val = 22.0 + (i * 0.2)
    ml.detect_anomaly('temperature', val)  # Remplit l'historique

prediction = ml.predict_next('temperature')
print(f"  Prédiction prochaine température: {prediction}°C")

# 4. Tester avec différents capteurs
print("\n🌡️ Test multi-capteurs:")
test_sensors = [
    ('co2', 450),
    ('co2', 460),
    ('co2', 470),
    ('co2', 480),
    ('co2', 1200)  # Anomalie CO2
]

for sensor, value in test_sensors:
    is_anomaly = ml.detect_anomaly(sensor, value)
    print(f"  {sensor.upper()}: {value} → Anomalie: {is_anomaly}")

print("\n" + "=" * 50)
print("✅ Test ML terminé!")