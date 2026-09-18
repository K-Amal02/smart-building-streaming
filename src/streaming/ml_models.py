# ml_models.py
import numpy as np
from collections import deque

class SmartBuildingML:
    """Modèles ML simples pour bâtiment intelligent"""
    
    def __init__(self):
        # Historique pour chaque capteur
        self.history = {
            'temperature': deque(maxlen=50),
            'co2': deque(maxlen=50),
            'humidity': deque(maxlen=50)
        }
    
    def detect_anomaly(self, sensor_type, value, threshold=2.5):
        """Détecte si une valeur est anormale"""
        history = list(self.history.get(sensor_type, []))
        
        if len(history) < 10:  # Pas assez de données
            self.history[sensor_type].append(value)
            return False
        
        # Calcul moyenne et écart-type
        mean = np.mean(history)
        std = np.std(history)
        
        if std == 0:
            self.history[sensor_type].append(value)
            return False
        
        # Score Z (combien d'écarts-types de la moyenne)
        z_score = abs(value - mean) / std
        
        # Mettre à jour l'historique
        self.history[sensor_type].append(value)
        
        return z_score > threshold
    
    def predict_next(self, sensor_type, method='simple'):
        """Prédit la prochaine valeur"""
        history = list(self.history.get(sensor_type, []))
        
        if len(history) < 5:
            return None
        
        if method == 'simple':
            # Moyenne des dernières valeurs
            return np.mean(history[-5:])
        elif method == 'trend':
            # Avec tendance
            if len(history) >= 10:
                recent_trend = np.polyfit(range(5), history[-5:], 1)[0]
                return history[-1] + recent_trend
            return history[-1]
        
        return None