# Pipeline de Streaming pour Bâtiments Intelligents

## 📋 Description
Projet de pipeline de streaming temps réel pour la surveillance des bâtiments intelligents utilisant Kafka, Spark Streaming et MongoDB.

## 🏗️ Architecture
- **Data Source**: [Dataset Smart Building System (Kaggle)] (https://www.kaggle.com/datasets/ranakrc/smart-building-system)
- **Streaming**: Apache Kafka + Spark Streaming
- **Storage**: MongoDB
- **Dashboard**: Streamlit

## 🚀 Installation
1- Cloner le Repo
```bash
git clone https://github.com/K-Amal02/smart-building-streaming.git
cd smart-building-streaming
```

2- Env Python
```bash
python -m venv venv
venv\Scripts\activate     #windows
pip install -r requirements.txt
```

3- Infrastructure Docker
```bash
cd docker
docker-compose up -d
```

4- Configuration Hadoop (Windows):
Spark nécessite 'winutils.exe' sous Windows. Télécharger 'winutils.exe' et 'hadoop.dll' et les placer dans `C:\hadoop\bin`. Définir avant chaque lancement :
```bash
set HADOOP_HOME=C:\hadoop
set PATH=%HADOOP_HOME%\bin;%PATH%
```

5- Lancement 
Dans 3 terminaux séparés :
```bash
# Terminal 1: Spark Streaming (traitement + ML + écriture MongoDB)
cd src/streaming
python spark_simple.py

# Terminal 2: Producer Kafka (génère/simule les données capteurs)
cd src/streaming
python data_producer.py

# Terminal 3: Dashboard
cd src/dashboard
streamlit run dashboard.py
```
