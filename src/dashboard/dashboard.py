"""
dashboard_final.py - Dashboard Smart Building avec thèmes sombre/clair
Design moderne et ergonomique avec visualisations attrayantes.
"""
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from pymongo import MongoClient
from datetime import datetime, timedelta
import time
import altair as alt

# ==================== CONFIGURATION ====================
st.set_page_config(
    page_title="🏢 Smart Building Analytics",
    page_icon="🏢",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==================== THÈMES PERSONNALISÉS ====================
def apply_custom_theme(theme='clair'):
    """Applique un thème visuel personnalisé"""
    
    if theme == 'sombre':
        # Thème sombre
        theme_config = {
            'bg_color': '#0E1117',
            'text_color': '#FAFAFA',
            'card_bg': '#262730',
            'primary': '#00D4AA',
            'secondary': '#FF4B4B',
            'success': '#00D4AA',
            'warning': '#FFA726',
            'border': '#424242'
        }
        chart_template = 'plotly_dark'
    else:
        # Thème clair
        theme_config = {
            'bg_color': '#FFFFFF',
            'text_color': '#31333F',
            'card_bg': '#F0F2F6',
            'primary': '#0068C9',
            'secondary': '#FF4B4B',
            'success': '#00A870',
            'warning': '#FFA726',
            'border': '#E0E0E0'
        }
        chart_template = 'plotly_white'
    
    # CSS personnalisé
    custom_css = f"""
    <style>
        /* Fond principal */
        .stApp {{
            background-color: {theme_config['bg_color']};
            color: {theme_config['text_color']};
        }}
        
        /* Cartes et conteneurs */
        .card {{
            background-color: {theme_config['card_bg']};
            border-radius: 15px;
            padding: 20px;
            border: 1px solid {theme_config['border']};
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
            margin-bottom: 20px;
        }}
        
        /* Titres */
        h1, h2, h3, h4 {{
            color: {theme_config['primary']} !important;
            font-weight: 700;
        }}
        
        /* Boutons */
        .stButton > button {{
            background-color: {theme_config['primary']};
            color: white;
            border-radius: 10px;
            border: none;
            padding: 10px 24px;
            font-weight: 600;
            transition: all 0.3s;
        }}
        
        .stButton > button:hover {{
            background-color: {theme_config['secondary']};
            transform: translateY(-2px);
            box-shadow: 0 6px 12px rgba(0,0,0,0.2);
        }}
        
        /* Métriques */
        [data-testid="stMetricValue"] {{
            font-size: 2.5rem !important;
            font-weight: 700;
            color: {theme_config['primary']};
        }}
        
        /* Sidebar */
        [data-testid="stSidebar"] {{
            background-color: {theme_config['card_bg']};
        }}
        
        /* Séparateurs */
        hr {{
            border-color: {theme_config['border']};
            margin: 2rem 0;
        }}
        
        /* Badges */
        .badge {{
            display: inline-block;
            padding: 4px 12px;
            border-radius: 20px;
            font-size: 0.8rem;
            font-weight: 600;
            margin: 2px;
        }}
        
        .badge-success {{ background-color: {theme_config['success']}; color: white; }}
        .badge-warning {{ background-color: {theme_config['warning']}; color: white; }}
        .badge-danger {{ background-color: {theme_config['secondary']}; color: white; }}
        
        /* Animation de chargement */
        @keyframes pulse {{
            0% {{ opacity: 1; }}
            50% {{ opacity: 0.5; }}
            100% {{ opacity: 1; }}
        }}
        
        .pulse {{
            animation: pulse 2s infinite;
        }}
    </style>
    """
    
    st.markdown(custom_css, unsafe_allow_html=True)
    return theme_config, chart_template

# ==================== INITIALISATION ====================
@st.cache_resource
def init_database():
    """Initialise la connexion MongoDB"""
    try:
        client = MongoClient('localhost', 27017, serverSelectionTimeoutMS=5000)
        client.admin.command('ismaster')
        db = client.smart_buildings
        return db, True
    except Exception as e:
        st.error(f"❌ Impossible de se connecter à MongoDB: {e}")
        return None, False

# ==================== SIDEBAR ====================
with st.sidebar:
    # Logo et titre
    st.markdown("""
    <div style='text-align: center; margin-bottom: 30px;'>
        <h1 style='color: #00D4AA;'>🏢</h1>
        <h3>Smart Building</h3>
        <p style='color: #888; font-size: 0.9rem;'>Dashboard Temps Réel</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Sélection du thème
    st.markdown("### 🎨 Personnalisation")
    theme = st.radio(
        "Thème visuel",
        ["clair ☀️", "sombre 🌙"],
        horizontal=True,
        label_visibility="collapsed"
    )
    
    theme_name = 'sombre' if 'sombre' in theme else 'clair'
    theme_config, chart_template = apply_custom_theme(theme_name)
    
    # Séparateur
    st.markdown("---")
    
    # Filtres
    st.markdown("### 🔧 Filtres")
    
    # Capteur
    sensor_options = {
        "🌡️ Température": "temperature",
        "💨 CO2": "co2", 
        "💧 Humidité": "humidity",
        "👤 Occupation": "occupancy",
        "💡 Lumière": "light"
    }
    
    sensor_display = st.selectbox(
        "Type de capteur",
        list(sensor_options.keys()),
        index=0
    )
    sensor_type = sensor_options[sensor_display]
    
    # Période
    period_options = {
        "🕐 5 min": 5,
        "🕑 15 min": 15,
        "🕒 1 heure": 60,
        "🕓 24 heures": 1440,
        "🕔 7 jours": 10080
    }
    
    period_display = st.selectbox(
        "Période",
        list(period_options.keys()),
        index=1
    )
    period_minutes = period_options[period_display]
    
    # Bâtiment/Pièce (si disponible)
    st.markdown("---")
    st.markdown("### 📍 Localisation")
    show_by_room = st.checkbox("Afficher par pièce", True)
    
    # Stats globales
    st.markdown("---")
    st.markdown("### 📊 Statistiques")
    
    db, db_connected = init_database()
    if db_connected:
        total_messages = db.raw_sensor_data.count_documents({})
        st.metric("Messages totaux", f"{total_messages:,}")
        
        alerts_count = db.raw_sensor_data.count_documents({
            "sensor_type": "temperature",
            "value": {"$gt": 28}
        })
        st.metric("Alertes température", alerts_count)

# ==================== HEADER PRINCIPAL ====================
# Barre de statut en haut
status_cols = st.columns([3, 1, 1])
with status_cols[0]:
    st.markdown(f"<h1>🏢 Smart Building Analytics</h1>", unsafe_allow_html=True)
    st.markdown(f"<p style='color: {theme_config['text_color']}; opacity: 0.8;'>Dashboard temps réel avec Kafka, Spark et MongoDB</p>", unsafe_allow_html=True)

with status_cols[1]:
    st.markdown(f"""
    <div class='card' style='text-align: center; padding: 10px;'>
        <p style='margin: 0; font-size: 0.8rem;'>Thème</p>
        <h4 style='margin: 5px 0;'>{'🌙 Sombre' if theme_name == 'sombre' else '☀️ Clair'}</h4>
    </div>
    """, unsafe_allow_html=True)

with status_cols[2]:
    current_time = datetime.now().strftime("%H:%M:%S")
    st.markdown(f"""
    <div class='card' style='text-align: center; padding: 10px;'>
        <p style='margin: 0; font-size: 0.8rem;'>Heure système</p>
        <h4 style='margin: 5px 0; color: {theme_config['primary']};' class='pulse'>{current_time}</h4>
    </div>
    """, unsafe_allow_html=True)

# ==================== SECTION 1: MÉTRIQUES EN TEMPS RÉEL ====================
st.markdown("## 📈 Vue d'ensemble en temps réel")

if db_connected:
    # Récupérer les dernières données
    time_limit = datetime.now() - timedelta(minutes=period_minutes)
    
    # Pipeline d'agrégation
    pipeline = [
        {"$match": {"kafka_timestamp": {"$gte": time_limit}}},
        {"$group": {
            "_id": "$sensor_type",
            "avg_value": {"$avg": "$value"},
            "max_value": {"$max": "$value"},
            "min_value": {"$min": "$value"},
            "count": {"$sum": 1}
        }}
    ]
    
    try:
        stats_data = list(db.raw_sensor_data.aggregate(pipeline))
        
        # Afficher les métriques par capteur
        cols = st.columns(5)
        
        sensor_icons = {
            "temperature": "🌡️",
            "co2": "💨",
            "humidity": "💧",
            "occupancy": "👤",
            "light": "💡"
        }
        
        sensor_units = {
            "temperature": "°C",
            "co2": "ppm",
            "humidity": "%",
            "occupancy": "",
            "light": "lux"
        }
        
        for stat in stats_data:
            sensor = stat["_id"]
            if sensor in sensor_icons:
                idx = list(sensor_icons.keys()).index(sensor)
                if idx < len(cols):
                    with cols[idx]:
                        st.markdown(f"""
                        <div class='card' style='text-align: center;'>
                            <h2 style='margin: 0;'>{sensor_icons[sensor]}</h2>
                            <h3 style='margin: 10px 0; color: {theme_config['primary']};'>
                                {stat['avg_value']:.1f}{sensor_units[sensor]}
                            </h3>
                            <p style='margin: 0; font-size: 0.9rem;'>{sensor.capitalize()}</p>
                            <div style='margin-top: 10px;'>
                                <span class='badge badge-success'>Max: {stat['max_value']:.1f}</span>
                                <span class='badge badge-warning'>Min: {stat['min_value']:.1f}</span>
                            </div>
                        </div>
                        """, unsafe_allow_html=True)
        
    except Exception as e:
        st.error(f"Erreur de récupération des données: {e}")
else:
    st.warning("⚠️ En attente de connexion à la base de données...")



# AJOUTEZ cette fonction utilitaire AVANT la section des visualisations :
def get_color_with_alpha(hex_color, alpha=0.2):
    """Convertit une couleur hex en rgba pour Plotly"""
    if hex_color.startswith('#'):
        hex_color = hex_color[1:]
    
    if len(hex_color) == 6:
        r = int(hex_color[0:2], 16)
        g = int(hex_color[2:4], 16)
        b = int(hex_color[4:6], 16)
        return f'rgba({r}, {g}, {b}, {alpha})'
    else:
        # Fallback si format invalide
        return f'rgba(0, 104, 201, {alpha})'

# PUIS dans votre graphique (ligne ~352) :
with tab1:
    # Graphique principal
    st.markdown(f"### {sensor_icons.get(sensor_type, '📊')} Évolution du {sensor_type}")
    
    # Récupérer les données du capteur sélectionné
    sensor_data = list(db.raw_sensor_data.find({
        "sensor_type": sensor_type,
        "kafka_timestamp":   {"$gte": time_limit}
    }).sort("kafka_timestamp", 1).limit(1000))
    
    if sensor_data:
        df = pd.DataFrame(sensor_data)
        
        # Créer le graphique Plotly
        fig = go.Figure()
        
        # Calculer la couleur avec transparence
        fill_color = get_color_with_alpha(theme_config['primary'], 0.2)
        
        fig.add_trace(go.Scatter(
            x=df['kafka_timestamp'],
            y=df['value'],
            mode='lines+markers',
            name=sensor_type,
            line=dict(color=theme_config['primary'], width=3),
            marker=dict(size=6),
            fill='tozeroy',
            fillcolor=fill_color  # ← CORRIGÉ !
        ))
        


# ==================== SECTION 2: VISUALISATIONS ====================
st.markdown("## 📊 Visualisations détaillées")

if db_connected and sensor_type:
    # Créer des onglets pour différents types de visualisations
    tab1, tab2, tab3 = st.tabs(["📈 Graphique temps réel", "🏢 Vue par pièce", "📋 Données brutes"])
    
    with tab1:
        # Graphique principal
        st.markdown(f"### {sensor_icons.get(sensor_type, '📊')} Évolution du {sensor_type}")
        
        # Récupérer les données du capteur sélectionné
        sensor_data = list(db.raw_sensor_data.find({
            "sensor_type": sensor_type,
            "kafka_timestamp": {"$gte": time_limit}
        }).sort("kafka_timestamp", 1).limit(1000))
        
        if sensor_data:
            df = pd.DataFrame(sensor_data)
            
            # Créer le graphique Plotly
            fig = go.Figure()
            
            fig.add_trace(go.Scatter(
                x=df['kafka_timestamp'],
                y=df['value'],
                mode='lines+markers',
                name=sensor_type,
                line=dict(color=theme_config['primary'], width=3),
                marker=dict(size=6),
                fill='tozeroy',
                fillcolor=f'{theme_config["primary"]}20'
            ))
            
            # Ajouter une ligne d'alerte si applicable
            alert_thresholds = {
                'temperature': 28,
                'co2': 1200,
                'humidity': 80,
                'light': 800
            }
            
            if sensor_type in alert_thresholds:
                fig.add_hline(
                    y=alert_thresholds[sensor_type],
                    line_dash="dash",
                    line_color=theme_config['secondary'],
                    annotation_text="Seuil d'alerte",
                    annotation_position="top right"
                )
            
            # Mise en forme
            fig.update_layout(
                template=chart_template,
                title=f"Évolution sur {period_display}",
                xaxis_title="Heure",
                yaxis_title=f"Valeur ({sensor_units.get(sensor_type, '')})",
                height=500,
                hovermode="x unified",
                plot_bgcolor=theme_config['card_bg'],
                paper_bgcolor=theme_config['bg_color']
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Statistiques sous le graphique
            if not df.empty:
                stat_cols = st.columns(4)
                metrics = [
                    ("Dernière valeur", df.iloc[-1]['value'], theme_config['primary']),
                    ("Maximum", df['value'].max(), theme_config['secondary']),
                    ("Minimum", df['value'].min(), theme_config['success']),
                    ("Écart-type", df['value'].std(), theme_config['warning'])
                ]
                
                for (name, value, color), col in zip(metrics, stat_cols):
                    with col:
                        st.markdown(f"""
                        <div class='card' style='text-align: center; border-left: 5px solid {color};'>
                            <p style='margin: 0; font-size: 0.9rem;'>{name}</p>
                            <h3 style='margin: 10px 0; color: {color};'>{value:.2f}</h3>
                        </div>
                        """, unsafe_allow_html=True)
        else:
            st.info("📭 Aucune donnée disponible pour cette période")
    
    with tab2:
        # Vue par pièce
        st.markdown(f"### 🏢 Distribution par pièce")
        
        if show_by_room:
            # Agrégation par pièce
            room_pipeline = [
                {"$match": {"sensor_type": sensor_type, "kafka_timestamp": {"$gte": time_limit}}},
                {"$group": {
                    "_id": "$room_id",
                    "avg_value": {"$avg": "$value"},
                    "data_points": {"$sum": 1}
                }},
                {"$sort": {"avg_value": -1}},
                {"$limit": 10}
            ]
            
            room_data = list(db.raw_sensor_data.aggregate(room_pipeline))
            
            if room_data:
                room_df = pd.DataFrame(room_data)
                
                # Graphique à barres
                fig_bar = px.bar(
                    room_df,
                    x='_id',
                    y='avg_value',
                    color='avg_value',
                    color_continuous_scale=[theme_config['primary'], theme_config['secondary']],
                    title=f"Moyenne par pièce - {sensor_type.capitalize()}"
                )
                
                fig_bar.update_layout(
                    template=chart_template,
                    xaxis_title="Pièce",
                    yaxis_title=f"Moyenne ({sensor_units.get(sensor_type, '')})",
                    height=400
                )
                
                st.plotly_chart(fig_bar, use_container_width=True)
                
                # Tableau des pièces
                st.markdown("##### Top 10 des pièces")
                room_df_display = room_df.rename(columns={
                    '_id': 'Pièce',
                    'avg_value': f'Moyenne ({sensor_units.get(sensor_type, "")})',
                    'data_points': 'Points de données'
                })
                
                # Style du tableau
                st.dataframe(
                    room_df_display.style
                    .background_gradient(subset=[f'Moyenne ({sensor_units.get(sensor_type, "")})'], 
                                       cmap='Blues')
                    .format({f'Moyenne ({sensor_units.get(sensor_type, "")})': '{:.2f}'}),
                    use_container_width=True
                )
            else:
                st.info("Aucune donnée par pièce disponible")
    
    with tab3:
        # Données brutes
        st.markdown("### 📋 Données brutes récentes")
        
        raw_data = list(db.raw_sensor_data.find(
            {"sensor_type": sensor_type},
            sort=[("kafka_timestamp", -1)],
            limit=50
        ))
        
        if raw_data:
            raw_df = pd.DataFrame(raw_data)
            
            # Formater les colonnes
            display_cols = ['kafka_timestamp', 'room_id', 'value', 'unit']
            if 'building_id' in raw_df.columns:
                display_cols.append('building_id')
            
            raw_df_display = raw_df[display_cols].copy()
            raw_df_display['kafka_timestamp'] = pd.to_datetime(raw_df_display['kafka_timestamp']).dt.strftime('%H:%M:%S')
            
            # Renommer les colonnes
            raw_df_display = raw_df_display.rename(columns={
                'kafka_timestamp': 'Heure',
                'room_id': 'Pièce',
                'value': 'Valeur',
                'unit': 'Unité',
                'building_id': 'Bâtiment'
            })
            
            # Afficher avec style
            st.dataframe(
                raw_df_display.style
                .applymap(lambda x: f'color: {theme_config["primary"]}' if isinstance(x, (int, float)) else ''),
                use_container_width=True,
                height=400
            )
            
            # Bouton d'export
            csv = raw_df_display.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Télécharger les données (CSV)",
                data=csv,
                file_name=f"smart_building_{sensor_type}_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                mime="text/csv",
            )
        else:
            st.info("Aucune donnée brute disponible")

# ==================== SECTION 3: ALERTES ====================
st.markdown("## 🚨 Alertes et notifications")

if db_connected:
    alert_cols = st.columns(2)
    
    with alert_cols[0]:
        st.markdown("### Alertes actives")
        
        # Détecter les alertes récentes
        alert_thresholds = {
            'temperature': 28,
            'co2': 1200,
            'humidity': 80,
            'light': 800
        }
        
        if sensor_type in alert_thresholds:
            alerts = list(db.raw_sensor_data.find({
                "sensor_type": sensor_type,
                "value": {"$gt": alert_thresholds[sensor_type]},
                "kafka_timestamp": {"$gte": datetime.now() - timedelta(hours=1)}
            }).sort("kafka_timestamp", -1).limit(10))
            
            if alerts:
                for alert in alerts:
                    alert_time = alert['kafka_timestamp'].strftime('%H:%M:%S') if isinstance(alert['kafka_timestamp'], datetime) else 'N/A'
                    
                    st.markdown(f"""
                    <div class='card' style='border-left: 5px solid {theme_config["secondary"]}; margin-bottom: 10px;'>
                        <div style='display: flex; justify-content: space-between; align-items: center;'>
                            <div>
                                <strong style='color: {theme_config["secondary"]};'>⚠️ Alerte {sensor_type}</strong>
                                <p style='margin: 5px 0; font-size: 0.9rem;'>
                                    Pièce: {alert.get('room_id', 'N/A')} | 
                                    Valeur: <strong>{alert['value']:.1f}</strong>
                                </p>
                            </div>
                            <span class='badge badge-danger'>Seuil: {alert_thresholds[sensor_type]}</span>
                        </div>
                        <p style='margin: 0; font-size: 0.8rem; color: #888;'>
                            {alert_time}
                        </p>
                    </div>
                    """, unsafe_allow_html=True)
            else:
                st.markdown(f"""
                <div class='card' style='text-align: center; background-color: {theme_config['success']}20;'>
                    <h3 style='color: {theme_config['success']};'>✅</h3>
                    <p>Aucune alerte détectée pour {sensor_type}</p>
                </div>
                """, unsafe_allow_html=True)
    
    with alert_cols[1]:
        st.markdown("### 🏢 État du système")
        
        # Métriques système
        sys_cols = st.columns(2)
        
        with sys_cols[0]:
            if db_connected:
                total_msgs = db.raw_sensor_data.count_documents({})
                st.markdown(f"""
                <div class='card' style='text-align: center;'>
                    <p style='margin: 0;'>Messages traités</p>
                    <h3 style='color: {theme_config["primary"]}; margin: 10px 0;'>{total_msgs:,}</h3>
                </div>
                """, unsafe_allow_html=True)
        
        with sys_cols[1]:
            # Simulation d'uptime
            uptime_hours = 24  # À remplacer par une vraie métrique
            st.markdown(f"""
            <div class='card' style='text-align: center;'>
                <p style='margin: 0;'>Uptime système</p>
                <h3 style='color: {theme_config["success"]}; margin: 10px 0;'>{uptime_hours}h</h3>
            </div>
            """, unsafe_allow_html=True)
        
        # Barre de progression (simulée)
        st.markdown("##### Charge système")
        system_load = 0.75  # 75%
        
        st.markdown(f"""
        <div style='background-color: {theme_config["border"]}; border-radius: 10px; height: 20px; margin: 10px 0;'>
            <div style='width: {system_load*100}%; 
                        background: linear-gradient(90deg, {theme_config["primary"]}, {theme_config["secondary"]});
                        height: 100%; 
                        border-radius: 10px;'>
            </div>
        </div>
        <div style='display: flex; justify-content: space-between; font-size: 0.9rem;'>
            <span>0%</span>
            <span><strong>{system_load*100:.0f}%</strong></span>
            <span>100%</span>
        </div>
        """, unsafe_allow_html=True)

# ==================== FOOTER ====================
st.markdown("---")

footer_cols = st.columns([2, 1, 1])
with footer_cols[0]:
    st.markdown(f"""
    <div style='color: {theme_config['text_color']}; opacity: 0.7; font-size: 0.9rem;'>
        <p><strong>🏢 Smart Building Dashboard</strong> | Pipeline de Streaming Temps Réel</p>
        <p>Technologies: Kafka, Spark Streaming, MongoDB, Streamlit</p>
    </div>
    """, unsafe_allow_html=True)

with footer_cols[1]:
    refresh_rate = 30  # secondes
    st.markdown(f"""
    <div style='text-align: center;'>
        <p style='font-size: 0.9rem; margin: 0;'>Actualisation</p>
        <p style='font-size: 1.2rem; margin: 5px 0; color: {theme_config['primary']};'>{refresh_rate}s</p>
    </div>
    """, unsafe_allow_html=True)

with footer_cols[2]:
    last_update = datetime.now().strftime("%H:%M:%S")
    st.markdown(f"""
    <div style='text-align: center;'>
        <p style='font-size: 0.9rem; margin: 0;'>Dernière mise à jour</p>
        <p style='font-size: 1.2rem; margin: 5px 0; color: {theme_config['success']};'>{last_update}</p>
    </div>
    """, unsafe_allow_html=True)

# ==================== AUTO-REFRESH ====================
if st.sidebar.checkbox("🔄 Auto-refresh (30s)", True, key="autorefresh"):
    time.sleep(30)
    st.rerun()

# ==================== BOUTON DE RECHARGE MANUEL ====================
if st.sidebar.button("🔄 Recharger les données", use_container_width=True):
    st.rerun()

# ==================== MODE DÉMO ====================
with st.sidebar.expander("🎯 Mode démonstration"):
    st.markdown("""
    **Fonctionnalités démo:**
    
    🎨 **Thèmes:** 
    - ☀️ Clair: Interface lumineuse
    - 🌙 Sombre: Interface nocturne
    
    📊 **Visualisations:**
    - Graphiques temps réel interactifs
    - Vue par pièce détaillée
    - Données brutes exportables
    
    🚨 **Alertes:**
    - Détection automatique des dépassements
    - Notifications en temps réel
    
    📈 **Métriques:**
    - Statistiques par capteur
    - État du système
    - Performance du pipeline
    """)
    
    # Simuler une alerte pour la démo
    if st.button("🚨 Simuler une alerte", use_container_width=True):
        st.session_state.demo_alert = True
        st.success("Alerte de démo activée!")