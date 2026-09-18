"""
dashboard_smart_building.py - Dashboard Smart Building complet
Design moderne avec thèmes sombre/clair corrigés
"""
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from pymongo import MongoClient
from datetime import datetime, timedelta
import time

# ==================== CONFIGURATION ====================
st.set_page_config(
    page_title="🏢 Smart Building Analytics",
    page_icon="🏢",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==================== FONCTIONS UTILITAIRES ====================
def hex_to_rgba(hex_color, alpha=1.0):
    """Convertit une couleur hex en rgba pour Plotly"""
    if hex_color.startswith('#'):
        hex_color = hex_color[1:]
    
    if len(hex_color) == 6:
        r = int(hex_color[0:2], 16)
        g = int(hex_color[2:4], 16)
        b = int(hex_color[4:6], 16)
        return f'rgba({r}, {g}, {b}, {alpha})'
    return f'rgba(0, 104, 201, {alpha})'  # Fallback

def apply_custom_theme(theme='clair'):
    """Applique un thème visuel personnalisé avec couleurs corrigées"""
    
    if theme == 'sombre':
        # Thème sombre - couleurs modifiées (bleu/blanc au lieu de vert)
        theme_config = {
            'bg_color': '#0E1117',
            'text_color': '#FFFFFF',  # Blanc au lieu de vert
            'card_bg': '#1E1E2E',
            'primary': '#0080FF',     # Bleu vif au lieu de vert
            'secondary': '#FF6B6B',
            'success': '#4ECDC4',
            'warning': '#FFD166',
            'info': '#06D6A0',
            'border': '#424657'
        }
        chart_template = 'plotly_dark'
    else:
        # Thème clair
        theme_config = {
            'bg_color': '#FFFFFF',
            'text_color': '#2C3E50',  # Bleu foncé
            'card_bg': '#F8F9FA',
            'primary': '#0068C9',
            'secondary': '#FF4B4B',
            'success': '#00A870',
            'warning': '#FFA726',
            'info': '#00C1D4',
            'border': '#E0E6ED'
        }
        chart_template = 'plotly_white'
    
    # CSS personnalisé
    custom_css = f"""
    <style>
        .stApp {{
            background-color: {theme_config['bg_color']};
            color: {theme_config['text_color']};
        }}
        
        .card {{
            background-color: {theme_config['card_bg']};
            border-radius: 12px;
            padding: 20px;
            border: 1px solid {theme_config['border']};
            box-shadow: 0 4px 12px rgba(0,0,0,0.08);
            margin-bottom: 20px;
        }}
        
        h1, h2, h3, h4 {{
            color: {theme_config['primary']} !important;
            font-weight: 700;
        }}
        
        .stButton > button {{
            background: linear-gradient(90deg, {theme_config['primary']}, {theme_config['info']});
            color: white;
            border-radius: 8px;
            border: none;
            padding: 10px 20px;
            font-weight: 600;
            transition: all 0.3s;
        }}
        
        .stButton > button:hover {{
            transform: translateY(-2px);
            box-shadow: 0 6px 20px rgba(0,0,0,0.15);
        }}
        
        [data-testid="stMetricValue"] {{
            font-size: 2.2rem !important;
            font-weight: 700;
            color: {theme_config['primary']};
        }}
        
        [data-testid="stMetricLabel"] {{
            color: {theme_config['text_color']} !important;
            font-weight: 500;
        }}
        
        .badge {{
            display: inline-block;
            padding: 4px 10px;
            border-radius: 16px;
            font-size: 0.8rem;
            font-weight: 600;
            margin: 2px;
        }}
        
        .badge-primary {{ background-color: {theme_config['primary']}20; color: {theme_config['primary']}; }}
        .badge-success {{ background-color: {theme_config['success']}20; color: {theme_config['success']}; }}
        .badge-warning {{ background-color: {theme_config['warning']}20; color: {theme_config['warning']}; }}
        .badge-danger {{ background-color: {theme_config['secondary']}20; color: {theme_config['secondary']}; }}
    </style>
    """
    
    st.markdown(custom_css, unsafe_allow_html=True)
    return theme_config, chart_template

@st.cache_resource
def init_database():
    """Initialise la connexion MongoDB"""
    try:
        client = MongoClient(
            'mongodb://admin:password@localhost:27018/',
            authSource='admin',
            serverSelectionTimeoutMS=5000
        )
        client.admin.command('ismaster')
        db = client.smart_buildings
        st.success("✅ Connexion MongoDB établie")
        return db
    except Exception as e:
        st.error(f"❌ Erreur MongoDB: {e}")
        return None

def get_sensor_data(db, sensor_type, time_limit):
    """Récupère les données de capteur"""
    try:
        data = list(db.raw_sensor_data.find({
            "sensor_type": sensor_type,
            "kafka_timestamp": {"$gte": time_limit}
        }).sort("kafka_timestamp", 1).limit(1000))
        return data
    except Exception as e:
        st.error(f"Erreur récupération données: {e}")
        return []

def create_time_series_chart(df, sensor_type, theme_config, chart_template):
    """Crée un graphique temporel"""
    if df.empty:
        return None
    
    fig = go.Figure()
    
    # Couleur de remplissage correcte
    fill_color = hex_to_rgba(theme_config['primary'], 0.2)
    
    fig.add_trace(go.Scatter(
        x=df['kafka_timestamp'],
        y=df['value'],
        mode='lines+markers',
        name=sensor_type.capitalize(),
        line=dict(color=theme_config['primary'], width=3),
        marker=dict(size=5),
        fill='tozeroy',
        fillcolor=fill_color  # Correction ici
    ))
    
    # Ajouter des lignes de seuil si définies
    thresholds = {
        'temperature': {'alert': 28, 'warning': 25},
        'co2': {'alert': 1200, 'warning': 1000},
        'humidity': {'alert': 80, 'warning': 70}
    }
    
    if sensor_type in thresholds:
        # Ligne d'alerte
        fig.add_hline(
            y=thresholds[sensor_type]['alert'],
            line_dash="dash",
            line_color=theme_config['secondary'],
            annotation_text="Seuil alerte",
            annotation_position="top right"
        )
        # Ligne d'avertissement
        fig.add_hline(
            y=thresholds[sensor_type]['warning'],
            line_dash="dot",
            line_color=theme_config['warning'],
            annotation_text="Seuil avertissement"
        )
    
    fig.update_layout(
        template=chart_template,
        title=f"Évolution {sensor_type}",
        xaxis_title="Temps",
        yaxis_title=f"Valeur ({get_unit(sensor_type)})",
        height=450,
        hovermode="x unified",
        plot_bgcolor=theme_config['card_bg'],
        paper_bgcolor=theme_config['bg_color'],
        font=dict(color=theme_config['text_color'])
    )
    
    return fig

def get_unit(sensor_type):
    """Retourne l'unité de mesure"""
    units = {
        'temperature': '°C',
        'humidity': '%',
        'co2': 'ppm',
        'light': 'lux',
        'pir': '',
        'occupancy': 'pers'
    }
    return units.get(sensor_type, '')

# ==================== SIDEBAR ====================
with st.sidebar:
    # Logo
    st.markdown("""
    <div style='text-align: center; margin-bottom: 30px;'>
        <h1 style='color: #0080FF; font-size: 3rem;'>🏢</h1>
        <h3 style='color: #FFFFFF;'>Smart Building</h3>
        <p style='color: #888;'>Dashboard Temps Réel</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Sélection thème
    st.markdown("### 🎨 Thème")
    theme = st.radio(
        "Sélectionnez le thème",
        ["clair ☀️", "sombre 🌙"],
        horizontal=True
    )
    theme_name = 'sombre' if 'sombre' in theme else 'clair'
    theme_config, chart_template = apply_custom_theme(theme_name)
    
    st.markdown("---")
    
    # Filtres
    st.markdown("### 🔧 Filtres")
    
    sensor_options = {
        "🌡️ Température": "temperature",
        "💨 CO2": "co2", 
        "💧 Humidité": "humidity",
        "👤 Occupation": "occupancy",
        "💡 Lumière": "light"
    }
    
    selected_sensor = st.selectbox(
        "Capteur",
        list(sensor_options.keys()),
        index=0
    )
    sensor_type = sensor_options[selected_sensor]
    
    period_options = {
        "5 minutes": 5,
        "15 minutes": 15,
        "1 heure": 60,
        "24 heures": 1440
    }
    
    selected_period = st.selectbox(
        "Période",
        list(period_options.keys()),
        index=1
    )
    period_minutes = period_options[selected_period]
    
    st.markdown("---")
    
    # Connexion DB
    db = init_database()
    db_connected = db is not None
    
    if db_connected:
        st.markdown("### 📊 Statistiques")
        try:
            total_count = db.raw_sensor_data.count_documents({})
            st.metric("Messages totaux", f"{total_count:,}")
            
            sensor_count = db.raw_sensor_data.distinct("sensor_type")
            st.metric("Types capteurs", len(sensor_count))
        except:
            pass

# ==================== HEADER ====================
col1, col2, col3 = st.columns([3, 1, 1])

with col1:
    st.markdown(f"<h1>🏢 Smart Building Analytics</h1>", unsafe_allow_html=True)
    st.markdown(f"<p style='color: {theme_config['text_color']}80;'>Surveillance temps réel des capteurs IoT</p>", unsafe_allow_html=True)

with col2:
    st.markdown(f"""
    <div class='card' style='text-align: center; padding: 10px;'>
        <p style='margin: 0; font-size: 0.8rem;'>Thème</p>
        <h4 style='margin: 5px 0;'>{'🌙 Sombre' if theme_name == 'sombre' else '☀️ Clair'}</h4>
    </div>
    """, unsafe_allow_html=True)

with col3:
    current_time = datetime.now().strftime("%H:%M:%S")
    st.markdown(f"""
    <div class='card' style='text-align: center; padding: 10px;'>
        <p style='margin: 0; font-size: 0.8rem;'>Heure</p>
        <h4 style='margin: 5px 0; color: {theme_config['primary']};'>{current_time}</h4>
    </div>
    """, unsafe_allow_html=True)

# ==================== MÉTRIQUES TEMPS RÉEL ====================
st.markdown("## 📈 Vue d'ensemble")

if db_connected:
    time_limit = datetime.now() - timedelta(minutes=period_minutes)
    
    # Métriques par capteur
    cols = st.columns(5)
    
    sensor_metrics = {}
    for sensor in ['temperature', 'co2', 'humidity', 'occupancy', 'light']:
        try:
            data = list(db.raw_sensor_data.find({
                "sensor_type": sensor,
                "kafka_timestamp": {"$gte": time_limit}
            }).limit(100))
            
            if data:
                df_temp = pd.DataFrame(data)
                sensor_metrics[sensor] = {
                    'avg': df_temp['value'].mean(),
                    'max': df_temp['value'].max(),
                    'min': df_temp['value'].min(),
                    'count': len(df_temp)
                }
        except:
            pass
    
    # Affichage des métriques
    sensor_icons = {
        'temperature': '🌡️',
        'co2': '💨',
        'humidity': '💧',
        'occupancy': '👤',
        'light': '💡'
    }
    
    for idx, (sensor, icon) in enumerate(sensor_icons.items()):
        if idx < len(cols):
            with cols[idx]:
                if sensor in sensor_metrics:
                    avg_val = sensor_metrics[sensor]['avg']
                    st.markdown(f"""
                    <div class='card' style='text-align: center;'>
                        <h2 style='margin: 5px 0;'>{icon}</h2>
                        <h3 style='margin: 10px 0; color: {theme_config['primary']};'>
                            {avg_val:.1f}{get_unit(sensor)}
                        </h3>
                        <p style='margin: 0; font-size: 0.9rem;'>{sensor.capitalize()}</p>
                        <div style='margin-top: 10px; font-size: 0.8rem;'>
                            <span class='badge badge-primary'>Max: {sensor_metrics[sensor]['max']:.1f}</span>
                            <span class='badge badge-warning'>Min: {sensor_metrics[sensor]['min']:.1f}</span>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                else:
                    st.markdown(f"""
                    <div class='card' style='text-align: center; opacity: 0.6;'>
                        <h2>{icon}</h2>
                        <p>{sensor.capitalize()}</p>
                        <p style='font-size: 0.8rem;'>Aucune donnée</p>
                    </div>
                    """, unsafe_allow_html=True)

# ==================== VISUALISATIONS PRINCIPALES ====================
st.markdown("## 📊 Visualisations")

if db_connected:
    tab1, tab2, tab3 = st.tabs(["📈 Graphique temps réel", "🏢 Vue par localisation", "📋 Données brutes"])
    
    with tab1:
        # Graphique principal
        sensor_data = get_sensor_data(db, sensor_type, time_limit)
        
        if sensor_data:
            df = pd.DataFrame(sensor_data)
            if not df.empty:
                fig = create_time_series_chart(df, sensor_type, theme_config, chart_template)
                if fig:
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Statistiques
                    stat_cols = st.columns(4)
                    stats = [
                        ("Dernière valeur", df.iloc[-1]['value'] if len(df) > 0 else 0),
                        ("Maximum", df['value'].max()),
                        ("Minimum", df['value'].min()),
                        ("Moyenne", df['value'].mean())
                    ]
                    
                    for (name, value), col in zip(stats, stat_cols):
                        with col:
                            st.markdown(f"""
                            <div class='card' style='text-align: center;'>
                                <p style='margin: 0; font-size: 0.9rem;'>{name}</p>
                                <h3 style='margin: 10px 0; color: {theme_config['primary']};'>{value:.2f}{get_unit(sensor_type)}</h3>
                            </div>
                            """, unsafe_allow_html=True)
            else:
                st.info(f"📭 Aucune donnée disponible pour {sensor_type}")
        else:
            st.info(f"📭 Aucune donnée disponible pour {sensor_type}")
    
    with tab2:
        # Vue par localisation
        try:
            # Vérifier quelles colonnes sont disponibles
            sample = db.raw_sensor_data.find_one({"sensor_type": sensor_type})
            if sample:
                st.info(f"Colonnes disponibles: {list(sample.keys())}")
                
                # Essayer différentes colonnes de localisation
                location_fields = ['room_id', 'location', 'building_id', 'room']
                location_field = None
                
                for field in location_fields:
                    if field in sample:
                        location_field = field
                        break
                
                if location_field:
                    # Agrégation par localisation
                    pipeline = [
                        {"$match": {"sensor_type": sensor_type, "kafka_timestamp": {"$gte": time_limit}}},
                        {"$group": {
                            "_id": f"${location_field}",
                            "avg_value": {"$avg": "$value"},
                            "count": {"$sum": 1}
                        }},
                        {"$sort": {"avg_value": -1}}
                    ]
                    
                    location_data = list(db.raw_sensor_data.aggregate(pipeline))
                    
                    if location_data:
                        location_df = pd.DataFrame(location_data)
                        
                        # Graphique
                        fig = px.bar(
                            location_df,
                            x='_id',
                            y='avg_value',
                            color='avg_value',
                            color_continuous_scale=[theme_config['primary'], theme_config['secondary']],
                            title=f"{sensor_type.capitalize()} par {location_field}"
                        )
                        
                        fig.update_layout(
                            template=chart_template,
                            xaxis_title=location_field.capitalize(),
                            yaxis_title=f"Moyenne ({get_unit(sensor_type)})",
                            height=400
                        )
                        
                        st.plotly_chart(fig, use_container_width=True)
                        
                        # Tableau
                        st.dataframe(
                            location_df.rename(columns={'_id': 'Localisation', 'avg_value': 'Moyenne', 'count': 'Points'}),
                            use_container_width=True
                        )
                    else:
                        st.info("Aucune donnée par localisation")
                else:
                    st.info("Aucune information de localisation disponible")
            else:
                st.info("Aucun échantillon de données disponible")
        except Exception as e:
            st.error(f"Erreur: {e}")
    
    with tab3:
        # Données brutes
        raw_data = list(db.raw_sensor_data.find(
            {"sensor_type": sensor_type},
            sort=[("kafka_timestamp", -1)],
            limit=100
        ))
        
        if raw_data:
            raw_df = pd.DataFrame(raw_data)
            
            # Identifier les colonnes à afficher
            available_cols = raw_df.columns.tolist()
            display_cols = ['kafka_timestamp', 'value']
            
            # Ajouter les colonnes supplémentaires disponibles
            for col in ['room_id', 'building_id', 'location', 'sensor_id', 'unit']:
                if col in available_cols:
                    display_cols.append(col)
            
            # Sélectionner uniquement les colonnes existantes
            existing_cols = [col for col in display_cols if col in available_cols]
            
            if existing_cols:
                raw_df_display = raw_df[existing_cols].copy()
                
                # Formater la date
                if 'kafka_timestamp' in raw_df_display.columns:
                    raw_df_display['kafka_timestamp'] = pd.to_datetime(
                        raw_df_display['kafka_timestamp']
                    ).dt.strftime('%H:%M:%S')
                
                # Renommer
                rename_map = {
                    'kafka_timestamp': 'Heure',
                    'value': 'Valeur',
                    'room_id': 'Pièce',
                    'building_id': 'Bâtiment',
                    'location': 'Localisation',
                    'sensor_id': 'ID Capteur',
                    'unit': 'Unité'
                }
                
                raw_df_display = raw_df_display.rename(columns=rename_map)
                
                # Afficher
                st.dataframe(raw_df_display, use_container_width=True, height=400)
                
                # Export CSV
                csv = raw_df_display.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 Télécharger CSV",
                    data=csv,
                    file_name=f"smart_building_{sensor_type}.csv",
                    mime="text/csv",
                )
            else:
                st.info("Aucune colonne disponible à afficher")
        else:
            st.info("📭 Aucune donnée brute disponible")

# ==================== ALERTES ====================
st.markdown("## 🚨 Alertes")

if db_connected:
    alert_col1, alert_col2 = st.columns(2)
    
    with alert_col1:
        st.markdown("### Alertes récentes")
        
        # Détecter les alertes
        alert_rules = {
            'temperature': {'min': 16, 'max': 28},
            'co2': {'max': 1200},
            'humidity': {'min': 30, 'max': 70}
        }
        
        if sensor_type in alert_rules:
            rules = alert_rules[sensor_type]
            query = {"sensor_type": sensor_type}
            
            conditions = []
            if 'min' in rules:
                conditions.append({"value": {"$lt": rules['min']}})
            if 'max' in rules:
                conditions.append({"value": {"$gt": rules['max']}})

            query = {"sensor_type": sensor_type}
            if conditions:
                query["$or"] = conditions
                
            query["kafka_timestamp"] = {"$gte": datetime.now() - timedelta(hours=1)}
            
            alerts = list(db.raw_sensor_data.find(query).sort("kafka_timestamp", -1).limit(5))
            
            if alerts:
                for alert in alerts:
                    alert_time = alert['kafka_timestamp'].strftime('%H:%M') if isinstance(alert['kafka_timestamp'], datetime) else 'N/A'
                    
                    st.markdown(f"""
                    <div class='card' style='border-left: 4px solid {theme_config["secondary"]}; margin-bottom: 10px;'>
                        <div style='display: flex; justify-content: space-between;'>
                            <div>
                                <strong>⚠️ {sensor_type.capitalize()}</strong>
                                <p style='margin: 5px 0;'>
                                    Valeur: <strong>{alert['value']:.1f}{get_unit(sensor_type)}</strong>
                                </p>
                            </div>
                            <span class='badge badge-danger'>Alerte</span>
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
                    <p>Aucune alerte pour {sensor_type}</p>
                </div>
                """, unsafe_allow_html=True)
    
    with alert_col2:
        st.markdown("### État système")
        
        sys_cols = st.columns(2)
        
        with sys_cols[0]:
            uptime = 99.8  # %
            st.markdown(f"""
            <div class='card' style='text-align: center;'>
                <p style='margin: 0;'>Disponibilité</p>
                <h3 style='color: {theme_config['success']}; margin: 10px 0;'>{uptime}%</h3>
            </div>
            """, unsafe_allow_html=True)
        
        with sys_cols[1]:
            latency = "120ms"
            st.markdown(f"""
            <div class='card' style='text-align: center;'>
                <p style='margin: 0;'>Latence</p>
                <h3 style='color: {theme_config['primary']}; margin: 10px 0;'>{latency}</h3>
            </div>
            """, unsafe_allow_html=True)

# ==================== FOOTER ====================
st.markdown("---")

footer_cols = st.columns(3)

with footer_cols[0]:
    st.markdown(f"""
    <div style='color: {theme_config['text_color']}80; font-size: 0.9rem;'>
        <p><strong>🏢 Smart Building Dashboard</strong></p>
        <p>Version 2.0 • {datetime.now().strftime('%d/%m/%Y')}</p>
    </div>
    """, unsafe_allow_html=True)

with footer_cols[1]:
    refresh_rate = 30
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
if st.sidebar.checkbox("🔄 Auto-refresh (30s)", value=True):
    time.sleep(30)
    st.rerun()

# ==================== BOUTON RECHARGE ====================
if st.sidebar.button("🔄 Recharger maintenant", use_container_width=True):
    st.rerun()