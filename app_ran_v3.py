from flask import Flask
from dash import Dash, dcc, html, dash_table
import pandas as pd
import numpy as np
import plotly.express as px
import psycopg2

# Conectar a la base de datos PostgreSQL
def get_db_connection():
    conn = psycopg2.connect(
        dbname='traffic_data', 
        user='postgres', 
        password='jean2024', 
        host='localhost', 
        port='5432'  # Cambia si es necesario
    )
    return conn

# Funciones de asignación
def assign_percentage_range(prb_percentage):
    ranges = [(0, 5), (5, 10), (10, 15), (15, 20), (20, 25), (25, 30),
              (30, 35), (35, 40), (40, 45), (45, 50), (50, 55), (55, 60),
              (60, 65), (65, 70), (70, 75), (75, 80), (80, 85), (85, 90),
              (90, 95), (95, 100)]
    
    for r in ranges:
        if r[0] <= prb_percentage < r[1]:
            return f"{r[0]} - {r[1]}"
    return "Unknown"

def assign_status(prb_percentage):
    if prb_percentage > 80:
        return "Sobrecargada"
    elif 0 <= prb_percentage <= 20:
        return "Subutilizada"
    elif 20 < prb_percentage <= 80:
        return "Ok"
    else:
        return "Unknown"

def load_and_process_data():
    conn = get_db_connection()
    cursor = conn.cursor()

    # Obtener nombres únicos de la columna node_name
    cursor.execute("SELECT DISTINCT node_name FROM prueba_data")
    unique_node_names = cursor.fetchall()

    avg_prb_df = pd.DataFrame()  # DataFrame para almacenar los promedios por node_name

    for (node_name,) in unique_node_names:
        # Ejecutar query para cada node_name
        cursor.execute("SELECT * FROM prueba_data WHERE node_name = %s", (node_name,))
        data = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]  # Obtener nombres de columnas

        # Crear DataFrame a partir de los datos
        df = pd.DataFrame(data, columns=columns)

        # Procesamiento de datos
        df["timestamp"] = pd.to_datetime(df["timestamp"], format="%Y-%m-%d %H:%M:%S.%f")
        
        # Crear nuevo DataFrame con los datos procesados
        new_df = pd.DataFrame({
            "Timestamp": df["timestamp"],
            "Week": df['timestamp'].dt.strftime('%Y-%U'),
            "Node": df["cell_name"].apply(lambda x: "_".join(x.split("_")[:-2])),
            "Cell": df["cell_name"].apply(lambda x: "_".join(x.split("_")[-2:])),
            "PRB_Usage_Percentage": (df["l_chmeas_prb_dl_used_avg"] / df["l_chmeas_prb_dl_avail"]) * 100
        })

        new_df["PRB_Usage_Percentage"] = pd.to_numeric(new_df["PRB_Usage_Percentage"], errors='coerce')

        # Filtrar el 25% superior
        def filter_top_25_percent(group):
            group_sorted = group.sort_values(by="PRB_Usage_Percentage", ascending=False)
            return group_sorted.head(int(0.25 * len(group_sorted)))

        # Aplicar filtro y calcular promedio
        top_25_df = filter_top_25_percent(new_df)
        avg_prb_node = top_25_df.groupby(["Node", "Cell"], as_index=False)["PRB_Usage_Percentage"].mean()
        
        # Asignar rangos y estados
        avg_prb_node["Percentage_Range (%)"] = avg_prb_node["PRB_Usage_Percentage"].apply(assign_percentage_range)
        avg_prb_node["Status"] = avg_prb_node["PRB_Usage_Percentage"].apply(assign_status)

        # Concatenar los resultados de este node_name en avg_prb_df
        avg_prb_df = pd.concat([avg_prb_df, avg_prb_node], ignore_index=True)

    return new_df, avg_prb_df


# Flask App
server = Flask(__name__)

# Dash App
app = Dash(__name__, server=server)

# Cargar y procesar datos
new_df, average_prb_percentage = load_and_process_data()

# Contar celdas por estado
count_ok = len(average_prb_percentage[average_prb_percentage['Status'] == 'Ok'])
count_sobrecargada = len(average_prb_percentage[average_prb_percentage['Status'] == 'Sobrecargada'])
count_subutilizada = len(average_prb_percentage[average_prb_percentage['Status'] == 'Subutilizada'])
count_total = len(average_prb_percentage)

# Gráfico de dona para la distribución de rangos
fig = px.pie(average_prb_percentage, names='Percentage_Range (%)', title="Distribución de PRB por Rango", hole=.5)

# Dash Layout
app.layout = html.Div([
    html.H1("MONITOREO DE CELDAS RADIO ACCESO", style={'textAlign': 'center'}),
    
    # Tarjetas de resumen
    html.Div([
        html.Div([
            html.H3(f"{count_ok}", style={'color': 'green'}),
            html.P("CELDAS OK")
        ], className="card"),
        
        html.Div([
            html.H3(f"{count_sobrecargada}", style={'color': 'red'}),
            html.P("CELDAS SOBRECARGADAS")
        ], className="card"),
        
        html.Div([
            html.H3(f"{count_subutilizada}", style={'color': 'orange'}),
            html.P("CELDAS SUBUTILIZADAS")
        ], className="card"),
        
        html.Div([
            html.H3(f"{count_total}", style={'color': 'blue'}),
            html.P("TOTAL CELDAS")
        ], className="card"),
    ], className="card-container"),
    
    # Sección con gráfica y tabla
    html.Div([
        html.Div([
            dcc.Graph(figure=fig)
        ], className="six columns"),
        
        html.Div([
            dash_table.DataTable(
                id='table',
                columns=[{"name": i, "id": i} for i in average_prb_percentage.columns],
                data=average_prb_percentage.to_dict('records'),
                style_table={'overflowX': 'auto'},
                style_header={
                    'backgroundColor': 'rgb(30, 30, 30)',
                    'color': 'white'
                },
                style_cell={
                    'backgroundColor': 'rgb(50, 50, 50)',
                    'color': 'white'
                },
            )
        ], className="six columns"),
    ], className="row"),
    
    html.Div([
        html.Button("ACTUALIZAR", id="btn-update", n_clicks=0, className="btn"),
        html.Button("DESCARGAR REPORTE", id="btn-download", className="btn")
    ], className="actions")
])

# Run Flask and Dash
if __name__ == '__main__':
    app.run_server(debug=True)

