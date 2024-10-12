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
def assign_percentage_range(prb_percentage): ## justificar rangos, buscar el por que
    if prb_percentage == 0:
        return "0 - 5"  # Manejar el caso de 0 explícitamente

    ranges = [
        (0, 5), (5, 10), (10, 15), (15, 20), (20, 25), (25, 30),
        (30, 35), (35, 40), (40, 45), (45, 50), (50, 55), (55, 60),
        (60, 65), (65, 70), (70, 75), (75, 80), (80, 85), (85, 90),
        (90, 95), (95, 100)
    ]
    
    for r in ranges:
        if r[0] < prb_percentage <= r[1]:  # Cambiar < a <= para incluir el límite superior
            return f"{r[0]} - {r[1]}"
    return "Unknown"

def assign_status(prb_percentage):
    if prb_percentage > 80:
        return "Sobrecargada"
    elif prb_percentage == 0:
        return "Subutilizada"  # Manejar el caso de 0 explícitamente
    elif 0 < prb_percentage <= 20:
        return "Subutilizada"  # También puedes incluir esta línea
    elif 20 < prb_percentage <= 80:
        return "Ok"
    else:
        return "Unknown"

def load_and_process_data():
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT DISTINCT node_name FROM prueba_data")
    unique_node_names = cursor.fetchall()

    avg_prb_df = pd.DataFrame()
    unique_combinations = set()

    for (node_name,) in unique_node_names:
        cursor.execute("SELECT * FROM prueba_data WHERE node_name = %s", (node_name,))
        data = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]

        df = pd.DataFrame(data, columns=columns)

        # Convertir el timestamp a tipo datetime
        df["timestamp"] = pd.to_datetime(df["timestamp"], format="%Y-%m-%d %H:%M:%S.%f")

        # Crear nuevo DataFrame
        new_df = pd.DataFrame({
            "Timestamp": df["timestamp"],
            "Week": df['timestamp'].dt.strftime('%Y-%U'),
            "Node": df["cell_name"].apply(lambda x: "_".join(x.split("_")[:-2])),
            "Cell": df["cell_name"].apply(lambda x: "_".join(x.split("_")[-2:])),
            "PRB_Usage_Percentage": np.where(
                (df["l_chmeas_prb_dl_avail"] == 0) | (df["l_chmeas_prb_dl_used_avg"] == 0), 
                0, 
                (df["l_chmeas_prb_dl_used_avg"] / df["l_chmeas_prb_dl_avail"]) * 100
            )
        })

        # Asegurarte de que el tipo de datos sea correcto
        new_df["PRB_Usage_Percentage"] = pd.to_numeric(new_df["PRB_Usage_Percentage"], errors='coerce').fillna(0)
        new_df["PRB_Usage_Percentage"].replace([np.inf, -np.inf], 0, inplace=True)

        # Agregar combinaciones únicas
        for _, row in new_df.iterrows():
            unique_combinations.add((row["Node"], row["Cell"]))

        def filter_top_25_percent(group):
            group = group.copy()  # Hacer una copia del grupo para evitar problemas de referencia
            group["PRB_Usage_Percentage"] = group["PRB_Usage_Percentage"].fillna(0)
            if len(group) < 4:
                return group  # Devolver el grupo original sin copia
            group_sorted = group.sort_values(by="PRB_Usage_Percentage", ascending=False)
            return group_sorted.head(int(0.25 * len(group_sorted)))  # Devolver solo las filas necesarias

        # Filtrar el top 25%
        top_25_df = filter_top_25_percent(new_df)

        # Agrupar y calcular el promedio
        avg_prb_node = top_25_df.groupby(["Node", "Cell"], as_index=False)["PRB_Usage_Percentage"].mean()

        avg_prb_node["Percentage_Range (%)"] = avg_prb_node["PRB_Usage_Percentage"].apply(assign_percentage_range)
        avg_prb_node["Status"] = avg_prb_node["PRB_Usage_Percentage"].apply(assign_status)

        # Concatenar los resultados
        avg_prb_df = pd.concat([avg_prb_df, avg_prb_node], ignore_index=True)

    all_combinations_df = pd.DataFrame(
        list(unique_combinations),
        columns=["Node", "Cell"]
    )
    all_combinations_df["PRB_Usage_Percentage"] = 0  

    # Merge sin generar copias
    avg_prb_df = pd.merge(all_combinations_df, avg_prb_df, on=["Node", "Cell"], how="left", suffixes=('', '_y'))

    # Convertir a float antes de asignar
    avg_prb_df["PRB_Usage_Percentage_y"] = avg_prb_df["PRB_Usage_Percentage_y"].astype(float).fillna(0)
    avg_prb_df["PRB_Usage_Percentage"] = avg_prb_df["PRB_Usage_Percentage"].astype(float).fillna(0)

    # Sumar los valores correctamente
    avg_prb_df["PRB_Usage_Percentage"] = (
        avg_prb_df["PRB_Usage_Percentage_y"] + 
        avg_prb_df["PRB_Usage_Percentage"]
    )

    avg_prb_df.drop(columns=["PRB_Usage_Percentage_y"], errors='ignore', inplace=True)

    # Asegúrate de que todas las operaciones de asignación sean directas
    avg_prb_df["PRB_Usage_Percentage"] = avg_prb_df["PRB_Usage_Percentage"].fillna(0).astype(float)

    # Asegurarte de que los 0 se clasifiquen correctamente al final
    avg_prb_df["Percentage_Range (%)"] = avg_prb_df["PRB_Usage_Percentage"].apply(assign_percentage_range)
    avg_prb_df["Status"] = avg_prb_df["PRB_Usage_Percentage"].apply(assign_status)

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
    html.H1("MONITOREO DE CELDAS RADIO ACCESO", 
            style={'textAlign': 'center', 
                   'marginBottom': '20px', 
                   'fontFamily': 'Poppins',  # Cambiado a Poppins
                   'color': '#2C3E50',
                   'fontSize': '36px',
                   'textShadow': '1px 1px 2px rgba(0, 0, 0, 0.3)'}),

    # Tarjetas de resumen
    html.Div([
        html.Div([
            html.H3(f"{count_ok}", style={'color': '#50C878', 'fontFamily': 'Poppins'}),  # Cambiado a Poppins
            html.P("CELDAS OK", style={'fontFamily': 'Open Sans', 'color': '#2C3E50'})
        ], className="card"),
        
        html.Div([
            html.H3(f"{count_sobrecargada}", style={'color': '#E74C3C', 'fontFamily': 'Poppins'}),
            html.P("CELDAS SOBRECARGADAS", style={'fontFamily': 'Open Sans', 'color': '#2C3E50'})
        ], className="card"),
        
        html.Div([
            html.H3(f"{count_subutilizada}", style={'color': '#F39C12', 'fontFamily': 'Poppins'}),
            html.P("CELDAS SUBUTILIZADAS", style={'fontFamily': 'Open Sans', 'color': '#2C3E50'})
        ], className="card"),
        
        html.Div([
            html.H3(f"{count_total}", style={'color': '#4A90E2', 'fontFamily': 'Poppins'}),
            html.P("TOTAL CELDAS", style={'fontFamily': 'Open Sans', 'color': '#2C3E50'})
        ], className="card"),
    ], className="card-container"),
    
    # Sección con gráfica y tabla
    html.Div([
        html.Div([
            dcc.Graph(figure=fig)
        ], className="six columns graph-container"),
        
        html.Div([
            dash_table.DataTable(
                id='table',
                columns=[{"name": i, "id": i} for i in average_prb_percentage.columns],
                data=average_prb_percentage.to_dict('records'),
                style_table={
                    'overflowX': 'auto',
                    'maxHeight': '400px',
                    'overflowY': 'scroll',
                    'border': 'none',
                    'borderRadius': '10px',
                    'backgroundColor': '#D0E2F2',
                },
                style_header={
                    'backgroundColor': '#4A90E2',
                    'color': 'white',
                    'fontWeight': 'bold',
                    'fontFamily': 'Poppins',
                    'textAlign': 'center'
                },
                style_cell={
                    'backgroundColor': 'rgb(255, 255, 255)',
                    'color': '#2C3E50',
                    'border': '1px solid #CCCCCC',
                    'fontFamily': 'Open Sans',
                    'textAlign': 'center'
                },
            )
        ], className="six columns table-container"),
        
    ], className="row"),
    
    # Botones de acción
    html.Div([
        html.Button("ACTUALIZAR", id="btn-update", n_clicks=0, className="btn", style={'border-radius': '8px', 'padding': '10px 20px', 'margin': '5px'}),
        html.Button("DESCARGAR REPORTE", id="btn-download", className="btn", style={'border-radius': '8px', 'padding': '10px 20px', 'margin': '5px'}),
    ], className="actions")
])





# Run Flask and Dash
if __name__ == '__main__':
    app.run_server(debug=True)

