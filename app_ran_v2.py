from flask import Flask
from dash import Dash, dcc, html, Input, Output, dash_table
import pandas as pd
import plotly.express as px

# Flask App
server = Flask(__name__)

# Dash App
app = Dash(__name__, server=server)

# Tu código para cargar y procesar datos aquí
def assign_percentage_range(prb_percentage):
    ranges = [(0, 5), (5, 10), (10, 15), (15, 20), (20, 25), (25, 30), (30, 35),
              (35, 40), (40, 45), (45, 50), (50, 55), (55, 60), (60, 65), (65, 70),
              (70, 75), (75, 80), (80, 85), (85, 90), (90, 95), (95, 100)]
    
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
    df = pd.read_csv("traffic_data_BTA El Ensueno.csv")  # Coloca la ruta adecuada

    df["timestamp"] = pd.to_datetime(df["timestamp"], format="%Y-%m-%d %H:%M:%S.%f")

    new_df = pd.DataFrame({
        "Timestamp": df["timestamp"],
        "Week": df['timestamp'].dt.strftime('%Y-%U'),
        "Node": df["cell_name"].apply(lambda x: "_".join(x.split("_")[:-2])),
        "Cell": df["cell_name"].apply(lambda x: "_".join(x.split("_")[-2:])),
        "PRB_Usage_Percentage": (df["l_chmeas_prb_dl_used_avg"] / df["l_chmeas_prb_dl_avail"]) * 100
    })

    new_df["PRB_Usage_Percentage"] = pd.to_numeric(new_df["PRB_Usage_Percentage"], errors='coerce')

    def filter_top_25_percent(group):
        group_sorted = group.sort_values(by="PRB_Usage_Percentage", ascending=False)
        top_25_percent = group_sorted.head(int(0.25 * len(group_sorted)))
        return top_25_percent

    grouped_df = new_df.groupby(["Week", "Node", "Cell"], group_keys=False)
    top_25_df = grouped_df.apply(filter_top_25_percent)

    top_25_df.to_csv("data_cell_report.csv", index=False)

    avg_prb_df = top_25_df.groupby(["Node", "Cell"], as_index=False)["PRB_Usage_Percentage"].mean()

    avg_prb_df["Percentage_Range (%)"] = avg_prb_df["PRB_Usage_Percentage"].apply(assign_percentage_range)
    avg_prb_df["Status"] = avg_prb_df["PRB_Usage_Percentage"].apply(assign_status)

    avg_prb_df.to_csv("cell_report.csv", index=False)

    return new_df, avg_prb_df

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
