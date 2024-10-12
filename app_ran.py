import dash
from dash import dcc, html
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
#
#
#
#
#
#
#
#
#
#
# Configuración de la aplicación Dash
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Cargar y procesar los datos
def load_and_process_data():
    df = pd.read_csv("traffic_data_BTA El Ensueno.csv")
    print("Acá vemos el df inicial \n", df)
    # Convertir la columna 'Timestamp' a tipo datetime si aún no lo está
    df["timestamp"] = pd.to_datetime(df["timestamp"], format="%Y-%m-%d %H:%M:%S.%f")
 
    # Mostrar el dataframe resultante
    print("Acá vemos el df modificado por semana y promedio de columnas numéricas \n", df)
    
    print("ahora empezamos a crear otras columnas para organizar los datos \n")
    new_df = pd.DataFrame({
        "Timestamp": df["timestamp"],
        "Week": df['timestamp'].dt.strftime('%Y-%U'),
        "Node": df["cell_name"].apply(lambda x: "_".join(x.split("_")[:-2])),
        "Cell": df["cell_name"].apply(lambda x: "_".join(x.split("_")[-2:])),
        "L.Traffic.ActiveUser.DL.Avg": df["l_traffic_activeuser_dl_avg"],
        "L.ChMeas.PRB.DL.Avail": df["l_chmeas_prb_dl_avail"],
        "L.ChMeas.PRB.DL.Used.Avg": df["l_chmeas_prb_dl_used_avg"],
        "L.Thrp.bits.DL(bit)": df["l_thrp_bits_dl"]
    })
    
    # Calcular el porcentaje de uso de PRB y agregarlo al dataframe
    new_df["PRB_Usage_Percentage"] = (new_df["L.ChMeas.PRB.DL.Used.Avg"] / new_df["L.ChMeas.PRB.DL.Avail"]) * 100
    print("Acá vemos el df con las columnas necesarias para los calculos \n", new_df)

    # Ordenar el DataFrame por 'Cell', 'Semana' y 'L.Traffic.ActiveUser.DL.Avg' de mayor a menor
    new_df = new_df.sort_values(by=['Cell', 'Week', 'L.ChMeas.PRB.DL.Used.Avg'], ascending=[True, True, False])
    print("Acá ordenamos el df por celda, semana y mayor a menor PRB \n", new_df)

    # Filtrar los primeros 42 datos de cada semana para cada tipo de 'Cell'
    new_df = new_df.groupby(['Cell', 'Week']).head(42)
    print("Acá solo cogemos los primeros 42 datos de cada semana por celda \n", new_df)#################################################
    
    # Calcular el promedio del porcentaje de uso de PRB por celda
    average_prb_percentage = new_df.sort_values(by=['Cell', 'L.ChMeas.PRB.DL.Used.Avg'], ascending=[True, False])
    print("Acá vemos el df ordenado por celdas y trafico de mayor a menor \n", new_df)

    new_df.to_csv("report_bta_el_ensueno_4week.csv", index=False)
    return new_df, average_prb_percentage

# Procesar datos
new_df, average_prb_percentage = load_and_process_data()
print("Acá vemos el df modificado para mostrar en la gráfica \n", new_df)
print("Promedio del porcentaje de uso de PRB por celda: \n", average_prb_percentage)

# Definir colores para sectores específicos
cell_colors = {
    "AWS_1": "#FFD700",  # Amarillo
    "AWS_2": "#0000FF",  # Azul
    "AWS_3": "#FF0000",  # Rojo
    "B28_4": "#FFD700",  # Amarillo medio
    "B28_5": "#0000FF",  # Azul medio
    "B28_6": "#FF0000",  # Rojo medio
    "B7_7": "#FFD700",  # Amarillo suave
    "B7_8": "#0000FF",  # Azul suave
    "B7_9": "#FF0000",  # Rojo suave
}

# Crear gráfico Dash
def create_dash_layout():
    fig = px.bar(
        new_df,
        x="Timestamp",
        y="L.ChMeas.PRB.DL.Used.Avg",
        color="Cell",
        title=f"Uso de PRB por sector (Promedio semanal) Site: {', '.join(map(str, new_df['Node'].unique()))}",
        labels={"L.ChMeas.PRB.DL.Used.Avg": "Promedio Semanal de Uso PRB"},
        color_discrete_map=cell_colors,
        #barmode="relative",  # Configurar para apilar las barras
        facet_col="Cell",  # Configurar para agrupar por celda en columnas
        hover_data={"L.ChMeas.PRB.DL.Used.Avg": True, "PRB_Usage_Percentage": True}  # Mostrar en el tooltip
    )

    return dbc.Container(
        [
            dbc.Row(
                dbc.Col(
                    html.H1("Dashboard de Monitoreo de Celdas"),
                    className="text-center mt-3",
                )
            ),
            dbc.Row(dbc.Col(dcc.Graph(id="prb-usage-graph", figure=fig))),
            dbc.Row(
                dbc.Col(
                    html.Div([
                        html.H3("Promedio del porcentaje de uso de PRB por celda"),
                        html.Table(
                            # Encabezados de la tabla
                            [html.Tr([html.Th(col, style={'padding': '30px', 'text-align': 'left'}) for col in average_prb_percentage.columns])] +
                            # Filas de la tabla
                            [html.Tr([html.Td(average_prb_percentage.iloc[i][col]) for col in average_prb_percentage.columns]) for i in range(len(average_prb_percentage))]
                        )
                    ]),
                    id="average-prb-percentage"
                )
            ),
        ],
        fluid=True,
    )

# Configurar el layout de la aplicación Dash
app.layout = create_dash_layout()

# Ejecutar la aplicación
if __name__ == "__main__":
    app.run_server(debug=True)