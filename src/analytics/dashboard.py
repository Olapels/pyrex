import dash
from dash import dcc, html, Input, Output
import plotly.express as px
import pandas as pd
import psycopg2
import os

# Database connection settings
DB_CONFIG = {
    "host": "pyrex-postgres-1",
    "database": "sec_analytics",
    "user": "postgres",
    "password": "password"
}

app = dash.Dash(__name__)

app.layout = html.Div(style={'backgroundColor': '#1e1e1e', 'color': 'white', 'padding': '20px'}, children=[
    html.H1("Pyrex Financial Analytics", style={'textAlign': 'center'}),
    
    html.Div([
        html.H3("Live Income Statement Metrics"),
        dcc.Interval(id='interval-component', interval=5*1000, n_intervals=0), # Update every 5s
        dcc.Graph(id='live-financial-chart'),
    ]),
    
    html.Div([
        html.H4("Raw Processed Data"),
        html.Div(id='data-table')
    ])
])

@app.callback(
    Output('live-financial-chart', 'figure'),
    Input('interval-component', 'n_intervals')
)
def update_graph(n):
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        # Adjust 'summary_report' to your actual Flink sink table name
        query = "SELECT ticker, revenue, net_income, event_time FROM summary_report ORDER BY event_time DESC LIMIT 50"
        df = pd.read_sql(query, conn)
        conn.close()

        if df.empty:
            return px.scatter(title="Waiting for data from Flink...")

        fig = px.bar(df, x="ticker", y="revenue", color="ticker", 
                     title="Revenue by Ticker (Latest Processed)",
                     template="plotly_dark")
        return fig
    except Exception as e:
        print(f"Error fetching from Postgres: {e}")
        return px.scatter(title="Database Connection Error")

if __name__ == '__main__':
    # Run on 0.0.0.0 so you can access it from your Mac browser
    app.run_server(host='0.0.0.0', port=8050, debug=True)
