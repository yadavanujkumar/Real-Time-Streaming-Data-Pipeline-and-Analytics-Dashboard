"""
Real-Time IoT Sensor Analytics Dashboard
Built with Streamlit for visualizing sensor data from Parquet files
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import glob
import os
import time

# Page configuration
st.set_page_config(
    page_title="IoT Sensor Analytics Dashboard",
    page_icon="🌡️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        padding: 1rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    </style>
    """, unsafe_allow_html=True)


class IoTDashboard:
    """Dashboard for visualizing IoT sensor data."""
    
    def __init__(self, data_path='/tmp/output'):
        """
        Initialize dashboard with data path.
        
        Args:
            data_path: Path to Parquet data files
        """
        self.data_path = data_path
        self.raw_data_path = os.path.join(data_path, 'raw_data')
        self.agg_data_path = os.path.join(data_path, 'aggregated_data')
    
    def load_raw_data(self, limit=1000):
        """
        Load raw sensor data from Parquet files.
        
        Args:
            limit: Maximum number of records to load
            
        Returns:
            DataFrame: Raw sensor data
        """
        try:
            # Find all Parquet files
            parquet_files = glob.glob(f"{self.raw_data_path}/**/*.parquet", recursive=True)
            
            if not parquet_files:
                return pd.DataFrame()
            
            # Read latest files (limit to recent data)
            parquet_files.sort(key=os.path.getmtime, reverse=True)
            files_to_read = parquet_files[:10]  # Read last 10 files
            
            df_list = []
            for file in files_to_read:
                df = pd.read_parquet(file)
                df_list.append(df)
            
            if df_list:
                df = pd.concat(df_list, ignore_index=True)
                # Sort by timestamp and limit
                df = df.sort_values('event_timestamp', ascending=False).head(limit)
                return df
            
            return pd.DataFrame()
            
        except Exception as e:
            st.error(f"Error loading raw data: {e}")
            return pd.DataFrame()
    
    def load_aggregated_data(self, limit=500):
        """
        Load aggregated sensor data from Parquet files.
        
        Args:
            limit: Maximum number of records to load
            
        Returns:
            DataFrame: Aggregated sensor data
        """
        try:
            # Find all Parquet files
            parquet_files = glob.glob(f"{self.agg_data_path}/**/*.parquet", recursive=True)
            
            if not parquet_files:
                return pd.DataFrame()
            
            # Read latest files
            parquet_files.sort(key=os.path.getmtime, reverse=True)
            files_to_read = parquet_files[:10]
            
            df_list = []
            for file in files_to_read:
                df = pd.read_parquet(file)
                df_list.append(df)
            
            if df_list:
                df = pd.concat(df_list, ignore_index=True)
                df = df.sort_values('window_start', ascending=False).head(limit)
                return df
            
            return pd.DataFrame()
            
        except Exception as e:
            st.error(f"Error loading aggregated data: {e}")
            return pd.DataFrame()
    
    def render_header(self):
        """Render dashboard header."""
        st.markdown('<h1 class="main-header">🌡️ IoT Sensor Analytics Dashboard</h1>', 
                   unsafe_allow_html=True)
        st.markdown("---")
    
    def render_metrics(self, df):
        """
        Render key metrics cards.
        
        Args:
            df: Raw sensor DataFrame
        """
        if df.empty:
            st.warning("No data available for metrics")
            return
        
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.metric(
                label="Active Sensors",
                value=df['sensor_id'].nunique(),
                delta=None
            )
        
        with col2:
            avg_temp = df['temperature'].mean()
            st.metric(
                label="Avg Temperature",
                value=f"{avg_temp:.1f}°C",
                delta=None
            )
        
        with col3:
            avg_humidity = df['humidity'].mean()
            st.metric(
                label="Avg Humidity",
                value=f"{avg_humidity:.1f}%",
                delta=None
            )
        
        with col4:
            avg_pressure = df['pressure'].mean()
            st.metric(
                label="Avg Pressure",
                value=f"{avg_pressure:.1f} hPa",
                delta=None
            )
        
        with col5:
            locations = df['location'].nunique()
            st.metric(
                label="Locations",
                value=locations,
                delta=None
            )
    
    def render_temperature_timeline(self, df):
        """
        Render temperature timeline chart.
        
        Args:
            df: Raw sensor DataFrame
        """
        if df.empty:
            return
        
        st.subheader("📈 Temperature Timeline by Sensor")
        
        fig = px.line(
            df.sort_values('event_timestamp'),
            x='event_timestamp',
            y='temperature',
            color='sensor_id',
            title='Temperature Readings Over Time',
            labels={'event_timestamp': 'Time', 'temperature': 'Temperature (°C)'}
        )
        
        fig.update_layout(
            height=400,
            hovermode='x unified',
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    def render_sensor_comparison(self, agg_df):
        """
        Render sensor comparison charts.
        
        Args:
            agg_df: Aggregated sensor DataFrame
        """
        if agg_df.empty:
            return
        
        st.subheader("📊 Sensor Performance Comparison")
        
        # Calculate averages per sensor
        sensor_stats = agg_df.groupby('sensor_id').agg({
            'avg_temperature': 'mean',
            'avg_humidity': 'mean',
            'avg_pressure': 'mean',
            'reading_count': 'sum'
        }).reset_index()
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Temperature by sensor
            fig_temp = px.bar(
                sensor_stats,
                x='sensor_id',
                y='avg_temperature',
                title='Average Temperature by Sensor',
                labels={'sensor_id': 'Sensor ID', 'avg_temperature': 'Avg Temperature (°C)'},
                color='avg_temperature',
                color_continuous_scale='RdYlBu_r'
            )
            fig_temp.update_layout(height=350)
            st.plotly_chart(fig_temp, use_container_width=True)
        
        with col2:
            # Humidity by sensor
            fig_hum = px.bar(
                sensor_stats,
                x='sensor_id',
                y='avg_humidity',
                title='Average Humidity by Sensor',
                labels={'sensor_id': 'Sensor ID', 'avg_humidity': 'Avg Humidity (%)'},
                color='avg_humidity',
                color_continuous_scale='Blues'
            )
            fig_hum.update_layout(height=350)
            st.plotly_chart(fig_hum, use_container_width=True)
    
    def render_location_heatmap(self, df):
        """
        Render location-based heatmap.
        
        Args:
            df: Raw sensor DataFrame
        """
        if df.empty:
            return
        
        st.subheader("🗺️ Temperature by Location")
        
        location_stats = df.groupby('location').agg({
            'temperature': ['mean', 'min', 'max'],
            'sensor_id': 'nunique'
        }).reset_index()
        
        location_stats.columns = ['location', 'avg_temp', 'min_temp', 'max_temp', 'sensor_count']
        
        fig = go.Figure(data=[
            go.Bar(
                name='Average',
                x=location_stats['location'],
                y=location_stats['avg_temp'],
                marker_color='lightblue'
            ),
            go.Bar(
                name='Maximum',
                x=location_stats['location'],
                y=location_stats['max_temp'],
                marker_color='red'
            ),
            go.Bar(
                name='Minimum',
                x=location_stats['location'],
                y=location_stats['min_temp'],
                marker_color='blue'
            )
        ])
        
        fig.update_layout(
            title='Temperature Statistics by Location',
            xaxis_title='Location',
            yaxis_title='Temperature (°C)',
            barmode='group',
            height=400
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    def render_aggregated_timeline(self, agg_df):
        """
        Render aggregated metrics timeline.
        
        Args:
            agg_df: Aggregated sensor DataFrame
        """
        if agg_df.empty:
            return
        
        st.subheader("📉 Aggregated Metrics Over Time")
        
        # Create subplots
        fig = make_subplots(
            rows=2, cols=1,
            subplot_titles=('Temperature Trends', 'Humidity Trends'),
            vertical_spacing=0.15
        )
        
        sorted_df = agg_df.sort_values('window_start')
        
        for sensor in sorted_df['sensor_id'].unique():
            sensor_data = sorted_df[sorted_df['sensor_id'] == sensor]
            
            # Temperature
            fig.add_trace(
                go.Scatter(
                    x=sensor_data['window_start'],
                    y=sensor_data['avg_temperature'],
                    mode='lines',
                    name=f'{sensor} (Temp)',
                    legendgroup=sensor
                ),
                row=1, col=1
            )
            
            # Humidity
            fig.add_trace(
                go.Scatter(
                    x=sensor_data['window_start'],
                    y=sensor_data['avg_humidity'],
                    mode='lines',
                    name=f'{sensor} (Humidity)',
                    legendgroup=sensor,
                    showlegend=False
                ),
                row=2, col=1
            )
        
        fig.update_xaxes(title_text="Time", row=2, col=1)
        fig.update_yaxes(title_text="Temperature (°C)", row=1, col=1)
        fig.update_yaxes(title_text="Humidity (%)", row=2, col=1)
        
        fig.update_layout(height=600, hovermode='x unified')
        
        st.plotly_chart(fig, use_container_width=True)
    
    def render_data_table(self, df, title="Recent Sensor Readings"):
        """
        Render data table.
        
        Args:
            df: DataFrame to display
            title: Table title
        """
        if df.empty:
            return
        
        st.subheader(title)
        
        # Select relevant columns and format
        display_cols = ['sensor_id', 'location', 'event_timestamp', 
                       'temperature', 'humidity', 'pressure', 'battery_level', 'status']
        
        if all(col in df.columns for col in display_cols):
            display_df = df[display_cols].head(20)
            st.dataframe(display_df, use_container_width=True)
        else:
            st.dataframe(df.head(20), use_container_width=True)
    
    def run(self):
        """Run the dashboard application."""
        # Render header
        self.render_header()
        
        # Sidebar
        with st.sidebar:
            st.header("⚙️ Settings")
            
            # Data refresh
            if st.button("🔄 Refresh Data"):
                st.rerun()
            
            # Auto-refresh
            auto_refresh = st.checkbox("Auto-refresh (30s)", value=False)
            
            # Data limit
            data_limit = st.slider("Data Records", 100, 2000, 1000, 100)
            
            st.markdown("---")
            
            # Data path configuration
            st.text_input("Data Path", value=self.data_path, disabled=True)
            
            st.markdown("---")
            st.caption("💡 Dashboard updates in real-time as new data arrives")
        
        # Auto-refresh logic
        if auto_refresh:
            time.sleep(30)
            st.rerun()
        
        # Load data
        with st.spinner("Loading sensor data..."):
            raw_df = self.load_raw_data(limit=data_limit)
            agg_df = self.load_aggregated_data(limit=data_limit // 2)
        
        # Check if data is available
        if raw_df.empty and agg_df.empty:
            st.warning("⚠️ No data available. Please ensure the streaming pipeline is running.")
            st.info("""
            **To start generating data:**
            1. Start Kafka broker
            2. Run the data producer: `python data_producer/kafka_producer.py`
            3. Run the Spark streaming job: `spark-submit spark_streaming/streaming_etl.py`
            4. Wait a few moments for data to be processed
            """)
            return
        
        # Render metrics
        if not raw_df.empty:
            self.render_metrics(raw_df)
            st.markdown("---")
        
        # Main visualizations
        tab1, tab2, tab3, tab4 = st.tabs(["📈 Overview", "📊 Analysis", "🗺️ Locations", "📋 Data"])
        
        with tab1:
            if not raw_df.empty:
                self.render_temperature_timeline(raw_df)
            if not agg_df.empty:
                self.render_aggregated_timeline(agg_df)
        
        with tab2:
            if not agg_df.empty:
                self.render_sensor_comparison(agg_df)
            else:
                st.info("Aggregated data not yet available")
        
        with tab3:
            if not raw_df.empty:
                self.render_location_heatmap(raw_df)
            else:
                st.info("Location data not yet available")
        
        with tab4:
            if not raw_df.empty:
                self.render_data_table(raw_df, "📋 Recent Sensor Readings")


def main():
    """Main application entry point."""
    # Get data path from environment or use default
    data_path = os.environ.get('DATA_PATH', '/tmp/output')
    
    # Create and run dashboard
    dashboard = IoTDashboard(data_path=data_path)
    dashboard.run()


if __name__ == "__main__":
    main()
