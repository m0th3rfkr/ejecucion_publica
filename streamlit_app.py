import streamlit as st
import pandas as pd
from io import StringIO
import datetime
import random

# Configuración de la página
st.set_page_config(page_title="PUBLIC EXECUTION Checker", page_icon="🎵", layout="wide")
st.title("🎵 PUBLIC EXECUTION Checker")

# Aviso de modo demo
st.warning("""
    **MODO DEMO**: Esta aplicación está funcionando en modo demostración sin conexión a Snowflake.
    Los datos mostrados son simulados para fines de demostración.
""")

# Datos de ejemplo para simular países
@st.cache_data
def get_sample_countries():
    return pd.DataFrame({
        'COUNTRY_CODE': ['US', 'GB', 'JP', 'BR', 'DE', 'FR', 'MX', 'CA', 'ES', 'IT'],
        'COUNTRY_NAME': ['United States', 'United Kingdom', 'Japan', 'Brazil', 'Germany', 
                         'France', 'Mexico', 'Canada', 'Spain', 'Italy']
    })

# Generar datos de ejemplo para resultados
def generate_sample_results(isrc_list, country_code):
    results = []
    
    owner_names = ["Atlantic Records", "Warner Records", "Elektra Records", "Reprise Records", 
                  "Parlophone Records", "Asylum Records", "Nonesuch Records"]
    
    right_types = ["Master", "Distribution", "Publishing"]
    
    for isrc in isrc_list:
        # Generar datos aleatorios para este ISRC
        artist = f"Artist {random.randint(1, 100)}"
        title = f"Track Title {random.randint(1, 500)}"
        owner = random.choice(owner_names)
        
        # Crear entre 1-3 resultados para cada ISRC
        for _ in range(random.randint(1, 3)):
            right_type = random.choice(right_types)
            
            # Fechas aleatorias
            from_date = datetime.datetime.now() - datetime.timedelta(days=random.randint(365, 1095))
            to_date = from_date + datetime.timedelta(days=random.randint(1825, 3650))
            
            # Simular territorio
            territories = f"{country_code},US,GB,EU"
            
            results.append({
                'ARTIST_DISPLAY_NAME': artist,
                'PRODUCT_TITLE': title,
                'WMI_IMPRINT_DESC': owner,
                'MARKETING_OWNER_NAME': owner,
                'TEXT': f"Main Artist: {artist}",
                'ISRC': isrc,
                'P_CREDIT': f"(P) {from_date.year} Warner Music Group",
                'WW_REPERTOIRE_OWNER': owner,
                'RIGHT_TYPE': right_type,
                'EFFECTIVE_FROM_DATE': from_date,
                'EFFECTIVE_TO_DATE': to_date,
                'TRACK_RIGHTS_TERRITORIES': territories
            })
    
    # Convertir a DataFrame
    if results:
        df = pd.DataFrame(results)
        return df
    else:
        return pd.DataFrame()

def process_file(df, isrc_column, selected_country, selected_country_code):
    """Process the uploaded file with ISRCs (demo mode)"""
    df[isrc_column] = df[isrc_column].astype(str).str.strip()
    isrc_list = df[df[isrc_column] != ''][isrc_column].unique().tolist()
    
    if not isrc_list:
        st.error("No valid ISRCs found in the input file")
        return
        
    st.info(f"Processing {len(isrc_list)} unique ISRCs...")
    
    with st.spinner('Analyzing data...'):
        # Simular un tiempo de procesamiento
        import time
        time.sleep(2)
        
        # Generar resultados de ejemplo
        results_df = generate_sample_results(isrc_list, selected_country_code)
        
        if not results_df.empty:
            st.write("### Results")
            st.dataframe(results_df)
            
            # Add download button
            st.download_button(
                label="📥 Download Results",
                data=results_df.to_csv(index=False).encode('utf-8'),
                file_name=f"demo_isrc_results_{selected_country_code}.csv",
                mime="text/csv"
            )
            
            # Display summary statistics
            st.write("### Summary")
            summary_df = results_df.groupby(['RIGHT_TYPE']).size().reset_index(name='count')
            st.dataframe(summary_df)
            
            # Mostrar aviso de modo demo
            st.info("⚠️ Recuerda que estos son datos SIMULADOS para propósitos de demostración")
        else:
            st.warning(f"No results found for the provided ISRCs in {selected_country}")

# Obtener países de ejemplo
countries_df = get_sample_countries()
selected_country = st.selectbox(
    "Select a country to filter territories",
    options=countries_df['COUNTRY_NAME'].tolist(),
    format_func=lambda x: x
)

# Get selected country code
selected_country_code = countries_df[countries_df['COUNTRY_NAME'] == selected_country]['COUNTRY_CODE'].iloc[0]

st.markdown("Upload ISRCs to check their status in the WMG database.")

# Crear tabs para diferentes métodos de entrada
tab1, tab2, tab3 = st.tabs(["Upload CSV", "Paste ISRCs", "Upload Excel"])

with tab1:
    st.header("Upload CSV file")
    uploaded_file = st.file_uploader(
        "Choose a CSV file with ISRCs", 
        type=["csv"],
        help="Your CSV file should contain ISRCs in one of the columns"
    )
    
    if uploaded_file is not None:
        try:
            df = pd.read_csv(uploaded_file)
            st.write("### Preview of uploaded data")
            st.dataframe(df.head())
            
            # If we have multiple columns, let user select ISRC column
            if len(df.columns) > 1:
                isrc_column = st.selectbox("Select the ISRC column", df.columns)
            else:
                isrc_column = df.columns[0]
            
            # Process button
            if st.button("Process ISRCs from CSV", type="primary"):
                process_file(df, isrc_column, selected_country, selected_country_code)
                
        except Exception as e:
            st.error(f"Error processing CSV file: {str(e)}")
            st.info("Please make sure your CSV file is properly formatted")

with tab2:
    st.header("Paste ISRCs")
    uploaded_data = st.text_area(
        "Paste your ISRCs here",
        height=200,
        help="You can paste ISRCs directly (one per line or space-separated) or paste CSV data"
    )

    if uploaded_data:
        try:
            # Handle the case where just ISRCs are pasted
            if '\n' not in uploaded_data and ',' not in uploaded_data:
                # Split by whitespace and create a DataFrame
                isrcs = [isrc.strip() for isrc in uploaded_data.split() if isrc.strip()]
                df = pd.DataFrame({'ISRC': isrcs})
                isrc_column = 'ISRC'  # Set default column name for ISRC list
            else:
                # Try to parse as CSV
                df = pd.read_csv(StringIO(uploaded_data))
                # Column selector only if we have multiple columns
                if len(df.columns) > 1:
                    isrc_column = st.selectbox("Select the ISRC column", df.columns)
                else:
                    isrc_column = df.columns[0]
            
            st.write("### Preview of data")
            st.dataframe(df.head())
            
            # Process button
            if st.button("Process Pasted ISRCs", type="primary"):
                process_file(df, isrc_column, selected_country, selected_country_code)
                
        except Exception as e:
            st.error(f"An error occurred: {str(e)}")
            st.info("Please make sure your data is properly formatted.")

with tab3:
    st.header("Upload Excel file")
    excel_file = st.file_uploader(
        "Choose an Excel file with ISRCs",
        type=["xlsx", "xls"],
        help="Your Excel file should contain ISRCs in one of the columns"
    )
    
    if excel_file is not None:
        try:
            # Read excel file
            df = pd.read_excel(excel_file)
            st.write("### Preview of uploaded data")
            st.dataframe(df.head())
            
            # Sheet selector if multiple sheets
            if len(pd.ExcelFile(excel_file).sheet_names) > 1:
                sheet_name = st.selectbox(
                    "Select sheet", 
                    options=pd.ExcelFile(excel_file).sheet_names
                )
                df = pd.read_excel(excel_file, sheet_name=sheet_name)
                st.write(f"Data from sheet: {sheet_name}")
                st.dataframe(df.head())
            
            # If we have multiple columns, let user select ISRC column  
            if len(df.columns) > 1:
                isrc_column = st.selectbox("Select the ISRC column", df.columns)
            else:
                isrc_column = df.columns[0]
                
            # Process button
            if st.button("Process ISRCs from Excel", type="primary"):
                process_file(df, isrc_column, selected_country, selected_country_code)
                
        except Exception as e:
            st.error(f"Error processing Excel file: {str(e)}")
            st.info("Please make sure your Excel file is properly formatted")

# Nota sobre conectividad
st.markdown("---")
expander = st.expander("🔧 Información para conectar con Snowflake")
with expander:
    st.markdown("""
    ### Posibles soluciones al error de conexión con Snowflake:
    
    1. **Verificar el formato de la cuenta de Snowflake**:
       - Prueba con `ENT_OKTA_SNOWFLAKE_DATALAB.us-east-1` o la región correcta
       - Consulta con el equipo de IT el formato exacto para conexiones externas
    
    2. **Políticas de seguridad**:
       - Snowflake puede estar bloqueando conexiones desde Streamlit Cloud
       - Consulta si necesitas configurar IP fijas o autenticación adicional
    
    3. **Alternativas de despliegue**:
       - Considera desplegar en un servidor interno con acceso a Snowflake
       - Explora Snowflake Streamlit Integration si está disponible
    
    4. **Configuración en secretos**:
       El formato correcto de los secretos debería ser:
       ```
       SNOWFLAKE_USER = "tu_usuario"
       SNOWFLAKE_PASSWORD = "tu_contraseña"
       SNOWFLAKE_ACCOUNT = "tu_cuenta" 
       SNOWFLAKE_WAREHOUSE = "TECH_SANDBOX_WH_M"
       SNOWFLAKE_DATABASE = "TECH_SANDBOX"
       SNOWFLAKE_SCHEMA = "ANTONIO_M"
       SNOWFLAKE_ROLE = "ENT_OKTA_SNOWFLAKE_DATALAB_TECH"
       ```
    """)

# Información de contacto
st.markdown("---")
st.markdown("""
**Para soporte técnico**: Contacta al equipo de IT o al administrador de Snowflake
""")
