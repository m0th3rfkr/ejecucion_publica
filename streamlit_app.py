import streamlit as st
import pandas as pd
from io import StringIO
import snowflake.connector
from snowflake.connector import DictCursor
import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Configuración de la página
st.set_page_config(page_title="PUBLIC EXECUTION Checker", page_icon="🎵", layout="wide")
st.title("🎵 PUBLIC EXECUTION Checker")

# Configuración de conexión a Snowflake
def init_connection():
    return snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA'),
        role=os.getenv('SNOWFLAKE_ROLE')
    )

# Inicializar conexión
conn = init_connection()

# Ejecutar query y obtener resultados como DataFrame
@st.cache_data
def run_query(query):
    with conn.cursor(DictCursor) as cur:
        cur.execute(query)
        results = cur.fetchall()
    return pd.DataFrame(results)

# Función para obtener países
@st.cache_data
def get_countries():
    query = """
    SELECT COUNTRY_CODE, COUNTRY_NAME 
    FROM DF_PROD_DAP_MISC.DAP.DIM_COUNTRY 
    WHERE COUNTRY_CODE != 'ZZ' AND COUNTRY_CODE != 'U'
    ORDER BY COUNTRY_NAME
    """
    return run_query(query)

def generate_query(isrc_list, country_code):
    """Generate SQL query for checking ISRCs with country filter"""
    clean_isrcs = [str(isrc).strip() for isrc in isrc_list if isrc is not None and str(isrc).strip() != '']
    if not clean_isrcs:
        st.error("No valid ISRCs found in the input file")
        return None
    isrc_values = "','".join(clean_isrcs)
    
    return f"""
    WITH BASE_PRODUCT AS (
        SELECT DISTINCT
            p.artist_display_name as ARTIST_DISPLAY_NAME,
            p.product_title as PRODUCT_TITLE,
            p.wmi_imprint_desc as WMI_IMPRINT_DESC,
            owner.marketing_owner_name as MARKETING_OWNER_NAME,
            rac.text as TEXT,
            p.product_id as ISRC,
            ass.P_CREDIT,
            ass.WW_REPERTOIRE_OWNER
        FROM DF_PROD_DAP_MISC.DAP.DIM_PRODUCT p
        LEFT JOIN DF_PROD_DAP_MISC.DAP.FACT_AUDIO_STREAMING_AGG_YEARLY f 
            ON p.product_key = f.product_key
        LEFT JOIN DF_PROD_DAP_MISC.DAP.DIM_MARKETING_OWNER owner 
            ON owner.marketing_owner_key = f.marketing_owner_key
        LEFT JOIN CORP_GCDMI_PROD.REPORTING.RPT_ASSET_COMPANY_RELATIONSHIP cr 
            ON cr.id = f.product_key
        LEFT JOIN CORP_GCDMI_PROD.REPORTING.RPT_ASSET_CREDIT rac 
            ON rac.asset_id = f.product_key
        LEFT JOIN CORP_GCDMI_PROD.REPORTING.RPT_ASSET ass 
            ON ass.ISRC = p.product_id
        WHERE 
            p.product_id_type = 'ISRC'
            AND p.product_id IN ('{isrc_values}')
            AND p.wmi_imprint_desc = owner.marketing_owner_name
    ),
    RIGHTS_INFO AS (
        SELECT DISTINCT
            pc.GAID AS ISRC,
            MIN(t.COUNTRY_LIST_SORTED) as COUNTRY_LIST_SORTED,
            r.RIGHT_TYPE,
            r.EFFECTIVE_TO_DATE::TIMESTAMP_NTZ as EFFECTIVE_TO_DATE,
            r.EFFECTIVE_FROM_DATE::TIMESTAMP_NTZ as EFFECTIVE_FROM_DATE,
            ROW_NUMBER() OVER (PARTITION BY pc.GAID 
                             ORDER BY 
                                CASE r.RIGHT_TYPE 
                                    WHEN 'Master' THEN 1 
                                    WHEN 'Distribution' THEN 2 
                                    ELSE 3 
                                END,
                                r.EFFECTIVE_FROM_DATE ASC) as rn
        FROM CORP_GCDMI_PROD.REPORTING.RPT_PRODUCT_COMPONENT pc 
        JOIN CORP_GCDMI_PROD.REPORTING.RPT_PRODUCT_COMPONENT_RIGHT r 
            ON r.PRODUCT_COMPONENT_ID = pc.ID
        JOIN CORP_GCDMI_PROD.REPORTING.RPT_TERRITORY t 
            ON t.TERRITORY_ID = r.TERRITORY_ID
        WHERE 
            pc.GAID IN ('{isrc_values}')
            AND (r.EFFECTIVE_TO_DATE > CURRENT_DATE() OR r.EFFECTIVE_TO_DATE IS NULL)
            AND r.IS_DELETED = 'N'
            AND t.COUNTRY_LIST_SORTED LIKE '%{country_code}%'
        GROUP BY 
            pc.GAID,
            r.RIGHT_TYPE,
            r.EFFECTIVE_TO_DATE,
            r.EFFECTIVE_FROM_DATE
    )
    SELECT DISTINCT
        b.*,
        r.RIGHT_TYPE,
        r.EFFECTIVE_TO_DATE,
        r.EFFECTIVE_FROM_DATE,
        r.COUNTRY_LIST_SORTED as TRACK_RIGHTS_TERRITORIES
    FROM BASE_PRODUCT b
    JOIN RIGHTS_INFO r ON b.ISRC = r.ISRC
    WHERE r.rn = 1
    ORDER BY b.ISRC;
    """

def process_file(df, isrc_column, selected_country, selected_country_code):
    """Process the uploaded file with ISRCs"""
    df[isrc_column] = df[isrc_column].astype(str).str.strip()
    isrc_list = df[df[isrc_column] != ''][isrc_column].unique().tolist()
    st.info(f"Processing {len(isrc_list)} unique ISRCs...")

    query = generate_query(isrc_list, selected_country_code)
    if query is None:
        return
        
    with st.spinner('Executing query...'):
        try:
            # Ejecutar query y obtener resultados
            results_df = run_query(query)
            
            # Convert timestamp columns and handle out-of-bounds dates
            timestamp_columns = ['EFFECTIVE_TO_DATE', 'EFFECTIVE_FROM_DATE']
            for col in timestamp_columns:
                if col in results_df.columns:
                    # Convert timestamps while handling out-of-bounds dates
                    results_df[col] = pd.to_datetime(results_df[col].apply(
                        lambda x: None if x is None or pd.Timestamp(x).year > 2262 else x
                    ))

            if not results_df.empty:
                st.write("### Results")
                # Convert to more efficient data types
                for col in results_df.select_dtypes(['object']).columns:
                    try:
                        results_df[col] = results_df[col].astype('string')
                    except:
                        pass  # Si falla, mantener el tipo original
                
                st.dataframe(results_df)
                
                # Add download button
                st.download_button(
                    label="📥 Download Results",
                    data=results_df.to_csv(index=False).encode('utf-8'),
                    file_name=f"isrc_results_{selected_country_code}.csv",
                    mime="text/csv"
                )
                
                # Display summary statistics
                st.write("### Summary")
                summary_df = results_df.groupby(['RIGHT_TYPE']).size().reset_index(name='count')
                st.dataframe(summary_df)
                
            else:
                st.warning(f"No results found for the provided ISRCs in {selected_country}")
        except Exception as e:
            st.error(f"An error occurred: {str(e)}")
            st.info("Please check the query execution and data types.")

# Get countries and create selector
try:
    countries_df = get_countries()
    selected_country = st.selectbox(
        "Select a country to filter territories",
        options=countries_df['COUNTRY_NAME'].tolist(),
        format_func=lambda x: x
    )

    # Get selected country code
    selected_country_code = countries_df[countries_df['COUNTRY_NAME'] == selected_country]['COUNTRY_CODE'].iloc[0]
except Exception as e:
    st.error(f"Error al conectar con Snowflake: {str(e)}")
    st.info("Por favor verifica tus credenciales de Snowflake")
    st.stop()

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

# Cerrar conexión al final
if 'conn' in locals():
    conn.close()
