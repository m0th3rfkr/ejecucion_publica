import streamlit as st
import pandas as pd
import snowflake.connector as sf
from snowflake.connector.errors import ProgrammingError, DatabaseError
import logging
from typing import Optional
import io

# Configure page settings
st.set_page_config(
    page_title="ISRC Checker",
    page_icon="🎵",
    layout="wide"
)

class SnowflakeConnector:
    def __init__(self):
        self.connection = None
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def connect(self) -> None:
        try:
            # Connect using OAuth
            self.connection = sf.connect(
                user=st.secrets["snowflake_user"],
                account='wmg-datalab',
                warehouse='RM_ANALYST_SANDBOX_WH_L',
                database='DF_PROD_DAP_MISC',
                schema='DAP',
                authenticator='oauth',
                token=st.secrets.get("snowflake_oauth_token"),
                role='RM_ANALYST'
            )
            
            self.logger.info("Successfully connected to Snowflake")
            st.success("Connected to Snowflake successfully!")
            
        except Exception as e:
            error_message = str(e)
            self.logger.error(f"Failed to connect to Snowflake: {error_message}")
            st.error(f"Failed to connect to Snowflake: {error_message}")
            raise

    def execute_query(self, query: str) -> Optional[pd.DataFrame]:
        if not self.connection:
            self.connect()
            
        try:
            cursor = self.connection.cursor()
            with st.spinner('Executing query...'):
                result = cursor.execute(query).fetch_pandas_all()
            st.success('Query executed successfully!')
            return result
        except (ProgrammingError, DatabaseError) as e:
            st.error(f"Query execution failed: {str(e)}")
            return None
        finally:
            if cursor:
                cursor.close()

    def close(self) -> None:
        if self.connection:
            self.connection.close()
            self.logger.info("Connection closed")

def generate_query(isrc_list: list) -> str:
    # Convert all ISRCs to strings and clean them
    clean_isrcs = [str(isrc).strip() for isrc in isrc_list if isrc is not None and str(isrc).strip() != '']
    
    if not clean_isrcs:
        st.error("No valid ISRCs found in the input file")
        return None
        
    isrc_values = "','".join(clean_isrcs)
    
    return f"""
    WITH CTE_BASE AS (
        SELECT
            c.*,
            p.product_id,
            p.artist_display_name,
            p.product_title,
            f.product_key,
            owner.marketing_owner_name
        FROM TECH_SANDBOX.ANTONIO_M.BRAZIL1PARTE3 c
        LEFT JOIN DF_PROD_DAP_MISC.DAP.DIM_PRODUCT p ON p.product_id = c.ISRC
        LEFT JOIN DF_PROD_DAP_MISC.DAP.FACT_AUDIO_STREAMING_AGG_YEARLY f ON p.product_key = f.product_key
        LEFT JOIN DF_PROD_DAP_MISC.DAP.DIM_MARKETING_OWNER owner ON owner.marketing_owner_key = f.marketing_owner_key
        WHERE 
            p.product_id_type = 'ISRC'
            AND c.ISRC IN ('{isrc_values}')
    ),
    CTE_RIGHTS AS (
        SELECT
            pc.GAID AS ISRC,
            ARRAY_SORT(ARRAY_UNION_AGG(STRTOK_TO_ARRAY(t.COUNTRY_LIST_SORTED, ','))) AS TRACK_RIGHTS_TERRITORIES,
            r.RIGHT_TYPE,
            r.EFFECTIVE_TO_DATE
        FROM 
            CORP_GCDMI_PROD.REPORTING.RPT_PRODUCT_COMPONENT pc
        JOIN CORP_GCDMI_PROD.REPORTING.RPT_PRODUCT_COMPONENT_RIGHT r ON r.PRODUCT_COMPONENT_ID = pc.ID
        JOIN CORP_GCDMI_PROD.REPORTING.RPT_TERRITORY t ON t.TERRITORY_ID = r.TERRITORY_ID
        WHERE 
            (r.EFFECTIVE_TO_DATE > CURRENT_DATE() OR r.EFFECTIVE_TO_DATE IS NULL)
            AND (r.RIGHT_TYPE = 'Master' OR r.RIGHT_TYPE = 'Distribution')
            AND r.IS_DELETED = 'N'
            AND pc.GAID IN ('{isrc_values}')
        GROUP BY 
            pc.GAID, 
            r.RIGHT_TYPE, 
            r.EFFECTIVE_TO_DATE
    ),
    CTE_COMBINED AS (
        SELECT
            base.*,
            rights.TRACK_RIGHTS_TERRITORIES,
            rights.RIGHT_TYPE,
            rights.EFFECTIVE_TO_DATE
        FROM 
            CTE_BASE base
        LEFT JOIN CTE_RIGHTS rights ON base.ISRC = rights.ISRC
    ),
    CTE_DISTINCT AS (
        SELECT DISTINCT
            *
        FROM CTE_COMBINED
        WHERE TRACK_RIGHTS_TERRITORIES IS NOT NULL
    )
    SELECT *
    FROM CTE_DISTINCT;
    """

def main():
    st.title("🎵 ISRC Checker")
    st.markdown("""
    Upload a file containing ISRCs to check their status in the database.
    
    **Accepted file formats:** CSV, Excel (.xlsx)
    """)

    # File uploader
    uploaded_file = st.file_uploader("Choose a file", type=['csv', 'xlsx'])
    
    if uploaded_file is not None:
        try:
            # Read the file
            if uploaded_file.name.endswith('.csv'):
                df = pd.read_csv(uploaded_file)
            else:
                df = pd.read_excel(uploaded_file)

            # Display preview of uploaded data
            st.write("### Preview of uploaded data")
            st.dataframe(df.head(), use_container_width=True)

            # Assuming the ISRC column name might vary, let user select it
            isrc_column = st.selectbox("Select the ISRC column", df.columns)

            if st.button("Process ISRCs", type="primary"):
                # Convert ISRC column to string type and clean data
                df[isrc_column] = df[isrc_column].astype(str).str.strip()
                
                # Get unique ISRCs, excluding empty strings
                isrc_list = df[df[isrc_column] != ''][isrc_column].unique().tolist()
                
                # Show number of ISRCs to be processed
                st.info(f"Processing {len(isrc_list)} unique ISRCs...")
                
                # Initialize Snowflake connection
                with st.spinner('Connecting to Snowflake...'):
                    snowflake_conn = SnowflakeConnector()

                try:
                    # Generate and execute query
                    query = generate_query(isrc_list)
                    if query is None:
                        return
                        
                    results_df = snowflake_conn.execute_query(query)

                    if results_df is not None and not results_df.empty:
                        st.write("### Results")
                        st.dataframe(results_df, use_container_width=True)

                        # Download button
                        output = io.BytesIO()
                        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                            results_df.to_excel(writer, index=False)
                        output.seek(0)
                        
                        st.download_button(
                            label="📥 Download Results",
                            data=output,
                            file_name="isrc_results.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                    else:
                        st.warning("No results found for the provided ISRCs")
                finally:
                    snowflake_conn.close()

        except Exception as e:
            st.error(f"An error occurred: {str(e)}")
            st.info("Please make sure your file is properly formatted and try again.")

if __name__ == "__main__":
    main()
