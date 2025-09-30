import psycopg2
import logging

# Database connection
SRC_DB = {
    "dbname": "odisha_db",
    "user": "highcourt",
    "password": "highcourt",
    "host": "localhost",
    "port": 5432
}

# Configure logging
logging.basicConfig(
    filename="fetch_item_metadata.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def fetch_item_metadata(item_id):
    """
    Fetch all metadata for a given item_id from DSpace database.
    Logs results in a tabular manner.
    """
    conn = psycopg2.connect(**SRC_DB)
    cur = conn.cursor()
    
    query = """
    SELECT
        ms.short_id || '.' || mf.element || 
        COALESCE('.' || mf.qualifier, '') AS metadata_field,
        mv.text_value,
        mv.text_lang
    FROM item i
    JOIN metadatavalue mv ON i.uuid = mv.dspace_object_id
    JOIN metadatafieldregistry mf ON mv.metadata_field_id = mf.metadata_field_id
    JOIN metadataschemaregistry ms ON mf.metadata_schema_id = ms.metadata_schema_id
    WHERE i.item_id = %s
    ORDER BY metadata_field, mv.place
    """
    
    cur.execute(query, (item_id,))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    # Log table header
    logging.info(f"{'Field Name':40} | {'Value':50} | {'Language':10}")
    logging.info("-" * 110)

    metadata = {}
    for field, value, language in rows:
        entry = {"value": value, "language": language}
        if field in metadata:
            metadata[field].append(entry)
        else:
            metadata[field] = [entry]
        
        # Log in a tabular format
        logging.info(f"{field:40} | {str(value):50} | {str(language):10}")

    logging.info(f"Total fields fetched: {len(metadata)}")
    return metadata

if __name__ == "__main__":
    item_id = 1032154  # Replace with your item ID
    fetch_item_metadata(item_id)
    logging.info("Metadata fetch completed successfully.")
