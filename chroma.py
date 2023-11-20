import os
import pandas as pd
from dotenv import load_dotenv
import chromadb
from chromadb.config import Settings
from chromadb.utils import embedding_functions
import psycopg2

# Function to establish a database connection
def create_connection():
    try:
        conn = psycopg2.connect(
            dbname='lab18', 
            user='brayangc', 
            password='password', 
            host='localhost', 
            port='5432'
        )
        return conn
    except psycopg2.Error as e:
        print("Error connecting to PostgreSQL:", e)
        return None
    
def get_vectors_sql(cursor):
    # Execute a SELECT query to retrieve all records from the 'movies' table
    cursor.execute("SELECT * FROM vectors")

    # Fetch all rows from the result set
    rows = cursor.fetchall()

    # Get vectors from each record
    movieids = []
    movieids_json = []
    vectors = []
    for row in rows:
        # Unpack the row details
        movieid, vector = row

        movieids.append(f"{movieid}")
        movieids_json.append({"movieid": f"{movieid}"})
        vectors.append(vector)
    
    return {
        "movieids": movieids,
        "movieids_json": movieids_json,
        "vectors": vectors
    }

def query_chroma(collection, query, results):
    results = collection.query(
        query_texts=[f"{query}"],
        n_results=results
    )

    for i, ids in enumerate(results['ids'][0]):
        print(f"Movie ID: {ids}")
        print(f"Document: {results['documents'][0][i]}")
        print(f"Metadata: {results['metadatas'][0][i]}")
        print(f"Distance: {results['distances'][0][i]}\n")

def update_chroma():
    # Establish connection with the database
    connection = create_connection()
    if connection:
        cursor = connection.cursor()
    else:
        print("Connection to the database failed.")
    
    # Set the chroma client
    client = chromadb.Client(Settings(chroma_db_impl="duckdb+parquet", persist_directory="db/"))
    
    # Get the movies_collection
    movies_collection = client.get_or_create_collection(name="Movies")

    movies_collection.add(
        documents = get_vectors_sql(cursor)['vectors'],
        metadatas = get_vectors_sql(cursor)['movieids_json'],
        ids = get_vectors_sql(cursor)['movieids']
    )

def initialize_chroma():
    # Set the chroma client
    client = chromadb.Client(Settings(chroma_db_impl="duckdb+parquet", persist_directory="db/"))
    
    # Get the movies_collection
    movies_collection = client.get_or_create_collection(name="Movies")

    return client, movies_collection


#client.reset()
#print(client.list_collections())
#movies_collection = client.get_or_create_collection(name="Movies")
#print(movies_collection.get())
