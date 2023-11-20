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

# Function to get movie details using an established connection
def get_movie_details(cursor, movie_id):
    try:
        # Query to fetch movie details based on the movieId
        query = f"SELECT * FROM movies WHERE movieid = {movie_id}"

        # Execute the query
        cursor.execute(query)
        
        # Fetch all the results
        movie_details = cursor.fetchall()

        for movie in movie_details:
            # Unpack the movie details
            movie_id, title, genres = movie

        return {
            "movieid": movie_id, 
            "title": title, 
            "genres": genres
        }
        
    except (Exception, psycopg2.Error) as error:
        print("get_movie_details")
        print("Error fetching data from PostgreSQL table:", error)
        return None

# Function to get movie details using an established connection
def get_links_details(cursor, movie_id):
    try:
        # Query to fetch movie details based on the movieId
        query = f"SELECT * FROM links WHERE movieid = {movie_id}"

        # Execute the query
        cursor.execute(query)
        
        # Fetch all the results
        movie_details = cursor.fetchall()

        for movie in movie_details:
            # Unpack the movie details
            movie_id, imdb_id, tmdb_id = movie

        return {
            "movieid": movie_id, 
            "imdbid": imdb_id, 
            "tmdbid": tmdb_id
        }
        
    except (Exception, psycopg2.Error) as error:
        print("get_links_details")
        print("Error fetching data from PostgreSQL table:", error)
        return None

# Function to get movie details using an established connection
def get_ratings_details(cursor, movie_id):
    try:
        # Query to fetch movie details based on the movieId
        query = f"SELECT * FROM ratings WHERE movieid = {movie_id}"

        # Execute the query
        cursor.execute(query)
        
        # Fetch all the results
        ratings_details = cursor.fetchall()

        result = []
        for rating in ratings_details:
            # Unpack the rating details
            userid, movieid, rating, timestamp = rating
            result.append({
                "userid": userid, 
                "movieid": movieid, 
                "rating": rating,
                "timestamp": timestamp
            })

        return result
        
    except (Exception, psycopg2.Error) as error:
        print("get_ratings_details")
        print("Error fetching data from PostgreSQL table:", error)
        return None
    
def get_rating_avg_details(cursor, movie_id):
    try:
        # Query to fetch movie details based on the movieId
        query = f"SELECT * FROM ratings WHERE movieid = {movie_id}"

        # Execute the query
        cursor.execute(query)
        
        # Fetch all the results
        ratings_details = cursor.fetchall()

        n = 0
        rating_sum = 0

        for rating in ratings_details:
            # Unpack the rating details
            userid, movieid, rating, timestamp = rating
            # Increase the rating counter and addition the rating to the raitin_sum
            n += 1
            rating_sum += rating
        
        if (n != 0):
            return {
                "movieid": movieid, 
                "rating_avg": round(rating_sum/n, 1),
            }
        else:
            return None
            
        
    except (Exception, psycopg2.Error) as error:
        print("get_rating_avg_details")
        print("Error fetching data from PostgreSQL table:", error)
        return None

def get_tags_details(cursor, movie_id):
    try:
        # Query to fetch movie details based on the movieId
        query = f"SELECT * FROM tags WHERE movieid = {movie_id}"

        # Execute the query
        cursor.execute(query)
        
        # Fetch all the results
        tags_details = cursor.fetchall()

        result = []
        for tag in tags_details:
            # Unpack the tag details
            userid, movieid, tag, timestamp = tag
            result.append({
                "userid": userid, 
                "movieid": movieid, 
                "tag": tag,
                "timestamp": timestamp
            })

        return result
        
    except (Exception, psycopg2.Error) as error:
        print("get_tags_details")
        print("Error fetching data from PostgreSQL table:", error)
        return None

def get_tags_unique_details(cursor, movie_id):
    try:
        # Query to fetch movie details based on the movieId
        query = f"SELECT * FROM tags WHERE movieid = {movie_id}"
        
        # Execute the query
        cursor.execute(query)
        
        # Fetch all the results
        tags_details = cursor.fetchall()

        if (len(tags_details) != 0):
            tags = set()
            for tag in tags_details:
                # Unpack the tag details
                userid, movieid, tag, timestamp = tag
                # Add the tag to the tags set
                tags.add(tag)

            return {
                "movieid": movieid, 
                "tags": tags
            }
        else:
            return None
        
    except (Exception, psycopg2.Error) as error:
        print("get_tags_unique_details")
        print("Error fetching data from PostgreSQL table:", error)
        return None

def create_vector(cursor, movie_id):
    movie_details = get_movie_details(cursor, movie_id)
    movie_id = movie_details["movieid"]
    title = movie_details["title"]
    genres = movie_details["genres"]

    rating_avg_detail = get_rating_avg_details(cursor, movie_id)
    if rating_avg_detail is None:    
        rating_avg = ''
    else:
        rating_avg = rating_avg_detail["rating_avg"]

    tag_details = get_tags_unique_details(cursor, movie_id)
    if tag_details is None:
        tags = ''
    else:
        tags = tag_details["tags"]
    
    result = f"{title}, {genres}, {rating_avg}"
    for tag in tags:
        result += f', {tag}'
    
    return result

def insert_vector_sql(connection, cursor, movie_id, vector):
    # sql statement for inserting records
    sql = "INSERT INTO vectors (movieid, vector) VALUES (%s, %s)"

    # Execute the SQL statement with the provided data
    cursor.execute(sql,(movie_id, vector))

    # Commit the changes to the database
    connection.commit()

def insert_multiple_vectors_sql(connection, cursor):
    # Execute a SELECT query to retrieve all records from the 'movies' table
    cursor.execute("SELECT movieid FROM movies")

    # Fetch all rows from the result set
    rows = cursor.fetchall()

    # Get movieids from each record
    for row in rows:
        movie_id = row[0]
        vector = create_vector(cursor, movie_id)
        insert_vector_sql(connection, cursor, movie_id, vector)

connection = create_connection()
if connection:
    cursor = connection.cursor()
    insert_multiple_vectors_sql(connection, cursor)
else:
    print("Connection to the database failed.")
cursor.close()
connection.close()
