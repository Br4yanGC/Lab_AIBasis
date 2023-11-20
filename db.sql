-- Create database
CREATE DATABASE lab18;

-- Connect to the newly created database
\c lab18;

-- Create tables
CREATE TABLE links (
    movieId INT,
    imdbId INT,
    tmdbId INT
);

CREATE TABLE movies (
    movieId INT,
    title TEXT,
    genres TEXT
);

CREATE TABLE ratings (
    userId INT,
    movieId INT,
    rating FLOAT,
    timestamp INT
);

CREATE TABLE tags (
    userId INT,
    movieId INT,
    tag TEXT,
    timestamp INT
);

CREATE TABLE vectors (
    movieId INT,
    vector TEXT
);

-- Import data from CSV into tables
COPY links FROM '/Users/brayangc/winter-holidays/ingsoft2_labs/lab_18/data/links.csv' DELIMITER ',' CSV HEADER;
COPY movies FROM '/Users/brayangc/winter-holidays/ingsoft2_labs/lab_18/data/movies.csv' DELIMITER ',' CSV HEADER;
COPY ratings FROM '/Users/brayangc/winter-holidays/ingsoft2_labs/lab_18/data/ratings.csv' DELIMITER ',' CSV HEADER;
COPY tags FROM '/Users/brayangc/winter-holidays/ingsoft2_labs/lab_18/data/tags.csv' DELIMITER ',' CSV HEADER;

-- psql -U brayangc -f ./db.sql