CREATE KEYSPACE movielens WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};

USE movielens;

CREATE TABLE movies (
    movie_id INT PRIMARY KEY,
    title TEXT,
    avg_rating DECIMAL,
    total_ratings INT,
    genres SET<TEXT>
);

CREATE TABLE ratings_by_movie (
    movie_id INT,
    user_id INT,
    rating DECIMAL,
    ts INT,
    PRIMARY KEY(movie_id, user_id)
);

CREATE TABLE ratings_by_user (
    user_id INT,
    movie_id INT,
    rating DECIMAL,
    ts INT,
    PRIMARY KEY(user_id, movie_id)
);
