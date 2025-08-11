import mysql.connector
import csv

def create_table_users(cursor):
    cursor.execute("""
        CREATE TABLE Users (
            uid INT,
            email TEXT NOT NULL,
            joined_date DATE NOT NULL,
            nickname TEXT NOT NULL,
            street TEXT,
            city TEXT,
            state TEXT,
            zip TEXT,
            genres TEXT,
            PRIMARY KEY (uid)
        );
    """)

def create_table_producers(cursor):
    cursor.execute("""
        CREATE TABLE Producers (
            uid INT,
            bio TEXT,
            company TEXT,
            PRIMARY KEY (uid),
            FOREIGN KEY (uid) REFERENCES Users(uid) ON DELETE CASCADE
        );
    """)

def create_table_viewers(cursor):
    cursor.execute("""
        CREATE TABLE Viewers (
            uid INT,
            subscription ENUM('free', 'monthly', 'yearly'),
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            PRIMARY KEY (uid),
            FOREIGN KEY (uid) REFERENCES Users(uid) ON DELETE CASCADE
        );
    """)

def create_table_releases(cursor):
    cursor.execute("""
        CREATE TABLE Releases (
            rid INT,
            producer_uid INT NOT NULL,
            title TEXT NOT NULL,
            genre TEXT NOT NULL,
            release_date DATE NOT NULL,
            PRIMARY KEY (rid),
            FOREIGN KEY (producer_uid) REFERENCES Producers(uid) ON DELETE CASCADE
        );
    """)

def create_table_movies(cursor):
    cursor.execute("""
        CREATE TABLE Movies (
            rid INT,
            website_url TEXT,
            PRIMARY KEY (rid),
            FOREIGN KEY (rid) REFERENCES Releases(rid) ON DELETE CASCADE
        );
    """)

def create_table_series(cursor):
    cursor.execute("""
        CREATE TABLE Series (
            rid INT,
            introduction TEXT,
            PRIMARY KEY (rid),
            FOREIGN KEY (rid) REFERENCES Releases(rid) ON DELETE CASCADE
        );
    """)

def create_table_videos(cursor):
    cursor.execute("""
        CREATE TABLE Videos (
            rid INT,
            ep_num INT NOT NULL,
            title TEXT NOT NULL,
            length INT NOT NULL,
            PRIMARY KEY (rid, ep_num),
            FOREIGN KEY (rid) REFERENCES Releases(rid) ON DELETE CASCADE
        );
    """)

def create_table_sessions(cursor):
    cursor.execute("""
        CREATE TABLE Sessions (
            sid INT,
            uid INT NOT NULL,
            rid INT NOT NULL,
            ep_num INT NOT NULL,
            initiate_at DATETIME NOT NULL,
            leave_at DATETIME NOT NULL,
            quality ENUM('480p', '720p', '1080p'),
            device ENUM('mobile', 'desktop'),
            PRIMARY KEY (sid),
            FOREIGN KEY (uid) REFERENCES Viewers(uid) ON DELETE CASCADE,
            FOREIGN KEY (rid, ep_num) REFERENCES Videos(rid, ep_num) ON DELETE CASCADE
        );
    """)

def create_table_reviews(cursor):
    cursor.execute("""
        CREATE TABLE Reviews (
            rvid INT,
            uid INT NOT NULL,
            rid INT NOT NULL,
            rating DECIMAL(2, 1) NOT NULL CHECK (rating BETWEEN 0 AND 5),
            body TEXT,
            posted_at DATETIME NOT NULL,
            PRIMARY KEY (rvid),
            FOREIGN KEY (uid) REFERENCES Viewers(uid) ON DELETE CASCADE,
            FOREIGN KEY (rid) REFERENCES Releases(rid) ON DELETE CASCADE
        );
    """)

def drop_all_tables(cursor):
    cursor.execute("DROP TABLE IF EXISTS Reviews;")
    cursor.execute("DROP TABLE IF EXISTS Sessions;")
    cursor.execute("DROP TABLE IF EXISTS Videos;")
    cursor.execute("DROP TABLE IF EXISTS Series;")
    cursor.execute("DROP TABLE IF EXISTS Movies;")
    cursor.execute("DROP TABLE IF EXISTS Releases;")
    cursor.execute("DROP TABLE IF EXISTS Viewers;")
    cursor.execute("DROP TABLE IF EXISTS Producers;")
    cursor.execute("DROP TABLE IF EXISTS Users;")

def initialize_db(db_connection):
    cursor = db_connection.cursor()
    cursor.execute("USE cs122a;")

    drop_all_tables(cursor)

    create_table_users(cursor)
    create_table_producers(cursor)
    create_table_viewers(cursor)
    create_table_releases(cursor)
    create_table_movies(cursor)
    create_table_series(cursor)
    create_table_videos(cursor)
    create_table_sessions(cursor)
    create_table_reviews(cursor)

    db_connection.commit()
    cursor.close()


def populate_users(db_connection, folder_name):
    cursor = db_connection.cursor()
    cursor.execute("USE cs122a;")

    with open(f"{folder_name}/users.csv", mode='r') as f:
        file = csv.reader(f)

        next(file)
        for row in file:
            cursor.execute("INSERT INTO Users (uid,email,joined_date,nickname,street,city,state,zip,genres) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                      (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8]))

    db_connection.commit()
    cursor.close()


def populate_producers(db_connection, folder_name):
    cursor = db_connection.cursor()
    cursor.execute("USE cs122a;")

    with open(f"{folder_name}/producers.csv", mode='r') as f:
        file = csv.reader(f)

        next(file)
        for row in file:
            cursor.execute("INSERT INTO Producers (uid,bio,company) VALUES (%s,%s,%s)", (row[0], row[1], row[2]))

    db_connection.commit()
    cursor.close()


def populate_viewers(db_connection, folder_name):
    cursor = db_connection.cursor()
    cursor.execute("USE cs122a;")
    with open(f"{folder_name}/viewers.csv", mode='r') as f:
        file = csv.reader(f)

        next(file)
        for row in file:
            cursor.execute("INSERT INTO Viewers (uid,subscription,first_name,last_name) VALUES (%s,%s,%s,%s)",
                      (row[0], row[1], row[2], row[3]))

    db_connection.commit()
    cursor.close()


def populate_releases(db_connection, folder_name):
    cursor = db_connection.cursor()
    cursor.execute("USE cs122a;")

    with open(f"{folder_name}/releases.csv", mode='r') as f:
        file = csv.reader(f)

        next(file)
        for row in file:
            cursor.execute("INSERT INTO Releases (rid,producer_uid,title,genre,release_date) VALUES (%s,%s,%s,%s,%s)",
                      (row[0], row[1], row[2], row[3], row[4]))

    db_connection.commit()
    cursor.close()


def populate_movies(db_connection, folder_name):
    cursor = db_connection.cursor()
    cursor.execute("USE cs122a;")

    with open(f"{folder_name}/movies.csv", mode='r') as f:
        file = csv.reader(f)

        next(file)
        for row in file:
            cursor.execute("INSERT INTO Movies (rid,website_url) VALUES (%s,%s)", (row[0], row[1]))

    db_connection.commit()
    cursor.close()


def populate_series(db_connection, folder_name):
    cursor = db_connection.cursor()
    cursor.execute("USE cs122a;")
    with open(f"{folder_name}/series.csv", mode='r') as f:
        file = csv.reader(f)

        next(file)
        for row in file:
            cursor.execute("INSERT INTO Series (rid,introduction) VALUES (%s,%s)",
                      (row[0], row[1]))

    db_connection.commit()
    cursor.close()


def populate_videos(db_connection, folder_name):
    cursor = db_connection.cursor()
    cursor.execute("USE cs122a;")
    with open(f"{folder_name}/videos.csv", mode='r') as f:
        file = csv.reader(f)

        next(file)
        for row in file:
            cursor.execute("INSERT INTO Videos (rid,ep_num,title,length) VALUES (%s,%s,%s,%s)", 
                           (row[0], row[1], row[2], row[3]))

    db_connection.commit()
    cursor.close()


def populate_sessions(db_connection, folder_name):
    cursor = db_connection.cursor()
    cursor.execute("USE cs122a;")
    with open(f"{folder_name}/sessions.csv", mode='r') as f:
        file = csv.reader(f)

        next(file)
        for row in file:
            cursor.execute("INSERT INTO Sessions (sid,uid,rid,ep_num,initiate_at,leave_at,quality,device) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", 
                           (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7]))

    db_connection.commit()
    cursor.close()


def populate_reviews(db_connection, folder_name):
    cursor = db_connection.cursor()
    cursor.execute("USE cs122a;")
    with open(f"{folder_name}/reviews.csv", mode='r') as f:
        file = csv.reader(f)

        next(file)
        for row in file:
            cursor.execute("INSERT INTO Reviews (rvid,uid,rid,rating,body,posted_at) VALUES (%s,%s,%s,%s,%s,%s)", 
                           (row[0], row[1], row[2], row[3], row[4], row[5]))

    db_connection.commit()
    cursor.close()


def populate_db(db_connection, folder_name):
    cursor = db_connection.cursor()
    cursor.execute("USE cs122a;")

    populate_users(db_connection, folder_name)
    populate_producers(db_connection, folder_name)
    populate_viewers(db_connection, folder_name)
    populate_releases(db_connection, folder_name)
    populate_movies(db_connection, folder_name)
    populate_series(db_connection, folder_name)
    populate_videos(db_connection, folder_name)
    populate_sessions(db_connection, folder_name)
    populate_reviews(db_connection, folder_name)


    db_connection.commit()
    cursor.close()
