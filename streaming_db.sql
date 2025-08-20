DROP DATABASE IF EXISTS cs122a;
CREATE DATABASE cs122a;
USE cs122a;

-- Define entities and their supporting tables below here

-- Q1: User Identification and Roles (SQL DDL for entity(ies) table(s) only)

-- Users Table (10 points)
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

-- Producers Table (Delta Table for ISA Relationship) (10 points)
CREATE TABLE Producers (
    uid INT,
    bio TEXT,
    company TEXT,
    PRIMARY KEY (uid),
    FOREIGN KEY (uid) REFERENCES Users(uid) ON DELETE CASCADE
);

-- Viewers Table (Delta Table for ISA Relationship)(10 points)
CREATE TABLE Viewers (
    uid INT,
    subscription ENUM('free', 'monthly', 'yearly'),
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    PRIMARY KEY (uid),
    FOREIGN KEY (uid) REFERENCES Users(uid) ON DELETE CASCADE
);


-- Q2: Content Management (SQL DDL for entity(ies) table(s) only)

-- Releases Table(10 points)
CREATE TABLE Releases (
    rid INT,
    producer_uid INT NOT NULL,
    title TEXT NOT NULL,
    genre TEXT NOT NULL,
    release_date DATE NOT NULL,
    PRIMARY KEY (rid),
    FOREIGN KEY (producer_uid) REFERENCES Producers(uid) ON DELETE CASCADE
);

-- Movies Table (Delta Table for ISA Relationship)(10 points)
CREATE TABLE Movies (
    rid INT,
    website_url TEXT,
    PRIMARY KEY (rid),
    FOREIGN KEY (rid) REFERENCES Releases(rid) ON DELETE CASCADE
);

-- Table (Delta Table for ISA Relationship)(10 points)
CREATE TABLE Series (
    rid INT,
    introduction TEXT,
    PRIMARY KEY (rid),
    FOREIGN KEY (rid) REFERENCES Releases(rid) ON DELETE CASCADE
);

-- Videos Table (Weak Entity)(10 points)
CREATE TABLE Videos (
    rid INT,
    ep_num INT NOT NULL,
    title TEXT NOT NULL,
    length INT NOT NULL,
    PRIMARY KEY (rid, ep_num),
    FOREIGN KEY (rid) REFERENCES Releases(rid) ON DELETE CASCADE
);

-- Q3: Streaming Sessions (SQL DDL for entity(ies) table(s) only)

-- Sessions Table (15 points)
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


-- Q4: Community Interactions (SQL DDL for entity(ies) table(s) only)

-- Reviews Table (15 points)
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

LOAD DATA LOCAL INFILE 'users.csv' into table users FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS;
LOAD DATA LOCAL INFILE 'producers.csv' into table producers FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS;
LOAD DATA LOCAL INFILE 'viewers.csv' into table viewers FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS;
LOAD DATA LOCAL INFILE 'releases.csv' into table releases FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS;
LOAD DATA LOCAL INFILE 'movies.csv' into table movies FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS;
LOAD DATA LOCAL INFILE 'series.csv' into table series FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS;
LOAD DATA LOCAL INFILE 'videos.csv' into table videos FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS;
LOAD DATA LOCAL INFILE 'sessions.csv' into table sessions FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS;
LOAD DATA LOCAL INFILE 'reviews.csv' into table reviews FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS;
