# Streaming Platform Database Management System

## Overview

This project is a Python-based MySQL database management system for a fictional streaming platform. It defines the database schema, supports bulk data import from CSV files, and provides command-line operations for inserting, updating, deleting, and querying data.

## Features

* Database initialization
  * Automatically creates all necessary tables.
  * Supports dropping and re-creating tables for a clean start.
* Data population
  * Imports user, producer, viewer, release, movie, series, video, session, and review data from CSV files.
* Command-line operations
  * Insert new viewers and movies
  * Add genres to users
  * Delete viewers
  * Insert streaming sessions
  * Update release titles
  * List releases reviewed by a specific viewer
  * Get the most popular releases by review count
  * Retrieve release and video details by session ID
  * List active viewers within a given date range
  * Count unique viewers for a given release

 ## Database Schema

* Users: General user information
* Producers: Users who publish releases
* Viewers: Users who watch releases
* Releases: All content published (movies and series)
* Movies: Releases that are movies
* Series: Releases that are series
* Videos: Individual episodes in a series
* Sessions: Viewing sessions by users
* Reviews: User reviews for releases
 
