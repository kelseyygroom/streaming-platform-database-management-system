import sys
import mysql.connector
from setup_db import initialize_db, populate_db
import sys

###############################################################################################
#
# Note, these are
#
# 8. If the output is boolean, print “Success” or “Fail”.
# 9. If the output is a result table, print each record in one line and separate columns with
# ‘,’ - just like the format of the dataset file.

# Todo: for all functions, should probalby put cursor.close() and db.close() calls after
#  except block like in insert_viewer so we can be sure they are closed whether or not we
#  have an exception.
###############################################################################################

def open_db_connection():
    return mysql.connector.connect(
        host="localhost",
        user="test",
        password="password",
        database="cs122a",
        allow_local_infile=True
    )

def import_data(folder_name):
# assumes the folder to read is in the same directory as project.py.
# todo: determine if this assumption is correct. check ed discussion, ask TA, etc.
    try:
        #print(f"Importing folder: {folder_name}")
        db_connection = open_db_connection()
        initialize_db(db_connection)
        populate_db(db_connection, folder_name)
        db_connection.close()
        print("Success")
    except Exception as e:
        #print(f"Error importing folder '{folder_name}': {e}")
        print("Fail")


def insert_viewer(
    uid,
    email,
    nickname,
    street,
    city,
    state,
    zip,
    genres,
    joined_date,
    first,
    last,
    subscription
):
    db = open_db_connection()
    cursor = db.cursor()
    #Tries to insert a user first
    try:
       pass
       cursor.execute("INSERT INTO USERS (uid,email,joined_date,nickname,street,city,state,zip,genres) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                      (uid, email, joined_date, nickname, street, city, state, zip, genres))
       db.commit()
    except Exception:
        #Should only fail when there's a duplicate in the system
       pass
   
    #Then tries to insert a viewer
    try:
        #print(f"Inserting Viewer: uid={uid}, email={email}, nickname={nickname}, street={street}, city={city}, state={state}, zip={zip}, genres={genres}, joined_date={joined_date}, first={first}, last={last}, subscription={subscription}")
       
        cursor.execute("INSERT INTO Viewers (uid, subscription, first_name, last_name) VALUES (%s,%s,%s,%s)",
                       (uid, subscription, first, last))
        db.commit()
        print("Success")
    except Exception as e:
        print("Fail")
        #print(e)
    finally:
        cursor.close()
        db.close()


def add_genre(uid, genre):
    try:
        #print(f"Adding Genre: uid={uid}, genre={genre}")
       
        db = open_db_connection()
        cursor = db.cursor()

        cursor.execute("SELECT genres FROM Users WHERE uid = %s", (uid,))
        genres = cursor.fetchone()
       
        if genres != None:                    # only add semicolon if existing genres for User
            genres = genres[0].strip()
            
            if genre in genres.split(";"):
                raise ValueError("Genre already exists in User record")
            
            genre = genres + ";" + genre
       
        cursor.execute("UPDATE Users set genres = %s WHERE uid = %s;", (genre, uid))
        db.commit()
        db.close()
        print("Success")
    except Exception as e:
        #print(f"Error adding genre: {e}")
        print("Fail")

def delete_viewer(uid):
    try:
        #print(f"Deleting viewer: uid={uid}")
       
        db_connection = open_db_connection()
        cursor = db_connection.cursor()

        cursor.execute("DELETE FROM Viewers WHERE uid = %s", (uid,))
       
        db_connection.commit()
        db_connection.close()
        print("Success")
    except Exception as e:
        #print(f"Error deleting viewer: {e}")
        print("Fail")


def insert_movie(rid, website_url):
    try:
        #print(f"Inserting movie: rid={rid}, website_url={website_url}")
       
        db_connection = open_db_connection()
        cursor = db_connection.cursor()
        cursor.execute("INSERT INTO Movies (rid,website_url) VALUES (%s,%s)",
                      (rid, website_url))
       
        db_connection.commit()
        db_connection.close()
        print("Success")
    except Exception as e:
        #print(f"Error inserting movie: {e}")
        print("Fail")


def insert_session(
    sid,
    uid,
    rid,
    ep_num,
    initiate_at,
    leave_at,
    quality,
    device
):
    try:
        #print(f"Inserting Session: sid={sid}, uid={uid}, rid={rid}, ep_num={ep_num}, initiate_at={initiate_at}, leave_at={leave_at}, quality={quality}, device={device}")
       
        db_connection = open_db_connection()
        cursor = db_connection.cursor()
        cursor.execute("INSERT INTO Sessions (sid,uid,rid,ep_num,initiate_at,leave_at,quality,device)VALUES (%s,%s,%s,%s,%s,%s,%s,%s);",
                      (sid,uid,rid,ep_num,initiate_at,leave_at,quality,device))
        db_connection.commit()
        db_connection.close()
        print("Success")
    except Exception as e:
        #print(f"Error inserting session: {e}")
        print("Fail")

def update_release(rid, title):
    try:
        #print(f"Updating Release: rid={rid}, title={title}")    
        db = open_db_connection()
        cursor = db.cursor()
        cursor.execute("UPDATE Releases set title = %s WHERE rid = %s;", (title, rid))
        db.commit()
        db.close()
        print("Success")
    except Exception as e:
        #print(f"Error updating release: {e}")
        print("Fail")

def list_releases(uid):
    try:
        #print(f"Listing releases reviewed by viewer: uid={uid}")
       
        db = open_db_connection()
        cursor = db.cursor()
        cursor.execute("""SELECT r.rid, r.genre, r.title
                       FROM Releases r
                       INNER JOIN Reviews rv ON r.rid = rv.rid
                       WHERE uid = %s
                       ORDER BY r.title ASC""", (uid,))

        results = cursor.fetchall()
        # print("rid,genre,title") --- EDIT: don't need to print heading
        for row in results:
            print(f"{row[0]},{row[1]},{row[2]}")
           
        db.close()            
    except Exception as e:
        #print(f"Error listing releases: {e}")
        pass            # shouldn't print fail on error

def popular_release(n):
    try:
        #print(f"Listing popular releases: n={n}")
       
        db = open_db_connection()
        cursor = db.cursor()
        # Database logic goes here
        cursor.execute('''SELECT R.rid, R.title, COUNT(rv.rid) AS reviewCount
                       FROM Releases as R
                       INNER JOIN Reviews rv ON R.rid = rv.rid
                       GROUP BY R.rid
                       ORDER BY reviewCount DESC, R.rid DESC
                       LIMIT %s''', (int(n),))
       
        results = cursor.fetchall()
       
        #Not realy sure how we're supposed to print, do we need title? (EdStem #415)
        #We should check assigment and ed discussion after we get all the core stuff working
        # then fix these things for all functions where we have to print (CM)
        #print("rid, title, reviewCount")
        for row in results:
            print(f"{row[0]},{row[1]},{row[2]}")    
        db.close()
    except Exception as e:
        #print(f"Error listing popular releases: {e}")
        print("Fail")

def release_title(sid):
    try:
        #print(f"Getting title for release: sid={sid}")
       
        db = open_db_connection()
        cursor = db.cursor()

        cursor.execute("""
            SELECT r.rid, r.title AS release_title, r.genre,
                   v.title AS video_title, v.ep_num, v.length
            FROM Sessions AS s
            INNER JOIN Releases r ON r.rid = s.rid
            INNER JOIN Videos v ON r.rid = v.rid
            WHERE s.sid = %s
            ORDER BY r.title ASC;
            """, (sid,)
        )
        results = cursor.fetchall()

        #print("rid,release_title,genre,video_title,ep_num,length")
        for row in results:
            print(f"{row[0]},{row[1]},{row[2]},{row[3]},{row[4]},{row[5]}")
    except Exception as e:
       # print(f"Error getting release title: {e}")
        print("Fail")
    cursor.close()
    db.close()


def active_viewers(n, start, end):
    try:
        #print(f"Listing active viewers: n={n}, start={start}, end={end}")
       
        db = open_db_connection()
        cursor = db.cursor()
        query = """
            SELECT v.uid AS UID, v.first_name AS First_Name, v.last_name AS Last_Name
            FROM Viewers AS v
            JOIN Sessions AS s ON v.uid = s.uid
            WHERE s.initiate_at BETWEEN %s AND %s
            GROUP BY v.uid
            HAVING COUNT(s.sid) >= %s
            ORDER BY v.uid ASC;
        """
        cursor.execute(query, (start, end, n))
        results = cursor.fetchall()
        db.close()

        #print("UID,first name,last name")
        for row in results:  
            print(f"{row[0]},{row[1]},{row[2]}")
    except Exception as e:
        #print(f"Error listing active viewers: {e}")
        print("Fail")

def videos_viewed(rid):
    try:
       # print(f"Listing count of unique viewers for release: rid={rid}")
       
        db_connection = open_db_connection()

        # Database logic goes here ############ TODO: Thought we were done, but STILL NEED TO DO THIS ONE ###########

        db_connection.close()
       
    except Exception as e:
        #print(f"Error listing viewer count: {e}")
        print("Fail")

def main():
    function = sys.argv[1].lower()
   
    #7) If the input is NULL, treat it as the None type in Python, not a string called “NULL”.
    for i in range(len(sys.argv)):
        if sys.argv[i] == "NULL":
            sys.argv[i] = None
           

    if function == "import":
        import_data(sys.argv[2])
    elif function == "insertviewer":
        insert_viewer(
            sys.argv[2],  # uid
            sys.argv[3],  # email
            sys.argv[4],  # nickname
            sys.argv[5],  # street
            sys.argv[6],  # city
            sys.argv[7],  # state
            sys.argv[8],  # zip
            sys.argv[9],  # genres
            sys.argv[10], # joined_date
            sys.argv[11], # first
            sys.argv[12], # last
            sys.argv[13]  # subscription
        )
    elif function == "addgenre":
        add_genre(
            sys.argv[2],  # uid
            sys.argv[3]   # genre
        )
    elif function == "deleteviewer":
        delete_viewer(
            sys.argv[2]   # uid
        )
    elif function == "insertmovie":
        insert_movie(
            sys.argv[2],  # rid
            sys.argv[3]   # website_url
        )
    elif function == "insertsession":
        insert_session(
            sys.argv[2],  # sid
            sys.argv[3],  # uid
            sys.argv[4],  # rid
            sys.argv[5],  # ep_num
            sys.argv[6],  # initiate_at
            sys.argv[7],  # leave_at
            sys.argv[8],  # quality
            sys.argv[9]   # device
        )
    elif function == "updaterelease":
        update_release(
            sys.argv[2],  # rid
            sys.argv[3]   # title
        )
    elif function == "listreleases":
        list_releases(
            sys.argv[2]  # uid
        )
    elif function == "popularrelease":
        popular_release(
            sys.argv[2]   # N
        )
    elif function == "releasetitle":
        release_title(
            sys.argv[2]   # sid
        )
    elif function == "activeviewer":
        active_viewers(
            sys.argv[2],  # n
            sys.argv[3],  # start
            sys.argv[4]   # end
        )
    elif function == "videosviewed":
        videos_viewed(
            sys.argv[2]   # rid
        )

if __name__ == "__main__":
    main()