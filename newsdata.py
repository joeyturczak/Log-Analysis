#!/usr/bin/env python3

import psycopg2


def db_connect(database_name="news"):
    """Connect to a database"""
    try:
        db = psycopg2.connect(database="{}".format(database_name))
        cursor = db.cursor()
        return db, cursor
    except:
        print("Error connecting to database")


def most_popular_posts():
    """Return the three most popular articles of all time"""
    db, c = db_connect()

    c.execute("select title, count(*) as views "
              "from articles, log "
              "where log.path = concat('/article/', articles.slug) "
              "group by title "
              "order by views desc "
              "limit 3;")

    return c.fetchall()

    db.close()


def most_popular_authors():
    """Return the most popular authors of all time"""
    db, c = db_connect()

    c.execute("select name, count(*) as views "
              "from authors, articles, log "
              "where authors.id = articles.author "
              "and log.path = concat('/article/', articles.slug) "
              "group by name "
              "order by views desc;")
    return c.fetchall()

    db.close()


def request_errors():
    """Return the days on which more than 1% of requests had errors"""
    db, c = db_connect()

    c.execute("select time, round((errors * 100.0) / total, 1) as error_perc "
              "from (select time::date as time, count(*) as total, count(case "
              "when status != '200 OK' then 1 end) as errors from log "
              "group by time::date) as subq where ((errors * 100) / "
              "total) > 1;")
    return c.fetchall()

    db.close()

if __name__ == '__main__':
    # Get and print out three most popular articles
    print("The three most popular articles of all time: \n")
    rows = most_popular_posts()
    for row in rows:
        print("%s -- %s views\n" % (row[0], row[1]))

    # Get and print out the most popular authors of all time
    print("\nThe most popular authors of all time: \n")
    rows = most_popular_authors()
    for row in rows:
        print("%s -- %s views\n" % (row[0], str(row[1])))

    # Get and print out the days which more than 1% of requests lead to errors
    print("\nThe days which more than 1% of requests lead to errors: \n")
    rows = request_errors()
    for row in rows:
        print("%s -- %s%% errors\n" % (str(row[0]), str(row[1])))
