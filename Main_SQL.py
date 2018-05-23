#! /usr/bin/env python3

import psycopg2

DBNAME = "news"

query_1_most_popular_articles = """ SELECT title, count(log.time) 
                                    FROM articles, log 
                                    WHERE path LIKE concat('%', slug) 
                                    GROUP BY title 
                                    ORDER BY count(log.time) desc 
                                    LIMIT 3;
                                """

query_2_all_time_most_popular_authors = """ SELECT authors.name, count(log.time)
                                            FROM articles, authors, log 
                                            WHERE path LIKE concat('%', slug) and authors.id = articles.author 
                                            GROUP BY authors.name 
                                            ORDER BY count(log.time) desc 
                                            LIMIT 4;
                                        """

query_3_day_with_most_errors = """ SELECT time AS day, CONCAT(round(percentage,2),'% errors')
                                   FROM errorPercent
                                   WHERE percentage > 1
                                   ORDER BY time desc;
                               """


def main():
    conn = psycopg2.connect(database=DBNAME)
    cursor = conn.cursor()
    cursor.execute(query_1_most_popular_articles)
    results = cursor.fetchall()
    print("1. What are the most popular three articles of all time?\n")
    for i in results:
        print('\t' + str(i[0]) + ' | --> | ' + str(i[1]) + ' views')

    cursor.execute(query_2_all_time_most_popular_authors)
    results = cursor.fetchall()
    print("\n2. Who are the most popular article authors of all time?\n")
    for i in results:
        print('\t' + str(i[0]) + ' | --> | ' + str(i[1]) + ' views')

    cursor.execute(query_3_day_with_most_errors)
    results = cursor.fetchall()
    print("\n3. On which days did more than 1% of requests lead to errors?\n")
    for i in results:
        print('\t' + str(i[0]) + ' | --> | ' + str(i[1]) + ' %')

    conn.close()

    if __name__ == "__main__":
        main()