import mysql.connector
from mysql.connector.errors import IntegrityError
from mysql.connector.errors import DataError
import time

def delete_data(name):
    cnx = mysql.connector.connect(user='root', password='123456',
                                  host='127.0.0.1',
                                  database='IMDB')
    cursor = cnx.cursor()
    sql_Delete_query = """DELETE FROM {}""".format(name)
    cursor.execute(sql_Delete_query)
    cnx.commit()

def show_data_in_table(name):
    cnx = mysql.connector.connect(user='root', password='123456',
                                  host='127.0.0.1',
                                  database='IMDB')
    cursor = cnx.cursor()
    cursor.execute('SELECT * FROM {}'.format(name))
    for x in cursor:
        print(x)
        time.sleep(0.1)


def fill_full_from_title_basics():
    cnx = mysql.connector.connect(user='root', password='123456',
                                  host='127.0.0.1',
                                  database='IMDB')
    cursor = cnx.cursor()
    add_title_basics = ('INSERT INTO title_basics '
                        '(titleBasicsId, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runTimeMinutes, genres)'
                        'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)')
    with open("D:\\IMDB_datasets\\title.basics.tsv\\data.tsv", 'r', encoding='utf-8') as file:

        counter = 0
        while True:
            try:
                line = file.readline()
                line = line.replace('\n', '').replace('\\N', '').split('\t')
                try:
                    int(line[7])
                except ValueError:
                    line[7] = 0

                if counter == 0:
                    counter += 1
                    continue
                if counter % 10000 == 0:
                    cnx.commit()
                try:
                    value_title_basics = (line[0].replace('tt', ''), line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8])
                    cursor.execute(add_title_basics, value_title_basics)
                except IntegrityError:
                    pass
            except IndexError:
                break
        cnx.commit()
        cursor.close()
        cnx.close()

def fill_full_from_title_akas():
    cnx = mysql.connector.connect(user='root', password='123456',
                                  host='127.0.0.1',
                                  database='IMDB')
    cursor = cnx.cursor()
    add_title_akas = ('INSERT INTO title_akas '
                        '(title, region, language, types, attributes, isOriginalTitle, title_basics_titleBasicsId)'
                        'VALUES (%s, %s, %s, %s, %s, %s, %s)')
    with open("D:\\IMDB_datasets\\title.akas.tsv\\data.tsv", 'r', encoding='utf-8') as file:
        counter = 0
        while True:
            try:
                line = file.readline()
                line = line.replace('\n', '').replace('\\N', '').split('\t')
                if counter == 0:
                    counter += 1
                    continue
                if counter%10000==0:
                    cnx.commit()

                try:
                    value_title_akas = (line[2], line[3], line[4], line[5], line[6], line[7], line[0].replace('tt', ''))
                    print(line)
                    cursor.execute(add_title_akas, value_title_akas)
                    counter+=1
                except IntegrityError:
                    pass
            except IndexError:
                break
        cnx.commit()
        cursor.close()
        cnx.close()

def fill_full_from_title_crew():
    cnx = mysql.connector.connect(user='root', password='123456',
                                  host='127.0.0.1',
                                  database='IMDB')
    cursor = cnx.cursor()
    add_title_crew = ('INSERT INTO title_crew'
                        '(title_basics_titleBasicsId, directors, writers)'
                        'VALUES (%s, %s, %s)')
    with open("D:\\IMDB_datasets\\title.crew.tsv\\data.tsv", 'r', encoding='utf-8') as file:
        counter = 0
        while True:
            try:
                line = file.readline()
                line = line.replace('\n', '').replace('\\N', '').split('\t')
                if counter == 0:
                    counter += 1
                    continue
                if counter % 10000 == 0:
                    cnx.commit()
                try:
                    value_title_crew = (line[0].replace('tt', ''), line[1], line[2])
                    print(line)
                    cursor.execute(add_title_crew, value_title_crew)
                except IntegrityError:
                    pass
                except DataError:
                    line[1] = str(line[1].split(',')[0:1000])
                    line[2] = str(line[2].split(',')[0:1000])
                    value_title_crew = (line[0].replace('tt', ''), line[1], line[2])
                    print(line)
                    cursor.execute(add_title_crew, value_title_crew)
            except IndexError:
                break
        cnx.commit()
        cursor.close()
        cnx.close()

def fill_full_from_title_episode():
    cnx = mysql.connector.connect(user='root', password='123456',
                                  host='127.0.0.1',
                                  database='IMDB')
    cursor = cnx.cursor()
    add_title_crew = ('INSERT INTO title_episode'
                        '(title_basics_titleBasicsId, parentTconst, seasonNumber, episodeNumber)'
                        'VALUES (%s, %s, %s, %s)')
    with open("D:\\IMDB_datasets\\title.episode.tsv\\data.tsv", 'r', encoding='utf-8') as file:
        counter = 0
        for line in file.readlines():
            line = line.replace('\n', '').replace('\\N', '').split('\t')
            if counter == 0:
                counter += 1
                continue
            if counter % 10000 == 0:
                cnx.commit()
            try:
                line[2] = int(line[2])
            except ValueError:
                line[2] = 0
            try:
                line[3] = int(line[3])
            except ValueError:
                line[3] = 0
            try:
                value_title_crew = (line[0].replace('tt', ''), line[1].replace('tt', ''), line[2], line[3])
                print(line)
                cursor.execute(add_title_crew, value_title_crew)
            except IntegrityError:
                pass

        cnx.commit()
        cursor.close()
        cnx.close()

def fill_full_from_title_principals():
    cnx = mysql.connector.connect(user='root', password='123456',
                                  host='127.0.0.1',
                                  database='IMDB')
    cursor = cnx.cursor()
    add_title_principals = ('INSERT INTO title_principals '
                        '(title_basics_titleBasicsId, name_basics_nconst, category, job, characters)'
                        'VALUES (%s, %s, %s, %s, %s)')
    with open("D:\\IMDB_datasets\\title.principals.tsv\\data.tsv", 'r', encoding='utf-8') as file:
        counter = 0
        while True:
            try:
                line = file.readline()
                line = line.replace('\n', '').replace('\\N', '').split('\t')
                if counter == 0:
                    counter += 1
                    continue
                if counter%10000==0:
                    cnx.commit()

                try:
                    value_title_principals = (line[0].replace('tt', ''), line[2].replace('nm', ''), line[3], line[4], line[5])
                    # print(line)
                    cursor.execute(add_title_principals, value_title_principals)
                    counter+=1
                except IntegrityError:
                    pass
                except DataError:
                    print(line)
                    line[4] = str(line[4].split(',')[0:50])
                    line[5] = str(line[5].split(',')[0:50])
                    value_title_principals = (line[0].replace('tt', ''), line[2].replace('nm', ''), line[3], line[4], line[5])
                    cursor.execute(add_title_principals, value_title_principals)
            except IndexError:
                break
        cnx.commit()
        cursor.close()
        cnx.close()

def fill_full_from_title_ratings():
    cnx = mysql.connector.connect(user='root', password='123456',
                                  host='127.0.0.1',
                                  database='IMDB')
    cursor = cnx.cursor()

    add_title_ratings = ('INSERT INTO title_ratings'
                        '(title_basics_titleBasicsId, averageRating, numVotes)'
                        'VALUES (%s, %s, %s)')
    with open("D:\\IMDB_datasets\\title.ratings.tsv\\data.tsv", 'r', encoding='utf-8') as file:
        counter = 0
        while True:
            try:
                line = file.readline()
                line = line.replace('\n', '').replace('\\N', '').split('\t')
                if counter == 0:
                    counter += 1
                    continue
                if counter%10000==0:
                    cnx.commit()

                try:
                    line[2] = int(line[2])
                except ValueError:
                    line[2] = 0
                try:
                    line[1] = float(line[1])
                except ValueError:
                    line[1] = 0


                try:
                    value_title_ratings = (line[0].replace('tt', ''), line[1], line[2])
                    print(line)
                    cursor.execute(add_title_ratings, value_title_ratings)
                    counter+=1
                except IntegrityError:
                    pass
            except IndexError:
                break


        cnx.commit()
        cursor.close()
        cnx.close()

def fill_full_from_name_basics():
    cnx = mysql.connector.connect(user='root', password='123456',
                                  host='127.0.0.1',
                                  database='IMDB')
    cursor = cnx.cursor()
    add_name_basics = ('INSERT INTO name_basics'
                        '(nconst, primaryName, birthYear, deathYear, primaryProfession, knownForTitles)'
                        'VALUES (%s, %s, %s, %s, %s, %s)')
    with open("D:\\IMDB_datasets\\name.basics.tsv\\data.tsv", 'r', encoding='utf-8') as file:
        counter = 0
        while True:
            try:
                line = file.readline()
                line = line.replace('\n', '').replace('\\N', '').split('\t')
                if counter == 0:
                    counter += 1
                    continue
                if counter%10000==0:
                    cnx.commit()

                try:
                    value_name_basics = (line[0].replace('nm', ''), line[1], line[2], line[3], line[4], line[5])
                    print(line)
                    cursor.execute(add_name_basics, value_name_basics)
                    counter+=1
                except IntegrityError:
                    pass
                except DataError:
                    line[4] = str(line[4].split(',')[0:50])
                    line[5] = str(line[5].split(',')[0:50])
                    value_name_basics = (line[0].replace('nm', ''), line[1], line[2], line[3], line[4], line[5])
                    cursor.execute(add_name_basics, value_name_basics)
            except IndexError:
                break
        cnx.commit()
        cursor.close()
        cnx.close()


# fill_full_from_title_basics()
# fill_full_from_title_akas()
# fill_full_from_title_crew()
# fill_full_from_title_episode()
# fill_full_from_title_ratings()
# fill_full_from_name_basics()
#fill_full_from_title_principals()