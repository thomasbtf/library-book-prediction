import sqlite3
import pandas as pd
import os

database = 'book_database.db'

if os.path.exists(database):
  os.remove(database)
else:
  print('The file does not exist')

#-- create database and connect
conn = sqlite3.connect(database)
c = conn.cursor()

#-- performe sql commmands
#   create Books table
query = '''CREATE TABLE Books (
    ISBN VARCHAR NOT NULL,
    PRIMARY KEY (ISBN))'''
c.execute(query)
conn.commit()

#--- Store isbns in database
catalog = pd.read_pickle(r"D:\Users\Thomas\Google Drive\1. Master Studium\0. Thesis\Code\data\cleaned_data\catalog_cleaned.pkl")
isbns = pd.DataFrame(catalog[~catalog.ISBN.isnull()].ISBN.drop_duplicates().sort_values())
isbns.to_sql("Books", conn, if_exists= 'append', index = False)

#-- Create MARC21 tabke
query = '''CREATE TABLE MARC21 (
    ISBN VARCHAR NOT NULL,
    tag int NOT NULL,
    code VARCHAR NOT NULL,
    value text NOT NULL,
    PRIMARY KEY (ISBN, tag, code, value)
    )'''
c.execute(query)
conn.commit()

#-- Create downloaded table
query = '''CREATE TABLE Downloaded (
    ISBN VARCHAR NOT NULL,
    MARC21 ints,
    Blurb int,
    PRIMARY KEY (ISBN))'''
c.execute(query)
conn.commit()

query = '''CREATE TABLE Blurb (
    ISBN VARCHAR NOT NULL,
    text text NOT NULL,
    PRIMARY KEY (ISBN)
    )'''
conn.execute(query)
conn.commit()


conn.close()
