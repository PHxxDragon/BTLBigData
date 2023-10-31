import json 
import pymongo
import mysql.connector
from collections import namedtuple

DATA_FILE_PATH = "2023-10-30-15.json"

MONGODB_URL = "mongodb://localhost:27017/"
MONGO_DATABASE_NAME = "ghArchiveDB"
MONGO_COLLECTION_NAME = "ghlogs"

MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASSWORD = "mysql"
MYSQL_DATABASE_NAME = "ghArchiveDB"
MYSQL_TABLE_USER_NAME = "gh_user"
MYSQL_TABLE_REPO_NAME = "gh_repo"
TABLE_USER_SCHEMA = namedtuple("TableUserColumns", ["id", "login", "display_login", "gravatar_id", "url", "avatar_url"])
MYSQL_TABLE_USER_COLS = TABLE_USER_SCHEMA("ID", "LOGIN", "DISPLAY_LOGIN", "GRAVATAR_ID", "URL", "AVATAR_URL")
TABLE_REPO_SCHEMA = namedtuple("TableRepoColumns", ["id", "name", "url"])
MYSQL_TABLE_REPO_COLS = TABLE_REPO_SCHEMA("ID", "NAME", "URL")
USER_INSERT_SQL = f"""INSERT INTO {MYSQL_TABLE_USER_NAME} (
    {MYSQL_TABLE_USER_COLS.id}, 
    {MYSQL_TABLE_USER_COLS.login},
    {MYSQL_TABLE_USER_COLS.display_login},
    {MYSQL_TABLE_USER_COLS.avatar_url},
    {MYSQL_TABLE_USER_COLS.gravatar_id},
    {MYSQL_TABLE_USER_COLS.url}
) VALUES (%s, %s, %s, %s, %s, %s)"""
REPO_INSERT_SQL = f"""INSERT INTO {MYSQL_TABLE_REPO_NAME} (
    {MYSQL_TABLE_REPO_COLS.id}, 
    {MYSQL_TABLE_REPO_COLS.name},
    {MYSQL_TABLE_REPO_COLS.url}
) VALUES (%s, %s, %s)"""

def test_print_json(input_json) -> None:
    print(json.dumps(input_json, indent=4))

class MySQLImporter:
    def __init__(self) -> None:
        self.connector = mysql.connector.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASSWORD)
        self.cursor = self.connector.cursor()
        self.cursor.execute(f"DROP DATABASE IF EXISTS {MYSQL_DATABASE_NAME}")
        self.cursor.execute(f"CREATE DATABASE {MYSQL_DATABASE_NAME}")
        self.cursor.execute(f"USE {MYSQL_DATABASE_NAME}")
        self.cursor.execute(f"""CREATE TABLE {MYSQL_TABLE_USER_NAME} (
            {MYSQL_TABLE_USER_COLS.id} VARCHAR(40),
            {MYSQL_TABLE_USER_COLS.login} VARCHAR(100),
            {MYSQL_TABLE_USER_COLS.display_login} VARCHAR(100),
            {MYSQL_TABLE_USER_COLS.avatar_url} VARCHAR(1000),
            {MYSQL_TABLE_USER_COLS.gravatar_id} VARCHAR(100),
            {MYSQL_TABLE_USER_COLS.url} VARCHAR(1000)
        )""")
        self.cursor.execute(f"""CREATE TABLE {MYSQL_TABLE_REPO_NAME} (
            {MYSQL_TABLE_REPO_COLS.id} VARCHAR(40),
            {MYSQL_TABLE_REPO_COLS.name} VARCHAR(1000),
            {MYSQL_TABLE_REPO_COLS.url} VARCHAR(1000)     
        )""")

    def insert_json(self, input_json) -> None:
        user_values = (
            input_json["actor"]["id"],
            input_json["actor"]["login"],
            input_json["actor"]["display_login"],
            input_json["actor"]["avatar_url"],
            input_json["actor"]["gravatar_id"],
            input_json["actor"]["url"]
        )
        self.cursor.execute(USER_INSERT_SQL, user_values)
        repo_values = (
            input_json["repo"]["id"],
            input_json["repo"]["name"],
            input_json["repo"]["url"]
        )
        self.cursor.execute(REPO_INSERT_SQL, repo_values)


class MongoDBImporter:
    def __init__(self) -> None:
        client = pymongo.MongoClient(MONGODB_URL)
        gh_database = client[MONGO_DATABASE_NAME]
        self.gh_collection = gh_database[MONGO_COLLECTION_NAME]
        self.gh_collection.drop()
        self.pending_rows = []

    def insert_json(self, input_json) -> None:
        processed_json = self._preprocess_mongodb(input_json)
        self._insert_to_mongodb(processed_json)

    def flush(self) -> None:
        self.gh_collection.insert_many(self.pending_rows)
        self.pending_rows.clear()

    def _preprocess_mongodb(self, input_json):
        input_json["actor"] = input_json["actor"]["id"]
        input_json["repo"] = input_json["repo"]["id"]
        return input_json

    def _insert_to_mongodb(self, data_json) -> None:
        self.pending_rows.append(data_json)
        if (len(self.pending_rows) > 1000):
            self.flush()

def main() -> None:
    data_file = open(DATA_FILE_PATH, 'r')
    mongodb_importer = MongoDBImporter()
    mysql_importer = MySQLImporter()
    for line in data_file.readlines():
        mongodb_importer.insert_json(json.loads(line))
        mysql_importer.insert_json(json.loads(line))
    mongodb_importer.flush()

if __name__ == "__main__":
    main()