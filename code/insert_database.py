import json 
import pymongo
import mysql.connector
from collections import namedtuple

DATA_FILE_PATH = "2023-10-30-15.json"


MONGODB_URL = "mongodb://localhost:27017/"
MONGO_DATABASE_NAME = "gharchivedb"
MONGO_COLLECTION_NAME = "ghlogs"


MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASSWORD = "mysql"
MYSQL_DATABASE_NAME = "gharchivedb"

MYSQL_TABLE_USER_NAME = "gh_user"
TABLE_USER_SCHEMA = namedtuple("TableUserColumns", ["id", "login", "display_login", "gravatar_id", "url", "avatar_url"])
MYSQL_TABLE_USER_COLS = TABLE_USER_SCHEMA("id", "login", "display_login", "gravatar_id", "url", "avatar_url")
USER_CREATE_SQL = f"""CREATE TABLE {MYSQL_TABLE_USER_NAME} (
    {MYSQL_TABLE_USER_COLS.id} VARCHAR(40) PRIMARY KEY,
    {MYSQL_TABLE_USER_COLS.login} VARCHAR(100),
    {MYSQL_TABLE_USER_COLS.display_login} VARCHAR(100),
    {MYSQL_TABLE_USER_COLS.avatar_url} VARCHAR(1000),
    {MYSQL_TABLE_USER_COLS.gravatar_id} VARCHAR(100),
    {MYSQL_TABLE_USER_COLS.url} VARCHAR(1000)
)"""
USER_INSERT_SQL = f"""INSERT INTO {MYSQL_TABLE_USER_NAME} (
    {MYSQL_TABLE_USER_COLS.id}, 
    {MYSQL_TABLE_USER_COLS.login},
    {MYSQL_TABLE_USER_COLS.display_login},
    {MYSQL_TABLE_USER_COLS.avatar_url},
    {MYSQL_TABLE_USER_COLS.gravatar_id},
    {MYSQL_TABLE_USER_COLS.url}
) VALUES (%s, %s, %s, %s, %s, %s) 
ON DUPLICATE KEY UPDATE
    {MYSQL_TABLE_USER_COLS.id}={MYSQL_TABLE_USER_COLS.id}
"""

MYSQL_TABLE_REPO_NAME = "gh_repo"
TABLE_REPO_SCHEMA = namedtuple("TableRepoColumns", ["id", "name", "url"])
MYSQL_TABLE_REPO_COLS = TABLE_REPO_SCHEMA("id", "name", "url")
REPO_CREATE_SQL = f"""CREATE TABLE {MYSQL_TABLE_REPO_NAME} (
    {MYSQL_TABLE_REPO_COLS.id} VARCHAR(40) PRIMARY KEY,
    {MYSQL_TABLE_REPO_COLS.name} VARCHAR(1000),
    {MYSQL_TABLE_REPO_COLS.url} VARCHAR(1000)     
)"""
REPO_INSERT_SQL = f"""INSERT INTO {MYSQL_TABLE_REPO_NAME} (
    {MYSQL_TABLE_REPO_COLS.id}, 
    {MYSQL_TABLE_REPO_COLS.name},
    {MYSQL_TABLE_REPO_COLS.url}
) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE
    {MYSQL_TABLE_REPO_COLS.id}={MYSQL_TABLE_REPO_COLS.id}
"""

MYSQL_TABLE_ORG_NAME = "gh_org"
TABLE_ORG_SCHEMA = namedtuple("TableOrgColumns", ["id", "login", "gravatar_id", "url", "avatar_url"])
MYSQL_TABLE_ORG_COLS = TABLE_ORG_SCHEMA("id", "login", "gravatar_id", "url", "avatar_url")
ORG_CREATE_SQL = f"""CREATE TABLE {MYSQL_TABLE_ORG_NAME} (
    {MYSQL_TABLE_ORG_COLS.id} VARCHAR(40) PRIMARY KEY,
    {MYSQL_TABLE_ORG_COLS.login} VARCHAR(100),
    {MYSQL_TABLE_ORG_COLS.avatar_url} VARCHAR(1000),
    {MYSQL_TABLE_ORG_COLS.gravatar_id} VARCHAR(100),
    {MYSQL_TABLE_ORG_COLS.url} VARCHAR(1000)     
)"""
ORG_INSERT_SQL = f"""INSERT INTO {MYSQL_TABLE_ORG_NAME} (
    {MYSQL_TABLE_ORG_COLS.id}, 
    {MYSQL_TABLE_ORG_COLS.login},
    {MYSQL_TABLE_ORG_COLS.gravatar_id},
    {MYSQL_TABLE_ORG_COLS.url},
    {MYSQL_TABLE_ORG_COLS.avatar_url}
) VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE
    {MYSQL_TABLE_ORG_COLS.id}={MYSQL_TABLE_ORG_COLS.id}
"""


def test_print_json(input_json) -> None:
    print(json.dumps(input_json, indent=4))

class MySQLImporter:
    def __init__(self) -> None:
        self.connector = mysql.connector.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASSWORD)
        self.cursor = self.connector.cursor()
        self.cursor.execute(f"DROP DATABASE IF EXISTS {MYSQL_DATABASE_NAME}")
        self.cursor.execute(f"CREATE DATABASE {MYSQL_DATABASE_NAME}")
        self.cursor.execute(f"USE {MYSQL_DATABASE_NAME}")
        self.cursor.execute(USER_CREATE_SQL)
        self.cursor.execute(REPO_CREATE_SQL)
        self.cursor.execute(ORG_CREATE_SQL)
        self.count = 0

    def insert_json(self, input_json) -> None:
        actor_key = "actor"
        user_values = (
            input_json[actor_key]["id"],
            input_json[actor_key]["login"],
            input_json[actor_key]["display_login"],
            input_json[actor_key]["avatar_url"],
            input_json[actor_key]["gravatar_id"],
            input_json[actor_key]["url"]
        )
        self.cursor.execute(USER_INSERT_SQL, user_values)

        repo_key = "repo"
        repo_values = (
            input_json[repo_key]["id"],
            input_json[repo_key]["name"],
            input_json[repo_key]["url"]
        )
        self.cursor.execute(REPO_INSERT_SQL, repo_values)

        org_key = "org"
        if (org_key in input_json):
            org_values = (
                input_json[org_key]["id"],
                input_json[org_key]["login"],
                input_json[org_key]["avatar_url"],
                input_json[org_key]["gravatar_id"],
                input_json[org_key]["url"]
            )
            self.cursor.execute(ORG_INSERT_SQL, org_values)
        
        self.count = self.count + 1
        if (self.count > 1000):
            self.flush()
            self.count = 0

    def flush(self) -> None:
        self.connector.commit()

    def close(self) -> None:
        self.cursor.close()
        self.connector.close()


class MongoDBImporter:
    def __init__(self) -> None:
        self.client = pymongo.MongoClient(MONGODB_URL)
        self.gh_database = self.client[MONGO_DATABASE_NAME]
        for name in self.gh_database.list_collection_names():
            self.gh_database[name].drop()
        self.pending_rows: dict[str, list[str]] = dict()

    def insert_json(self, input_json) -> None:
        processed_json = self._preprocess_mongodb(input_json)
        self._insert_to_mongodb(processed_json)

    def close(self) -> None:
        for key, value in self.pending_rows.items():
            self.gh_database[key].insert_many(value)
        self.pending_rows = dict()
        self.client.close()

    def _preprocess_mongodb(self, input_json):
        input_json["actor"] = input_json["actor"]["id"]
        input_json["repo"] = input_json["repo"]["id"]
        if ("org" in input_json):
            input_json["org"] = input_json["org"]["id"]
        return input_json

    def _insert_to_mongodb(self, data_json) -> None:
        collection_name = MONGO_COLLECTION_NAME
        if ("type" in data_json):
            collection_name += data_json["type"].lower()
        if ("payload" in data_json and "action" in data_json["payload"]):
            collection_name += data_json["payload"]["action"].lower()

        if (collection_name not in self.pending_rows):
            self.pending_rows[collection_name] = []
        
        self.pending_rows[collection_name].append(data_json)
        if (len(self.pending_rows[collection_name]) > 1000):
            self.gh_database[collection_name].insert_many(self.pending_rows[collection_name])
            self.pending_rows[collection_name] = []

def main() -> None:
    data_file = open(DATA_FILE_PATH, 'r')
    mongodb_importer = MongoDBImporter()
    mysql_importer = MySQLImporter()
    n = 0
    for line in data_file.readlines():
        n = n + 1
        mongodb_importer.insert_json(json.loads(line))
        mysql_importer.insert_json(json.loads(line))
        if (n % 1000 == 0):
            print(f"Processed line {n}")
    mongodb_importer.close()
    mysql_importer.close()

if __name__ == "__main__":
    main()