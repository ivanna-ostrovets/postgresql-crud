"""Example of usage Database class of postgresql-crud package"""
import ConfigParser

from database import Database

CONFIG = ConfigParser.RawConfigParser()
CONFIG.read('config.ini')

USER = CONFIG.get('credentials', 'user')
PASSWORD = CONFIG.get('credentials', 'password')
HOST = CONFIG.get('credentials', 'host')
DBNAME = CONFIG.get('credentials', 'dbname')
PORT = CONFIG.get('credentials', 'port')

CONNECTION_STRING = "dbname=%s user=%s password=%s host=%s port=%s" \
    % (DBNAME, USER, PASSWORD, HOST, PORT)

DATABASE = Database(CONNECTION_STRING)

DATABASE.insert(
    "persons",
    {"name": "Anakin Skywalker", "phrase": "I will come back and free you mom, I promise!"}
)
DATABASE.delete("persons", where={"name": "LIKE 'A%'"})
DATABASE.insert("persons", {"name": "Darth Vader", "phrase": "I am your father!"})

DATABASE.insert("persons", {"name": "Luke Skywalker", "phrase": "Noooooooooooooooo!"})

print DATABASE.select("persons", ["name", "phrase"])
print DATABASE.select("persons", ["name", "phrase"], where={"name": "LIKE 'L%'"})

DATABASE.update("persons", {"phrase": "Yeeeeeeeees!"}, where={"name": "LIKE 'L%'"})
print DATABASE.select("persons", ["name", "phrase"], where={"name": "LIKE 'L%'"})

DATABASE.close()
