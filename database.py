import psycopg2

class Database(object):
    """Connect to database. Provide basic PostgreSQL CRUD functionality.

    Attributes:
        connection (obj): Database connection.
        cursor (obj): Database cursor object.
    """

    def __init__(self, connection_string):
        """Connect to database. Create cursor object.

        Args:
            connection_string (str): String with database credentials.
        """
        try:
            self.connection = psycopg2.connect(connection_string)
            self.cursor = self.connection.cursor()
        except Exception, e:
            print "Exception", e

    def insert(self, table_name, data):
        """Insert data to database table.

        Args:
            table_name (str): Name of table to insert into.
            data (dict): Key-value pairs of data to insert.

        Examples:
            >>> instance.insert("table name", {"foo": "bar"})
        """
        keys = data.keys()
        values = [data.get(key) for key in keys]

        values = list(map(
            lambda value: "".join(("'", value, "'")),
            filter(lambda x: isinstance(x, basestring), values)
        ))

        try:
            self.cursor.execute(
                "INSERT INTO %s (%s) VALUES (%s)" % (table_name, ", ".join(keys), ", ".join(values))
            )
            self.connection.commit()
        except Exception, e:
            print "Exception", e

    def select(self, table_name, fields, **kwargs):
        """Select data from database table. Return list.

        Args:
            table_name (str): Name of table to select from.
            fields (list): List of fields to select.

        Keyword Args:
            where (dict): Key-value pairs of fields and their values to filter data.
                Values must be written with necessary expressions (=, <, LIKE, etc).

        Examples:
            >>> instance.select("table name", ["foo", "bar"], where={"foo": "> 5"})
        """
        query_string = "SELECT %s FROM %s" % (", ".join(fields), table_name)

        if "where" in kwargs:
            conditions = self.__where(kwargs.get("where"))
            query_string = " ".join((query_string, conditions))

        try:
            self.cursor.execute(query_string)
            rows = self.cursor.fetchall()

            return [[column for column in row] for row in rows]
        except Exception, e:
            print "Exception", e

    def update(self, table_name, data, **kwargs):
        """Select data from database table.

        Args:
            table_name (str): Name of table to select from.
            data (dict): Key-value pairs of data to update.

        Keyword Args:
            where (dict): Key-value pairs of fields and their values to filter data.
                Values must be written with necessary expressions (=, <, LIKE, etc).

        Examples:
            >>> instance.update("table name", {"foo": "bar"}, where={"foo": "= foobar"})
        """
        keys = data.keys()
        values = [data.get(key) for key in keys]

        values = list(map(
            lambda value: "".join(("'", value, "'")),
            filter(lambda x: isinstance(x, basestring), values)
        ))

        data_to_update = ["=".join(item) for item in zip(keys, values)]

        query_string = "UPDATE %s SET %s" % (table_name, ", ".join(data_to_update))

        if "where" in kwargs:
            conditions = self.__where(kwargs.get("where"))
            query_string = " ".join((query_string, conditions))

        try:
            self.cursor.execute(query_string)
            self.connection.commit()
        except Exception, e:
            print "Exception", e

    def delete(self, table_name, **kwargs):
        """Delete data from database table.

        Args:
            table_name (str): Name of table to delete from.

        Keyword Args:
            where (dict): Key-value pairs of fields and their values to filter data.
                Values must be written with necessary expressions (=, <, LIKE, etc).

        Examples:
            >>> instance.delete("table name", where={"foo": "= bar"})
        """
        query_string = "DELETE FROM %s" % (table_name)

        if "where" in kwargs:
            conditions = self.__where(kwargs.get("where"))
            query_string = " ".join((query_string, conditions))

        try:
            self.cursor.execute(query_string)
            self.connection.commit()
        except Exception, e:
            print "Exception", e

    def __where(self, conditions):
        """Return string with formed WHERE statement.

        Args:
            options (dict): Key-value pairs of fields and their values to filter data.
                Values must be written with necessary expressions (=, <, LIKE, etc).
        """
        conditions_list = [" ".join((key, value)) for key, value in conditions.iteritems()]

        return " ".join(("WHERE", ", ".join(conditions_list)))

    def close(self):
        """Close database connection."""
        try:
            self.cursor.close()
            self.connection.close()
        except Exception, e:
            print "Exception", e
