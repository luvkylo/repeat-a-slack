import psycopg2


class Redshift:
    def __init__(self, user, password, host, database, port):
        self.connection = psycopg2.connect(user=user,
                                           password=password,
                                           host=host,
                                           dbname=database,
                                           port=port)
        self.cursor = self.connection.cursor()

    def execute(self, argStr):
        print("Executing query...")
        self.cursor.execute(argStr)
        print("Query executed")

    def closeEverything(self):
        print("Closing everything...")
        self.cursor.close()
        self.connection.commit()
        self.connection.close()
        print("Everything is closed")
