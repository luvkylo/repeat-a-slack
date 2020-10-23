import psycopg2
import psycopg2.errors as e


class Redshift:
    def __init__(self, user, password, host, database, port):
        try:
            self.connection = psycopg2.connect(
                user=user,
                password=password,
                host=host,
                dbname=database,
                port=port
            )
            self.cursor = self.connection.cursor()
        except (e.ConnectionException, e.SqlclientUnableToEstablishSqlconnection, e.ConnectionDoesNotExist, e.ConnectionFailure) as err:
            print(
                "Redshift Failed to connect, please check if your vpn is on and is set to correct region")
            raise ConnectionError(err)

    def execute(self, argStr):
        print("Executing query...")
        try:
            self.cursor.execute(argStr)
            print("Query executed")
        except e.DatabaseError as err:
            print("Database Error!!!!")
            raise err
        except e.DataError as err:
            print("Data Error, possible invalid query string")
            raise err
        except e.ProgrammingError as err:
            print("Syntax Error, possible invalid query string")
            raise err
        except e.OperationalError as err:
            print("Operational Error!!!")
            raise err
        except e.InternalError as err:
            print("Internal Error!!!")
            raise err

    def closeEverything(self):
        print("Closing everything...")
        try:
            self.cursor.close()
            self.connection.commit()
            self.connection.close()
        except e.DatabaseError as err:
            print("Database Error!!!!")
            raise err
        except e.OperationalError as err:
            print("Operational Error!!!")
            raise err
        except e.InternalError as err:
            print("Internal Error!!!")
            raise err

        print("Everything is closed")
