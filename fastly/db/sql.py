import psycopg2
import sys
import time
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
            self.user = user
            self.password = password
            self.host = host
            self.database = database
            self.port = port
            self.cursor = self.connection.cursor()
        except (e.ConnectionException, e.SqlclientUnableToEstablishSqlconnection, e.ConnectionDoesNotExist, e.ConnectionFailure) as err:
            print(
                "Redshift Failed to connect, please check if your vpn is on and is set to correct region")
            raise ConnectionError(err)

    def returnResult(self):
        return self.cursor.fetchall()

    def execute(self, argStr):
        print("Executing query...")
        try:
            self.cursor.execute(argStr)
            print("Query executed")
        except e.DatabaseError as err:
            print("Database Error!!!!")
            print(err.args)
            if 'SSL connection' in err.args[0]:
                try:
                    print(
                        "Detected operation error, attempting to reconnect to the database")
                    self.connection = psycopg2.connect(
                        user=self.user,
                        password=self.password,
                        host=self.host,
                        dbname=self.database,
                        port=self.port
                    )
                    self.cursor = self.connection.cursor()
                    print("Reconnection successful, executing query now")
                    self.execute(argStr=argStr)
                except (e.ConnectionException, e.SqlclientUnableToEstablishSqlconnection, e.ConnectionDoesNotExist, e.ConnectionFailure) as err:
                    print(
                        "Redshift Failed to connect, please check if your vpn is on and is set to correct region")
                    raise ConnectionError(err)
                except Exception as err:
                    raise(err)
            elif 'Serializable isolation violation on table' in err.args[0]:
                print('Concurrent writing...sleeping for 30 seconds')
                self.connection.rollback()
                time.sleep(30)
                self.execute(argStr=argStr)
            else:
                print(err)
                raise err
        except e.DataError as err:
            print("Data Error, possible invalid query string")
            print(err)
            raise err
        except e.ProgrammingError as err:
            print("Syntax Error, possible invalid query string")
            print(err)
            raise err
        except e.OperationalError as err:
            print("Operational Error!!!")
            print(err)
            try:
                print(
                    "Detected operation error, attempting to reconnect to the database")
                self.connection = psycopg2.connect(
                    user=self.user,
                    password=self.password,
                    host=self.host,
                    dbname=self.database,
                    port=self.port
                )
                self.cursor = self.connection.cursor()
                print("Reconnection successful, executing query now")
                self.execute(argStr=argStr)
            except (e.ConnectionException, e.SqlclientUnableToEstablishSqlconnection, e.ConnectionDoesNotExist, e.ConnectionFailure) as err:
                print(
                    "Redshift Failed to connect, please check if your vpn is on and is set to correct region")
                raise ConnectionError(err)
            except Exception as err:
                raise(err)
        except e.InternalError as err:
            print("Internal Error!!!")
            print(err)
            raise err

    def closeEverything(self):
        print("Closing everything...")
        try:
            self.cursor.close()
            self.connection.commit()
            self.connection.close()
        except e.DatabaseError as err:
            print("Database Error!!!!")
            print(err)
            raise err
        except e.OperationalError as err:
            print("Operational Error!!!")
            print(err)
            raise err
        except e.InternalError as err:
            print("Internal Error!!!")
            print(err)
            raise err

        print("Everything is closed")
