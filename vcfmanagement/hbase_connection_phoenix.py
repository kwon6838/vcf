import phoenixdb
import phoenixdb.cursor
import os


class Hbase_phoenix_connection:
    directory = ""

    def get_connection(self):
        database_url = 'http://150.183.247.84:8765/'
        conn = phoenixdb.connect(database_url, autocommit=True)
        return conn.cursor()

    def exist_table(self, table_name):
        try:
            cursor = self.get_connection()
            sql = "SELECT * FROM "+ table_name
            result = cursor.execute(sql)
            print(type(result))
            print(result)
            return True
        except phoenixdb.errors.ProgrammingError as e:
            print(e.message)
            print("Tabel is not exist...", type(e), e)
            pass
        return False
    
    def drop_table(self, table_name):
        cursor = self.get_connection()
        sql = "DROP TABLE IF EXISTS " + table_name
        print(sql)
        cursor.execute(sql)

    def create_table(self, table_sql):
        cursor = self.get_connection()
        cursor.execute(table_sql)

    def insert_table(self, insert_sql):
        cursor = self.get_connection()
        cursor.execute(insert_sql)
    
    def bulk_upsert(self, sql_list):
        cursor = self.get_connection()
        try:
            for sql in sql_list:
                # print("upsert : ", sql)
                cursor.execute(sql)
        except:
            print(sql)
    

# cursor = conn.cursor()
# cursor.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, username VARCHAR)")
# cursor.execute("UPSERT INTO users VALUES (?, ?)", (1, 'admin'))
# cursor.execute("SELECT * FROM users")
# print(cursor.fetchall())

# cursor = conn.cursor(cursor_factory=phoenixdb.cursor.DictCursor)
# cursor.execute("SELECT * FROM users WHERE id=1")
# print(cursor.fetchone()['USERNAME'])