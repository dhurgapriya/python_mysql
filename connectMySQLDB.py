import mysql.connector  
from mysql.connector import Error

class ConnectMySQL:

    def __init__(self,hostname="localhost",user="root",passwd="password",database="MYSQLDB")
        '''
        Initialize the default variable
        '''
        self.hostname = hostname
        self.user = user
        self.passwd = passwd
        self.database = database
        self.conn = None
        self.cursor = None
        self.query = None
    
    def do_error(self,Error):
        print(str(Error))
        sys.exit(0)

    def db_connect(self):
        '''
        Helps to connect to Mysql DB
        '''
        try:
            self.conn = mysql.connector.connect(
                host = self.hostname,
                user = self.user,
                passwd = self.passwd,
                database = self.database
            )
            if self.conn.is_connected():
                print('Connected to MySQL database')
        except Exception as e:
            self.do_error(str(e))

    def execute_query(self,query): 
        '''
        Helps to execute the mysql query using cursor
        '''
        try:
            print("Query to be executed :", query)
            self.cursor = self.conn.cursor()
            self.cursor.execute(query)
            self.conn.commit()
            print("Query executed successfully")
        except Exception as e:
            self.do_error("Error occurred while executing query {} ".format(e))

    def select_query(self,query):
        '''
        Helps to select the mysql query and print the content
        '''
        try:
            print("Query to be executed : {}", query)
            self.cursor = self.conn.cursor()
            self.cursor.execute(query)
            records = self.cursor.fetchall()
            print("Total number of records: ", self.cursor.rowcount)
            for r in records:
                print(r)
        except Exception as e:
            self.do_error("Error occurred while executing select query {} ".format(e))
    
    def insert_query(self,query,values):
        '''
        Todo
        '''
        try:
            print("Query to be executed :", query)
            print("Values to be inserted :", values)
            self.cursor = self.conn.cursor()
            self.cursor.execute(query,values)
            self.conn.commit()
            print("Query executed successfully")
        except Exception as e:
            self.do_error("Error occurred while executing insert query {} ".format(e))

    def close_connection(self):
        '''
        Close the MySql connection
        '''
        try:
            if (self.conn.is_connected()):
                self.conn.close()
                print("MySQL connection is closed")
        except Exception as e:
            self.do_error("Error occurred while closing connection {} ".format(e))
    
    def close_cursor(self):
        '''
        Closing the cursor connection
        '''
        try:
            self.cursor.close()
            print("MySQL cursor is closed")
        except Exception as e:
            self.do_error("Error occurred while closing cursor {} ".format(e))

    def create_table(self,tableName,columnData,primaryKey=None):
        '''
        Helps to create table in the MYSQL DB connected
        tableName - name of the table to be created 
        columnData - details of the columns in the table
        primaryKey - primary key of the table

        example :
        tableName = "USERS"
        columnData =  {
                    "Name": {
                        "columnDataType": "VARCHAR(255)",
                        "isNULL": True
                    },
                    "ID": {
                        "columnDataType": "int(10)",
                        "isNULL": True
                    },
                    "PhoneNumber": {
                        "columnDataType": "int(10)",
                        "isNULL": True
                    },
                    "Age": {
                        "columnDataType": "int(20)",
                        "isNULL": False
                    }
                }
        primaryKey = "ID"

        '''
        try:
            table = "CREATE TABLE " + tableName + "("
            for (key,value) in columnData.items():
                table += key + " "
                table += value["columnDataType"] + " "
                if(value["isNULL"]):
                    table+= "NOT NULL"
                table+= ","
            table = table[:-1] + ");"
            print(table)
            self.execute_query(table)
            if primaryKey and primaryKey in columnData.keys():
                pk = "ALTER TABLE " + tableName + " ADD PRIMARY KEY (" + primaryKey + ")"
                self.execute_query(pk)
        except Exception as e:
            self.do_error("Error occurred while creating table {} ".format(e))
    

if __name__ == "__main__":
    object_temp = ConnectMySQL()
    object_temp.db_connect()
    column = {
	    "Name" : { "columnDataType":"VARCHAR(255)", "isNULL" : True},
	    "ID" : {"columnDataType":"int(10)", "isNULL" : True},
	    "PhoneNumber" : {"columnDataType":"int(10)", "isNULL" : True},
	    "Age" : {"columnDataType" : "int(20)", "isNULL" : False}
        }  
    object_temp.create_table("USERS",column)
    object_temp.close_cursor()
    object_temp.close_connection()




