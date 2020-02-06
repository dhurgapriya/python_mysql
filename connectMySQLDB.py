import mysql.connector  
from mysql.connector import Error
import sys

class ConnectMySQL:

     def __init__(self,hostname="localhost",user="root",passwd="password",database="MYSQLDB"):
        '''
        Initialize the default variable
        '''
        self.hostname = hostname
        self.user = user
        self.passwd = passwd
        self.database = database
        self.conn = None
        self.cursor = None
    
     def do_error(self,Error):
        '''
        Helps to handle exception and errors
        '''
        print(str(Error))
        if self.conn and self.conn.is_connected():
            self.close_connection()
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
            if not self.conn.is_connected():
                self.do_error('Connected to MySQL database')
        except Exception as e:
            self.do_error(str(e))

    def execute_query(self,query,value=None):
        '''
        Helps to execute the mysql query using cursor
        query - the query to be executed
        '''
        try:
            #print("Query to be executed :", query)
            self.cursor = self.conn.cursor()
            if value:
                self.cursor.executemany(query,value)
            else:
                self.cursor.execute(query)
            self.conn.commit()
            #print("Query executed successfully")
        except Exception as e:
            self.do_error("Error occurred while executing query {} ".format(e))

    def select_query(self,query,tableName):
        '''
        Helps to select the mysql query and print the content
        tableName - name of the table to select
        '''
        try:
            query = "SELECT * FROM " + tableName
            # print("Query to be executed : {}", query)
            self.cursor = self.conn.cursor()
            self.cursor.execute(query)
            records = self.cursor.fetchall()
            # print("Total number of records: ", self.cursor.rowcount)
            # for r in records:
            #     print(r)
            return records
        except Exception as e:
            self.do_error("Error occurred while executing select query {} ".format(e))
    
    def insert_values_into_table(self,tableName,columns,values):
        '''
        Helps to insert a row into a table in MySQL Database
        tableName - name of the table in which values to be inserted
        columns - name of columns to be inserted
        values - values to be inserted in the column

        example:
        tableName = "USERS"
        columns = ("Name","ID","PhoneNumber","Age")
        value = [("dp",23,98763223567,23),("samantha",25,96478743457,33),("chay",64,76754645223,89)] 

        '''
        try:
            query = "INSERT INTO " + tableName + "("
            for column in columns:
                query += column + ","
            query = query[:-1] + ") VALUES ("  
            for column in columns:
                query += "%s" + ","
            query = query[:-1] + ")"
            # print("Query to be executed :", query)
            # print("Values to be inserted :", values)
            self.execute_query(query,values)
            # print("Query executed successfully")
        except Exception as e:
            self.do_error("Error occurred while executing insert query {} ".format(e))

    def close_connection(self):
        '''
        Close the MySql connection
        '''
        try:
            if (self.conn.is_connected()):
                self.conn.close()
                # print("MySQL connection is closed")
        except Exception as e:
            self.do_error("Error occurred while closing connection {} ".format(e))
    
    def close_cursor(self):
        '''
        Closing the cursor connection
        '''
        try:
            self.cursor.close()
            # print("MySQL cursor is closed")
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
            query = "CREATE TABLE " + tableName + "("
            for (key,value) in columnData.items():
                query += key + " "
                query += value["columnDataType"] + " "
                if(value["isNULL"]):
                    query+= "NOT NULL"
                query+= ","
            query = query[:-1] + ");"
            self.execute_query(query)
            if primaryKey and primaryKey in columnData.keys():
                pk = "ALTER TABLE " + tableName + " ADD PRIMARY KEY (" + primaryKey + ")"
                self.execute_query(pk)
        except Exception as e:
            self.do_error("Error occurred while creating table {} ".format(e))
    
    def delete_values_from_table(self,tableName,columnName,value):
        '''
        Helps to delete values from the table in MySQL DB
        tableName - name of the table to be created 
        columnName - name of the column to be deleted from the table
        value - value of the column to be deleted from the table

        example:
        tableName = "USERS"
        columnName = "Name"
        value = "Chay"
        '''
        try:
            query = "DELETE FROM " + tableName + " WHERE " + columnName + " = '" + value +"'" 
            self.execute_query(query)
        except Exception as e:
            self.do_error("Error occurredd while deleting values from table {}".format(e))

    def update_values_in_table(self,tableName,columnToBeUpdated,valueToBeUpdated,conditionKey,conditionValue):
        '''
        Helps to delete values from the table in MySQL DB
        tableName - name of the table to be created 
        columnToBeUpdated - name of the column to be updated in the table
        valueToBeUpdated - value of the column to be updated in the table
        conditionKey - name of the column to be selected in the table
        conditionValue - value of the column to be selected in the table

        example:
        tableName = "USERS"
        columnToBeUpdated = "Age"
        valueToBeUpdated = "33"
        conditionKey = "Name"
        conditionValue = "Samantha"
        '''
        try:
            query = "UPDATE " + tableName + " SET " + columnToBeUpdated + " = '" + valueToBeUpdated +"'" + " WHERE " + conditionKey + " = '" + conditionValue + "'"
            self.execute_query(query)
        except Exception as e:
            self.do_error("Error occurredd while updating values from table {}".format(e))

'''      
Example main function in which all the above methods are used.
'''
# if __name__ == "__main__":
#     object_temp = ConnectMySQL()
#     object_temp.db_connect()
#     column = {
# 	    "Name" : { "columnDataType":"VARCHAR(255)", "isNULL" : True},
# 	    "ID" : {"columnDataType":"int(10)", "isNULL" : True},
# 	    "PhoneNumber" : {"columnDataType":"VARCHAR(15)", "isNULL" : True},
# 	    "Age" : {"columnDataType" : "int(20)", "isNULL" : False}
#         }  
#     object_temp.create_table("USERS",column)
#     columns = ("Name","ID","PhoneNumber","Age")
#     value = [("dp",23,98763223567,23),("samantha",25,96478743457,33),("chay",64,76754645223,89)] 
#     object_temp.insert_values_into_table("USERS",columns,value)
#     object_temp.delete_values_from_table("USERS","Name","chay")
#     object_temp.update_values_in_table("USERS","Age","33","Name","Samantha")  
#     object_temp.select_query("USERS")
#     object_temp.close_cursor()
#     object_temp.close_connection()





