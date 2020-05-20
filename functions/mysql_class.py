import mysql.connector # pip install mysql-connector-python

def FindCountyId(string, counties):
    for key in counties:
        targets = counties[key]
        for target in targets:
            if not string.find(target):
                return key

class MySQLWithPython(): # the function to do SQL command through python

    """
    pip install mysql-connector-python
    """

    def __init__(self, config):

        self.config = config

        # whether to SKIP All Error
        self.whether_SKIP_all_error = False

    def ExecuteMysqlCommand(self, sqlcommands, return_result = True):

        config = self.config

        try:
            # connect to database
            connection = mysql.connector.connect(**config)

            # check whether connected
            if connection.is_connected():
                db_Info = connection.get_server_info()
                print("Connected to MySQL Server version ", db_Info)
                cursor = connection.cursor()
                cursor.execute("select database();")
                record = cursor.fetchone()
                print("You're connected to database: ", record)

            # excute sql code
            cursor = connection.cursor()

            # execute the sql command: (all the command must be in list)
            for sqlcommand in sqlcommands:
                cursor.execute(sqlcommand)
                if return_result is True:
                    result = cursor.fetchall()
                connection.commit()
                print('Succuessfully execute MySQL Command: ', sqlcommand)

        except mysql.connector.Error as error:
            # if error arouses during the try, print out the error
            print("Failed in MySQL: {}".format(error))
        
        finally:
            if (connection.is_connected()):
                cursor.close()
                connection.close()
                print("MySQL connection is closed")

        if return_result is True:
            return result

    def CommandTableLength(self, table_name):

        command = "SELECT COUNT(*) FROM " + table_name + ";"
        command = [command]

        return command

    def CommandAddColumn(self, table_name, column_name, datatype):
        '''
        ALTER TABLE 資料表名稱 ADD COLUMN 欄位名稱 形態(長度);
        '''
        command = "ALTER TABLE " + table_name + " ADD COLUMN " + column_name + " " + datatype + ";"
        command = [command]

        return command

    def CommandFindColumn(self, table_name):

        command = "SHOW COLUMNS FROM " + table_name + ";"
        command = [command]

        return command

    def CommandShowElementFromColumn(self, table_name, column_name):

        command = "Select " + column_name + " From " + table_name + ";"
        command = [command]

        return command

    def CommandFindCounty(self, table, counties):

        # get the number of rows
        command = self.CommandTableLength(table)
        length = self.ExecuteMysqlCommand(command)
        length = length[0][0]

        # get the columns from table
        command = self.CommandFindColumn(table)
        columns = self.ExecuteMysqlCommand(command)
        column_names = [column[0] for column in columns]

        county_list = [None] * length
        for column_name in column_names:
            command = self.CommandShowElementFromColumn(table, column_name)
            elements = self.ExecuteMysqlCommand(command)
            i = 0
            for element in elements:
                try:
                    county = FindCountyId(element[0], counties)
                    if county is None:
                        pass
                    else:
                        county_list[i] = FindCountyId(element[0], counties)
                except Exception as e:
                    pass
                i += 1
        return county_list

    def CommandUpdateCounty(self, table_name, values):
        '''
        UPDATE employees
        SET 
        email = 'mary.patterson@classicmodelcars.com'
        WHERE
        employeeNumber = 1056;
        '''
        # get the number of rows
        command = self.CommandTableLength(table_name)
        length = self.ExecuteMysqlCommand(command)
        length = length[0][0]
        print(length)

        for i in range(len(values)):
            value = values[i]
            print(i, value)
            # print(value)
            if value is None:
                pass
            else:
                value = str(value)
                command = "UPDATE " + table_name + " SET county = " + "'" + value + "'" + " WHERE id = " + str(i) + ";"
                # print(command)
                command = [command]
                self.ExecuteMysqlCommand(command, return_result = False)

    def CommandDeleteColumn(self, table_name, column_name):
        """
        ALTER TABLE table_name
        DROP COLUMN column_name;
        """
        command = 'ALTER TABLE ' + table_name + ' DROP COLUMN ' + column_name + ';'
        command = [command]

        return command

    def CommandChangeDataType(self, table_name, column_name, datatype):
        """
        ALTER TABLE table_name MODIFY columnname DATATYPE;
        """
        command = "ALTER TABLE " + table_name + " MODIFY " + column_name + " " + datatype + ";"
        command = [command]

        return command

    def CommandBuildRelationship(self, table_name):
        """
        ALTER TABLE `accounts` ADD FOREIGN KEY (`customer_id`) REFERENCES `customers` (`customer_id`);
        """
        command = "ALTER TABLE " + table_name + " ADD FOREIGN KEY (county) REFERENCES counties(id);"
        command = [command]

        return command

    def CommandShowTable(self, table_name):
        command = "SELECT * FROM " + table_name + ";"
        command = [command]

        return command

