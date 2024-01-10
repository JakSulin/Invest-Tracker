"""
    It is a module containing a class for interacting with an SQLite database, ideveloped for web scraping and investment tracking applications. It will certainly prove useful in subsequent applications.


    Methods:
    - __init__(self, database_name="db"): Initializes the Database object.
        - If the specified database file exists, connects to it; otherwise, creates a new database file.

    - create_table(self, table_name: str, *columns): Creates a new table in the database with the specified name and columns.

    - check_table_exists(self, table_name): Checks if a table with the given name already exists in the database.

    - drop_table(self, table_name: str): Drops (deletes) a table from the database.

    - get_all_table_names(self): Retrieves a list of all table names in the database.

    - get_column_names(self, table_name: str): Retrieves a list of column names for a given table.

    - is_table_empty(self, table_name: str): Checks if a table is empty.

    - get_table(self, table_name: str): Retrieves all rows from a specified table.

    - get_table_df(self, table_name: str, *column_names, sort_col: str = None, sort_order: str = None, where_condition: str = None): 
        Retrieves data from a specified SQLite table and returns it as a pandas DataFrame.

    - get_table_df_with_conditions(self, table_name: str, *column_names, condition_operator: str = "AND", limit: int = None, **kwargs): 
        Retrieves data from a specified table with specified conditions and returns it as a pandas DataFrame.

    - insert_row(self, table_name: str, *values): Inserts a new row into the specified table in the database.

    - get_first_row_value(self, table_name: str, *column_names): Retrieves values from the first row of specified columns.

    - get_last_row_value(self, table_name: str, *column_names): Retrieves values from the last row of specified columns.

    - get_last_id(self, table_name: str): Retrieves the last ID from a specified table.

    - update_data(self, table_name: str, attribute_name: str, attribute_value: str, item_id: int): Updates data in a specified table.

    - update_table(self, table_name: str, update_dict, where_dict): Updates rows in a specified table based on given key-value pairs.

    - __del__(self): Closes the database connection when the object is deleted.
"""  















import sqlite3
import os
import os.path
import pandas as pd

class Database:

    def __init__(self, database_name: str="db"):
        """
        Initializes a Database object.

        Connects to an existing SQLite database file or creates a new one if it doesn't exist.

        :param database_name: The name of the SQLite database file. Defaults to "db".
        :type database_name: str
        :return: None
        :rtype: None

        Example usage:
        ```python
        # Creating or connecting to an SQLite database named "example.db"
        db = Database("example.db")
        ```
        """        
        if os.path.isfile(database_name):
            print(f"{database_name} exists in the current directory.")
            self.conn = sqlite3.connect(database_name)
            self.cur = self.conn.cursor()
            self.name = database_name
            print(f"Connected to {database_name}. ")
        else:
            print(f"{database_name} does not exist in the current directory.")
            print(f"Creating {database_name} data base ....................")
            self.conn = sqlite3.connect(database_name)
            self.cur = self.conn.cursor()
            self.name = database_name
            print(f"DONE -> {database_name}  created.")
    
    def create_table(self, table_name: str, *columns):
        """
        Creates a new table in the database with the specified name and columns.

        If the table already exists, it prints a message indicating that the table cannot be created.

        :param table_name: The name of the table to be created.
        :type table_name: str
        :param columns: Variable of column names and their types separated using ",".
        :type columns: str
        :return: None
        :rtype: None

        Example usage:
        ```python
        # Creating a new table named "employees" with columns "id INTEGER PRIMARY KEY AUTOINCREMENT", "name TEXT", "age INTEGER"
        db.create_table("employees", "id INTEGER PRIMARY KEY AUTOINCREMENT", "name TEXT", "age INTEGER")
        ```
        """
        if not self.check_table_exists(table_name):
            column_definition = ', '.join([f"{column}" for column in columns])
            print(f"CREATING -> '{table_name}' in '{self.name}' data base.")
            self.cur.executescript(f"""CREATE TABLE IF NOT EXISTS '{table_name}' ({column_definition})""")
            self.conn.commit()
            print(f"The {table_name} table has been created.")
        else:
            print(f"Table {table_name} exists. Can't create new")
        

    def check_table_exists(self, table_name):
        """
        Checks if a table with the given name already exists in the database.

        :param table_name: The name of the table to check.
        :type table_name: str
        :return: True if the table exists, False otherwise.
        :rtype: bool

        Example usage:
        ```python
        # Checking if a table named "employees" exists
        if db.check_table_exists("employees"):
            print("The 'employees' table exists.")
        else:
            print("The 'employees' table does not exist.")
        ```
        """        
        self.cur.execute(f"""SELECT name FROM sqlite_master WHERE type == 'table' AND name == '{table_name}'""")
        result = self.cur.fetchone()
        return True if result else False
    

    def drop_table(self, table_name: str):
        """
        Drops (deletes) a table from the database.

        If the specified table exists, it is deleted; otherwise, a message is printed indicating that the table does not exist.

        :param table_name: The name of the table to be deleted.
        :type table_name: str
        :return: None
        :rtype: None

        Example usage:
        ```python
        # Dropping a table named "employees"
        db.drop_table("employees")
        ```
        """        
        if self.check_table_exists(table_name):
            print(f"DELETING -> '{table_name}' from '{self.name}' data base.")
            self.cur.execute(f"""DROP TABLE IF EXISTS {table_name};""")
            self.conn.commit()
            print("DONE")
        else: 
            print(f"The {table_name} table does not exist.")
    
    def get_all_table_names(self):
        """
        Retrieves a list of all table names in the database.

        :return: A list containing the names of all tables in the database.
        :rtype: list

        Example usage:
        ```python
        # Retrieving all table names in the database
        table_names = db.get_all_table_names()
        print("Tables in the database:", table_names)
        ```
        """
        self.cur.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        table_names = [table[0] for table in self.cur.fetchall()]
        return table_names
    
    def get_column_names(self, table_name: str):
        """
        Retrieves a list of column names for a given table.

        :param table_name: The name of the table to get column names from.
        :type table_name: str
        :return: A list containing the names of columns in the specified table.
        :rtype: list

        Example usage:
        ```python
        # Retrieving column names for a table named "employees"
        column_names = db.get_column_names("employees")
        print("Column names in 'employees' table:", column_names)
        ```
        """
        self.cur.execute(f"PRAGMA table_info('{table_name}')")
        columns_names = [column[1] for column in self.cur.fetchall()]
        return columns_names
    
    def is_table_empty(self, table_name: str):
        """
        Checks if a table is empty.

        :param table_name: The name of the table to check.
        :type table_name: str
        :return: True if the table is empty, False otherwise.
        :rtype: bool

        Example usage:
        ```python
        # Checking if a table named "employees" is empty
        if db.is_table_empty("employees"):
            print("The 'employees' table is empty.")
        else:
            print("The 'employees' table is not empty.")
        ```
        """        
        self.cur.execute(f"SELECT COUNT(*) FROM '{table_name}'")
        count = self.cur.fetchone()[0]
        return count == 0
    
    def get_table(self, table_name: str):
        """
        Retrieves all rows from a specified table.

        :param table_name: The name of the table to retrieve data from.
        :type table_name: str
        :return: A list containing all rows from the specified table.
        :rtype: list

        Example usage:
        ```python
        # Retrieving all rows from a table named "employees"
        table_data = db.get_table("employees")
        print("Data from 'employees' table:", table_data)
        ```
        """        
        self.cur.execute(f"SELECT * FROM '{table_name}'")
        table_data = self.cur.fetchall()
        return table_data

    def get_table_df(self, table_name: str, *column_names, sort_col: str=None, sort_order: str=None, where_condition: str=None):
        """
        Returns a pandas DataFrame with the specified columns or all columns of a given SQLite table.
        Optionally, the function can also sort the DataFrame by a specified column and sort order.
        
        :param table_name: The name of the SQLite table to get the data from.
        :type table_name: str
        :param column_names: The names of the columns to be selected. If no column names are provided, all columns will be selected.
        :type column_names: str
        :param sort_col: The name of the column to sort the DataFrame by.
        :type sort_col: str
        :param sort_order: The sort order to apply to the specified column. It can be "ASC" for ascending order or "DESC" for descending order.
        :type sort_order: str
        :return: A pandas DataFrame with the selected columns (or all columns if no columns are specified) and optionally sorted by the specified column.
        :rtype: pd.DataFrame
        :raises ValueError: If sort_order is not "ASC" or "DESC" or None.
        """
        if column_names:
            column_string = ", ".join(column_names)
        else:
            column_string = "*"
         
        query = f"SELECT {column_string} FROM '{table_name}' "
        #query = f"SELECT {column_string} FROM {table_name} "
        if where_condition:
            query += " WHERE "
            query += where_condition

        if sort_col:
            if sort_order == "ASC":
                #query = f"SELECT {column_string} FROM '{table_name}' ORDER BY {sort_col} ASC"
                query += f" ORDER BY {sort_col} ASC"
            elif sort_order == "DESC":
                #query = f"SELECT {column_string} FROM '{table_name}' ORDER BY {sort_col} DESC"
                query += f" ORDER BY {sort_col} DESC"
            else:
                raise ValueError("sort_order must be 'asc' or 'desc'")

        print(query)
        table_df = pd.read_sql_query(query, self.conn)
        return table_df
    
    def get_table_df_with_conditions(self, table_name: str, *column_names, condition_operator: str="AND", limit: int=None, **kwargs): 
        """
            Retrieve data from the specified table in the database and return it as a Pandas DataFrame.

            :param table_name: The name of the table to retrieve data from.
            :type table_name: str
            :param condition_operator: The operator to use when joining multiple conditions together (e.g. "AND" or "OR").
                                       Defaults to "AND".
            :type condition_operator: str
            :param limit: The maximum number of rows to return. Defaults to None (i.e. no limit).
            :type limit: int or None
            :param **kwargs: Keyword arguments that specify the columns to filter by, and the values to filter for.
                             The format for each argument should be "column_name" = "value", or "column_name" = "comparison_operator__value",
                             where comparison_operator is one of "<", ">", "<=", ">=", "=", "!=", "LIKE", "BETWEEN".
                             For example: get_table_df_with_conditions("table_name", name = "John", age = ">__18", city = "!=__New York")
            :return: A DataFrame containing the retrieved data.
            :rtype: pandas.DataFrame
        """
        
        if column_names:
            column_string = ", ".join(column_names)
        else:
            column_string = "*"        
        
        query = f"SELECT {column_string} FROM {table_name}"

        conditions = []
        for column_name, value in kwargs.items():
            if "__" in value:
                comparison_operator, value = value.split('__')
            else:
                comparison_operator = "=="   
                
            if comparison_operator == "BETWEEN":
                values = value.split(' ')
                conditions.append(f"""{column_name} {comparison_operator} '{values[0]}' AND '{values[2]}' """)
            else:
                conditions.append(f"""{column_name} {comparison_operator} '{value}' """)

        if conditions:
            condition_statement = f" {condition_operator} ".join(conditions)
            where_clause = "WHERE " + condition_statement
            query += " " + where_clause
        
        if limit:
            query += f" LIMIT {limit}"
        print(query)
        result_df = pd.read_sql(query, self.conn)
        return result_df
        
    def insert_row(self, table_name: str, *values):
        """
        Insert a new row into the specified table in the database.

        :param table_name: The name of the table to insert the row into.
        :type table_name: str
        :param values: The values to insert into the columns of the new row.
                       Can be a tuple of values for a single row, or a list of tuples for multiple rows.
        :type values: Tuple or List[Tuple]
        :return: None
        :rtype: None

        Example usage:
        >>> insert_row('mytable', ('value1', 'value2', 'value3'))
        >>> insert_row('mytable', [('value1', 'value2', 'value3'), ('value4', 'value5', 'value6')])

        """
        
        if isinstance(values[0], list):
            for item in values[0]:
                self.cur.execute(f"INSERT INTO {table_name} VALUES({','.join(['?' for _ in item])})", item)
        else:
            self.cur.executemany(f"INSERT INTO {table_name} VALUES({','.join(['?' for _ in values[0]])})", values)
        self.conn.commit()

    def insert(self, table_name: str, *values):
        """
        Inserts one or more rows into the specified table in the database.

        The method supports inserting a single row or multiple rows at once.

        :param table_name: The name of the table to insert the row(s) into.
        :type table_name: str
        :param values: Values to insert into the columns of the new row(s).
                    Can be a tuple of values for a single row or a list of tuples for multiple rows.
        :type values: Tuple or List[Tuple]
        :return: None
        :rtype: None

        Example usage:
        ```python
        # Inserting a single row into a table named "employees"
        db.insert("employees", ('Jan Kowalski', 30, 'Software Engineer'))

        # Inserting multiple rows into a table named "employees"
        db.insert("employees", ('Ala Nowak', 25, 'Data Scientist'), ('Krzysztof Skoczylas', 35, 'Manager'))
        ```
        """

        if isinstance(values[0], list):
            for item in values[0]:
                self.cur.execute(f"INSERT INTO {table_name} VALUES({','.join(['?' for _ in item])})", item)
        else:
            self.cur.executemany(f"INSERT INTO {table_name} VALUES({','.join(['?' for _ in values[0]])})", values)
        self.conn.commit()

    def get_first_row_value(self, table_name: str, *column_names):
        """       
        Get the values of specified columns or whole values if columns are not specified from the first row of a specified table.
        
        :param table_name: The name of the table to get the values from.
        :type table_name: str
        :param column_names: The names of the columns to get the values from.
        :type column_names: str
        :return: A tuple of the values from the first row of the specified columns.
        :rtype: tuple
        """
        if column_names:
            column_string = ", ".join(column_names)
        else:
            column_string = "*"

        query = f"SELECT {column_string} FROM {table_name} ORDER BY rowid ASC LIMIT 1"
        self.cur.execute(query)
        result = self.cur.fetchone()
        if len(result) == 1:
            result = result[0]
        return result    

    def get_last_row_value(self, table_name: str, *column_names):
        """       
        Get the values of specified columns or whole values if columns are not specified from the last row of a specified table.

        :param table_name: The name of the table to get the values from.
        :type table_name: str
        :param column_names: The names of the columns to get the values from.
        :type column_names: str
        :return: A tuple of the values from the last row of the specified columns.
        :rtype: tuple
        """
        if column_names:
            column_string = ", ".join(column_names)
        else:
            column_string = "*"
            
        query = f"SELECT {column_string} FROM {table_name} ORDER BY rowid DESC LIMIT 1"
        self.cur.execute(query)
        result = self.cur.fetchone()
        if len(result) == 1:
            result = result[0]
        return result        
    
    def get_last_id(self, table_name: str):
        """
        Retrieves the last inserted ID (primary key) in the specified table.

        Note: This assumes the table has an 'id' column or a column with auto-incrementing values.

        :param table_name: The name of the table to retrieve the last ID from.
        :type table_name: str
        :return: The last inserted ID in the specified table.
        :rtype: int

        Example usage:
        ```python
        # Retrieving the last inserted ID from a table named "employees"
        last_id = db.get_last_id("employees")
        print("Last inserted ID in 'employees' table:", last_id)
        ```
        """        
        self.cur.execute(f"SELECT id FROM {table_name}")
        last_id = self.cur.fetchall()[-1][0]
        return last_id  

    def update_data(self, table_name: str, attribute_name: str, attribute_value: str, item_id: int):
        """
        Updates a specific attribute in a row of the specified table.

        :param table_name: The name of the table to update.
        :type table_name: str
        :param attribute_name: The name of the attribute (column) to update.
        :type attribute_name: str
        :param attribute_value: The new value to set for the specified attribute.
        :type attribute_value: str
        :param item_id: The ID of the row to update.
        :type item_id: int
        :return: None
        :rtype: None

        Example usage:
        ```python
        # Updating the 'age' attribute to 35 for the row with ID 1 in a table named "employees"
        db.update_data("employees", "age", "35", 1)
        ```
        """
        self.cur.execute(f"UPDATE {table_name} set {attribute_name} = {attribute_value} where id = {item_id}")
        self.conn.commit()

    def update_table(self, table_name: str, update_dict, where_dict):
        """
        Update a row in the specified table based on the given key-value pair(s).
        :param table_name: The name of the table to update.
        :param update_dict: A dictionary of column name-value pairs to update.
        :param where_dict: A dictionary of key-value pairs that determine which rows to update.
        """ 
        update_str = ', '.join([f"'{key}' = ?" for key in update_dict.keys()])
        where_str = ' AND '.join([f'{key} = ?' for key in where_dict.keys()])
        
        sql = f"UPDATE '{table_name}' SET {update_str} WHERE {where_str}"
        print(sql)
        values = tuple(update_dict.values()) + tuple(where_dict.values())
        self.cur.execute(sql, values)
        self.conn.commit()

    def __del__(self):
        self.conn.close()

