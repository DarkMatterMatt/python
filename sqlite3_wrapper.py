#!/usr/bin/env python3
##############################################################################
##                                                                          ##
##                              Matthew Moran                               ##
##                                   2018                                   ##
##                                                                          ##
##############################################################################
##                                                                          ##
##  Module: sqlite3_wrapper.py                                              ##
##                                                                          ##
##  Description: A high level wrapper class for Sqlite3 databases. Add,     ##
##                 remove and clear tables quickly. Get results that match  ##
##                 a dictionary of values. Insert or update (upsert) in one ##
##                 function call (uses two SQL executions).                 ##
##                                                                          ##
##    Changelog: B=Bug, A=Add, R=Remove, C=Change                           ##
##    2.0.5 - C: Remove duplicates requested in a "get_list"                ##
##                                                                          ##
##        Usage: See test() function at the bottom for examples             ##
##                                                                          ##
##############################################################################

import sqlite3
import re

__version__ = "2.0.5"
_DEBUG = False

EQUAL               = "="
EQ                  = "="
NOTEQUAL            = "!="
NEQ                 = "!="
NOT                 = "!="
GREATERTHAN         = ">"
GT                  = ">"
GREATERTHANOREQUAL  = ">="
GTEQ                = ">="
LESSTHAN            = "<"
LT                  = "<"
LESSTHANOREQUAL     = "<="
LTEQ                = "<="
VALID_COMPARISONS   = [EQ, NEQ, GT, GTEQ, LT, LTEQ]

class InvalidName(Exception): pass
class KeyNotInTable(Exception): pass
class TableNotInDatabase(Exception): pass
class InvalidComparisonType(Exception): pass

class Database:
    def __init__(self, fname, tables_map):
        """Open database object, initialise tables map."""
        self.conn = sqlite3.connect(fname, detect_types=sqlite3.PARSE_DECLTYPES)
        self.c = self.conn.cursor()
        self.safeNamePattern = re.compile(r"[A-Za-z_]\w*$")
        self.tables = tables_map
        
        # check that all names are valid
        for name, table in self.tables.items():
            self._assert_safe_name(name)
            for key, data_type in table:
                self._assert_safe_name(key)
        
    def _assert_safe_name(self, name):
        """Check if name is allowed (a-z _ 0-9 and doesn't start with a digit)."""
        if self.safeNamePattern.match(name) == None:
            raise InvalidName("'{}' is not a valid table name. Name must start with a ".format(name) +
                "letter or underscore and can only contain letters, numbers and underscores.")
        
    def _assert_valid_comparison_type(self, comparison_type):
        """Check if comparison type is valid."""
        if not comparison_type in VALID_COMPARISONS:
            raise InvalidComparisonType("Comparison type '{}' is not valid. ".format(comparison_type) +
                "Valid comparison types are defined as: EQUAL [EQ], NOTEQUAL [NEQ], LESSTHAN [LT], "
                "LESSTHANOREQUAL [LTEQ], GREATERTHAN [GT], GREATERTHANOREQUAL [GTEQ]")
                
    def _assert_table_in_map(self, table_name):
        """Check that table is in the tables map."""
        if not table_name in self.tables:
            raise TableNotInDatabase("Table '{}' is not in the database (is missing from the tables map).".format(table_name))
        
    def _assert_key_in_table(self, table_name, key):
        """Check that key is in the table in the tables map."""
        key = key.lower()
        if key == "rowid": # allow "rowid" in any table
            return
        for k, data_type in self.tables[table_name]:
            if k.lower() == key:
                return
        raise KeyNotInTable("Key '{}' is not in the table (is missing from the table's map).".format(key))
        
    def _process_select_dict(self, table_name, select_dict_orig):
        """Check that all key:values are valid and have a comparison type."""
        # make a copy of select_dict
        select_dict = dict(select_dict_orig)
        
        for k, v in select_dict.items():
            # make sure all values have their comparison type (EQUAL, GREATERTHAN, etc.)
            if isinstance(v, tuple):
                if len(v) != 2:
                    raise SyntaxError("Incorrect number of values in tuple. Format is (key, COMPARISON_TYPE).")
                
                # check that the comparison type is valid
                self._assert_valid_comparison_type(v[1])
                    
            else:
                v = (v, EQUAL)
                select_dict[k] = v
                
            # handle NULLs which are compared differently
            if v[0] == None:
                v = (v[0], "IS" if v[1] == EQUAL else "IS NOT")
                select_dict[k] = v
                
            # check that all keys are valid
            self._assert_key_in_table(table_name, k)
                
        return select_dict
        
    def cursor(self):
        """Get a reference to the database cursor."""
        return self.c
            
    def commit(self):
        """Commit changes to the database."""
        self.conn.commit()
        
    def close(self):
        """Close database without saving. Commit first to save changes."""
        self.conn.close()
    
    def create_table(self, table_name):
        """Creates a table, using information from the tables map."""
        self._assert_table_in_map(table_name)
        params = []
        for x in self.tables[table_name]:
            params.append(x[0] + " " + x[1])
        params_string = ', '.join(params)
        if _DEBUG: print("@SQL CREATE TABLE IF NOT EXISTS {} ({})".format(table_name, params_string))
        self.c.execute("CREATE TABLE IF NOT EXISTS {} ({})".format(table_name, params_string))
        
    def delete_table(self, table_name):
        """Deletes a table if it exists."""
        self._assert_table_in_map(table_name)
        if _DEBUG: print("@SQL DROP TABLE IF EXISTS {}".format(table_name))
        self.c.execute("DROP TABLE IF EXISTS {}".format(table_name))
        
    def reset_table(self, table_name):
        """Removes all data in the table."""
        self._assert_table_in_map(table_name)
        if _DEBUG: print("@SQL DELETE FROM {}".format(table_name))
        self.c.execute("DELETE FROM {}".format(table_name))
        
    def table_exists(self, table_name):
        """Check if the table exists."""
        self._assert_table_in_map(table_name)
        if _DEBUG: print("@SQL SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        self.c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        return self.c.fetchone() != False
        
    def get_all(self, table_name, select_dict, get_list=None):
        """Get all records that match all key:values in dict."""
        self._assert_table_in_map(table_name)
            
        # make sure everything in the select_dict is valid
        select_dict = self._process_select_dict(table_name, select_dict)
        
        # make sure get_list is valid, get all by default
        if get_list:
            for k in set(get_list):
                self._assert_key_in_table(table_name, k)
        else:
            get_list = [x[0] for x in self.tables[table_name]]
            
        # make a list of things to match, and their match type (>, =, etc.)
        params = []
        select = []
        for k in select_dict:
            v, c = select_dict[k] # (value, comparison_type)
            select.append("{} {} ?".format(k, c))
            params.append(v)
            
        # make strings to send to SQL
        get    = ", ".join(get_list)
        select = " AND ".join(select)
        if _DEBUG: print("@SQL SELECT {} FROM {} WHERE {}".format(get, table_name, select), params)
        self.c.execute("SELECT {} FROM {} WHERE {}".format(get, table_name, select), params)
        results = self.c.fetchall()
        
        # make a list of dictionaries containing the requested results
        get_list_of_dicts = []
        for result in results:
            # results are {key:value} pairs in a dictionary
            get_list_of_dicts.append(dict(zip(get_list, result)))
        return get_list_of_dicts
        
    def get(self, table_name, select_dict, get_list=None):
        """Get the first record that match all key:values in dict."""
        # get a list of all results first
        result = self.get_all(table_name, select_dict, get_list)
        # return the first result if there is one
        return result[0] if result else None
        
    def put(self, table_name, select_dict, put_dict, force_new_record=False):
        """Updates or creates a new record."""
        # check that table name is valid
        self._assert_table_in_map(table_name)
            
        # make sure everything in the select_dict is valid
        select_dict = self._process_select_dict(table_name, select_dict)
        
        # check that all keys in put_dict are valid
        for key in put_dict:
            self._assert_key_in_table(table_name, key)
        
        # add everything in select_dict to put_dict
        for k, v in select_dict.items():
            if not k in put_dict:
                put_dict[k] = v[0]
        
        # create a list of all the values going in
        params = []
        for _, v in put_dict.items():
            params.append(v)
        
        if not force_new_record:
            # check if there is already a record in the database
            result = self.get(table_name, select_dict, ("rowid",))
            
            # if there is a record, update it
            if result:
                params.append(result["rowid"])
                put = ", ".join("{} = ?".format(k) for k in put_dict)
                if _DEBUG: print("@SQL UPDATE {} SET {} WHERE rowid=?".format(table_name, put), params)
                self.c.execute("UPDATE {} SET {} WHERE rowid=?".format(table_name, put), params)
                return result["rowid"]
        
        # create a new record
        put = ", ".join(put_dict)
        placeholders = ", ".join(["?"] * len(put_dict))
        if _DEBUG: print("@SQL INSERT INTO {} ({}) VALUES ({})".format(table_name, put, placeholders), params)
        self.c.execute("INSERT INTO {} ({}) VALUES ({})".format(table_name, put, placeholders), params)
        return self.c.lastrowid
            
    def post(self, table_name, post_dict):
        """Creates a new record."""
        # same as a put request, but force a new record
        self.put(table_name, {}, post_dict, force_new_record=True)
    
    def delete(self, table_name, select_dict):
        """Deletes all records that match all key:values in dict."""
        # check that table name is valid
        self._assert_table_in_map(table_name)
            
        # make sure everything in the select_dict is valid
        select_dict = self._process_select_dict(table_name, select_dict)
            
        # make a list of things to match, and their match type (>, =, etc.)
        params = []
        select = []
        for k in select_dict:
            v, c = select_dict[k] # (value, comparison_type)
            select.append("{} {} ?".format(k, c))
            params.append(v)
            
        # make strings to send to SQL
        select = " AND ".join(select)
        if _DEBUG: print("@SQL DELETE FROM {} WHERE {}".format(table_name, select), params)
        self.c.execute("DELETE FROM {} WHERE {}".format(table_name, select), params)
        
        # return number of rows deleted
        return self.c.rowcount
        
def test():
    from pprint import pprint
    from datetime import datetime
    import os
    
    global _DEBUG
    _DEBUG = True
    
    # map of every table and column in database
    tables_map = {
        "testTable1": [
            ("t1_w",    "TEXT UNIQUE PRIMARY KEY"),
            ("t1_x",    "TEXT"),
            ("t1_y",    "INTEGER"),
            ("t1_z",    "TIMESTAMP")
        ],
        "test_table_2": [
            ("t1_x",    "INTEGER PRIMARY KEY AUTOINCREMENT"),
            ("t1_y",    "TEXT"),
            ("t1_z",    "INTEGER"),
        ]
    }
    
    # open database
    db = Database("test.db", tables_map)
    db.create_table("testTable1")   # create table if it doesn't already exist
    db.create_table("test_table_2") # create table if it doesn't already exist
    db.reset_table("testTable1")    # deletes everything in table
    
    # demo insert into table
    db.put(
        "testTable1",
        {"t1_y": 99, "t1_z": datetime.now()}, # unique identifiers (all must match)
        {"t1_w": "One primary key here"}      # new data to insert
    )

    # demo insert into table
    print("Insert into table...")
    lastrowid = db.put(
        "testTable1",
        {"t1_w": "I'm Unique"},             # unique identifiers (all must match)
        {"t1_y": 5, "t1_z": datetime.now()} # new data to insert
    )
    print("inserted row ID was", lastrowid)
    
    # demo update row in table
    print("Update row in table...")
    lastrowid = db.put(
        "testTable1",
        {"t1_w": "I'm Unique", "t1_y": 5},  # unique identifiers (all must match)
        {"t1_y": 99, "t1_x": "RandomText"}  # new data to insert
    )
    print("updated row ID was", lastrowid)
    
    # demo get all rows that match
    print("Get all matches:")
    pprint(db.get_all(
        "testTable1",
        {"t1_y": 99},
        # get all columns by default
    ))
    
    # demo get first row that matches
    print("Get first match:")
    pprint(db.get(
        "testTable1",
        {"t1_y": 99},
        ["t1_w", "t1_z"] # only get these columns
    ))
    
    # demo delete all rows that match
    print("Delete matches...",)
    rows_deleted = db.delete(
        "testTable1",
        {"t1_y": 99}
    )
    print("deleted {} rows.".format(rows_deleted))
    
    # save and close database
    db.commit()
    db.close()
    
    # delete database
    os.remove("test.db")
    
    
if __name__ == "__main__":
    test()