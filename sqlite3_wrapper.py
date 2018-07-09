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
##    2.1.0 - A: Foreign key support                                        ##
##    2.1.0 - A: Foreign key support                                        ##
##    2.2.0 - A: Sorting support                                            ##
##            A: Decent documentation (in the form of Docstrings & comments)##
##            A: Alias functions upsert=put, insert=post                    ##
##    2.3.0 - A: create|delete|reset_all_tables functions                   ##
##                                                                          ##
##        Usage: (See test() for examples. see Docstrings for more detail.) ##
##             > import sqlite3_wrapper as sql                              ##
##             > db = sql.Database("name.db", database_structure)           ##
##             > db.put(table, selection_dictionary, dictionary_to_put)     ##
##                  returns the rowid updated                               ##
##             > db.upsert - alias for put                                  ##
##             > db.post - same as put, but forces new record               ##
##             > db.insert - alias for post                                 ##
##             > db.get_all(table, selection_dict, optional_list_to_get)    ##
##                  returns a dictionary containing every (requested) value ##
##             > db.get - same as get_all, but returns the first match only ##
##             > db.delete(table, selection_dict)                           ##
##             > db.create|delete|reset_table(table_name)                   ##
##             > db.commit|close(table_name)                                ##
##                                                                          ##
##############################################################################
import sqlite3
import re

__version__ = "2.3.0"
_DEBUG = False

EQ                 = "="
NEQ                = "!="
GT                 = ">"
GTEQ               = ">="
LT                 = "<"
LTEQ               = "<="
EQUAL              = EQ
NOTEQUAL           = NEQ
NOT                = NEQ
GREATERTHAN        = GT
GREATERTHANOREQUAL = GTEQ
LESSTHAN           = LT
LESSTHANOREQUAL    = LTEQ
VALID_COMPARISONS  = [EQ, NEQ, GT, GTEQ, LT, LTEQ]

ASC                = "ASC"
DESC               = "DESC"
ASCENDING          = ASC
UP                 = ASC
DESCENDING         = DESC
DOWN               = DESC
VALID_SORTING      = [ASC, DESC]

class InvalidName(Exception): pass
class KeyNotInTable(Exception): pass
class TableNotInDatabase(Exception): pass
class InvalidComparisonType(Exception): pass
class InvalidSortingType(Exception): pass


class Database:
    def __init__(self, fname, database_structure):
        """Open database from filename, initialise database structure.

        database structure should be of the format:
        {table_name: [(key_name, key_type), (key_name, key_type), ... ], ... }
        e.g. {"books": [("name", "TEXT PRIMARY KEY"), ("pages", "INTEGER")]}
        """
        self.conn = sqlite3.connect(fname, detect_types=sqlite3.PARSE_DECLTYPES)
        self.c = self.conn.cursor()
        self.safeNamePattern = re.compile(r"[a-z_]\w*$", re.IGNORECASE)
        self.tables = database_structure

        foreignKeyPattern = re.compile(r"FOREIGN KEY \(([a-z_]\w*)\)", re.IGNORECASE)

        # check that all names are valid
        for table_name, table in self.tables.items():
            self._assert_safe_name(table_name)
            for key, data_type in table:
                # support FOREIGN KEYs
                match = foreignKeyPattern.match(key)
                if match:
                    key = match.group(1)
                    self._assert_key_in_table(table_name, key)

                self._assert_safe_name(key)

    def _assert_safe_name(self, name):
        """Check that name is allowed (alphanumeric, underscores and doesn't start with a digit)."""
        if self.safeNamePattern.match(name) == None:
            raise InvalidName("'{}' is not a valid table name. Name must start with a ".format(name) +
                "letter or underscore and can only contain letters, numbers and underscores.")

    def _assert_valid_comparison_type(self, comparison_type):
        """Check that comparison type is valid."""
        if not comparison_type in VALID_COMPARISONS:
            raise InvalidComparisonType("Comparison type '{}' is not valid. ".format(comparison_type) +
                "Valid comparison types are defined as: EQUAL [EQ], NOTEQUAL [NEQ], LESSTHAN [LT], "
                "LESSTHANOREQUAL [LTEQ], GREATERTHAN [GT], GREATERTHANOREQUAL [GTEQ]")

    def _assert_valid_sorting_type(self, sorting_type):
        """Check that sorting type is valid."""
        if not sorting_type in VALID_SORTING:
            raise InvalidSortingType("Sorting type '{}' is not valid. ".format(comparison_type) +
                "Valid comparison types are defined as: ASC, DESC")

    def _assert_table_in_database_structure(self, table_name):
        """Check that table is in the database structure."""
        if not table_name in self.tables:
            raise TableNotInDatabase("Table '{}' is not in the database (is missing from the database structure).".format(table_name))

    def _assert_key_in_table(self, table_name, key):
        """Check that key is in the table in the database structure."""
        key = key.lower()
        if key == "rowid": # allow "rowid" in any table
            return
        for k, data_type in self.tables[table_name]:
            if k.lower() == key:
                return
        raise KeyNotInTable("Key '{}' is not in the table (is missing from {} in the database structure).".format(key, table_name))

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
        """Return a reference to the database cursor."""
        return self.c

    def commit(self):
        """Commit changes to the database."""
        self.conn.commit()

    def close(self):
        """Close database without saving. Commit first to save changes."""
        self.conn.close()

    def create_table(self, table_name):
        """Create the table using information from the database structure."""
        self._assert_table_in_database_structure(table_name)
        # join list of key:datatypes tuples into KEY DATATYPE, KEY DATATYPE, ...
        params = ', '.join(" ".join(x) for x in self.tables[table_name])
        if _DEBUG: print("@SQL CREATE TABLE IF NOT EXISTS {} ({})".format(table_name, params))
        self.c.execute("CREATE TABLE IF NOT EXISTS {} ({})".format(table_name, params))
        
    def create_all_tables(self):
        """Create all the tables in the database structure."""
        for table_name in self.tables:
            self.create_table(table_name)

    def delete_table(self, table_name):
        """Delete the table if it exists."""
        self._assert_table_in_database_structure(table_name)
        if _DEBUG: print("@SQL DROP TABLE IF EXISTS {}".format(table_name))
        self.c.execute("DROP TABLE IF EXISTS {}".format(table_name))
        
    def delete_all_tables(self):
        """Delete all the tables in the database structure."""
        for table_name in self.tables:
            self.delete_table(table_name)

    def reset_table(self, table_name):
        """Remove every row from the table."""
        self._assert_table_in_database_structure(table_name)
        if _DEBUG: print("@SQL DELETE FROM {}".format(table_name))
        self.c.execute("DELETE FROM {}".format(table_name))
        
    def reset_all_tables(self):
        """Reset all the tables in the database structure."""
        for table_name in self.tables:
            self.reset_table(table_name)

    def table_exists(self, table_name):
        """Determine whether the table exists."""
        self._assert_table_in_database_structure(table_name)
        if _DEBUG: print("@SQL SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        self.c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        return self.c.fetchone() != False

    def get_all(self, table_name, select_dict, get_list=None, sort_by=None):
        """Get all records that match all key:values in the selection dictionary.

        table_name  -- The table name to search for matches in.
        select_dict -- A dictionary of key:(value, comparison) to match. All must match.
                       e.g {"author":("Bob", EQUAL), "pages":(100, GREATERTHAN)}
        get_list    -- A list of columns to return. Defaults to return all columns.
        sort_by     -- A list of tuples to order the results by.
                       e.g. [("author",DESCENDING), ("pages":ASCENDING)]
        RETURN >>> >>> A list (of matches) of dictionaries containing the requested columns.
        """
        self._assert_table_in_database_structure(table_name)

        # make sure everything in the select_dict is valid
        select_dict = self._process_select_dict(table_name, select_dict)

        # make sure get_list is valid, get all by default
        if get_list:
            for k in set(get_list):
                self._assert_key_in_table(table_name, k)
        else:
            get_list = []
            for x in self.tables[table_name]:
                if not x[0].startswith("FOREIGN KEY"):
                    get_list.append(x[0])

        if sort_by:
            # if sort by is a column name
            if isinstance(sort_by, str):
                sort_by = [(sort_by, ASC)]
            # if sort by is a single tuple
            elif len(sort_by) == 2 and sort_by[1] in VALID_SORTING:
                sort_by = [tuple(sort_by)]

            # sort by is a list of tuples
            for i, k in enumerate(sort_by):
                # make sure all values have their sorting type (ASC or DESC)
                if isinstance(k, tuple):
                    if len(k) != 2:
                        raise SyntaxError("Incorrect number of values in tuple. Format is (key, ASC or DESC).")
                    # check that the key and comparison type are valid
                    self._assert_key_in_table(table_name, k[0])
                    self._assert_valid_sorting_type(k[1])

                else:
                    sort_by[i] = (k, ASC)
                    self._assert_key_in_table(table_name, k)

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
        sort   = " ORDER BY " + ", ".join(" ".join(x) for x in sort_by) if sort_by else ""
        if _DEBUG: print("@SQL SELECT {} FROM {} WHERE {}{}".format(get, table_name, select, sort), params)
        self.c.execute("SELECT {} FROM {} WHERE {}{}".format(get, table_name, select, sort), params)
        results = self.c.fetchall()

        # make a list of dictionaries containing the requested results
        get_list_of_dicts = []
        for result in results:
            # results are {key:value} pairs in a dictionary
            get_list_of_dicts.append(dict(zip(get_list, result)))
        return get_list_of_dicts

    def get(self, table_name, select_dict, get_list=None, sort_by=None):
        """Get the first record that match all key:values in the selection dictionary.

        table_name  -- The table name to search for matches in.
        select_dict -- A dictionary of key:(value, comparison) to match. All must match.
                       e.g {"author":("Bob", EQUAL), "pages":(100, GREATERTHAN)}
        get_list    -- A list of columns to return. Defaults to return all columns.
        sort_by     -- A list of tuples to order the results by.
                       e.g. [("author",DESCENDING), ("pages":ASCENDING)]
        RETURN >>> >>> A dictionaries containing the requested columns or None if there is no match.
        """
        # get a list of all results first
        result = self.get_all(table_name, select_dict, get_list, sort_by)
        # return the first result if there is one
        return result[0] if result else None

    def put(self, table_name, select_dict, put_dict, force_new_record=False):
        """Update if exists or create a record.

        table_name  -- The table name to search for matches in.
        select_dict -- A dictionary of key:(value, comparison) to match. All must match.
                       e.g {"author":("Bob", EQUAL), "pages":(100, GREATERTHAN)}
        put_dict    -- A dictionary of data to upsert into the database.
        RETURN >>> >>> The RowID of the edited row (or -1 if no row was changed)
        """
        # check that table name is valid
        self._assert_table_in_database_structure(table_name)

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
        """Create a new record.

        table_name  -- The table name to search for matches in.
        put_dict    -- A dictionary of data to insert into the database.
        RETURN >>> >>> The RowID of the edited row (or -1 if no row was changed)
        """
        # same as a put request, but force a new record
        return self.put(table_name, {}, post_dict, force_new_record=True)

    def delete(self, table_name, select_dict):
        """Delete all records that match all key:values in the selection dictionary.

        table_name  -- The table name to search for matches in.
        select_dict -- A dictionary of key:(value, comparison) to match. All must match.
                       e.g {"author":("Bob", EQUAL), "pages":(100, GREATERTHAN)}
        RETURN >>> >>> The number of rows deleted
        """
        # check that table name is valid
        self._assert_table_in_database_structure(table_name)

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

    # alias functions
    upsert = put
    insert = post


def test():
    from pprint import pprint
    from datetime import datetime
    import os

    global _DEBUG
    _DEBUG = True

    # map of every table and column in database
    # each table is a list of tuples to preserve key order
    database_structure = {
        "testTable1": [
            ("w",    "TEXT UNIQUE PRIMARY KEY"),
            ("x",    "TEXT"),
            ("y",    "INTEGER"),
            ("z",    "TIMESTAMP"),
            ("f",    "INTEGER"),
            ("FOREIGN KEY (f)", "REFERENCES test_table_2(x)"),
        ],
        "test_table_2": [
            ("x",    "INTEGER PRIMARY KEY AUTOINCREMENT"),
            ("y",    "TEXT"),
            ("z",    "INTEGER"),
        ]
    }

    # open database
    db = Database("test.db", database_structure)
    db.create_table("testTable1")    # create table if it doesn't already exist
    db.create_table("test_table_2")  # create table if it doesn't already exist
    db.reset_table("testTable1")     # deletes everything in table

    # demo insert into table
    db.put(
        "testTable1",
        {"y": 99, "z": datetime.now()},  # unique identifiers (all must match)
        {"w": "One primary key here"}    # new data to insert
    )
    # demo insert into table
    print("Insert into table...")
    lastrowid = db.put(
        "testTable1",
        {"w": "I'm Unique"},           # unique identifiers (all must match)
        {"y": 5, "z": datetime.now()}  # new data to insert
    )
    print("inserted row ID was", lastrowid)

    # demo update row in table
    print("Update row in table...")
    lastrowid = db.upsert(
        "testTable1",
        {"w": "I'm Unique", "y": 5},  # unique identifiers (all must match)
        {"y": 99, "x": "RandomText"}  # new data to insert
    )
    print("updated row ID was", lastrowid)

    # demo get all rows that match
    print("Get all matches:")
    pprint(db.get_all(
        "testTable1",
        {"y": 99},
        # get all columns by default
    ))

    # demo get first row that matches
    print("Get first match:")
    pprint(db.get(
        "testTable1",
        {"y": 99},
        ["w", "z"], # only get these columns
        sort_by=[("y", ASC), ("x", DESC)]
    ))

    # demo delete all rows that match
    print("Delete matches...",)
    rows_deleted = db.delete(
        "testTable1",
        {"y": 99}
    )
    print("deleted {} rows.".format(rows_deleted))

    # save and close database
    db.commit()
    db.close()

    # delete database
    os.remove("test.db")


if __name__ == "__main__":
    test()
