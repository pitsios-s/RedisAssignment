import redis
import pandas
import re

# Connect to local redis, on port 6379 using database 0.
_redis = redis.StrictRedis(host='192.168.229.129', port=6379, db=0)

# This dictionary will be used in order to store data frames that correspond to the tables in the FROM clause from the
# specification file. The key will be the table's name and the value will be the data frame itself.
data_frames = {}

# Attributes dictionary will store as key the name of the relation and as value, as set containing all
# the attributes of this relation that we want to project.
attributes = {}


def is_int(item):
        """ Checks if a given string represents an integer or not. """
        try:
            int(item)
            return True
        except ValueError:
            return False


def is_float(item):
    """ Checks if a given string represents an float number or not. """
    try:
        float(item)
        return True
    except ValueError:
        return False


def convert_string(string):
    """ Converts a string to an integer or floating point number, if conversion is possible. """

    if is_int(string):
        return int(string)
    elif is_float(string):
        return float(string)
    else:
        return string


def find_select_attributes(file_lines):
    """ This function is responsible for reading the first line of the query specification file, in order to extract all
    the projection attributes, alongside with the tables that they belong to and store them in a dictionary. """
    global attributes

    # Get the first line and split it on ',' character, in order to get all the projections columns.
    select_attributes = file_lines[0].split(',')

    # Loop for every item in the projection columns.
    for item in select_attributes:

        # Those attributes will be in the form 'Relation.column'. So if we split them in the '.' character,
        # we get two strings with the first being the relation's name and the second, a column that we want
        # to project.
        values = item.split('.')
        _key = values[0].strip().lower()
        _value = values[1].strip().lower()

        # If the "attributes" dictionary already contains a key with the relation's name, then add to it's
        # list the new attribute, otherwise, add a new entry which has as value a set containing a single
        # element.
        if _key in attributes:
            attributes[_key].add(_value)
        else:
            attributes[_key] = {_value}


def create_data_frames(all_lines):
    """ This function is responsible for reading the second line of the query specification file, which contains the
     names of the relations that we want to get the attributes from. After that, it reads all the corresponding hashes
     from redis, getting only the appropriate 'columns' that we stored earlier and creates a data frame using the
     pandas library which contains those records. """
    global data_frames

    # Split the second line on the ',' character, in order to get the names of all the relations.
    table_names = all_lines[1].split(',')

    # Loop for every relation name
    for table in table_names:

        # Remove any whitespaces.
        current_table = table.strip().lower()

        # Use the Redis' 'KEYS' command to get all the key names of the hashes, that begin with the current table's name
        key_names = _redis.keys(current_table + ':*')

        # Get all the projection attributes that correspond to the current table, using the "attributes" dictionary.
        if current_table in attributes:
            attrs = attributes[current_table]
        else:
            attrs = []

        # A dictionary that will keep a list of values for every key in all hashes that share the same name.
        # e.g. for all hashes that are named student:*, let's say that we want the values of the attribute grade
        # from all those hashes. Then the key_values dictionary will keep records that have as a key the name of the
        # attribute (grade) and as value a list that contains the associated values from all 'student' hashes.
        key_values = {}

        # Loop for every hash key that the Redis' 'KEYS' function returned....
        for hash_key in key_names:

            # Use Redis' 'HGETALL' command, in order to get a dictionary of key-value pairs, that correspond to the
            # current hash key.
            hash_values = _redis.hgetall(hash_key)

            # If we have already declared which attributes of the current table we will need, loop for every one of them
            if len(attrs) > 0:

                # Loop for every attribute that we want to project.
                for attr in attrs:
                    v = (hash_values[attr.encode('utf-8')]).decode('utf-8')

                    # If v is an integer of a floating point number, convert it.
                    v = convert_string(v)

                    # Finally add v in the key_values dict as a value, under the key that corresponds to the current
                    # attribute.
                    if attr in key_values:
                        key_values[attr].append(v)
                    else:
                        key_values[attr] = [v]

            # Otherwise, keep all the columns for the current table.
            else:
                for (k, v) in hash_values.items():
                    k = k.decode('utf-8')
                    v = v.decode('utf-8')

                    if k in key_values:
                        key_values[k].append(v)
                    else:
                        key_values[k] = [v]

        # Now, in the data_frames dictionary, add a new data frame for the current relation.
        data_frames[table.strip().lower()] = pandas.DataFrame(data=key_values)


def filter_simple_condition(literal):
    """ This function will be used in order to filter our data frames, when we have simple where conditions, with only
     one literal. """
    global data_frames

    left_right = re.split('<>|>|<|=', literal.strip())
    left_right[1] = left_right[1].replace("'", "")

    table_attribute = left_right[0].split('.')
    df_new = data_frames[table_attribute[0].strip().lower()]

    if '<>' in literal:
        return df_new[df_new[table_attribute[1].strip().lower()] != convert_string(left_right[1].strip())]
    elif '>' in literal:
        return df_new[df_new[table_attribute[1].strip().lower()] > convert_string(left_right[1].strip())]
    elif '<' in literal:
        return df_new[df_new[table_attribute[1].strip().lower()] < convert_string(left_right[1].strip())]
    elif '=' in literal:
        return df_new[df_new[table_attribute[1].strip().lower()] == convert_string(left_right[1].strip())]


def handle_and_clauses(all_lines):
    """ This function will be used to evaluate expressions that contain one or more and clauses and returns a data
    frame that those constraints are fulfilled. """

    # Split the line on the AND expression.
    and_clauses = re.split(' +and +', all_lines[2].strip())

    # If we have at least one AND clause in our expression...
    if len(and_clauses) > 1:

        # A vector that contains one data frame for each literal in the and clause.
        d_frames = []

        # Loop for every literal.
        for literal in and_clauses:
            # Evaluate the literal, filter the appropriate data frame and store the result in d_frames list.
            d_frames.append(filter_simple_condition(literal))

        # Now, in order to get one final data frame, we will proceed by joining all the data frames that we got from
        # before.
        data_frame_final = d_frames[0]
        for i in range(1, len(d_frames)):
            data_frame_final = data_frame_final.merge(d_frames[i], how="inner")

        return data_frame_final

    # If there is not any and clause, return a data frame that evaluates the single literal.
    else:
        return filter_simple_condition(lines[2])


def filter_results(all_lines):
    """ This function will be used in order to process the third and final line of the query specification file which
    contains the where clause. """

    # Split the line on the OR expression.
    or_clauses = re.split(' +or +', all_lines[2].strip())

    # If we have at least one OR clause in our expression...
    if len(or_clauses) > 1:
        pass
    else:
        return handle_and_clauses(all_lines)


# The main function of the program.
if __name__ == '__main__':

    # Repeat until user does not want to continue with another query.
    while True:

        # Request the file name from stdin.
        file_name = input('Please enter the path of the query file (Absolute or Relative): ')

        try:
            # Open the file given above, in read mode.
            with open(file_name, 'r') as in_file:

                # Read all the 3 lines from the specification file at once.
                lines = in_file.readlines()

                # Fill the "attributes" dictionary.
                find_select_attributes(lines)

                # Create all the necessary data frames.
                create_data_frames(lines)

                # Filter the results according to the WHERE clause.
                print(filter_results(lines))
                for (key, value) in data_frames.items():
                    print(value)

        except FileNotFoundError:
            print('Exception occurred, File not found.')
        except Exception as e:
            print('Exception occurred: ' + str(e))
        finally:
            choice = input('Would you like to import another file? (yes/no): ').strip().lower()

            while (not choice == 'yes') and (not choice == 'no'):
                choice = input('Invalid input.\nWould you like to import another file? (yes/no): ').strip().lower()

            if choice == 'no':
                break

    print('Goodbye!')
