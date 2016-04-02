import redis
import pandas
import re

# Connect to local redis, on port 6379 using database 0.
_redis = redis.StrictRedis(host='192.168.229.129', port=6379, db=0)

# A data frame that contains the cross product from all the tables in the WHERE clause.
data_frame = None

# A list that contains all the attributes which are going to be projected.
attributes = []


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
    the projection attributes and store them in a list. """
    global attributes

    # Get the first line and split it on ',' character, in order to get all the projections columns.
    select_attributes = file_lines[0].split(',')

    # Loop for every item in the projection columns.
    for item in select_attributes:
        attributes.append(item.lower().strip())


def create_data_frames(all_lines):
    """ This function is responsible for reading the second line of the query specification file, which contains the
     names of the relations that we want to get the attributes from. After that, it reads all the corresponding hashes
     from redis, getting only the appropriate 'columns' that we stored earlier and creates a data frame using the
     pandas library which contains those records. """
    global data_frame

    # Split the second line on the ',' character, in order to get the names of all the relations.
    table_names = all_lines[1].split(',')

    data_frames = []

    # Loop for every relation name
    for table in table_names:

        # Remove any whitespaces.
        current_table = table.strip().lower()

        # Use the Redis' 'KEYS' command to get all the key names of the hashes, that begin with the current table's name
        key_names = _redis.keys(current_table + ':*')

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

            for (k, v) in hash_values.items():
                k = current_table + '.' + k.decode('utf-8')
                v = v.decode('utf-8')

                if k in key_values:
                    key_values[k].append(v)
                else:
                    key_values[k] = [v]

        # Now, in the data_frames dictionary, add a new data frame for the current relation.
        data_frames.append(pandas.DataFrame(data=key_values))

    # Combine all data frames into one, containing all the cross products.
    data_frame = data_frames[0]
    data_frame['join_key'] = 0

    for i in range(1, len(data_frames)):
        data_frame_next = data_frames[i]
        data_frame_next['join_key'] = 0

        data_frame = data_frame.merge(data_frame_next, how='inner', on='join_key')

    del data_frame['join_key']


def evaluate_simple_condition(literal):
    """ This function will be used in order to filter our data frames, when we have simple where conditions, with only
     one literal. """
    global data_frame

    left_right = re.split('<>|>|<|=', literal.strip())
    left_right[1] = left_right[1].replace("'", "")

    attr = left_right[0].lower().strip()

    if '<>' in literal:
        if len(left_right[1].split('.')) == 2:
            return data_frame[data_frame[attr] != data_frame[left_right[1].strip()]]
        else:
            return data_frame[data_frame[attr] != convert_string(left_right[1].strip())]
    elif '>' in literal:
        if len(left_right[1].split('.')) == 2:
            return data_frame[data_frame[attr] > data_frame[left_right[1].strip()]]
        else:
            return data_frame[data_frame[attr] > convert_string(left_right[1].strip())]
    elif '<' in literal:
        if len(left_right[1].split('.')) == 2:
            return data_frame[data_frame[attr] < data_frame[left_right[1].strip()]]
        else:
            return data_frame[data_frame[attr] < convert_string(left_right[1].strip())]
    elif '=' in literal:
        if len(left_right[1].split('.')) == 2:
            return data_frame[data_frame[attr] == data_frame[left_right[1].strip()]]
        else:
            return data_frame[data_frame[attr] == convert_string(left_right[1].strip())]


def handle_and_clauses(and_clause):
    """ This function will be used to evaluate expressions that contain one or more AND clauses and returns a data
    frame that those constraints are fulfilled. """

    # Split the line on the AND expression.
    and_clauses = re.split(' +[a|A][n|N][d|D] +', and_clause)

    # If we have at least one AND clause in our expression...
    if len(and_clauses) > 1:

        # A vector that contains one data frame for each literal in the AND clause.
        d_frames = []

        # Loop for every literal.
        for literal in and_clauses:
            # Evaluate the literal, filter the appropriate data frame and store the result in d_frames list.
            d_frames.append(evaluate_simple_condition(literal))

        # Now, in order to get one final data frame, we will proceed by joining all the data frames that we got before.
        data_frame_final = d_frames[0]
        for i in range(1, len(d_frames)):
            data_frame_final = data_frame_final.merge(d_frames[i], how='inner', left_index=True, right_index=True)

        return data_frame_final

    # If there is not any and clause, return a data frame that evaluates the single literal.
    else:
        return evaluate_simple_condition(and_clause)


def filter_results(all_lines):
    """ This function will be used in order to process the third and final line of the query specification file which
    contains the WHERE clause. """
    global data_frame

    # Split the line on the OR expression.
    or_clauses = re.split(' +[o|O][r|R] +', all_lines[2].strip())

    # If we have at least one OR clause in our expression...
    if len(or_clauses) > 1:

        # A vector that contains one data frame for each literal in the OR clause.
        d_frames = []

        # Loop for every literal in the OR clauses.
        for literal in or_clauses:
            # Add a data frame in d_frames list, that fulfils the specified literal in the OR clause.
            d_frames.append(handle_and_clauses(literal))

        # indexes is a set that will be keeping the union of  the row numbers of the individual data frames in d_frames.
        indexes = set()
        for i in range(0, len(d_frames)):
            indexes = indexes.union(list(d_frames[i].index))

        # Create and return a data frame, using only the indexes computed above. This is the final result.
        return data_frame[data_frame.index.isin(indexes)]

    else:
        return handle_and_clauses(all_lines[2].strip())


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

                print(filter_results(lines)[attributes])
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
