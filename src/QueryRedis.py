import redis
import pandas
import re

# Connect to local redis, on port 6379 using database 0.
_redis = redis.StrictRedis(host='127.0.0.1', port=6379, db=0)

# A data frame that contains the cross product from all the tables in the WHERE clause.
data_frame = pandas.DataFrame()

# A list that contains all the attributes which are going to be projected.
attributes = []


def is_float(column):
    """ Checks if a given string represents an float number or not. """
    global data_frame

    try:
        data_frame[column].astype(float)
        return True
    except ValueError:
        return False


def convert_columns():
    """ Converts the values of a data frame's column from string to a floating point numbers, if is possible. """
    global data_frame

    for column in data_frame.columns:
        if is_float(column):
            data_frame[column] = data_frame[column].astype(float)


def find_select_attributes(file_lines):
    """ This function is responsible for reading the first line of the query specification file, in order to extract all
    the projection attributes and store them in a list. """
    global attributes

    # Get the first line and split it on ',' character, in order to get all the projections columns.
    select_attributes = file_lines[0].split(',')

    # Loop for every item in the projection columns.
    for item in select_attributes:
        attributes.append(item.lower().strip().replace('.', '_'))


def create_data_frames(all_lines):
    """ This function is responsible for reading the second line of the query specification file, which contains the
     names of the relations that we want to get the attributes from. After that, it reads all the corresponding hashes
     from redis, and stores them in a data frame which contains those records, using the pandas library  """
    global data_frame

    # Split the second line on the ',' character, in order to get the names of all the relations.
    table_names = all_lines[1].split(',')

    # A list that will temporarily store a different data frame for each table in the second line in the specifications.
    data_frames = []

    # Loop for every relation name
    for table in table_names:

        # Remove any whitespaces and make the table's name lowercase.
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

            # Use Redis' 'HGETALL' command, in order to get a dictionary of all key-value pairs, for the current hash.
            hash_values = _redis.hgetall(hash_key)

            # Loop for every key-value pair that exist inside a Redis hash.
            for (k, v) in hash_values.items():
                # Results are binary strings which are difficult to be handled, so we convert them into UTF-8 strings.
                k = (current_table + '_' + k.decode('utf-8')).lower()
                v = v.decode('utf-8')

                if k in key_values:
                    key_values[k].append(v)
                else:
                    key_values[k] = [v]

        # Now, in the data_frames dictionary, add a new data frame for the current relation.
        data_frames.append(pandas.DataFrame(data=key_values))

    # Combine all data frames into one, containing all the cross products.
    data_frame = data_frames[0]

    # Create a temporary pseudo-key, that will use in order to join the data frames.
    data_frame['_temporary_join_key_'] = 0

    # Loop for every other frame, except the first one.
    for i in range(1, len(data_frames)):
        data_frame_next = data_frames[i]
        data_frame_next['_temporary_join_key_'] = 0

        # Join the two data frames, using the pandas "merge" function.
        data_frame = data_frame.merge(data_frame_next, how='inner', on='_temporary_join_key_')

    # Delete the temporary key, since we don't need it anymore.
    del data_frame['_temporary_join_key_']

    # Finally, call the convert_columns() function, in order to convert any numbers from strings into floating points.
    convert_columns()


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

                # Get the condition from the third line and convert dots into underscores and '=' into '=='
                condition = re.sub(r'([a-z]+)(\.)([a-z]+)', r'\1_\3', re.sub(r'[^><]=', '==', lines[2].lower()))

                # Use the pandas "query" function, in order to get only those lines that the condition holds true.
                data_frame.query(condition, inplace=True)

                # Extract only the columns that we want to display and print the final result.
                print("\n", data_frame[attributes])
        except FileNotFoundError:
            print('\nException occurred, File not found.')
        except Exception as e:
            print('\nException occurred: ' + str(e))
        finally:
            choice = input('\nWould you like to import another file? (yes/no): ').strip().lower()

            while (not choice == 'yes') and (not choice == 'no'):
                choice = input('\nInvalid input.\nWould you like to import another file? (yes/no): ').strip().lower()

            if choice == 'no':
                break

    print('\nGoodbye!')
