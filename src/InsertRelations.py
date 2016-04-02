import redis

# Connect to local redis, on port 6379 using database 0.
_redis = redis.StrictRedis(host='127.0.0.1', port=6379, db=0)

# Create a pipeline, that will be used for bulk inserts into redis.
redis_pipeline = _redis.pipeline()

# This dictionary will keep all the key-values pairs that are needed to be inserted in a redis hash.
# Those pairs are the one that we will take from a row containing data that comes from the relational info file.
redis_hash_values = {}

# This variable indicates the number of commands that are needed to be added in the pipeline, before attempting to
# execute those commands in redis.
BULK_INSERT_SIZE = 100


def add_key_value(key, value):
    """ This function is used to simply add a key-value pair to the data dictionary, in order to insert the values
    later into redis. """
    global redis_hash_values

    redis_hash_values[key] = value.strip()


# The main function of the program.
if __name__ == '__main__':

    # Repeat until user does not want to continue with another relation.
    while True:

        # Request the file name from stdin.
        file_name = input('\nPlease enter the path of the relational info file (Absolute or Relative): ')

        try:
            # Open the file given above, in read mode.
            with open(file_name, 'r') as in_file:

                # The name of the relational table, as it is given inside the file.
                table_name = ''

                # The table's columns.
                table_attributes = []

                # This boolean attribute indicates whether we have read the first line,
                # which is the table's name, or not.
                table_name_found = False

                # This boolean attribute indicates whether we are before the ';' separator, or not.
                separator_found = False

                # The total number of hashes that we attempted to insert into Redis.
                total_hashes = 0

                # The total number of hashes that were successfully inserted into Redis.
                succeeded_hashes = 0

                # Loop for every line in the file...
                for line in in_file:

                    # If we have a blank line, do nothing and continue with the next iteration.
                    if line.strip() == '':
                        continue

                    # Read until reaching the ';' character, which is an indicator of table's data.
                    if not separator_found:
                        if not table_name_found:
                            table_name = line.strip().lower().replace(' ', '_')
                            table_name_found = True
                        else:
                            if line.strip() == ';':
                                separator_found = True
                            else:
                                table_attributes.append(line.strip().lower().replace(' ', '_'))
                    else:
                        values = line.strip().split(';')

                        if not len(table_attributes) == len(values):
                            print('\nNumber of values does not match the number of attributes!')
                        else:
                            # Create a redis hash using the relational table's name, followed by ':' and the record's id
                            redis_hash_name = table_name + ':' + values[0].strip().lower().replace(' ', '_')

                            # Initialize the key-value dictionary, in case it contains records from a previous iteration
                            redis_hash_values = {}

                            list(map(add_key_value, table_attributes, values))

                            # Add the current command to the pipeline.
                            redis_pipeline.hmset(redis_hash_name, redis_hash_values)

                            # If we have a suitable number of commands in the pipeline, execute them.
                            if len(redis_pipeline) >= BULK_INSERT_SIZE:
                                results = redis_pipeline.execute()

                                total_hashes += BULK_INSERT_SIZE
                                succeeded_hashes = len([x for x in results if x])

                # Execute all the pipelined commands again, in case we have any leftovers.
                total_hashes += len(redis_pipeline)
                results = redis_pipeline.execute()
                succeeded_hashes = len([x for x in results if x])

        except FileNotFoundError:
            print('\nException occurred, File not found.')
        except Exception as e:
            print('\nException occurred.' + str(e))
        finally:
            print('\nSuccessfully inserted {0} / {1} total hashes into Redis'.format(succeeded_hashes, total_hashes))

            choice = input('\nWould you like to import another file? (yes/no): ').strip().lower()

            while (not choice == 'yes') and (not choice == 'no'):
                choice = input('\nInvalid input.\nWould you like to import another file? (yes/no): ').strip().lower()

            if choice == 'no':
                break

    print('\nGoodbye!')
