import redis

# Connect to local redis, on port 6379 using database 0.
_redis = redis.StrictRedis(host='192.168.229.129', port=6379, db=0)

# The 'main' function of the program.
if __name__ == '__main__':

    while True:
        # Request the file name from stdin.
        file_name = input('Please enter the path of the query file (Absolute or Relative): ')

        try:
            # Open the file given above, in read mode.
            with open(file_name, 'r') as in_file:
                lines = in_file.readlines()

                attributes = {}

                select_attributes = lines[0].split(',')

                for item in select_attributes:
                    values = item.split('.')
                    key = values[0].strip().lower()
                    value = values[1].strip().lower()

                    if key in attributes:
                        attributes[key].append(value)
                    else:
                        attributes[key] = [value]

                table_names = lines[1].split(',')
                for table in table_names:
                    current_table = table.strip().lower()
                    key_names = _redis.keys(current_table + ':*')

                    attrs = attributes[current_table]
                    print(*attrs, sep=' ')

                    for hash_key in key_names:
                        hash_values = _redis.hgetall(hash_key)

                        vals = []
                        for attr in attrs:
                            vals.append("'" + (hash_values[attr.encode('utf-8')]).decode('utf-8') + "'")
                        print(*vals, sep=' ')
        except FileNotFoundError:
            print('Exception occurred, File not found.')
        except Exception as e:
            print(str(e))
        finally:
            choice = input('Would you like to import another file? (yes/no): ').strip().lower()

            while (not choice == 'yes') and (not choice == 'no'):
                choice = input('Invalid input.\nWould you like to import another file? (yes/no): ').strip().lower()

            if choice == 'no':
                break

    print('Goodbye!')
