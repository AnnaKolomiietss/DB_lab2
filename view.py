import datetime
import time
import validator


class View:
    def __init__(self):
        self.valid = validator.Validator()

    def cannot_delete(self) -> None:
        print('this record is connected with another table, deleting will '
              'throw error')

    def sql_error(self, e) -> None:
        print("[INFO] Error while working with Postgresql", e)

    def insertion_error(self) -> None:
        print('Something went wrong (record with such id exists or inappropriate foreign key values)')

    def updation_error(self) -> None:
        print('Something went wrong (record with such id does not exist or inappropriate foreign key value)')

    def deletion_error(self) -> None:
        print('record with such id does not exist')

    def invalid_interval(self) -> None:
        print('invalid interval input')

    def print_time(self, start) -> None:
        print("--- %s seconds ---" % (time.time() - start))

    def print_search(self, result):
        print('search result:')
        for row in result:
            for i in range(0, len(row)):
                print(row[i])
            print('_____________________________________')

    def print_category(self, table):
        print('Category table:')
        for row in table:
            print('id:', row[0], '\tname:', row[1], '\tdescription:', row[2])
            print('_____________________________________')

    def print_category_pill(self, table):
        print('Category_Pill table:')
        for row in table:
            print('id:', row[0], '\tpill_id:', row[1], '\tcategory_id:', row[2])
            print('_____________________________________')

    def print_pill(self, table):
        print('Pill table:')
        for row in table:
            print('id:', row[0], '\tmanufacturer_id:', row[1], '\tname:', row[2], '\tprice:', row[3])
            print('___________________________________________________________________')

    def print_manufacturer(self, table):
        print('Manufacturer table:')
        for row in table:
            print('id:', row[0], '\tname:', row[1], '\tcountry:', row[2],  '\temail', row[3])
            print('_________________________________________')

    def print_menu(self):
        print('print_table - table output \n\targument (table_name) is required')
        print('delete_record - deletes the record from table \n'
              '\targuments (table_name, key_name, key_value) are required')
        print('update_record - updates record with specified id in table\n'
              '\tCategory args (table_name, id, name, description)\n'
              '\tCategory_pill args (table_name, id, pill_id, category_id)\n'
              '\tPill args (table_name, id, manufacturer_id, name, price)\n'
              '\tManufacturer args (table_name, id, country, name, email)')
        print('insert_record - inserts record into table \n'
              '\tCategory args (table_name, id, name, name, description)\n'
              '\tCategory_pill args (table_name, id, pill_id, category_id)\n'
              '\tPill args (table_name, id, manufacturer_id, name, price)\n'
              '\tManufacturer args (table_name, id, country, name, email)')
        print('generate_randomly - generates n random records in table\n'
              '\targuments (table_name, n) are required')
        print('search_records - search for records in two or more tables using one or more keys \n'
              '\targuments (table1_name, table2_name, table1_key, table2_key) are required, \n'
              '\tif you want to perform search in more tables: \n'
              '\t(table1_name, table2_name, table3_name, table1_key, table2_key, table3_key, table13_key) \n'
              '\t(table1_name, table2_name, table3_name, table4_name, table1_key, table2_key, table3_key, table13_key, '
              'table4_key, table24_key)')

    def proceed_search(self, search_num):
        search = ''
        for i in range(0, search_num):
            while True:
                search_type = input('specify the type of data you want to search for '
                                    '(numeric or string): ')
                if search_type == 'numeric' or search_type == 'string':
                    break
            key = input('specify the name of key by which you`d like to perform search '
                        'in form: table_number.key_name: ')

            if search_type == 'numeric':
                a = input('specify the left end of search interval: ')
                b = input('specify the right end of search interval: ')
                if search == '':
                    search = self.numeric_search(a, b, key)
                else:
                    search += ' and ' + self.numeric_search(a, b, key)

            elif search_type == 'string':
                string = input('specify the string you`d like to search for: ')
                if search == '':
                    search = self.string_search(string, key)
                else:
                    search += ' and ' + self.string_search(string, key)
        return search

    def numeric_search(self, a: str, b: str, key: str):
        try:
            a, b = int(a), int(b)
        except ValueError:
            self.invalid_interval()
        else:
            return f"{a}<{key} and {key}<{b}"

    def string_search(self, string: str, key: str):
        return f"{key} LIKE \'{string}\'"

    def get_search_num(self):
        return input('specify the number of attributes you`d like to search by: ')

    def invalid_search_num(self):
        print('should be number different from 0')

    def argument_error(self):
        print('no required arguments specified')

    def wrong_table(self):
        print('wrong table name')

    def no_command(self):
        print('no command name specified, type help to see possible commands')

    def wrong_command(self):
        print('unknown command name, type help to see possible commands')
