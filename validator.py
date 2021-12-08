import datetime


class Validator:
    def __init__(self):
        self.error = ''
        self.er_flag = False

    def check_table_name(self, arg: str):
        if arg.upper() in ['CATEGORY', 'CATEGORY_PILL', 'PILL', 'MANUFACTURER']:
            return arg
        else:
            self.er_flag = True
            self.error = f'table {arg} does not exist in the database'
            print(self.error)
            return False

    def check_pkey_value(self, arg: str, min_val: int, max_val: int):
        try:
            value = int(arg)
        except ValueError:
            self.er_flag = True
            self.error = f'{arg} is not correct primary key value'
            print(self.error)
            return 0
        else:
            if min_val <= value <= max_val:
                return value
            else:
                self.er_flag = True
                self.error = f'{arg} is not existing primary key value'
                print(self.error)
                return 0

    def check_pk_name(self, table_name: str, key_name: str):
        table_name = table_name.upper()
        if table_name == 'CATEGORY' and key_name == 'id' \
                or table_name == 'CATEGORY_PILL' and key_name == 'id' \
                or table_name == 'PILL' and key_name == 'id' \
                or table_name == 'MANUFACTURER' and key_name == 'id':
            return key_name
        else:
            self.er_flag = True
            self.error = f'key {key_name} is not a primary key of table {table_name}'
            print(self.error)
            return False

    def check_pk(self, val, count):
        try:
            value = int(val)
        except ValueError:
            self.er_flag = True
            self.error = f'{val} is not correct primary key value'
            print(self.error)
            return 0
        else:
            if count and not count == (0,):
                return value
            else:
                return 0

    def check_key_names(self, table_name: str, key: str):
        if table_name == 'Category' and key in ['id', 'name', 'description']:
            return True
        elif table_name == 'Category_pill' and key in ['id', 'pill_id', 'category_id']:
            return True
        elif table_name == 'Pill' and key in ['id', 'manufacturer_id', 'name', 'price']:
            return True
        elif table_name == 'Manufacturer' and key in ['id', 'country', 'name', 'email']:
            return True
        else:
            self.er_flag = True
            self.error = f'{key} is not correct name for {table_name} table'
            print(self.error)
            return False

    def check_possible_keys(self, table_name: str, key: str, val):
        if table_name == 'Pill':

            if key in ['id', ' manufacturer_id']:
                try:
                    value = int(val)
                except ValueError:
                    self.er_flag = True
                    self.error = f'{val} is not correct key value'
                    print(self.error)
                    return False
                else:
                    return True
            elif key in ['name']:
                return True
            elif key == 'price':
                try:
                    value = float(val)
                except ValueError:
                    self.er_flag = True
                    self.error = f'{val} is not correct price value'
                    print(self.error)
                    return False
                else:
                    return True
            else:
                self.er_flag = True
                self.error = f'{key} is not correct name for Pill table'
                print(self.error)
                return False
        elif table_name == 'Manufacturer':
            if key == 'id':
                try:
                    value = int(val)
                except ValueError:
                    self.er_flag = True
                    self.error = f'{val} is not correct key value'
                    print(self.error)
                    return False
                else:
                    return True
            elif key in ['country', 'name', 'email']:
                return True
            else:
                self.er_flag = True
                self.error = f'{key} is not correct name for Manufacturer table'
                print(self.error)
                return False
        elif table_name == 'Category':
            if key in ['id']:
                try:
                    value = int(val)
                except ValueError:
                    self.er_flag = True
                    self.error = f'{val} is not correct key value'
                    print(self.error)
                    return False
                else:
                    return True
            elif key == ['name', 'description']:
                return True
            else:
                self.er_flag = True
                self.error = f'{key} is not correct name for Category table'
                print(self.error)
                return False
        elif table_name == 'Category_pill':
            if key == ['id', 'pill_id', 'category_id']:
                try:
                    value = int(val)
                except ValueError:
                    self.er_flag = True
                    self.error = f'{val} is not correct key value'
                    print(self.error)
                    return False
                else:
                    return True
            # else:
            #     self.er_flag = True
            #     # self.error = f'{key} is not correct name for Category_pill table'
            #     print(self.error)
            #     return False
