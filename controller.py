import psycopg2
from psycopg2 import Error
import model
import view
import datetime
import time


class Controller:
    def __init__(self):
        self.v = view.View()
        self.m = model.Model()

    def print(self, table_name):
        t_name = self.v.valid.check_table_name(table_name).upper()
        if t_name:
            if t_name == 'CATEGORY':
                self.v.print_category(self.m.print_category())
            elif t_name == 'CATEGORY_PILL':
                self.v.print_category_pill(self.m.print_category_pill())
            elif t_name == 'PILL':
                self.v.print_pill(self.m.print_pill())
            elif t_name == 'MANUFACTURER':
                self.v.print_manufacturer(self.m.print_manufacturer())

    def delete(self, table_name, key_name, value):
        t_name = self.v.valid.check_table_name(table_name)
        k_name = self.v.valid.check_pk_name(table_name, key_name)
        if t_name and k_name:
            count = self.m.find(t_name, k_name, value)
            k_val = self.v.valid.check_pk(value, count)
            if k_val:
                t_name = t_name
                if t_name == 'Category' or t_name == 'Pill':
                    if t_name == 'Category':
                        count_c_p = self.m.find('Category_pill', 'category_id', value)[0]
                    if t_name == 'Pill':
                        count_c_p = self.m.find('Category_pill', 'pill_id', value)[0]
                    if count_c_p:
                        self.v.cannot_delete()
                    else:
                        try:
                            self.m.delete_data(table_name, key_name, k_val)
                        except (Exception, Error) as _ex:
                            self.v.sql_error(_ex)
                elif t_name == 'Manufacturer':
                    count_p = self.m.find('Pill', 'manufacturer_id', value)[0]
                    if count_p:
                        self.v.cannot_delete()
                    else:
                        try:
                            self.m.delete_data(table_name, key_name, k_val)
                        except (Exception, Error) as _ex:
                            self.v.sql_error(_ex)
                else:
                    try:
                        self.m.delete_data(table_name, key_name, k_val)
                    except (Exception, Error) as _ex:
                        self.v.sql_error(_ex)
            else:
                self.v.deletion_error()

    def update_category_pill(self, key: str, pill_id: int, category_id: int):
        if self.v.valid.check_possible_keys('Category_pill', 'id', key):
            count_c_p = self.m.find('Category_pill', 'id', int(key))
            c_p_val = self.v.valid.check_pk(key, count_c_p)
        if self.v.valid.check_possible_keys('Pill', 'id', pill_id):
            count_p = self.m.find('Pill', 'id', int(pill_id))
            p_val = self.v.valid.check_pk(pill_id, count_p)
        if self.v.valid.check_possible_keys('Category', 'id', category_id):
            count_c = self.m.find('Category', 'id', int(category_id))
            c_val = self.v.valid.check_pk(category_id, count_c)

        try:
            self.m.update_data_category_pill(key, p_val, c_val)
        except (Exception, Error) as _ex:
            self.v.sql_error(_ex)

    def update_category(self, key: str, name: str, description: str):
        if self.v.valid.check_possible_keys('Category', 'id', key):
            count_c = self.m.find('Category', 'id', int(key))
            c_val = self.v.valid.check_pk(key, count_c)

        try:
            self.m.update_data_category(c_val, name, description)
        except (Exception, Error) as _ex:
            self.v.sql_error(_ex)

    def update_pill(self, key: str, manufacturer_id: int, name: str, price: int):
        if self.v.valid.check_possible_keys('Pill', 'id', key):
            count_p = self.m.find('Pill', 'id', int(key))
            p_val = self.v.valid.check_pk(key, count_p)
        if self.v.valid.check_possible_keys('Manufacturer', 'id', manufacturer_id):
            count_m = self.m.find('Manufacturer', 'id', int(manufacturer_id))
            m_val = self.v.valid.check_pk(manufacturer_id, count_m)

        try:
            self.m.update_data_pill(p_val, m_val, name, price)
        except (Exception, Error) as _ex:
            self.v.sql_error(_ex)

    def update_manufacturer(self, key: str, country: str, name: str, email: str):
        if self.v.valid.check_possible_keys('Manufacturer', 'id', key):
            count_m = self.m.find('Manufacturer', 'id', int(key))
            m_val = self.v.valid.check_pk(key, count_m)

        try:
            self.m.update_data_manufacturer(m_val, country, name, email)
        except (Exception, Error) as _ex:
            self.v.sql_error(_ex)

    def insert_pill(self, key: str, manufacturer_id: int, name: str, price: int):
        if self.v.valid.check_possible_keys('Pill', 'id', key):
            count_p = self.m.find('Pill', 'id', int(key))
        if self.v.valid.check_possible_keys('Manufacturer', 'id', manufacturer_id):
            count_m = self.m.find('Manufacturer', 'id', int(manufacturer_id))
            m_val = self.v.valid.check_pk(manufacturer_id, count_m)

        try:
            self.m.insert_data_pill(int(key), m_val, name, price)
        except (Exception, Error) as _ex:
            self.v.sql_error(_ex)

    def insert_category_pill(self, key: str, pill_id: int, category_id: int):
        if self.v.valid.check_possible_keys('Category_pill', 'id', key):
            count_c_p = self.m.find('Category_pill', 'id', int(key))
        if self.v.valid.check_possible_keys('Pill', 'id', pill_id):
            count_p = self.m.find('Pill', 'id', int(pill_id))
            p_val = self.v.valid.check_pk(pill_id, count_p)
        if self.v.valid.check_possible_keys('Category', 'id', category_id):
            count_c = self.m.find('Category', 'id', int(category_id))
            c_val = self.v.valid.check_pk(category_id, count_c)

        try:
            self.m.insert_data_category_pill(int(key), p_val, c_val)
        except (Exception, Error) as _ex:
            self.v.sql_error(_ex)

    def insert_category(self, key: str, name: str, description: str):
        if self.v.valid.check_possible_keys('Category', 'id', key):
            count_c = self.m.find('Category', 'id', int(key))
        try:
            self.m.insert_data_category(int(key), name, description)
        except (Exception, Error) as _ex:
            self.v.sql_error(_ex)

    def insert_manufacturer(self, key: str, name: str, country: str, email: str):
        if self.v.valid.check_possible_keys('Manufacturer', 'id', key):
            count_m = self.m.find('Manufacturer', 'id', int(key))

        try:
            self.m.insert_data_manufacturer(int(key), name, country, email)
        except (Exception, Error) as _ex:
            self.v.sql_error(_ex)

    def generate(self, table_name: str, n: int):
        t_name = self.v.valid.check_table_name(table_name).upper()
        if t_name:
            if t_name == 'CATEGORY':
                self.m.category_data_generator(n)
            elif t_name == 'CATEGORY_PILL':
                self.m.category_pill_data_generator(n)
            elif t_name == 'PILL':
                self.m.pill_data_generator(n)
            elif t_name == 'MANUFACTURER':
                self.m.manufacturer_data_generator(n)

    def search_two(self, table1_name: str, table2_name: str, table1_key: str, table2_key: str, search: str):
        t1_n = self.v.valid.check_table_name(table1_name)
        t2_n = self.v.valid.check_table_name(table2_name)
        if t1_n and self.v.valid.check_key_names(t1_n, table1_key) and t2_n \
                and self.v.valid.check_key_names(t2_n, table2_key):
            start_time = time.time()
            result = self.m.search_data_two_tables(table1_name, table2_name, table1_key, table2_key,
                                                   search)
            self.v.print_time(start_time)

            self.v.print_search(result)

    def search_three(self, table1_name: str, table2_name: str, table3_name: str,
                     table1_key: str, table2_key: str, table3_key: str, table13_key: str,
                     search: str):
        t1_n = self.v.valid.check_table_name(table1_name)
        t2_n = self.v.valid.check_table_name(table2_name)
        t3_n = self.v.valid.check_table_name(table3_name)
        if t1_n and self.v.valid.check_key_names(t1_n, table1_key) and self.v.valid.check_key_names(t1_n, table13_key) \
                and t2_n and self.v.valid.check_key_names(t2_n, table2_key) \
                and t3_n and self.v.valid.check_key_names(t3_n, table3_key) \
                and self.v.valid.check_key_names(t3_n, table13_key):
            start_time = time.time()
            result = self.m.search_data_three_tables(table1_name, table2_name, table3_name,
                                                     table1_key, table2_key, table3_key, table13_key,
                                                     search)
            self.v.print_time(start_time)
            self.v.print_search(result)

    def search_four(self, table1_name: str, table2_name: str, table3_name: str, table4_name: str,
                    table1_key: str, table2_key: str, table3_key: str, table13_key: str,
                    table4_key: str, table24_key: str,
                    search: str):
        t1_n = self.v.valid.check_table_name(table1_name)
        t2_n = self.v.valid.check_table_name(table2_name)
        t3_n = self.v.valid.check_table_name(table3_name)
        t4_n = self.v.valid.check_table_name(table2_name)
        if t1_n and self.v.valid.check_key_names(t1_n, table1_key) and self.v.valid.check_key_names(t1_n, table13_key) \
                and t2_n and self.v.valid.check_key_names(t2_n, table2_key) \
                and self.v.valid.check_key_names(t2_n, table24_key) \
                and t3_n and self.v.valid.check_key_names(t3_n, table3_key) \
                and self.v.valid.check_key_names(t3_n, table13_key) \
                and t4_n and self.v.valid.check_key_names(t4_n, table4_key) \
                and self.v.valid.check_key_names(t4_n, table24_key):

            start_time = time.time()
            result = self.m.search_data_all_tables(table1_name, table2_name, table3_name, table4_name,
                                                   table1_key, table2_key, table3_key, table13_key,
                                                   table4_key, table24_key,
                                                   search)
            self.v.print_time(start_time)
            self.v.print_search(result)
