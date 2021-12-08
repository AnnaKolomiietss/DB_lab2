import datetime
import psycopg2 as ps


class Model:
    def __init__(self):
        self.conn = None
        try:
            self.conn = ps.connect(
                dbname="pharmacy",
                user='postgres',
                password="markovka",
                host='127.0.0.1',
                port="5432",
            )
        except(Exception, ps.DatabaseError) as error:
            print("[INFO] Error while working with Postgresql", error)

    def request(self, req: str):
        try:
            cursor = self.conn.cursor()
            print(req)
            cursor.execute(req)
            self.conn.commit()
            return True
        except(Exception, ps.DatabaseError, ps.ProgrammingError) as error:
            print(error)
            self.conn.rollback()
            return False

    def get(self, req: str):
        try:
            cursor = self.conn.cursor()
            print(req)
            cursor.execute(req)
            self.conn.commit()
            return cursor.fetchall()
        except(Exception, ps.DatabaseError, ps.ProgrammingError) as error:
            print(error)
            self.conn.rollback()
            return False

    def get_el(self, req: str):
        try:
            cursor = self.conn.cursor()
            print(req)
            cursor.execute(req)
            self.conn.commit()
            return cursor.fetchone()
        except(Exception, ps.DatabaseError, ps.ProgrammingError) as error:
            print(error)
            self.conn.rollback()
            return False

    def count(self, table_name: str):
        return self.get_el(f"select count(*) from public.\"{table_name}\"")

    def find(self, table_name: str, key_name: str, key_value: int):
        return self.get_el(f"select count(*) from public.\"{table_name}\" where {key_name}={key_value}")

    def max(self, table_name: str, key_name: str):
        return self.get_el(f"select max({key_name}) from public.\"{table_name}\"")

    def min(self, table_name: str, key_name: str):
        return self.get_el(f"select min({key_name}) from public.\"{table_name}\"")

    def print_category(self) -> None:
        return self.get(f"SELECT * FROM public.\"Category\"")

    def print_category_pill(self) -> None:
        return self.get(f"SELECT * FROM public.\"Category_pill\"")

    def print_pill(self) -> None:
        return self.get(f"SELECT * FROM public.\"Pill\"")

    def print_manufacturer(self) -> None:
        return self.get(f"SELECT * FROM public.\"Manufacturer\"")

    def delete_data(self, table_name: str, key_name: str, key_value) -> None:
        self.request(f"DELETE FROM public.\"{table_name}\" WHERE {key_name}={key_value};")

    def update_data_category(self, key_value: int, name: str, description: str) -> None:
        self.request(f"UPDATE public.\"Category\" SET name=\'{name}\', description=\'{description}\' "
                     f" WHERE id={key_value};")

    def update_data_category_pill(self, key_value: int, pill_id: int, category_id: int) -> None:
        self.request(f"UPDATE public.\"Category_pill\" SET pill_id=\'{pill_id}\', category_id=\'{category_id}\' "
                     f" WHERE id={key_value};")

    def update_data_pill(self, key_value: int, manufacturer_id: int, name: str, price: int) -> None:
        self.request(f"UPDATE public.\"Pill\" SET manufacturer_id=\'{manufacturer_id}\', name=\'{name}\', price=\'{price}\'"
                     f" WHERE id={key_value};")

    def update_data_manufacturer(self, key_value: int, name: str, country: str, email: str) -> None:
        self.request(f"UPDATE public.\"Manufacturer\" SET name=\'{name}\', country=\'{country}\', email=\'{email}\'"
                     f" WHERE id={key_value};")

    def insert_data_category(self, key_value: int, name: str, description: str) -> None:
        self.request(f"insert into public.\"Category\" (id, name, description) "
                     f"VALUES ({key_value}, \'{name}\', \'{description}\');")

    def insert_data_category_pill(self, key_value: int, pill_id: int, category_id: int) -> None:
        self.request(f"insert into public.\"Category_pill\" (id, pill_id, category_id) "
                     f"VALUES ({key_value}, \'{pill_id}\', \'{category_id}\');")

    def insert_data_pill(self, key_value: int, manufacturer_id: int, name: str, price: int) -> None:
        self.request(f"insert into public.\"Pill\" (id, manufacturer_id, name, price) "
                     f"VALUES ({key_value}, \'{manufacturer_id}\', \'{name}\', \'{price}\')")

    def insert_data_manufacturer(self, key_value: int, name: str, country: str, email: str) -> None:
        self.request(f"insert into public.\"Manufacturer\" (id, name, country, email) "
                     f"VALUES ({key_value}, \'{name}\', \'{country}\', \'{email}\')")

    def category_data_generator(self, times: int) -> None:
        for i in range(times):
            self.request("insert into public.\"Category\""
                         "select (SELECT MAX(id)+1 FROM public.\"Category\"), "
                         "array_to_string(ARRAY(SELECT chr((97 + round(random() * 25)) :: integer) \
                         FROM generate_series(1, FLOOR(RANDOM()*(10-4)+4):: integer)), ''), "
                         "array_to_string(ARRAY(SELECT chr((97 + round(random() * 25)) :: integer) \
                         FROM generate_series(1, FLOOR(RANDOM()*(10-4)+4):: integer)), ''); ")

    def category_pill_data_generator(self, times: int) -> None:
        for i in range(times):
            self.request("insert into public.\"Category_pill\" "
                         "select (SELECT (MAX(id)+1) FROM public.\"Category_pill\"), "
                         "(SELECT id FROM public.\"Pill\" LIMIT 1 OFFSET "
                         "(round(random() *((SELECT COUNT(id) FROM public.\"Pill\")-1)))), "
                         "(SELECT id FROM public.\"Category\" LIMIT 1 OFFSET "
                         "(round(random() *((SELECT COUNT(id) FROM public.\"Category\")-1))));")

    def pill_data_generator(self, times: int) -> None:
        for i in range(times):
            self.request("insert into public.\"Pill\" select (SELECT MAX(id)+1 FROM public.\"Pill\"), "
                         "(SELECT id FROM public.\"Manufacturer\" LIMIT 1 OFFSET "
                         "(round(random() *((SELECT COUNT(id) FROM public.\"Manufacturer\")-1)))), "
                         "array_to_string(ARRAY(SELECT chr((97 + round(random() * 25)) :: integer) "
                         "FROM generate_series(1, FLOOR(RANDOM()*(15-5)+5):: integer)), ''), "
                         "FLOOR(RANDOM()*(100000-1)+1);")

    def manufacturer_data_generator(self, times: int) -> None:
        for i in range(times):
            self.request("insert into public.\"Manufacturer\" select (SELECT MAX(id)+1 FROM public.\"Manufacturer\"), "
                         "array_to_string(ARRAY(SELECT chr((97 + round(random() * 25)) :: integer) "
                         "FROM generate_series(1, FLOOR(RANDOM()*(25-10)+10):: integer)), ''), "
                         "array_to_string(ARRAY(SELECT chr((97 + round(random() * 25)) :: integer) "
                         "FROM generate_series(1, FLOOR(RANDOM()*(10-4)+4):: integer)), ''), "
                         "array_to_string(ARRAY(SELECT chr((97 + round(random() * 25)) :: integer) "
                         "FROM generate_series(1, FLOOR(RANDOM()*(25-10)+10):: integer)), '');")

    def search_data_two_tables(self, table1_name: str, table2_name: str, table1_key, table2_key,
                               search: str):
        return self.get(f"select * from public.\"{table1_name}\" as one inner join public.\"{table2_name}\" as two "
                        f"on one.\"{table1_key}\"=two.\"{table2_key}\" "
                        f"where {search}")

    def search_data_three_tables(self, table1_name: str, table2_name: str, table3_name: str,
                                 table1_key, table2_key, table3_key, table13_key,
                                 search: str):
        return self.get(f"select * from public.\"{table1_name}\" as one inner join public.\"{table2_name}\" as two "
                        f"on one.\"{table1_key}\"=two.\"{table2_key}\" inner join public.\"{table3_name}\" as three "
                        f"on three.\"{table3_key}\"=one.\"{table13_key}\""
                        f"where {search}")

    def search_data_all_tables(self, table1_name: str, table2_name: str, table3_name: str, table4_name: str,
                               table1_key, table2_key, table3_key, table13_key,
                               table4_key, table24_key,
                               search: str):
        return self.get(f"select * from public.\"{table1_name}\" as one inner join public.\"{table2_name}\" as two "
                        f"on one.\"{table1_key}\"=two.\"{table2_key}\" inner join public.\"{table3_name}\" as three "
                        f"on three.\"{table3_key}\"=one.\"{table13_key}\" inner join public.\"{table4_name}\" as four "
                        f"on four.\"{table4_key}\"=two.\"{table24_key}\""
                        f"where {search}")
