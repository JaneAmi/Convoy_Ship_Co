import json
import pandas as pd
import re
import csv
import sqlite3 as sq


def check_f(us_file):
    if re.match('.+s3db$', us_file):
        return us_file
    elif re.match('.+\\[CHECKED]\\.csv$', us_file):
        us_file = check_f(sqlite_df(us_file))
        scoring(us_file)
    elif re.match('.+.csv$', us_file):
        us_file = check_f(dt_correction(us_file))
    elif re.match('.+xlsx$', us_file):
        us_file = check_f(ex_to_csv(us_file))
    return us_file


def ex_to_csv(data_f_xlsx):
    my_df = pd.read_excel(data_f_xlsx, sheet_name='Vehicles', dtype=str)
    data_f_csv, rows_d = f'{data_f_xlsx.removesuffix("xlsx")}csv', my_df.shape[0]
    my_df.to_csv(data_f_csv, index=False)
    print('1 line was' if rows_d == 1 else f'{rows_d} lines were', f'imported to {data_f_csv}')
    return data_f_csv


def dt_correction(data_f_csv):  # take headers and correct data to int
    with open(data_f_csv, newline='') as data:
        file_reader = csv.reader(data, delimiter=',')
        titles, nc, new_data = [], 0, []  # titles - list for headers, nc for number of corrections, new data - nested list for data
        for line in file_reader:
            if not titles:
                titles = line
            else:
                tmp_list = []  # clean list for each circle
                for i in line:
                    new_i = re.sub('\\D', '', i)
                    nc += 1 if new_i != i else 0
                    tmp_list.append(new_i)  # list for data in one row
                new_data.append(tmp_list)

    new_fn = f'{data_f_csv.removesuffix(".csv")}[CHECKED].csv'
    new_df = pd.DataFrame(new_data, columns=titles)
    new_df.to_csv(new_fn, index=False)
    print(f'{nc} cells were' if nc != 1 else '1 cell was', f'corrected in {new_fn}')
    return new_fn


def sqlite_df(checked_df):
    new_fn = f'{checked_df.removesuffix("[CHECKED].csv")}.s3db'
    conn = sq.connect(new_fn)
    cursor_name = conn.cursor()
    cursor_name.execute('''CREATE TABLE convoy(
    vehicle_id INT PRIMARY KEY,
    engine_capacity INT NOT NULL,
    fuel_consumption INT NOT NULL,
    maximum_load INT NOT NULL);''')
    conn.commit()
    with open(checked_df, newline='') as data:
        file_reader = csv.reader(data, delimiter=',')
        n, nc, new_data = 0, 0, []  # titles - list for headers, nc for number of corrections, new data - nested list for data
        for line in file_reader:
            if n == 0:
                n += 1
            else:
                tmp_list = []  # clean list for each circle
                for i in line:
                    tmp_list.append(int(i))  # list for data in one row
                tmp_list = tuple(tmp_list)
                cursor_name.execute(f'''INSERT INTO convoy(vehicle_id, engine_capacity,fuel_consumption, maximum_load)
                VALUES {tmp_list}''')
                conn.commit()
                n += 1
    conn.close()
    print(f'{n - 1} records were' if n - 1 != 1 else '1 record was', f'inserted into {new_fn}')
    return new_fn


def scoring(sq3_df):
    conn = sq.connect(sq3_df)
    cursor_name = conn.cursor()
    cursor_name.execute('ALTER TABLE convoy ADD COLUMN score INT NOT NULL DEFAULT 0')
    cursor_name.execute('UPDATE convoy SET score = 2 WHERE maximum_load >= 20')
    cursor_name.execute('UPDATE convoy SET score = score+1 WHERE fuel_consumption*4.5>230')
    cursor_name.execute('UPDATE convoy SET score = score+2 WHERE fuel_consumption*4.5<=230')
    cursor_name.execute('UPDATE convoy SET score = score+2 WHERE fuel_consumption*4.5/engine_capacity<1')
    cursor_name.execute('UPDATE convoy SET score = score+1 WHERE FLOOR(fuel_consumption*4.5/engine_capacity)=1')
    conn.commit()
    conn.close()


def json_df(sq3_df):
    new_fnj = f'{sq3_df.removesuffix("s3db")}json'
    new_fnx = f'{sq3_df.removesuffix("s3db")}xml'
    titles = ['vehicle_id', 'engine_capacity', 'fuel_consumption', 'maximum_load']
    conn = sq.connect(sq3_df)
    cursor_name = conn.cursor()
    # get data for json file
    result_json = cursor_name.execute(f'SELECT {", ".join(titles)} FROM convoy WHERE score>3')
    all_rows_json = result_json.fetchall()
    conn.commit()
    # get data for xml file
    result_xml = cursor_name.execute(f'SELECT {", ".join(titles)} FROM convoy WHERE score<=3')
    all_rows_xml = result_xml.fetchall()
    conn.commit()
    conn.close()
    # create json file
    new_l, nl = [], 0
    for row in all_rows_json:
        ni, ndt = 0, {}
        for i in row:
            ndtt = {titles[ni]: i}
            ndt.update(ndtt)
            ni += 1
        new_l.append(ndt)
        nl += 1
    new_dict = {'convoy': new_l}
    with open(new_fnj, "w") as json_file:
        json.dump(new_dict, json_file)
    print(f'{nl} vehicles were' if nl != 1 else '1 vehicle was', f'saved into {new_fnj}')
    # create xml file
    nl, str_xmlt = 0, '<convoy>'
    if all_rows_xml:
        for row in all_rows_xml:
            ni, sxt = 0, '<vehicle>'
            for i in row:
                sxtt = f'<{titles[ni]}>{i}</{titles[ni]}>'
                sxt = f'{sxt}{sxtt}'
                ni += 1
            sxttt = f'{sxt}</vehicle>'
            str_xmlt = f'{str_xmlt}{sxttt}'
            nl += 1
    str_xml = f'{str_xmlt}</convoy>'
    with open(new_fnx, 'w') as file:
        file.write(str_xml)
    print(f'{nl} vehicles were' if nl != 1 else '1 vehicle was', f'saved into {new_fnx}')


u_file = check_f(input('Input file name\n'))

json_df(u_file)
