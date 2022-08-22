import pandas as pd
import sqlite3
import json
import csv
import re


def get_csv(file_name):
    dataset = pd.read_excel(file_name, sheet_name='Vehicles', dtype=str)
    file_name = file_name.rstrip('xlsx') + 'csv'
    dataset.to_csv(file_name, index=False, header=True)
    dataset = pd.read_csv(file_name)
    print(f'{dataset.shape[0]} {"lines were" if dataset.shape[0] > 1 else "line was"} added to {file_name}')
    return dataset, file_name


def clean_csv(file_name, dataset):
    file_name = f'{file_name.rsplit(".csv")[0]}[CHECKED].csv'
    with open(file_name, 'w', encoding='utf-8') as new_dataset:
        new_writer = csv.writer(new_dataset, delimiter=',', lineterminator="\n")
        new_writer.writerow(dataset.columns)
        counter = len([True for line in list(dataset.values) for val in line if re.sub(r'\D', '', val) != val])
        new_writer.writerows([[re.sub(r'\D', '', val) for val in line] for line in dataset.values])
    dataset = pd.read_csv(file_name, delimiter=',')
    print(f'{counter} {"cells were" if counter > 1 else "cell was"} corrected in {file_name}')
    return dataset, file_name


def write_db(file_name, dataset):
    file_name = re.sub(r'\[CHECKED].csv\Z', '.s3db', file_name)
    conn = sqlite3.connect(file_name)
    cursor = conn.cursor()
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS convoy(
        {dataset.columns[0]} INTEGER NOT NULL PRIMARY KEY,
        {dataset.columns[1]} INTEGER NOT NULL,
        {dataset.columns[2]} INTEGER NOT NULL,
        {dataset.columns[3]} INTEGER NOT NULL,
        score INTEGER NOT NULL
    )
    """)

    counter = 0
    for line in dataset.values:
        line = list(line).copy()
        cursor.execute("INSERT OR IGNORE INTO convoy VALUES ({}, {}, {}, {}, {})".format(
            *[int(i) for i in line], score(*[int(j) for j in line[1:]])))
        counter += 1
    conn.commit()
    conn.close()
    print(f'{counter} {"records were" if dataset.shape[0] > 1 else "record was"} inserted to {file_name}')
    return file_name


def write_json(file_name, dataset_json):
    file_name = re.sub(r'\.s3db\Z', '.json', file_name)
    dataset_dict = dataset_json.drop(columns=['score']).to_dict(orient='records')
    with open(file_name, 'w') as file:
        json.dump({'convoy': dataset_dict}, file, indent=4)
    print(f'{len(dataset_dict)} {"vehicles were" if len(dataset_dict) != 1 else "vehicle was"} saved into {file_name}')


def write_xml(file_name, dataset_xml):
    conn = sqlite3.connect(file_name)
    conn.close()
    dataset_xml = dataset_xml.drop(columns=['score'])
    file_name = re.sub(r'\.s3db\Z', '.xml', file_name)
    with open(file_name, 'w') as file:
        if dataset_xml.shape[0] != 0:
            file.writelines(
                dataset_xml.to_xml(index=False, xml_declaration=False, root_name='convoy', row_name='vehicle'))
        else:
            file.writelines(['<convoy>', '</convoy>'])
    print(f'{dataset_xml.shape[0]} {"vehicles were" if dataset_xml.shape[0] != 1 else "vehicle was"} saved into '
          f'{file_name}')


def score(capacity, consumption, load):
    pts = 0
    if (450 * consumption / 100 / capacity) < 1:
        pts += 2
    elif (450 * consumption / 100 / capacity) < 2:
        pts += 1
    if 4.5 * consumption <= 230:
        pts += 2
    else:
        pts += 1
    if load >= 20:
        pts += 2
    return pts


def split_db(file_name):
    conn = sqlite3.connect(file_name)
    dataset_json = pd.read_sql(con=conn, sql='SELECT * FROM convoy WHERE score > 3 ORDER BY score DESC')
    dataset_xml = pd.read_sql(con=conn, sql='SELECT * FROM convoy WHERE score <= 3 ORDER BY score DESC')
    return dataset_json, dataset_xml


def run():
    file_name = input('Input file_name name\n').strip()
    if file_name.endswith('xlsx'):
        dataset, file_name = get_csv(file_name)
    if file_name.endswith('.csv') and not file_name.endswith('[CHECKED].csv'):
        dataset = pd.read_csv(file_name, dtype=str)
        dataset, file_name = clean_csv(file_name, dataset)
    if file_name.endswith('[CHECKED].csv'):
        dataset = pd.read_csv(file_name, dtype=str, delimiter=',')
        file_name = write_db(file_name, dataset)
    dataset_json, dataset_xml = split_db(file_name)
    write_json(file_name, dataset_json)
    write_xml(file_name, dataset_xml)


if __name__ == '__main__':
    run()
