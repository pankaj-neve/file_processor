import pandas as pd
import logging
import traceback
import os

path = '/tmp/'

def read_table_file(file):
    df = pd.read_table(file)
    return df


def process_dataframe(df, file_path):
    df.drop_duplicates(
        subset=['id','first_name', 'last_name', 'email', 'job_title'],
        keep="first", inplace=True
    )
    df['gross_salary'] = df['basic_salary'] + df['allowances']
    average_salary = round(df['gross_salary'].mean(),2)
    second_high_salary = sorted(list(set(df['gross_salary'].to_list())))[-2]
    footer = \
        pd.DataFrame({
            'id': ['Second Highest Salary = {}'.format(second_high_salary)],
            'first_name': ['Average Salary  = {}'.format(average_salary)]
        })
    df = pd.concat([df, footer], ignore_index=True)
    df.to_csv(file_path, index=False)

    return file_path


def main(files, out_file_path):
    logging.info("Start Main process")
    arr = []
    try:
        for file in files:
            df = read_table_file(file)
            arr.append(df)
        new_df = pd.concat(arr)
        file_path = process_dataframe(new_df, out_file_path)
    except Exception as e:
        logging.error("Exception Occurred")
        logging.error(traceback.format_exc())
        logging.error("Error: {}".format(e))

if __name__ == '__main__':
    logging.basicConfig(level="DEBUG")
    dir_path = os.path.dirname(os.path.realpath(__file__))
    logging.info("dir_path : {}".format(dir_path))
    file1 = "{}/input_files/DATA1.dat".format(dir_path)
    file2 = "{}/input_files/DATA.dat".format(dir_path)
    out_file_path = "{}/result_files/final_result_file.csv".format(dir_path)
    logging.info("out_file_path : {}".format(out_file_path))
    logging.info("input file : {}".format(file1))
    logging.info("input file 2 : {}".format(file2))
    main([file1, file2], out_file_path)