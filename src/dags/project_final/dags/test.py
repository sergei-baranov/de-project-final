import os

curr_dir_path = os.path.dirname(__file__)
print(curr_dir_path)
sql_path = os.path.abspath(os.path.join(curr_dir_path, '..', '..', '..', 'sql'))
print(sql_path)

