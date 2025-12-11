import os
import re
import pandas as pd

directory = '/Users/yangxk/data/getl/20251205_215532/summary'
# match strings like: transFromRM-UG, transFromRDF-MG, transFromLPG-SG
pattern = re.compile(r'^transFrom(?:RM|RDF|LPG)-(?:UG|MG|SG)$')

max_memory = None
max_file = None
max_row_idx = None
max_line_number = None

empty_files = []
skipped_no_column = []
skipped_no_matches = []

for filename in sorted(os.listdir(directory)):
    if not filename.endswith('.csv'):
        continue
    filepath = os.path.join(directory, filename)
    try:
        df = pd.read_csv(filepath)
    except pd.errors.EmptyDataError:
        # cache this empty-file error
        empty_files.append(filename)
        continue
    except Exception as e:
        # other parse errors: record and continue
        skipped_no_column.append((filename, str(e)))
        continue

    if df.empty:
        empty_files.append(filename)
        continue

    # get first column values as strings
    first_col_vals = df.iloc[:, 0].astype(str).str.strip()

    # build mask: exact 'Load Data' or matches transFromRM/RDF/LPG-UG/MG/SG
    mask = (first_col_vals == 'Load Data') | first_col_vals.str.match(pattern)

    if not mask.any():
        skipped_no_matches.append(filename)
        continue

    if 'Max Memory (B)' not in df.columns:
        skipped_no_column.append((filename, 'Missing column: Max Memory (B)'))
        continue

    # coerce to numeric, ignore non-numeric
    mem_series = pd.to_numeric(df.loc[mask, 'Max Memory (B)'], errors='coerce')
    mem_series = mem_series.dropna()
    if mem_series.empty:
        skipped_no_matches.append(filename)
        continue

    local_max = mem_series.max()
    if max_memory is None or local_max > max_memory:
        max_memory = local_max
        # idx is the index label in the dataframe for the matching max row
        idx = mem_series.idxmax()
        max_row_idx = idx
        # compute CSV file line number: header + data rows -> header is line 1, data row idx 0 -> file line = idx + 2
        try:
            max_line_number = int(idx) + 2
        except Exception:
            # if index is non-integer (unlikely), set to None and also keep the index label
            max_line_number = None
        max_file = filename

# final output
if max_memory is None:
    print('未找到符合条件的 Max Memory 值。')
else:
    print(f'最大值: {max_memory}')
    print(f'文件: {max_file}')
    print(f'行号(文件内，1-based，包含header): {max_line_number} (pandas index: {max_row_idx})')

# report cached/skipped info
if empty_files:
    print('\n空文件 (已跳过):')
    for f in empty_files:
        print('  -', f)

if skipped_no_column:
    print('\n被跳过的文件 (缺少列或解析错误):')
    for item in skipped_no_column:
        print('  -', item[0], '-', item[1])

if skipped_no_matches:
    print('\n包含数据但无匹配行或无数值 Max Memory 的文件:')
    for f in skipped_no_matches:
        print('  -', f)
