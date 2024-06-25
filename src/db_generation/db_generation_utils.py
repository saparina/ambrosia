from collections import defaultdict, deque
import re
import sqlite3
import string
import inflect

p_engine = inflect.engine()

class CreateTableError(Exception):
    pass

class InsertValueError(Exception):
    pass

# FROM SPIDER CODE
def convert_fk_index(data):
    fk_holder = []
    for fk in data["foreign_keys"]:
        tn, col, ref_tn, ref_col = fk[0][0], fk[0][1], fk[1][0], fk[1][1]
        ref_cid, cid = None, None
        try:
            tid = data["table_names_original"].index(tn)
            ref_tid = data["table_names_original"].index(ref_tn)

            for i, (tab_id, col_org) in enumerate(data["column_names_original"]):
                if tab_id == ref_tid and ref_col == col_org:
                    ref_cid = i
                elif tid == tab_id and col == col_org:
                    cid = i
            if ref_cid and cid:
                fk_holder.append([cid, ref_cid])
        except Exception as e:
            print(e)
    return fk_holder

# FROM SPIDER CODE
def dump_db_json_schema(db, f):
    """read table and column info"""

    conn = sqlite3.connect(db)
    conn.execute("pragma foreign_keys=ON")
    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table';")

    data = {
        "db_id": f,
        "table_names_original": [],
        "table_names": [],
        "column_names_original": [(-1, "*")],
        "column_names": [(-1, "*")],
        "column_types": ["text"],
        "primary_keys": [],
        "foreign_keys": [],
    }

    fk_holder = []
    for i, item in enumerate(cursor.fetchall()):
        table_name = item[0]
        data["table_names_original"].append(table_name)
        data["table_names"].append(table_name.lower().replace("_", " "))
        fks = conn.execute(
            "PRAGMA foreign_key_list('{}') ".format(table_name)
        ).fetchall()
        fk_holder.extend([[(table_name, fk[3]), (fk[2], fk[4])] for fk in fks])
        cur = conn.execute("PRAGMA table_info('{}') ".format(table_name))
        for j, col in enumerate(cur.fetchall()):
            data["column_names_original"].append((i, col[1]))
            data["column_names"].append((i, col[1].lower().replace("_", " ")))
            # varchar, '' -> text, int, numeric -> integer,
            col_type = col[2].lower()
            # if ("date" in col[1].lower() or "year" in col[1].lower() or "time" in col[1].lower()) \
            #     and "id" not in col[1].lower():
            #     data["column_types"].append("time")
            if (
                "char" in col_type
                or col_type == ""
                or "text" in col_type
                or "var" in col_type
            ):
                data["column_types"].append("text")
            elif (
                "int" in col_type
                or "numeric" in col_type
                or "decimal" in col_type
                or "number" in col_type
                or "id" in col_type
                or "real" in col_type
                or "double" in col_type
                or "float" in col_type
            ):
                data["column_types"].append("number")
            elif "date" in col_type or "time" in col_type or "year" in col_type:
                data["column_types"].append("time")
            elif "boolean" in col_type:
                data["column_types"].append("boolean")
            elif "blob" in col_type:
                data["column_types"].append("blob")
            else:
                data["column_types"].append("others")

            if col[5] == 1:
                data["primary_keys"].append(len(data["column_names"]) - 1)

    data["foreign_keys"] = fk_holder
    data["foreign_keys"] = convert_fk_index(data)

    return data

def remove_unique_check_constraint(sql_statement):
    modified_statement = []
    p = re.compile(r'\bCHECK\s*\([^)]*\)\s*')
    for row in sql_statement.split('\n'):
        matches = p.search(row)
        if matches:
            start_idx = matches.span()[0]
            stack = []
            for token_idx, token in enumerate(row[start_idx:]):
                if token == '(':
                    stack.append(token)
                elif token == ')':
                    stack.pop()
                    if not stack:
                        break

            if start_idx + token_idx + 1 != len(row):
                row = row[:start_idx] + row[start_idx + token_idx + 1:]
            else:
                row = row[:start_idx]
        modified_statement.append(row)

    sql_statement = '\n'.join(modified_statement)

    modified_statement = []
    p = re.compile(r'\bUNIQUE\s*\([^)]*\)\s*')
    for row in sql_statement.split('\n'):
        matches = p.search(row)
        if matches:
            start_idx = matches.span()[0]
            stack = []
            for token_idx, token in enumerate(row[start_idx:]):
                if token == '(':
                    stack.append(token)
                elif token == ')':
                    stack.pop()
                    if not stack:
                        break

            if start_idx + token_idx + 1 != len(row):
                row = row[:start_idx] + row[start_idx + token_idx + 1:]
            else:
                row = row[:start_idx]
        modified_statement.append(row)

    modified_statement = '\n'.join(modified_statement)
    return modified_statement

def parse_statements(result, generation_statement):
    result = result.split('\n')
    idx_start = None
    statements = []
    for row_i, row in enumerate(result):
        if not row:
            continue

        if generation_statement in row:
            result[row_i] = row[row.find(generation_statement):]
            idx_start = row_i

        if idx_start is not None and row[-1] == ";":
            statements.append('\n'.join(result[idx_start:row_i + 1]))
            idx_start = None

    statements = list(dict.fromkeys(statements))
    if statements:
        new_statements = []
        for stat in statements:
            if generation_statement == 'CREATE TABLE':
                new_statements.append(
                    remove_unique_check_constraint(
                        stat.replace(" ENUM(", " CHECK(").replace(" NOT NULL", "").replace(" UNIQUE,", ",").replace(" INT PRIMARY KEY ", " INTEGER PRIMARY KEY ").replace(" SERIAL ", " INTEGER ").replace("\\", "'")
                        )
                )
            else:
                new_statements.append(stat.replace("\\", "'"))
        statements = new_statements
    return statements

def execute_statements(db_file, statements):
    conn = None
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    for statement in statements:
        c.execute(statement)
    conn.commit()
    conn.close()

def format_db_names(input_string):
    # words = re.split('[\W_]+', input_string)
    camel_case_string = '_'.join(word.capitalize().replace('-', '_') for word in input_string.split(' '))
    return camel_case_string

def format_value(x, is_like=False): 
    if is_like:
        assert isinstance(x, str), x
        return f'"%{x}%"'
    else:
        return f'"{x}"' if isinstance(x, str) else str(x)

def remove_punctuation(input_string):
        translator = str.maketrans("", "", string.punctuation)
        return input_string.translate(translator)

def format_for_equal_compare(name):
    name = name.replace('_', ' ').replace('-', ' ').lower()
    name = remove_punctuation(name.replace(' ', ''))
    return name

def compare_equal_db_names(name1, name2, type=None):
    if name1 == name2:
        return True
    name1 = format_for_equal_compare(name1)
    name2 = format_for_equal_compare(name2)
    if name1 == name2:
        return True
    if name1.strip() and name2.strip():
        return p_engine.compare(name1, name2) is not False
    else:
        return False

def compare_substr_db_names(name1, name2, type='value'):
    if not name1 or not name2:
        return False
    if type == 'value':
        return name1.lower() in name2.lower() or name2.lower() in name1.lower()
    else:
        name1, name2 = format_for_equal_compare(name1), format_for_equal_compare(name2)
        if name1 in name2 or name2 in name1:
            return True
        if name1.strip():
            name1 = p_engine.plural(name1)
        else:
            return False
        if name2.strip():
            name2 = p_engine.plural(name2)
        else:
            return False
        return name1 in name2 or name2 in name1

def check_row(name, row, compare_func=compare_equal_db_names): 
    for idx, x in enumerate(row):
        if compare_func(name, x):
            return idx
    return -1

def get_substr(name1, name2):
    formated_name1, formated_name2 = name1.lower(), name2.lower()
    if formated_name1 in formated_name2:
        start_idx = formated_name2.find(formated_name1)
        assert start_idx >= 0, (formated_name1, formated_name2)
        return name2[start_idx:start_idx+len(formated_name1)]
    else:
        start_idx = formated_name1.find(formated_name2)
        assert start_idx >= 0, (formated_name1, formated_name2)
        return name1[start_idx:start_idx+len(formated_name2)]

def check_num_columns(json_schema):
    num_columns = {}
    for tab_idx, _ in json_schema['column_names_original'][1:]:
        tab_name = json_schema['table_names_original'][tab_idx]
        if tab_name not in num_columns:
            num_columns[tab_name] = 0
        num_columns[tab_name] += 1

    for tab_name, count in num_columns.items():
        if count < 5:
            raise CreateTableError(f"Wrong number of columns in {tab_name}: {count}")
        
def get_table_col_dict(json_schema):
    tab_col_dict = {}
    for col_idx, (tab_idx, col_name) in enumerate(json_schema['column_names_original']):
        if col_name == "*":
            continue
        tab_name = json_schema['table_names_original'][tab_idx]
        if tab_name == 'sqlite_sequence':
            continue
        if tab_name not in tab_col_dict:
            tab_col_dict[tab_name] = []
        tab_col_dict[tab_name].append((col_idx, col_name))
    return tab_col_dict

def find_connected_tables(json_schema, tab_name):
    """
    Finds the connected graph of tables including the specified table.

    :param json_schema: The database schema information.
    :param tab_name: The name of the table to start the search from.
    :return: A set containing the names of all tables connected to the specified table.
    """
    # Convert table names to indices for easier comparison
    table_name_to_index = {name: i for i, name in enumerate(json_schema['table_names_original'])}
    
    # Initialize the queue with the index of the starting table
    if tab_name not in table_name_to_index:
        print(f"Table {tab_name} not found in the database schema.")
        return set()
    start_index = table_name_to_index[tab_name]
    queue = [start_index]
    
    # Set to keep track of visited tables
    visited = set([start_index])
    
    # Perform BFS
    while queue:
        current_index = queue.pop(0)
        
        # Check all foreign keys for connections
        for cid, ref_cid in json_schema['foreign_keys']:
            # Convert column index back to table index
            current_tid, ref_tid = json_schema['column_names_original'][cid][0], json_schema['column_names_original'][ref_cid][0]
            
            # Check if the current table or the referenced table is the one we're looking at
            if current_tid == current_index and ref_tid not in visited:
                queue.append(ref_tid)
                visited.add(ref_tid)
            elif ref_tid == current_index and current_tid not in visited:
                queue.append(current_tid)
                visited.add(current_tid)
    
    # Convert indices back to table names for the result
    connected_tables = [json_schema['table_names_original'][i] for i in visited]
    
    return connected_tables

def find_path(graph, start, goal):
    """Find shortest path between start and goal nodes in graph."""
    if start == goal:
        return [start]
    visited = {start}
    queue = deque([(start, [start])])
    while queue:
        current, path = queue.popleft()
        for neighbor in graph[current]:
            if neighbor == goal:
                return path + [goal]
            if neighbor not in visited:
                visited.add(neighbor)
                queue.append((neighbor, path + [neighbor]))
    return []

def get_join_columns_with_intermediates(json_schema, start_table, end_table):
    """Find join columns including through intermediate tables."""
    # Build graph from foreign key relationships
    graph = defaultdict(list)
    for fk in json_schema['foreign_keys']:
        src_idx, dest_idx = fk
        src_table_idx, _ = json_schema['column_names_original'][src_idx]
        dest_table_idx, _ = json_schema['column_names_original'][dest_idx]
        graph[src_table_idx].append(dest_table_idx)
        graph[dest_table_idx].append(src_table_idx)  # Assuming bidirectional for path finding

    # Convert table names to indices
    table_indices = {name: idx for idx, name in enumerate(json_schema['table_names_original'])}
    start_idx = table_indices[start_table]
    end_idx = table_indices[end_table]

    # Find path between tables
    path = find_path(graph, start_idx, end_idx)
    if not path:
        return []

    # Resolve join columns for each step in the path
    join_columns = []
    for i in range(len(path)-1):
        step_columns = []
        for fk in json_schema['foreign_keys']:
            src_idx, dest_idx = fk
            src_table_idx, _ = json_schema['column_names_original'][src_idx]
            dest_table_idx, _ = json_schema['column_names_original'][dest_idx]
            if src_table_idx == path[i] and dest_table_idx == path[i+1]:
                src_col_name = json_schema['column_names_original'][src_idx][1]
                dest_col_name = json_schema['column_names_original'][dest_idx][1]
                step_columns.append(((json_schema['table_names_original'][src_table_idx], src_col_name),
                                     (json_schema['table_names_original'][dest_table_idx], dest_col_name)))
                break  # Assuming one FK relationship per table pair for simplicity
            elif src_table_idx == path[i+1] and dest_table_idx == path[i]:
                src_table_idx, dest_table_idx = dest_table_idx, src_table_idx
                src_idx, dest_idx = dest_idx, src_idx
                src_col_name = json_schema['column_names_original'][src_idx][1]
                dest_col_name = json_schema['column_names_original'][dest_idx][1]
                step_columns.append(((json_schema['table_names_original'][src_table_idx], src_col_name),
                                     (json_schema['table_names_original'][dest_table_idx], dest_col_name)))
                break  # Assuming one FK relationship per table pair for simplicity
        join_columns.extend(step_columns)

    return join_columns

def format_join_columns(join_columns):
    str_json = ""
    for (tab_name, col_name), (ref_tab_name, ref_col_name) in join_columns:
        if not str_json:
            str_json =  f"{tab_name} JOIN {ref_tab_name} ON {tab_name}.{col_name} = {ref_tab_name}.{ref_col_name}"
        else:
            str_json += f" JOIN {ref_tab_name} ON {tab_name}.{col_name} = {ref_tab_name}.{ref_col_name}"
    return str_json