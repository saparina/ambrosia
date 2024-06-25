import sqlite3
import copy
import random

from db_generation_utils import *
from key_concepts import DBItem, AttachmentConceptsDB

def validate_attachment_1tab_val(logger, db_file, ambig_item, verbose=False):
    # validate genererated database for Attachment, 1tab_val configuration:
    # class1, class2 and common property should be values in the same table 

    try:
        json_schema = dump_db_json_schema(db_file, 'tmp')
    except Exception as e:
        raise CreateTableError(f'Something wrong with dump_db_json_schema')

    tab_col_dict = get_table_col_dict(json_schema)

    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    found_subclasses = False
    cached_results = {}

    # Try to find exact class1 and class2 values or substrings with class1 and class2 in one of the columns
    for type in ("1tab_val", "1tab_val_like"):
        comp_func = compare_equal_db_names if type == "1tab_val" else compare_substr_db_names

        for col_idx, (tab_idx, col_name) in enumerate(json_schema['column_names_original']):
            if col_name == "*" or json_schema['column_types'][col_idx] != 'text':
                continue
            
            tab_name = json_schema['table_names_original'][tab_idx]
            if tab_name == 'sqlite_sequence':
                continue

            if (tab_name, col_name) not in cached_results:
                c.execute(f"SELECT DISTINCT {col_name} FROM {tab_name}")
                result_original = c.fetchall()
                cached_results[(tab_name, col_name)] = result_original
            else:
                result_original = cached_results[(tab_name, col_name)]

            result = [str(row[0]) for row in result_original]
            idx_class1 = check_row(ambig_item.class1, result, comp_func)
            idx_class2 = check_row(ambig_item.class2, result, comp_func)
            if idx_class1 >= 0 and idx_class2 >= 0:
                found_subclasses = True
                val1, val2 = result_original[idx_class1][0], result_original[idx_class2][0]
                if type == "1tab_val_like":
                    val1 = get_substr(ambig_item.class1, str(val1))
                    val2 = get_substr(ambig_item.class2, str(val2))

                class1 = DBItem(tab_name=tab_name,
                                col_name=col_name,
                                value=val1)
                class2 = DBItem(tab_name=tab_name,
                                col_name=col_name,
                                value=val2)
                general_class = DBItem(tab_name=tab_name,
                                    col_name=col_name,)
                if verbose:
                    logger.info(f"Found subclasses: {class1}, {class2}, {general_class}")
                break

        if found_subclasses:
            break

    if not found_subclasses:
        conn.close()
        raise InsertValueError(f"Didn't find class1 {ambig_item.class1} and class2 {ambig_item.class2}")
    
    found_common_property = False
    for type in ("1tab_val", "1tab_val_like"):
        comp_func = compare_equal_db_names if type == "1tab_val" else compare_substr_db_names
        for _, col_name in tab_col_dict[general_class.tab_name]:
            if col_name == "*":
                continue
            if not found_common_property and comp_func(ambig_item.common_property, col_name, type='column'):
                found_common_property = True
                common_property = DBItem(tab_name=tab_name,
                                col_name=col_name)  
                
                break
        if found_common_property:
            break

    if not found_common_property:
        conn.close()
        raise CreateTableError(f"Didn't find common_property {ambig_item.common_property}")

    # Choose common property
    if type == "1tab_val":
        create_sql_query = lambda tab, output_columns, col, val: f"SELECT DISTINCT {output_columns} FROM {tab} WHERE {col} = {format_value(val)}"
    else:
        create_sql_query = lambda tab, output_columns, col, val: f"SELECT DISTINCT {output_columns} FROM {tab} WHERE {col} LIKE {format_value(val, is_like=True)}"
 
    # common property in the same table as entities
    sql_query1 = create_sql_query(general_class.tab_name,
                                common_property.col_name,
                                general_class.col_name,  
                                class1.value)
    
    sql_query2 = create_sql_query(general_class.tab_name,
                                common_property.col_name,
                                general_class.col_name,  
                                class2.value)

    c.execute(f"{sql_query1} INTERSECT {sql_query2}")

    logger.info(f"INTERSECT: {sql_query1} INTERSECT {sql_query2}")
    result_original_both = c.fetchall()
    result_original_both_filtered = result_original_both

    if len(result_original_both_filtered) > 0:
        result_original_both_filtered = [row for row in result_original_both if row[0]]
        if len(result_original_both_filtered) == 0:
            raise InsertValueError("Empty results of intersection after preprocessing")

    if len(result_original_both_filtered) > 0:
        logger.info("Found intersection")
        # if we have common rows - just randomly choose property (but not general_class)
        res = random.choice(result_original_both_filtered[0])
        common_property = DBItem(tab_name=common_property.tab_name,
                            col_name=common_property.col_name,
                            value=res)
        if res != ambig_item.common_value:
            logger.warning(f"Common value {res} instead of {ambig_item.common_value}")

         # check that we have at least 2 rows with at least one subclass
        check_sql_query1 = f"{sql_query1} AND {common_property.col_name} != {format_value(res)}"
        check_sql_query2 = f"{sql_query2} AND {common_property.col_name} != {format_value(res)}"
        
        # Check that we have at least one more row for each class
        c.execute(check_sql_query1)
        result_original_1 = c.fetchall()
        c.execute(check_sql_query2)
        result_original_2 = c.fetchall()

        if len(result_original_1) < 1 and len(result_original_2) < 1:
            raise InsertValueError(f"Not enough rows with subclasses: {class1} and {class2}")
    else:
        # In case we didn't find a common property:
        raise InsertValueError("Didn't find intersection")
    conn.close()
    return AttachmentConceptsDB(domain=json_schema['table_names_original'][0],
                        general_class=general_class, class1=class1, class2=class2,
                        common_property=common_property, template=ambig_item.template, type=type), json_schema, []

def validate_attachment_1tab_ref(logger, db_file, ambig_item, verbose=False):
    # validate genererated database for Attachment, 1tab_ref configuration:
    # class1, class2 should be values in the same table, common property is in another connected table 

    try:
        json_schema = dump_db_json_schema(db_file, 'tmp')
    except Exception as e:
        raise CreateTableError(f'Something wrong with dump_db_json_schema')

    tab_col_dict = get_table_col_dict(json_schema)

    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    found_subclasses = False
    found_common_property = False

    cached_results = {}

    # Try to find exact to class1/class2 values or substrings in one of the columns
    for type in ("1tab_ref", "1tab_ref_like"):
        comp_func = compare_equal_db_names if type == "1tab_ref" else compare_substr_db_names

        for col_idx, (tab_idx, col_name) in enumerate(json_schema['column_names_original']):
            if col_name == "*" or json_schema['column_types'][col_idx] != 'text':
                continue
            
            tab_name = json_schema['table_names_original'][tab_idx]
            if tab_name == 'sqlite_sequence':
                continue

            if (tab_name, col_name) not in cached_results:
                c.execute(f"SELECT DISTINCT {col_name} FROM {tab_name}")
                result_original = c.fetchall()
                cached_results[(tab_name, col_name)] = result_original
            else:
                result_original = cached_results[(tab_name, col_name)]

            result = [str(row[0]) for row in result_original]
            idx_class1 = check_row(ambig_item.class1, result, comp_func)
            idx_class2 = check_row(ambig_item.class2, result, comp_func)

            if idx_class1 >= 0 and idx_class2 >= 0:
                if found_subclasses:
                    logger.warning(f'Subclasses exist while found suitable {tab_name}.{col_name}, e.g. class1 {class1}')
                found_subclasses = True
                val1, val2 = result_original[idx_class1][0], result_original[idx_class2][0]
                if type == "1tab_ref_like":
                    val1 = get_substr(ambig_item.class1, str(val1))
                    val2 = get_substr(ambig_item.class2, str(val2))

                class1 = DBItem(tab_name=tab_name,
                                col_name=col_name,
                                value=val1)
                class2 = DBItem(tab_name=tab_name,
                                col_name=col_name,
                                value=val2)
                general_class = DBItem(tab_name=tab_name,
                                    col_name=col_name,)
                if verbose:
                    logger.info(f"Found subclasses: {class1}, {class2}, {general_class}")

        if found_subclasses:
            break

    if not found_subclasses:
        conn.close()
        raise InsertValueError(f"Didn't find class1 {ambig_item.class1} and class2 {ambig_item.class2}")
    

    found_common_property = False

    connected_tables = find_connected_tables(json_schema, general_class.tab_name)

    for type in ("1tab_ref", "1tab_ref_like"):
        comp_func = compare_equal_db_names if type == "1tab_ref" else compare_substr_db_names

        if found_common_property:
                break
        
        # Try to find table
        for tab_name in connected_tables:
            if tab_name == 'sqlite_sequence' or tab_name == general_class.tab_name:
                continue
    
            if comp_func(ambig_item.common_property, tab_name, type='column'):
                found_common_property = True
                common_property = DBItem(tab_name=tab_name)
                break

        if found_common_property:
            break

        # Try to find column
        for tab_name in connected_tables:
            if tab_name == 'sqlite_sequence' or tab_name == general_class.tab_name:
                continue

            if found_common_property:
                break

            for col_idx, col_name in tab_col_dict[tab_name]:
                if col_name == "*":
                    continue

                if comp_func(ambig_item.common_property, col_name, type='column'):
                    found_common_property = True
                    common_property = DBItem(tab_name=tab_name,
                                    col_name=col_name)
                    break    


    if not found_common_property:    
        conn.close()
        raise CreateTableError(f"Didn't find connected common_property {ambig_item.common_property}")
    
    if verbose:
        logger.info(f"Found common_property: {common_property}")

    join_columns =  get_join_columns_with_intermediates(json_schema, common_property.tab_name, general_class.tab_name)
    str_json = format_join_columns(join_columns)

    # Choose common property
    if type == "1tab_ref":
        create_sql_query = lambda join_tab, output_columns, tab, col, val: f"SELECT DISTINCT {output_columns} FROM {join_tab} WHERE {tab}.{col} = {format_value(val)}"
    else:
        create_sql_query = lambda join_tab, output_columns, tab, col, val: f"SELECT DISTINCT {output_columns} FROM {join_tab} WHERE {tab}.{col} LIKE {format_value(val, is_like=True)}"

    if common_property.db_type == 'table':
        all_cols = []
        for col_idx, col_name in tab_col_dict[common_property.tab_name]:
            if col_idx not in json_schema['primary_keys']:
                all_cols.append(f"{common_property.tab_name}.{col_name}")
        output_columns = ", ".join(all_cols)
    else:
        output_columns = f"{common_property.tab_name}.{common_property.col_name}"

    # extract rows with with both class1 and class2
    sql_query1 = create_sql_query(str_json,
                                output_columns,
                                general_class.tab_name, 
                                general_class.col_name,  
                                class1.value)
    
    sql_query2 = create_sql_query(str_json,
                                output_columns,
                                general_class.tab_name, 
                                general_class.col_name,  
                                class2.value)

    c.execute(f"{sql_query1} INTERSECT {sql_query2}")

    result_original_both = c.fetchall()

    if len(result_original_both) > 0:
        columns_results_filtered = {}
        num_cols = len(result_original_both[0])
        output_columns_splitted = output_columns.split(", ")
        assert num_cols == len(output_columns_splitted)
        for i_col, output_col in enumerate(output_columns_splitted):
            new_col = [row[i_col] for row in result_original_both if row[i_col]]
            if new_col:
                if output_col not in columns_results_filtered:
                    columns_results_filtered[output_col] = []
                columns_results_filtered[output_col] += new_col
        
        if len(columns_results_filtered) == 0:
            raise InsertValueError("Intersection is empty after filtering")
    else:
        raise InsertValueError("Intersection is empty")
                
    # we have common rows - just randomly choose property if common_property is table otherwise just leave it
    for col_name in columns_results_filtered.keys():
        col_res = columns_results_filtered[col_name]
        random.shuffle(col_res)
        columns_results_filtered[col_name] = col_res

    tmp_list = list(columns_results_filtered.items())
    random.shuffle(tmp_list)
    columns_results_filtered = {x[0]:x[1] for x in tmp_list}

    found_value = False
    for tab_col, col_res in columns_results_filtered.items():
        if found_value:
            break

        col_name = tab_col.split(".")[1]
        for res in col_res:
            common_property = DBItem(tab_name=common_property.tab_name,
                                col_name=col_name,
                                value=res)

            # check that we have at least 2 rows with at least one subclass 
            check_sql_query1 = f"{sql_query1} AND {common_property.tab_name}.{common_property.col_name} != {format_value(res)}"                    
            check_sql_query2 = f"{sql_query2} AND {common_property.tab_name}.{common_property.col_name} != {format_value(res)}"
            
            # Check that we have at least one more row for each class
            c.execute(check_sql_query1)
            result_original_1 = c.fetchall()

            c.execute(check_sql_query2)
            result_original_2 = c.fetchall()

            if len(result_original_1) >= 1 or len(result_original_2) >= 1:
                found_value = True
                break
    conn.close()

    if not found_value:
        raise InsertValueError(f"Didn't find value in {common_property}")
    
    if common_property.value != ambig_item.common_value:
        logger.warning(f"Common value {res} instead of {ambig_item.common_value}")

    return AttachmentConceptsDB(domain=json_schema['table_names_original'][0],
                        general_class=general_class, class1=class1, class2=class2,
                        common_property=common_property, template=ambig_item.template, type=type), json_schema, []

def validate_attachment_2tab_val(logger, db_file, ambig_item, verbose=False):
    # validate genererated database for Attachment, 2tab_val configuration:
    # class1 and class2 are in two different tables, each of them with the common property 

    ambig_item = copy.deepcopy(ambig_item)
    try:
        json_schema = dump_db_json_schema(db_file, 'tmp')
    except Exception as e:
        raise CreateTableError(f'Something wrong with dump_db_json_schema')

    tab_col_dict = get_table_col_dict(json_schema)

    found_common_property, found_class1, found_class2 = False, False, False

    # Firstly look for 2 table names class1 and class2
    for type in ("2tab_val", "2tab_val_like"):
        comp_func = compare_equal_db_names if type == "2tab_val" else compare_substr_db_names
        if found_class1 and found_class2:
            break

        for tab_name in json_schema['table_names_original']:
            if tab_name == 'sqlite_sequence':
                continue

            if comp_func(ambig_item.class1, tab_name, type='column'):
                found_class1 = True
                class1 = DBItem(tab_name=tab_name)
                if verbose:
                    logger.info(f"Found class1 {class1}")
            elif comp_func(ambig_item.class2, tab_name, type='column'):
                found_class2 = True
                class2 = DBItem(tab_name=tab_name)
                if verbose:
                    logger.info(f"Found class2 {class2}")
        
        if found_class1 and found_class2:
            break

        for col_idx, (tab_idx, col_name) in enumerate(json_schema['column_names_original']):
            tab_name = json_schema['table_names_original'][tab_idx]
            
            if col_name == "*" or tab_name == 'sqlite_sequence' or json_schema['column_types'][col_idx] != 'text':
                continue

            if comp_func(ambig_item.class1, col_name, type='column'):
                found_class1 = True
                class1 = DBItem(tab_name=tab_name,
                                    col_name=col_name)
                if verbose:
                        logger.info(f"Found class1 {class1}")
            elif comp_func(ambig_item.class2, col_name, type='column'):
                found_class2 = True
                class2 = DBItem(tab_name=tab_name,
                                    col_name=col_name)
                if verbose:
                    logger.info(f"Found class2 {class2}")

    if not found_class1:
        raise CreateTableError(f"Didn't find class1 {ambig_item.class1}")
    if not found_class2:
        raise CreateTableError(f"Didn't find class2 {ambig_item.class2}")
    if class1.db_type != class2.db_type:
        raise CreateTableError(f"Problem with types class1:{ambig_item.class1.db_type}, class2:{ambig_item.class2.db_type}")

    conn = sqlite3.connect(db_file)
    c = conn.cursor()

    # if we have 2 main table combination, common_property is a column common for both tables
    for type in ("2tab_val", "2tab_val_like"):
        comp_func = compare_equal_db_names if type == "2tab_val" else compare_substr_db_names
        col_common_property = None
        connected_common_property = set()

        for tab_name, cols in tab_col_dict.items():
            if tab_name != class1.tab_name and tab_name != class2.tab_name:
                continue

            for _, col_name in cols:
                if comp_func(ambig_item.common_property, col_name, type='column'):
                    col_common_property = col_name
                    connected_common_property.add(tab_name)
                    break
        
        if connected_common_property == set((class1.tab_name, class2.tab_name)):
            found_common_property = True
            break

    if not found_common_property:
        raise CreateTableError(f"Didn't find common_property {ambig_item.common_property}")
        
    
    # we need to check if we have class1 and subclass 2 rows with the same common_property
    c.execute(f"SELECT DISTINCT {class1.tab_name}.{col_common_property} FROM {class1.tab_name} INNER JOIN {class2.tab_name} ON {class1.tab_name}.{col_common_property} = {class2.tab_name}.{col_common_property};")
    result_original_both = c.fetchall()

    result_original_both_filtered = result_original_both

    if len(result_original_both_filtered) > 0:
        result_original_both_filtered = [row for row in result_original_both if row[0]]
        if len(result_original_both_filtered) == 0:
            raise InsertValueError("Empty results of intersection after preprocessing")

    if len(result_original_both_filtered) > 0:
        logger.info("Found intersection")
        # if we have common rows - just randomly choose property
        res = random.choice(result_original_both_filtered[0])
        common_property = DBItem(tab_name=class1.tab_name,
                            col_name=col_common_property,
                            value=res)
        if res != ambig_item.common_value:
            logger.warning(f"Common value {res} instead of {ambig_item.common_value}")

        # check that we have at least 2 rows with at least one subclass
        check_sql_query1 = f"SELECT DISTINCT {class1.tab_name}.{col_common_property} FROM {class1.tab_name} WHERE {col_common_property} != {format_value(res)}"
        check_sql_query2 = f"SELECT DISTINCT {class2.tab_name}.{col_common_property} FROM {class2.tab_name} WHERE {col_common_property} != {format_value(res)}"
        
        # Check that we have at least one more row for each class
        c.execute(check_sql_query1)
        result_original_1 = c.fetchall()
        c.execute(check_sql_query2)
        result_original_2 = c.fetchall()

        if len(result_original_1) < 1 and len(result_original_2) < 1:
            raise InsertValueError(f"Not enough rows with subclasses: {class1} and {class2}")
    else:
        # In case we didn't find a common property:
        raise InsertValueError("Didn't find intersection")

    conn.close()

    return AttachmentConceptsDB(domain=json_schema['table_names_original'][0],
                        general_class=ambig_item.general_class, class1=class1, class2=class2,
                        common_property=common_property, template=ambig_item.template, type=ambig_item.type), json_schema, []

def validate_attachment_2tab_ref(logger, db_file, ambig_item, verbose=False):
    # validate genererated database for Attachment, 2tab_ref configuration:
    # class1, class2 and common property are in three different connected tables
    
    ambig_item = copy.deepcopy(ambig_item)
    try:
        json_schema = dump_db_json_schema(db_file, 'tmp')
    except Exception as e:
        raise CreateTableError(f'Something wrong with dump_db_json_schema')

    tab_col_dict = get_table_col_dict(json_schema)
    found_common_property, found_class1, found_class2 = False, False, False

    for type in ("2tab_ref", "2tab_ref_like"):
        comp_func = compare_equal_db_names if type == "2tab_ref" else compare_substr_db_names
        if found_class1 and found_class2:
            break

        for tab_name in json_schema['table_names_original']:
            if tab_name == 'sqlite_sequence':
                continue

            if comp_func(ambig_item.class1, tab_name, type='column'):
                found_class1 = True
                class1 = DBItem(tab_name=tab_name)
                if verbose:
                    logger.info(f"Found class1 {class1}")
            elif comp_func(ambig_item.class2, tab_name, type='column'):
                found_class2 = True
                class2 = DBItem(tab_name=tab_name)
                if verbose:
                    logger.info(f"Found class2 {class2}")

        if found_class1 and found_class2:
            break

        for col_idx, (tab_idx, col_name) in enumerate(json_schema['column_names_original']):
            tab_name = json_schema['table_names_original'][tab_idx]
            
            if col_name == "*" or tab_name == 'sqlite_sequence' or json_schema['column_types'][col_idx] != 'text':
                continue

            if comp_func(ambig_item.class1, col_name, type='column'):
                found_class1 = True
                class1 = DBItem(tab_name=tab_name,
                                    col_name=col_name)
                if verbose:
                        logger.info(f"Found class1 {class1}")
            elif comp_func(ambig_item.class2, col_name, type='column'):
                found_class2 = True
                class2 = DBItem(tab_name=tab_name,
                                    col_name=col_name)
                if verbose:
                    logger.info(f"Found class2 {class2}")
            

    if not found_class1:
        raise CreateTableError(f"Didn't find class1 {ambig_item.class1}")
    if not found_class2:
        raise CreateTableError(f"Didn't find class2 {ambig_item.class2}")
    if class1.db_type != class2.db_type:
        raise CreateTableError(f"Problem with types class1:{ambig_item.class1.db_type}, class2:{ambig_item.class2.db_type}")

    # if we have 3 main table combination, common_property is a table with connections to both tables 
    found_common_property = False
    
    connected_tables_class1 = find_connected_tables(json_schema, class1.tab_name)
    connected_tables_class2 = find_connected_tables(json_schema, class2.tab_name)
    connected_tables = list(set(connected_tables_class1).intersection(set(connected_tables_class2)))


    for type in ("2tab_ref", "2tab_ref_like"):
        comp_func = compare_equal_db_names if type == "2tab_ref" else compare_substr_db_names

        if found_common_property:
                break

        # Try to find table
        for tab_name in connected_tables:
            if tab_name == 'sqlite_sequence' or tab_name == class1.tab_name or tab_name == class2.tab_name:
                continue
    
            if comp_func(ambig_item.common_property, tab_name, type='column'):
                found_common_property = True
                common_property = DBItem(tab_name=tab_name)
                break

        if found_common_property:
                break
            
        # Try to find column
        for tab_name in connected_tables:
            if tab_name == 'sqlite_sequence' or tab_name == class1.tab_name or tab_name == class2.tab_name:
                continue

            if found_common_property:
                break

            for col_idx, col_name in tab_col_dict[tab_name]:
                if col_name == "*":
                    continue

                if comp_func(ambig_item.common_property, col_name, type='column'):
                    found_common_property = True
                    common_property = DBItem(tab_name=tab_name,
                                    col_name=col_name)
                    
                    break
            
    if not found_common_property:    
        raise CreateTableError(f"Didn't find connected common_property {ambig_item.common_property}")
    
    if verbose:
        logger.info(f"Found common_property: {common_property}")

    conn = sqlite3.connect(db_file)
    c = conn.cursor()

    join_columns_class1 =  get_join_columns_with_intermediates(json_schema, common_property.tab_name, class1.tab_name)
    str_json_class1 = format_join_columns(join_columns_class1)

    join_columns_class2 =  get_join_columns_with_intermediates(json_schema, common_property.tab_name, class2.tab_name)
    str_json_class2 = format_join_columns(join_columns_class2)

    if common_property.db_type == 'table':
        all_cols = []
        for col_idx, col_name in tab_col_dict[common_property.tab_name]:
            if col_idx not in json_schema['primary_keys']:
                all_cols.append(f"{common_property.tab_name}.{col_name}")
        output_columns = ", ".join(all_cols)
    else:
        output_columns = f"{common_property.tab_name}.{common_property.col_name}"

    # we need to check if we have class1 and subclass 2 rows with the same common_property
    c.execute(f"SELECT DISTINCT {output_columns} FROM {str_json_class1} INTERSECT SELECT DISTINCT {output_columns} FROM {str_json_class2}")
    result_original_both = c.fetchall()

    if len(result_original_both) > 0:
        columns_results_filtered = {}
        num_cols = len(result_original_both[0])
        output_columns_splitted = output_columns.split(", ")
        assert num_cols == len(output_columns_splitted)
        for i_col, output_col in enumerate(output_columns_splitted):
            new_col = [row[i_col] for row in result_original_both if row[i_col]]
            if new_col:
                if output_col not in columns_results_filtered:
                    columns_results_filtered[output_col] = []
                columns_results_filtered[output_col] += new_col
        
        if len(columns_results_filtered) == 0:
            raise InsertValueError("Intersection is empty after filtering")
    else:
        raise InsertValueError("Intersection is empty")
                
    # we have common rows - just randomly choose property if common_property is table otherwise just leave it
    for col_name in columns_results_filtered.keys():
        col_res = columns_results_filtered[col_name]
        random.shuffle(col_res)
        columns_results_filtered[col_name] = col_res

    tmp_list = list(columns_results_filtered.items())
    random.shuffle(tmp_list)
    columns_results_filtered = {x[0]:x[1] for x in tmp_list}

    found_value = False
    for tab_col, col_res in columns_results_filtered.items():
        if found_value:
            break

        col_name = tab_col.split(".")[1]
        for res in col_res:
            common_property = DBItem(tab_name=common_property.tab_name,
                                col_name=col_name,
                                value=res)

            # check that we have at least 2 rows with at least one subclass 
            check_sql_query1 = f"SELECT DISTINCT {output_columns} FROM {str_json_class1} AND {common_property.tab_name}.{common_property.col_name} != {format_value(res)}"                    
            check_sql_query2 = f"SELECT DISTINCT {output_columns} FROM {str_json_class2} AND {common_property.tab_name}.{common_property.col_name} != {format_value(res)}"
            
            # Check that we have at least one more row for each class
            c.execute(check_sql_query1)
            result_original_1 = c.fetchall()

            c.execute(check_sql_query2)
            result_original_2 = c.fetchall()

            if len(result_original_1) >= 1 or len(result_original_2) >= 1:
                found_value = True
                break
    conn.close()

    if not found_value:
        raise InsertValueError(f"Didn't find value in {common_property}")
    
    if common_property.value != ambig_item.common_value:
        logger.warning(f"Common value {res} instead of {ambig_item.common_value}")

    conn.close()


    return AttachmentConceptsDB(domain=json_schema['table_names_original'][0],
                        general_class=ambig_item.general_class, class1=class1, class2=class2,
                        common_property=common_property, template=ambig_item.template, type=ambig_item.type), json_schema, []