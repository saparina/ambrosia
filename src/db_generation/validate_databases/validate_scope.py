import sqlite3
import copy

from db_generation_utils import *
from key_concepts import DBItem, ScopeConceptsDB

def add_common_components_scope(logger, scope_item, json_schema, cursor):
    # make specific_component connected to all entities 
    
    used_statements = []
    tab_col_dict = get_table_col_dict(json_schema)
    entities_db_name = scope_item.entities.get_name()
    components_db_name = scope_item.components.get_name()
    entities_components_db_name = scope_item.entities_components.get_name()

    # How many rows are in SUBJECTS
    sql_query = f"SELECT COUNT(*) FROM {entities_db_name}"
    cursor.execute(sql_query)
    num_entities = int(cursor.fetchall()[0][0])

    # Get foreign keys for SUBJECTS_ELEMENTS
    links = set()
    k2tab_subj_el_col_links = {}
    for key1, key2 in json_schema['foreign_keys']:
        tbl_idx, col_name = json_schema['column_names_original'][key1]
        tbl_name = json_schema['table_names_original'][tbl_idx]
        if tbl_name == entities_components_db_name:
            k2_tbl_idx, _ = json_schema['column_names_original'][key2]
            k2tab_subj_el_col_links[k2_tbl_idx] = col_name
            links.add(json_schema['table_names_original'][k2_tbl_idx])

    # Get primary key for SUBJECTS_ELEMENTS
    primary_key_name = None
    sql_query = f"SELECT l.name FROM pragma_table_info(\"{entities_components_db_name}\") as l WHERE l.pk <> 0;"
    cursor.execute(sql_query)
    primary_keys = cursor.fetchall()

    if len(primary_keys) == 1:
        primary_key_name = primary_keys[0][0]

    # get column names of keys to components and entities
    entities_in_entities_components, components_in_entities_components = None, None

    all_columns = []
    # firstly check substrings
    for tab_idx, col_name in json_schema['column_names_original'][1:]:
        tbl_name = json_schema['table_names_original'][tab_idx]
        if tbl_name != entities_components_db_name or col_name == "*":
            continue

        if col_name != primary_key_name and compare_substr_db_names(entities_db_name, col_name, type='columns') :
            if 'id' not in col_name and col_name not in links:
                logger.warning(f"Found entities in entities_components but it doesn't look like id: {col_name}")
            entities_in_entities_components = col_name
        elif col_name != primary_key_name and compare_substr_db_names(components_db_name, col_name, type='columns') :
            if 'id' not in col_name and col_name not in links:
                logger.warning(f"Found components in entities_components but it doesn't look like id: {col_name}")
            components_in_entities_components = col_name
        all_columns.append(col_name)

    if not entities_in_entities_components or not components_in_entities_components:
        # check foreign keys
        for k2_tab_id, subj_el_col in k2tab_subj_el_col_links.items():
            if entities_db_name == json_schema['table_names_original'][k2_tab_id]:
                entities_in_entities_components = subj_el_col
            elif components_db_name == json_schema['table_names_original'][k2_tab_id]:
                components_in_entities_components = subj_el_col
        
    if not entities_in_entities_components:
        raise CreateTableError(f"Didn't find links to entities ({entities_db_name}) in entities_components ({entities_components_db_name})")
    if not components_in_entities_components:
        raise CreateTableError(f"Didn't find links to components ({components_db_name}) in entities_components ({components_in_entities_components})")
    
    
    # How many rows are in SUBJECTSELEMENTS
    sql_query = f"SELECT COUNT(*) FROM {entities_components_db_name}"
    cursor.execute(sql_query)
    num_entities_components = int(cursor.fetchall()[0][0])

    # Insert values to SUBJECTSELEMENTS
    for cur_idx in range(1, num_entities + 1):
        try:
            cursor.execute(f"SELECT * FROM {entities_components_db_name} WHERE {entities_in_entities_components} = {cur_idx}")
            results = cursor.fetchall()
            vals = []
            if results:
                found_element = False
                for row in results:
                    if found_element:
                        break
                    for loc_col_idx, res in enumerate(row):
                        if all_columns[loc_col_idx] == components_in_entities_components and \
                            format_value(res) == format_value(scope_item.specific_component.scope_id + 1):
                            found_element = True
                            break
                if found_element:
                    continue

                results = results[0]
                for loc_col_idx, res in enumerate(results):
                    if all_columns[loc_col_idx] == components_in_entities_components:
                        vals.append(format_value(scope_item.specific_component.scope_id + 1))

                    elif all_columns[loc_col_idx] == primary_key_name:
                        vals.append(format_value(num_entities_components + 1))
                        num_entities_components += 1
                    else:
                        vals.append(format_value(res))
                    cols_to_add = all_columns
            else:
                cols_to_add = []
                for col_name in all_columns:
                    if col_name == components_in_entities_components:
                        vals.append(format_value(scope_item.specific_component.scope_id + 1))
                        cols_to_add.append(col_name)
                    elif col_name == entities_in_entities_components:
                        vals.append(format_value(cur_idx))
                        cols_to_add.append(col_name)
                    elif col_name == primary_key_name:
                        vals.append(format_value(num_entities_components + 1))
                        num_entities_components += 1
                        cols_to_add.append(col_name)

            sql_query = f"INSERT INTO {entities_components_db_name} ({', '.join(cols_to_add)}) VALUES\n" \
                            f"({', '.join(vals)});"
            logger.info(sql_query)
            cursor.execute(sql_query)
            used_statements.append(sql_query)
        except Exception as e:
            logger.warning(e)
            pass
    
    # check that there are other vlaues as well
    cursor.execute(f"SELECT * FROM {entities_components_db_name} WHERE {components_in_entities_components} != {format_value(scope_item.specific_component.scope_id + 1)}")
    diff_results = cursor.fetchall()
    if len(diff_results) == 0:
        # we don't have other values, so we'll add one more row
        # choosing new id
        try:
            cursor.execute(f"SELECT * FROM {components_db_name} "
                    f"WHERE {scope_item.specific_component.col_name} != {format_value(scope_item.specific_component.scope_id + 1)} "
                    f"ORDER BY RANDOM() LIMIT 1;")
            res_components = cursor.fetchall()[0]

            for loc_col_id, res in enumerate(res_components):
                if tab_col_dict[components_db_name][loc_col_id][0] in json_schema['primary_keys']:
                    new_id = res

            # we will double the last row with new specific element id
            vals = []
            if results:
                results = results[0]
                for loc_col_idx, res in enumerate(results):
                    if all_columns[loc_col_idx] == components_in_entities_components:
                        vals.append(format_value(new_id))
                    elif all_columns[loc_col_idx] == primary_key_name:
                        vals.append(format_value(num_entities_components + 1))
                        num_entities_components += 1
                    else:
                        vals.append(format_value(res))
                    cols_to_add = all_columns
            else:
                cols_to_add = []
                for col_name in all_columns:
                    if col_name == components_in_entities_components:
                        vals.append(format_value(new_id))
                        cols_to_add.append(col_name)
                    elif col_name == entities_in_entities_components:
                        vals.append(format_value(cur_idx))
                        cols_to_add.append(col_name)
                    elif col_name == primary_key_name:
                        vals.append(format_value(num_entities_components + 1))
                        num_entities_components += 1
                        cols_to_add.append(col_name)
            sql_query = f"INSERT INTO {entities_components_db_name} ({', '.join(cols_to_add)}) VALUES\n" \
                        f"({', '.join(vals)});"
            logger.info(sql_query)
            cursor.execute(sql_query)
            used_statements.append(sql_query)
        except Exception as e:
            logger.warning(e)
            pass
    return scope_item, used_statements


def validate_scope(logger, db_file, ambig_item, verbose=False):
    # validate genererated database for Scope:
    # entities, components and entities_components are three connected tables, 
    # specific_component is common to many entities (we extend this connection to all entities)
    ambig_item = copy.deepcopy(ambig_item)
    json_schema = dump_db_json_schema(db_file, 'tmp')

    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    found_common, found_entities, found_components, found_specific_component = False, False, False, False

    cached_results = {}
    for type in ("1table", "1table_like"):
        comp_func = compare_equal_db_names if type == "1table" else compare_substr_db_names

        for tab_idx, col_name in json_schema['column_names_original'][1:]:
            tab_name = json_schema['table_names_original'][tab_idx]
            if col_name == "*" or tab_name == 'sqlite_sequence':
                continue

            if (tab_name, col_name) not in cached_results:
                sql_query = f"SELECT {col_name} FROM {tab_name}"
                c.execute(sql_query)
                result_original = c.fetchall()
                cached_results[(tab_name, col_name)] = result_original
            else:
                result_original = cached_results[(tab_name, col_name)]

            if not found_specific_component:
                result = [str(row[0]).lower() for row in result_original]
                idx_specific = check_row(ambig_item.specific_component, result, comp_func)
                if idx_specific >= 0:
                    found_specific_component = True
                    specific_component = DBItem(tab_name=tab_name,
                                    col_name=col_name,
                                    value=result_original[idx_specific][0],
                                    scope_id=idx_specific)


                    if verbose:
                        logger.info(f"Found specific_component: {specific_component}")
                    
                    if not compare_equal_db_names(ambig_item.components, tab_name):
                        raise CreateTableError(f'Something wrong with components and specific_component: {ambig_item.components} and {specific_component}')
                
                    if len(result_original) < 3:
                        conn.close()
                        raise InsertValueError(f"Not enougth rows: {len(result_original)}, {result_original}, {tab_name}.{col_name}")

                    found_components = True
                    components = DBItem(tab_name=tab_name)
                    if verbose:
                        logger.info(f"Found components {components}")
                    continue
                    
            if type == "1table" and not found_entities and compare_equal_db_names(ambig_item.entities, tab_name):
                if len(result_original) < 3:
                    conn.close()
                    raise InsertValueError(f"Not enougth rows: {len(result_original)}, {result_original}, {tab_name}.{col_name}")
                found_entities = True
                entities = DBItem(tab_name=tab_name)
                if verbose:
                    logger.info(f"Found entities {entities}")
            elif type == "1table" and not found_common and compare_substr_db_names(ambig_item.components, tab_name, type='columns') and \
                compare_substr_db_names(ambig_item.entities, tab_name, type='columns'):
                found_common = True
                entities_components = DBItem(tab_name=tab_name)
                if verbose:
                    logger.info(f"Found entities_components {entities_components}")

        if found_specific_component:
            break


    if not found_specific_component:
        conn.close()
        raise InsertValueError(f"Didn't find specific_component {ambig_item.specific_component}")
    if not found_entities:
        conn.close()
        raise CreateTableError(f"Didn't find entities {ambig_item.entities}")
    if not found_components:
        conn.close()
        raise CreateTableError(f"Didn't find components {ambig_item.components}")
    
    if not found_common:
        # check table with two references: to entities and to components
        links_entities, links_components = set(), set()
        for key1, key2 in json_schema['foreign_keys']:
            tbl_idx, _ = json_schema['column_names_original'][key2]
            tab_name = json_schema['table_names_original'][tbl_idx]
            if tab_name ==  entities.get_name():
                tbl_idx, _ = json_schema['column_names_original'][key1]
                tab_name = json_schema['table_names_original'][tbl_idx]
                links_entities.add(tab_name)
            elif tab_name == components.get_name():
                tbl_idx, _ = json_schema['column_names_original'][key1]
                tab_name = json_schema['table_names_original'][tbl_idx]
                links_components.add(tab_name)
        
        common_tabs = links_entities.intersection(links_components)
        if len(common_tabs) == 1:
            found_common = True
            entities_components = DBItem(tab_name=common_tabs.pop())
            if verbose:
                    logger.info(f"Found entities_components as table with two references: {entities_components}")

    if not found_common:
        conn.close()
        raise CreateTableError(f"Didn't find entities_components {ambig_item.entities}_{ambig_item.components}")

    scope_item = ScopeConceptsDB(entities, components, specific_component, entities_components, template=ambig_item.template)
    scope_item, used_statements = add_common_components_scope(logger, scope_item, json_schema, c)
    conn.commit()
    conn.close()
    return scope_item, json_schema, used_statements