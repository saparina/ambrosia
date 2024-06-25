import sqlite3
import copy

from db_generation_utils import *
from key_concepts import DBItem, VagueConceptsDB

def validate_vague_2cols(logger, db_file, ambig_item, verbose=False):
    # validate genererated database for Vague, 2col configuration:
    # general_category1 and general_category2 are two columns in one table

    ambig_item = copy.deepcopy(ambig_item)
    json_schema = dump_db_json_schema(db_file, 'tmp')

    found_general_category1, found_general_category2, found_subject = False, False, False

    for tab_idx, col_name in json_schema['column_names_original'][1:]:
        tab_name = json_schema['table_names_original'][tab_idx]

        if tab_name == 'sqlite_sequence' or col_name == '*':
            continue

        if compare_equal_db_names(ambig_item.general_category1, col_name, type='columns'):
            general_category1 = DBItem(tab_name=tab_name, col_name=col_name)
            found_general_category1 = True
            if verbose:
                logger.info(f"Found general_category1 {general_category1}")
        elif compare_equal_db_names(ambig_item.general_category2, col_name, type='columns'):
            general_category2 = DBItem(tab_name=tab_name, col_name=col_name)
            found_general_category2 = True
            if verbose:
                logger.info(f"Found general_category2 {general_category2}")
        elif compare_equal_db_names(ambig_item.subject, col_name, type='columns'):
            found_subject = True
            subject = DBItem(tab_name=tab_name, col_name=col_name)
            if verbose:
                logger.info(f"Found subject {subject}")

        if not found_general_category1 and compare_substr_db_names(ambig_item.general_category1, col_name, type='columns'):
            general_category1 = DBItem(tab_name=tab_name, col_name=col_name)
            found_general_category1 = True
            if verbose:
                logger.info(f"Found general_category1 {general_category1}")
        elif not found_general_category2 and compare_substr_db_names(ambig_item.general_category2, col_name, type='columns'):
            general_category2 = DBItem(tab_name=tab_name, col_name=col_name)
            found_general_category2 = True
            if verbose:
                logger.info(f"Found general_category2 {general_category2}")
        elif not found_subject and compare_substr_db_names(ambig_item.subject, col_name, type='columns'):
            found_subject = True
            subject = DBItem(tab_name=tab_name, col_name=col_name)
            if verbose:
                logger.info(f"Found subject {subject}")
   
    if not found_general_category1:
        raise CreateTableError(f"Didn't find general_category1 {ambig_item.general_category1}")
    if not found_general_category2:
        raise CreateTableError(f"Didn't find general_category2 {ambig_item.general_category2}")
    if general_category1.tab_name != general_category2.tab_name:
        raise CreateTableError(f"Subjects are in different tables {ambig_item.general_category1.tab_name} and {ambig_item.general_category2.tab_name}")
    if not found_subject:
        for tab_name in json_schema['table_names_original']:
            if found_subject:
                break
            if compare_equal_db_names(ambig_item.subject, tab_name, type='columns'):
                found_subject = True
                subject = DBItem(tab_name=tab_name)
                if verbose:
                    logger.info(f"Found subject {subject}")
            elif not found_subject and compare_substr_db_names(ambig_item.subject, col_name, type='columns'):
                found_subject = True
                subject = DBItem(tab_name=tab_name, col_name=col_name)
                if verbose:
                    logger.info(f"Found subject {subject}")

        if not found_subject:
            raise CreateTableError(f"Didn't find subject {ambig_item.subject}")
    
    # check that entities are not empty
    conn = sqlite3.connect(db_file)
    c = conn.cursor()

    for subject in [general_category1, general_category2]:
        sql_query = f"SELECT {subject.col_name} FROM {subject.tab_name}"
        c.execute(sql_query)
        result_original = c.fetchall()
        result_original = [row[0] for row in result_original if row[0]]
        if len(result_original) < 1:
            raise InsertValueError(f'Not enough values in {subject}')
            
    conn.close()
    return VagueConceptsDB(subject, general_category1, general_category2, template=ambig_item.template, type='2cols'), json_schema, []


def validate_vague_2tabs(logger, db_file, ambig_item, verbose=False):
    # validate genererated database for Vague, 2tabs configuration:
    # general_category1 and general_category2 are two differnt tables
    
    ambig_item = copy.deepcopy(ambig_item)
    json_schema = dump_db_json_schema(db_file, 'tmp')
    tab_col_dict = get_table_col_dict(json_schema)

    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    found_general_category1, found_general_category2, found_subject = False, False, False

    for tab_name in tab_col_dict.keys():
        if compare_equal_db_names(ambig_item.general_category1, tab_name, type='columns'):
            if not found_general_category1:
                found_general_category1 = True
                general_category1 = DBItem(tab_name=tab_name)
                if verbose:
                    logger.info(f"Found general_category1 {general_category1}")
                
                sql_query = f"SELECT * FROM {tab_name}"
                c.execute(sql_query)
                result_original = c.fetchall()
                if len(result_original) < 1:
                    raise InsertValueError(f'Not enough values in {general_category1}')

        elif compare_equal_db_names(ambig_item.general_category2, tab_name, type='columns'):
            if not found_general_category2:
                found_general_category2 = True
                general_category2 = DBItem(tab_name=tab_name)
                if verbose:
                    logger.info(f"Found general_category2 {general_category2}")
                sql_query = f"SELECT * FROM {tab_name}"
                c.execute(sql_query)
                result_original = c.fetchall()
                if len(result_original) < 1:
                    raise InsertValueError(f'Not enough values in {general_category2}')
        elif compare_equal_db_names(ambig_item.subject, tab_name, type='columns'):
            if not found_subject:
                found_subject = True
                subject = DBItem(tab_name=tab_name)
                if verbose:
                    logger.info(f"Found subject {subject}")

            sql_query = f"SELECT * FROM {tab_name}"
            c.execute(sql_query)
            result_original = c.fetchall()
            if len(result_original) < 1:
                raise InsertValueError(f'Not enough values in {subject}')

        
        if not found_general_category1 and compare_substr_db_names(ambig_item.general_category1, tab_name, type='columns'):
            general_category1 = DBItem(tab_name=tab_name)
            found_general_category1 = True
            if verbose:
                logger.info(f"Found general_category1 {general_category1}")

            sql_query = f"SELECT * FROM {tab_name}"
            c.execute(sql_query)
            result_original = c.fetchall()
            if len(result_original) < 1:
                raise InsertValueError(f'Not enough values in {general_category1}')
            
        elif not found_general_category2 and compare_substr_db_names(ambig_item.general_category2, tab_name, type='columns'):
            general_category2 = DBItem(tab_name=tab_name)
            found_general_category2 = True
            if verbose:
                logger.info(f"Found general_category2 {general_category2}")

            sql_query = f"SELECT * FROM {tab_name}"
            c.execute(sql_query)
            result_original = c.fetchall()
            if len(result_original) < 1:
                raise InsertValueError(f'Not enough values in {general_category2}')
        elif not found_subject and compare_substr_db_names(ambig_item.subject, tab_name, type='columns'):
            found_subject = True
            subject = DBItem(tab_name=tab_name)
            if verbose:
                logger.info(f"Found subject {subject}")

            sql_query = f"SELECT * FROM {tab_name}"
            c.execute(sql_query)
            result_original = c.fetchall()
            if len(result_original) < 1:
                raise InsertValueError(f'Not enough values in {subject}')

    for tab_idx, col_name in json_schema['column_names_original'][1:]:
        tab_name = json_schema['table_names_original'][tab_idx]

        if tab_name == 'sqlite_sequence' or col_name == '*':
            continue

        if not found_general_category1 and compare_equal_db_names(ambig_item.general_category1, col_name, type='columns'):
            general_category1 = DBItem(tab_name=tab_name, col_name=col_name)
            found_general_category1 = True
            if verbose:
                logger.info(f"Found general_category1 {general_category1}")
             
            sql_query = f"SELECT {col_name} FROM {tab_name}"
            c.execute(sql_query)
            result_original = c.fetchall()
            result_original = [row[0] for row in result_original if row[0]]
            if len(result_original) < 1:
                raise InsertValueError(f'Not enough values in {general_category1}')
        elif not found_general_category2 and compare_equal_db_names(ambig_item.general_category2, col_name, type='columns'):
            general_category2 = DBItem(tab_name=tab_name, col_name=col_name)
            found_general_category2 = True
            if verbose:
                logger.info(f"Found general_category2 {general_category2}")

            sql_query = f"SELECT {col_name} FROM {tab_name}"
            c.execute(sql_query)
            result_original = c.fetchall()
            result_original = [row[0] for row in result_original if row[0]]
            if len(result_original)< 1:
                raise InsertValueError(f'Not enough values in {general_category2}')
        elif not found_subject and compare_equal_db_names(ambig_item.subject, col_name, type='columns'):
            found_subject = True
            subject = DBItem(tab_name=tab_name, col_name=col_name)
            if verbose:
                logger.info(f"Found subject {subject}")

            
            sql_query = f"SELECT {col_name} FROM {tab_name}"
            c.execute(sql_query)
            result_original = c.fetchall()
            if len(result_original) < 1:
                raise InsertValueError(f'Not enough values in {subject}')

        if not found_general_category1 and compare_substr_db_names(ambig_item.general_category1, col_name, type='columns'):
            general_category1 = DBItem(tab_name=tab_name, col_name=col_name)
            found_general_category1 = True
            if verbose:
                logger.info(f"Found general_category1 {general_category1}")

            sql_query = f"SELECT {col_name} FROM {tab_name}"
            c.execute(sql_query)
            result_original = c.fetchall()
            result_original = [row[0] for row in result_original if row[0]]
            if len(result_original) < 1:
                raise InsertValueError(f'Not enough values in {general_category1}')
        elif not found_general_category2 and compare_substr_db_names(ambig_item.general_category2, col_name, type='columns'):
            general_category2 = DBItem(tab_name=tab_name, col_name=col_name)
            found_general_category2 = True
            if verbose:
                logger.info(f"Found general_category2 {general_category2}")

            sql_query = f"SELECT {col_name} FROM {tab_name}"
            c.execute(sql_query)
            result_original = c.fetchall()
            result_original = [row[0] for row in result_original if row[0]]
            if len(result_original)< 1:
                raise InsertValueError(f'Not enough values in {general_category2}')
        elif not found_subject and compare_substr_db_names(ambig_item.subject, col_name, type='columns'):
            found_subject = True
            subject = DBItem(tab_name=tab_name, col_name=col_name)
            if verbose:
                logger.info(f"Found subject {subject}")

            sql_query = f"SELECT {col_name} FROM {tab_name}"
            c.execute(sql_query)
            result_original = c.fetchall()
            if len(result_original) < 1:
                raise InsertValueError(f'Not enough values in {subject}')
        
   
    conn.close()
    if not found_general_category1:
        raise CreateTableError(f"Didn't find general_category1 {ambig_item.general_category1}")
    if not found_general_category2:
        raise CreateTableError(f"Didn't find general_category2 {ambig_item.general_category2}")
    if not found_subject:
        raise CreateTableError(f"Didn't find subject {ambig_item.subject}")
    
    fk_key1 = set([key1 for key1, _ in json_schema['foreign_keys']])
    fk_key2 = set([key2 for _, key2 in json_schema['foreign_keys']])
    subject_key1 = set([col_idx for col_idx, _ in tab_col_dict[subject.tab_name] if col_idx in fk_key1])
    subject_key2 = set([col_idx for col_idx, _ in tab_col_dict[tab_name] if col_idx in fk_key2])
    general_category1_key1 = set([col_idx for col_idx, _ in tab_col_dict[general_category1.tab_name] if col_idx in fk_key1])
    general_category1_key2 = set([col_idx for col_idx, _ in tab_col_dict[general_category1.tab_name] if col_idx in fk_key2])
    general_category2_key1 = set([col_idx for col_idx, _ in tab_col_dict[general_category2.tab_name] if col_idx in fk_key1])
    general_category2_key2 = set([col_idx for col_idx, _ in tab_col_dict[general_category2.tab_name] if col_idx in fk_key2])

    # check the tables are connected
    subj1_connected, subj2_connected = False, False
    for key1, key2 in json_schema['foreign_keys']:
        if key1 in subject_key1 and key2 in general_category1_key2 or key2 in subject_key2 and key1 in general_category1_key1:
            subj1_connected = True
        elif key1 in subject_key1 and key2 in general_category2_key2 or key2 in subject_key2 and key1 in general_category2_key1:
            subj2_connected = True

    if not subj1_connected:
        raise CreateTableError(f"general_category1 is not connected to subject {general_category1} and {subject}")
    if not subj2_connected:
        raise CreateTableError(f"general_category2 is not connected to subject {general_category2} and {subject}")
    
    return VagueConceptsDB(subject, general_category1, general_category2, template=ambig_item.template, type='2tabs'), json_schema, []