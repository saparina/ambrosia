import os
import re
import copy
import random
import sqlite3
from collections import defaultdict

def get_column_names(db_path, table_name):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute(f"PRAGMA table_info({table_name});")
    rows = cursor.fetchall()
    column_names = [row[1] for row in rows]
    conn.close()
    return column_names

def merge_all_insert_statements(db_path, db_dump):
    # Regex to find all INSERT INTO statements
    pattern = re.compile(r"INSERT INTO \"(\w+)\" VALUES (\(.*?\));", re.IGNORECASE)
    
    # Dictionary to store values for each table
    table_inserts = defaultdict(list)

    new_content = ""

    # Process the dump line by line
    for line in db_dump.split('\n'):
        match = pattern.search(line)
        if "INSERT INTO " in line:
            table_name = line[line.find("INSERT INTO ") + len("INSERT INTO "):line.find(" VALUES(")]
            line = line[line.find("VALUES")+ len("VALUES"):-1]

            # Append values to the corresponding table's list
            if table_name != '"sqlite_sequence"':
                table_inserts[table_name].append(line)
        elif "BEGIN TRANSACTION" in line or "COMMIT" in line or "DELETE" in line:
            continue
        else:
            new_content += line + "\n"

    # Create a single INSERT INTO statement for each table
    for table_name, values in table_inserts.items():
        if values:
            col_names = get_column_names(db_path, table_name)
            new_content += f"INSERT INTO {table_name} (" + ",".join(col_names) + ") VALUES " + ",".join(values) + ";\n"

    return new_content

def filter_db_dump(db_dump, gold_ambig_queries):
    # filter dump
    db_dump_filtered = ''
    if isinstance(gold_ambig_queries, list):
        gold_ambig_queries = " ".join(gold_ambig_queries)
    all_queries_str = gold_ambig_queries.lower()
    skip = False
    for line in db_dump.split('\n'):
        if line.startswith("CREATE TABLE"):
            tab_name = line[len("CREATE TABLE "):line.find("(")].strip().lower()
            if tab_name.startswith('"') or tab_name.startswith("'"):
                tab_name = tab_name[1:-1]
            if tab_name in all_queries_str:
                db_dump_filtered += line + "\n"
                skip = False
            else:
                skip = True
        elif line.startswith("INSERT INTO"):
            skip = False
            tab_name = line[len("INSERT INTO "):line.find("(")].strip().lower()
            if tab_name.startswith('"') or tab_name.startswith("'"):
                tab_name = tab_name[1:-1]
            if tab_name in all_queries_str:
                db_dump_filtered += line + "\n"
        elif not skip:
            db_dump_filtered += line + "\n"
    return db_dump_filtered

def format_prompt(args, prompt_template, db_dump, question, tokenizer=None):
    cur_prompt = read_icl_prompt(args, prompt_template) if args.icl_pairs else copy.copy(prompt_template)

    cur_prompt = cur_prompt.replace('SQL_DATABASE_DUMP', db_dump)
    cur_prompt = cur_prompt.replace('QUESTION', question)

    if args.use_tgi:
        return tokenizer.apply_chat_template([{"role": "user", "content": cur_prompt}], tokenize=False)
    else:
        return cur_prompt
        
def format_icl_example_one_item_sql(ambig_item, unambig_items, num_ex):
    db_dump = merge_all_insert_statements(ambig_item["db_file"], ambig_item["db_dump"])
    db_dump = filter_db_dump(db_dump, ambig_item['ambig_queries'])
    
    icl_prompt = f"Example {num_ex}:\nGiven the following SQLite database schema:\n\n{db_dump}"
    icl_prompt += f"Answer the following:\n{ambig_item['question']}\n\nSQL query(s):\n"
    for _, gold_query in enumerate(ambig_item["gold_queries"]):
        icl_prompt += f"{gold_query}\n\n"

    for _, row in unambig_items.iterrows():
        ex = row.to_dict()
        assert ex['is_ambiguous'] is False and len(ex["gold_queries"]) == 1

        icl_prompt += f"Answer the following:\n{ex['question']}\n\nSQL query(s):\n"
        icl_prompt += f"{ ex['gold_queries'][0]}\n\n"
            
    return icl_prompt

def format_icl_example_one_item_detect(ambig_item, unambig_items, num_ex):
    db_dump = merge_all_insert_statements(ambig_item["db_file"], ambig_item["db_dump"])
    db_dump = filter_db_dump(db_dump, ambig_item['ambig_queries'])

    icl_prompt = f"Example {num_ex}:\nGiven the following SQLite database schema:\n\n{db_dump}"
    icl_prompt += f"Is the following question ambiguous:\n{ambig_item['question']}\n\nYes\n\n"

    for _, row in unambig_items.iterrows():
        ex = row.to_dict()
        assert ex['is_ambiguous'] is False and len(ex["gold_queries"]) == 1
        
        icl_prompt += f"Is the following question ambiguous:\n{ex['question']}\n\nNo\n\n"
            
    return icl_prompt

def write_icl_prompt(args, prompt_template, df_few_shot_examples):
    if not os.path.exists('data/icl_examples'):
        os.mkdir('data/icl_examples')

    suffix = "_detect" if args.ambig_detection else ""
    if args.seed:
        suffix += f"_rs{args.seed}"

    path_to_icl_file = os.path.join('data/icl_examples', f"icl_{args.icl_pairs}{suffix}")

    assert "EXAMPLES" in prompt_template        

    if not os.path.exists(path_to_icl_file):
        datasets_ambig = df_few_shot_examples[df_few_shot_examples['is_ambiguous'] == True]
        datasets_unambig = df_few_shot_examples[df_few_shot_examples['is_ambiguous'] == False]

        icl_prompt = ""

        for idx in range(args.icl_pairs):
            # choose ambig question
            random_ambig_question = random.choice(datasets_ambig['ambig_question'].unique())
            ambig_item = datasets_ambig[datasets_ambig['ambig_question'] == random_ambig_question]
            ambig_item = ambig_item.iloc[0].to_dict()
            unambig_items = datasets_unambig[datasets_unambig['ambig_question'] == random_ambig_question]

            # filter out this question
            datasets_ambig = datasets_ambig[datasets_ambig['ambig_question'] != random_ambig_question]
            
            if args.ambig_detection:
                icl_prompt += format_icl_example_one_item_detect(ambig_item, unambig_items, 1 + idx)
            else:
                icl_prompt += format_icl_example_one_item_sql(ambig_item, unambig_items, 1 + idx)

    if not os.path.exists(path_to_icl_file):    
        icl_examples_prompt = icl_prompt[:-2]

        with open(path_to_icl_file, 'w') as f:
            f.write(icl_examples_prompt)
    
def read_icl_prompt(args, prompt_template):
    suffix = "_detect" if args.ambig_detection else ""
    if args.seed:
        suffix += f"_rs{args.seed}"

    path_to_icl_file = os.path.join('data/icl_examples', f"icl_{args.icl_pairs}{suffix}")
    with open(path_to_icl_file, 'r') as f:
        icl_examples_prompt = f.read()

    return prompt_template.replace("EXAMPLES", icl_examples_prompt)
