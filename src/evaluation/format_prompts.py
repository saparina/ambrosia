import os
import copy
import random

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
        
def format_icl_example_one_item_sql(ambig_item, unambig_items, num_ex=None):
    db_dump = filter_db_dump(ambig_item["db_dump"], ambig_item['ambig_queries'])
    
    num_ex_str = f" {num_ex}" if num_ex else ""

    icl_prompt = f"Example{num_ex_str}:\nGiven the following SQLite database schema:\n\n{db_dump}"
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
    db_dump = filter_db_dump(ambig_item["db_dump"], ambig_item['ambig_queries'])

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

    path_to_icl_file = os.path.join('data/icl_examples', f"icl_{args.icl_strategy}{args.icl_pairs}{suffix}")

    assert "EXAMPLES" in prompt_template        

    if not os.path.exists(path_to_icl_file):
        datasets_ambig = df_few_shot_examples[df_few_shot_examples['is_ambiguous'] == True]
        datasets_unambig = df_few_shot_examples[df_few_shot_examples['is_ambiguous'] == False]

        if args.icl_strategy == "random":
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
  
            icl_examples_prompt = icl_prompt[:-2]
    
        else:
            icl_prompt = {'vague': "", 'attachment': "", 'scope': ""}

            for idx in range(args.icl_pairs):
                for ambig_type in ['vague', 'attachment', 'scope']:
                    cur_datasets_ambig = datasets_ambig[datasets_ambig['ambig_type'] == ambig_type]

                    random_ambig_question = random.choice(cur_datasets_ambig['ambig_question'].unique())
                    ambig_item = datasets_ambig[datasets_ambig['ambig_question'] == random_ambig_question]
                    ambig_item = ambig_item.iloc[0].to_dict()
                    unambig_items = datasets_unambig[datasets_unambig['ambig_question'] == random_ambig_question]

                    datasets_ambig = datasets_ambig[datasets_ambig['ambig_question'] != random_ambig_question]

                    icl_prompt[ambig_type] += format_icl_example_one_item_sql(ambig_item, unambig_items, 1 + idx)

            with open(args.icl_format_file, 'r') as f:
                examples_template = f.read()
        
            examples_template = examples_template.replace("SCOPE_EXAMPLE", icl_prompt['scope'][:-2])
            examples_template = examples_template.replace("ATTACHMENT_EXAMPLE", icl_prompt['attachment'][:-2])
            icl_examples_prompt = examples_template.replace("VAGUENESS_EXAMPLE", icl_prompt['vague'][:-2])

        with open(path_to_icl_file, 'w') as f:
            f.write(icl_examples_prompt)
    
def read_icl_prompt(args, prompt_template):
    suffix = "_detect" if args.ambig_detection else ""
    if args.seed:
        suffix += f"_rs{args.seed}"

    path_to_icl_file = os.path.join('data/icl_examples', f"icl_{args.icl_strategy}{args.icl_pairs}{suffix}")
    with open(path_to_icl_file, 'r') as f:
        icl_examples_prompt = f.read()

    return prompt_template.replace("EXAMPLES", icl_examples_prompt)
