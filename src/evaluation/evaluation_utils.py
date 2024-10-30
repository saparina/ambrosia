import time
import re
import sqlite3
from collections import defaultdict

import pandas as pd
import mlcroissant as mlc

from output_parsers import *

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

class Dataset:
    def __init__(self, croissant_file):
        dataset = mlc.Dataset(jsonld=croissant_file)
        self.data = dataset.records(record_set="examples")
        self.df = self._parse_jsonld(self.data)
        self.create_splits()
    
    def _parse_jsonld(self, jsonld_data):
        df = pd.json_normalize(jsonld_data)
        for column in df.columns:
            if df[column].dtype == object:
                df[column] = df[column].apply(lambda x: x.decode('utf-8') if isinstance(x, bytes) else x)
        df['gold_queries'] = df['gold_queries'].str.split('\n\n')
        df.loc[df['ambig_type'] == 'attachment', 'question'] += " Show them in one table."
        df.loc[df['ambig_type'] == 'attachment', 'ambig_question'] += " Show them in one table."
        df['db_dump'] = df.apply(lambda row: merge_all_insert_statements(row['db_file'], row['db_dump']), axis=1)
        return df
    
    def create_splits(self):
        self.df_test = self.df[self.df['split'] == 'test']
        self.df_few_shot_examples = self.df[self.df['split'] == 'few_shot_examples']

def generate(args, generator, prompt): 
    if args.use_vllm:
        completion = generator.completions.create(
                model=args.model_name,
                prompt=prompt,
                extra_body={"use_beam_search": True, "best_of": 5}, 
                temperature=0.0, 
                n=5,
                max_tokens=500, seed=args.seed)
        outputs = [row.text for row in completion.choices]
    elif args.use_openai:
        while True:
            try:
                response = generator.chat.completions.create(
                    model=args.model,
                    messages=[{'role': 'user', 'content': prompt}],
                    temperature=args.temperature, 
                    max_tokens=500,
                    seed=args.seed)
                break
            except Exception as e:
                print(e)
                time.sleep(30)

        outputs = response.choices[0].message.content
    elif args.use_openchat_api:
        response = generator.chat.completions.create(
            model="openchat_3.5",
            messages= [{"role": "user", "content": prompt}],
            temperature=args.temperature,
            top_p=args.top_p,
            seed=args.seed
        )
        outputs = response.choices[0].message.content
    elif args.use_tgi:
        params = {
            'prompt': prompt, 
            'max_new_tokens': args.max_new_tokens, 
            'temperature': args.temperature, 
            'do_sample': True
        }
        if args.top_k:
            params['top_k'] = args.top_k
        if args.top_p:
            params['top_p'] = args.top_p
        if args.seed:
            params['seed'] = args.seed
        if args.repetition_penalty:
            params['repetition_penalty'] = args.repetition_penalty
        outputs = generator.text_generation(**params)

    elif args.use_transformers_beam:
        sequences = generator(prompt,
                        num_return_sequences=args.num_return_sequences)
        assert len(sequences) == args.num_return_sequences
        if args.num_return_sequences == 1:
            outputs = sequences[0]['generated_text']
        else:
            outputs = [seq['generated_text'] for seq in sequences]

    else:    
        sequences = generator(prompt)
        outputs = sequences[0]['generated_text']
        
    return outputs


class EvaluatorConfig:
    def __init__(self, args, generator, dataset=None, tokenizer=None):
        self.args = args
        self.generator = generator
        self.tokenizer = tokenizer
        self.dataset = dataset
        
        if args.use_vllm or args.use_transformers_beam:
            self.parse_statements = parse_single_statement
        elif args.ambig_detection:
            self.parse_statements = parse_ambig_detection
        elif 'mistral' in args.model.lower():
            self.parse_statements = parse_statements_mistral
        elif 'mixtral' in args.model.lower():
            self.parse_statements = parse_statements_mixtral
        elif 'openchat' in args.model.lower() or 'gpt' in args.model.lower():
            self.parse_statements = parse_statements_openchat
        elif 'codellama' in args.model.lower():
            self.parse_statements = parse_statements_codellama
        elif 'llama' in args.model.lower():
            self.parse_statements = parse_statements_llama

        with open(self.args.prompt_file, 'r') as f:
            self.prompt_template = f.read()