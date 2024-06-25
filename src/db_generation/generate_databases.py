import os
import concurrent.futures
import json
import logging
import shutil
import argparse

from tqdm import tqdm 
import numpy as np
import torch
import traceback
from openai import OpenAI

from db_generation_utils import *
from validate_databases.validate_scope import *
from validate_databases.validate_attachment import *
from validate_databases.validate_vague import *
from key_concepts import AttachmentConcepts, ScopeConcepts, VagueConcepts

import random
random.seed(42)
np.random.seed(42)
torch.manual_seed(42)

def _parse_args():
    parser = argparse.ArgumentParser(description="Generate databases via CREATE TABLE and INSERT INTO SQL statements.")

    parser.add_argument("--verbose", action="store_true", help="Enable verbose output")

    parser.add_argument("--prompt_dir", type=str, default="src/prompts/db_generation/", help="Directory with the prompts")
    parser.add_argument("--data_dir", type=str, default="data/", help="Directory for result storage. Generated databases will be stored in [data_dir]/[ambig_type]/[domain]")
    parser.add_argument("--concepts_dir", type=str, default="data/key_concepts_relations/", help="Directory where key concepts are stored.")
    parser.add_argument("--concepts_file_prefix", type=str, default="concepts", help="Prefix for key concepts and relations file names")

    parser.add_argument("--ambig_type", type=str, choices=['scope', 'attachment', 'vague'], default="scope", help="Type of ambiguity: 'scope', 'attachment', or 'vague'")
    parser.add_argument('--types_of_tables', type=str, default=None, choices=['1tab_ref', '1tab_val', '2tab_ref', '2tab_val'], help="Types of tables to generate for attachment: '1tab_ref', '1tab_val', '2tab_ref', or '2tab_val'. Generate all types if not specified.")

    # Generation options
    parser.add_argument("--budget", type=int, default=50, help="Total number of attempts for generating one database")
    parser.add_argument("--num_attempts", type=int, default=5, help="Number of attempts for generating one statement")

    # Model options
    parser.add_argument("--model", type=str, default="openchat_3.5", help="Model name to use")
    parser.add_argument("--api_url", type=str, help="API URL to connect to")
    parser.add_argument("--num_workers", type=int, default=4, help="Number of worker processes")

    parser.add_argument("--temperature", type=float, default=0.6, help="Sampling temperature")
    parser.add_argument("--top_p", type=float, default=0.95, help="Nucleus sampling probability")
    parser.add_argument("--top_k", type=int, default=1, help="Top-k sampling value")

    return parser.parse_args()

def setup_logger(log_file):
    logger = logging.getLogger(log_file)  # Create a unique logger based on the log_file
    logger.setLevel(logging.DEBUG)  # Set to DEBUG to capture all levels of logging

    # Avoid adding multiple handlers if they are already present
    if not logger.handlers:
        fh = logging.FileHandler(log_file, mode='a')
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    return logger

class DatabaseGenerator:
    def __init__(self, model, api_url, db_dir, logger, 
                 budget=50, 
                 num_attempts=5,  
                 temperature=0.6, 
                 top_p=0.9,
                 top_k=1.0):
        self.model = model
        self.api_url = api_url
        self.client = OpenAI(api_key="dockerllmapikey", base_url=args.api_url)  

        self.db_dir = db_dir
        self.logger = logger

        self.budget = budget
        self.num_attempts = num_attempts
        assert budget % num_attempts == 0

        self.temperature = temperature
        self.top_p = top_p
        self.top_k = top_k

    def generate(self, messages):
        response = self.client.chat.completions.create(
            model=self.model,
            messages=messages,
            temperature=self.temperature,
            top_p=self.top_p,
            seed=42
        )

        return response.choices[0].message.content

    def generate_statements(self, create_table_prompt, insert_values_prompt, concepts, validate_func, db_name, verbose=False):
        item_idx = db_name.split('_')[0]
        tmp_db_file = None

        total_number_attempts = 0

        for generation_statement in ('CREATE TABLE', 'INSERT INTO'):
            if generation_statement == 'CREATE TABLE':
                self.used_statements = []
                
            for attempt_idx in range(self.num_attempts):
                if self.local_budget <= 0:
                    break
                total_number_attempts += 1

                # create prompt
                if generation_statement == 'CREATE TABLE':
                    cur_prompt = [{"role": "user", "content": create_table_prompt}]
                else:
                    insert_values_prompt = insert_values_prompt.replace('CREATE_TABLE_STATEMENTS', model_answer)
                    cur_prompt = [{"role": "user", "content": insert_values_prompt}]
             
                # generate statement via LLM
                result = self.generate(cur_prompt)
                self.local_budget -= 1
                
                try:
                    statements = parse_statements(result, generation_statement)
                    
                    if not statements:
                        self.logger.info(result)
                        raise ValueError("LLM Generation Error")

                    if verbose:
                        self.logger.info('\n'.join(statements))

                    tmp_db_file = os.path.join(self.db_dir, db_name, f"{db_name}_{generation_statement}_{attempt_idx}.sqlite")
            
                    if generation_statement == 'CREATE TABLE':
                        model_answer = '\n'.join(statements)
                    else:
                        execute_statements(tmp_db_file, self.used_statements[0])
                        execute_statements(tmp_db_file, ["PRAGMA ignore_check_constraints=ON;"])

                except Exception as e:
                    self.logger.error(traceback.format_exc())
                    self.logger.error(f'database {db_name} #{item_idx}: failed to parse {generation_statement} on the {attempt_idx} attempt: {e}\n')
                    if statements:
                        self.logger.info('\n'.join(statements))
                    # clean
                    if tmp_db_file and os.path.exists(tmp_db_file):
                        os.remove(tmp_db_file)
                    continue

                try:        
                    # execute via SQLite    
                    execute_statements(tmp_db_file, statements)
                    if verbose:
                        self.logger.info(f"{generation_statement} succesful")
                except Exception as e:
                    self.logger.error(traceback.format_exc())
                    self.logger.error(f'database {db_name} #{item_idx}: failed to execute {generation_statement} on the {attempt_idx} attempt: {e}\n')
                    if statements:
                        self.logger.info('\n'.join(statements))
                    # clean
                    if tmp_db_file and os.path.exists(tmp_db_file):
                        os.remove(tmp_db_file)
                    continue
 
                if generation_statement != 'CREATE TABLE':
                    try:
                        validate_func(self.logger, tmp_db_file, concepts, verbose)
                    except Exception as e:
                        self.logger.error(traceback.format_exc())
                        self.logger.error(f'database {db_name} #{item_idx}: failed to validate {generation_statement} on the {attempt_idx} attempt: {e}\n')
                        if statements:
                            self.logger.info('\n'.join(statements))
                        # clean
                        if tmp_db_file and os.path.exists(tmp_db_file):
                            os.remove(tmp_db_file)

                        if isinstance(e, CreateTableError):
                            break
                        else:
                            continue

                self.used_statements.append(statements)

                # clean
                if os.path.exists(tmp_db_file):
                    os.remove(tmp_db_file)
                break
 
        return total_number_attempts

    def generate_database(self, concepts, create_table_prompt, insert_values_prompt, db_name, 
                                validate_func, verbose=False):
        concepts_db = None
        item_idx = db_name.split('_')[0]

        if os.path.exists(os.path.join(self.db_dir, db_name)):
            shutil.rmtree(os.path.join(self.db_dir, db_name))
        os.mkdir(os.path.join(self.db_dir, db_name))
        
        db_file = os.path.join(self.db_dir, db_name, f"{db_name}.sqlite")
        
        self.logger.info(concepts)
        self.logger.info(f"db_file: {db_file}")

        self.local_budget = self.budget

        self.logger.info(f"CREATE TABLE Prompt: {create_table_prompt}")
        self.logger.info(f"INSERT VALUES Prompt: {insert_values_prompt}")

        total_number_attempts = 0
        while self.local_budget:
            total_number_attempts += self.generate_statements(create_table_prompt, insert_values_prompt, concepts, validate_func, db_name, verbose=verbose)

            try:
                assert len(self.used_statements) == 2, self.used_statements
                # commit to real file
                execute_statements(db_file, self.used_statements[0] + ["PRAGMA ignore_check_constraints=ON;"] + self.used_statements[1])
                # validate
                concepts_db, _, more_used_statements = validate_func(self.logger, db_file, concepts, verbose)
                self.used_statements += [more_used_statements]
                if verbose:
                    self.logger.info("Validated succesful")
                json.dump(concepts_db.dump_json(), open(os.path.join(self.db_dir, db_name, 'concepts_db.json'), 'w'), indent=4)
                json.dump(concepts.dump_json(), open(os.path.join(self.db_dir, db_name, 'concepts.json'), 'w'), indent=4)
                
                used_statements_str = ""
                for statements in self.used_statements:
                    used_statements_str += '\n'.join(statements)
                self.logger.info(f"{item_idx}\n{used_statements_str}\n")

                with open(os.path.join(self.db_dir, db_name, 'used_statements.json'), 'w') as f:
                    f.write(used_statements_str)

                self.logger.info(f'Write {db_name} database after {total_number_attempts} attempts\n')
                if verbose:
                    self.logger.info(concepts_db)
                break
            except Exception as e:
                self.logger.error(traceback.format_exc())
                self.logger.error(f'{db_name} database: something wrong on {total_number_attempts} attempts: {e}\n')
                if os.path.exists(db_file):
                    os.remove(db_file)
                
        if not concepts_db and os.path.exists(os.path.join(self.db_dir, db_name)):
            shutil.rmtree(os.path.join(self.db_dir, db_name))

        return concepts_db

    
def create_prompt(cur_prompt, concepts, domain=None, num_tables=None, config_type=None):
    def props(cls):   
        return {i: val for i, val in cls.__dict__.items() if i[:1] != '_'}

    attributes = props(concepts)
    for attr, value in attributes.items():
        if not value:
            continue
    
        if config_type and '1tab' in config_type and attr.upper() in ('SUBCLASS1', 'SUBCLASS2', 'COMMON_VALUE'):
            # do not format values
            cur_prompt = cur_prompt.replace(attr.upper(), value)
        elif config_type and '2tab' in config_type and attr.upper() in ('COMMON_VALUE'):
            # do not format values
            cur_prompt = cur_prompt.replace(attr.upper(), value)
        else:
            cur_prompt = cur_prompt.replace(attr.upper(), format_db_names(value))

    if num_tables:
        cur_prompt = cur_prompt.replace("NUMTABLES", str(num_tables))

    if domain:
        cur_prompt = cur_prompt.replace("DOMAIN", domain)

    return cur_prompt

def load_concepts(concepts_dir, ambig_type, concepts_file_prefix):
    dir_name = os.path.join(concepts_dir, ambig_type)

    all_concepts = {}
    num_concepts = 0
    for ambig_file in os.listdir(dir_name):
        if concepts_file_prefix not in ambig_file:
            continue
    
        all_concepts_domain = []
        domain = ambig_file.split(f'{args.ambig_type}_concepts_')[-1]
        if concepts_file_prefix:
            domain = domain.split(concepts_file_prefix)[0]
        if not domain:
            domain = 'random'

        concepts = json.load(open(os.path.join(dir_name, ambig_file), 'r'))

        if ambig_type == "attachment":
            AmbigItems = AttachmentConcepts
        elif ambig_type == "scope":
            AmbigItems = ScopeConcepts
        elif ambig_type == "vague":
            AmbigItems = VagueConcepts

        for item_components in concepts:
            all_concepts_domain.append(AmbigItems().load_json(item_components))
        all_concepts[domain] = all_concepts_domain
        num_concepts += len(all_concepts_domain)

    print(f'Loaded {num_concepts} {ambig_type} concepts')
    return all_concepts


def load_prompts(prompt_dir, ambig_type):
    all_create_tbl_prompts = {}
    all_insert_vals_prompts = {}

    if ambig_type == 'attachment':
        for config in ['1tab_val', '1tab_ref','2tab_val', '2tab_ref']:
            with open(os.path.join(prompt_dir, ambig_type, f'generate_create_table_statements_{config}'), 'r') as f:
                all_create_tbl_prompts[f"{args.ambig_type}_{config}"] = f.read()

            with open(os.path.join(prompt_dir, ambig_type, f'generate_insert_into_statements_{config}'), 'r') as f:
                all_insert_vals_prompts[f"{args.ambig_type}_{config}"] = f.read()

    elif ambig_type == 'scope':
        with open(os.path.join(prompt_dir, ambig_type, f'generate_create_table_statements'), 'r') as f:
            all_create_tbl_prompts[f"{args.ambig_type}"] = f.read()
        with open(os.path.join(prompt_dir, ambig_type, f'generate_insert_into_statements'), 'r') as f:
            all_insert_vals_prompts[f"{args.ambig_type}"] = f.read()

    elif ambig_type == 'vague':
        with open(os.path.join(prompt_dir, ambig_type, f'generate_insert_into_statements'), 'r') as f:
            insert_values_general = f.read()

        with open(os.path.join(prompt_dir, ambig_type, f'generate_create_table_statements_2cols'), 'r') as f:
            all_create_tbl_prompts[f"{args.ambig_type}_2cols"] = f.read()
            all_insert_vals_prompts[f"{args.ambig_type}_2cols"] = insert_values_general

        with open(os.path.join(prompt_dir, ambig_type, f'generate_create_table_statements_2tabs'), 'r') as f:
            all_create_tbl_prompts[f"{args.ambig_type}_2tabs"] = f.read()
            all_insert_vals_prompts[f"{args.ambig_type}_2tabs"] = insert_values_general
                
    assert len(all_create_tbl_prompts) == len(all_insert_vals_prompts)
    print(f'Found {len(all_create_tbl_prompts)} prompts for {ambig_type}')
    return all_create_tbl_prompts, all_insert_vals_prompts


def generate_all_databases(args, db_generator, concepts, domain, item_idx, verbose=False):
    if args.ambig_type == 'attachment':
        if args.types_of_tables:
            all_tables = [args.types_of_tables]
        else:
            all_tables = ['1tab_val', '1tab_ref','2tab_val', '2tab_ref']

        for config_type in all_tables:
            if verbose:
                db_generator.logger.info(f"Generate {config_type} table db")
            
            if config_type == '1tab_val':
                validate_func = validate_attachment_1tab_val
            elif config_type == '1tab_ref':
                validate_func = validate_attachment_1tab_ref
            elif config_type == '2tab_val':
                validate_func = validate_attachment_2tab_val
            elif config_type == '2tab_ref':
                validate_func = validate_attachment_2tab_ref

            concepts.type = f"{config_type}"

            prompt_name = f"{args.ambig_type}_{config_type}"
            create_table_template = all_create_tbl_prompts[prompt_name]
            insert_values_template = all_insert_vals_prompts[prompt_name]

            create_table_prompt = create_prompt(create_table_template, concepts, domain=domain, config_type=config_type)
            insert_values_prompt = create_prompt(insert_values_template, concepts, domain=domain, config_type=config_type)
            
            focus = '_'.join(concepts.general_class.split(' '))
            db_name = f"{args.ambig_type}_{config_type}_{focus}".lower()
            
            concepts_db = db_generator.generate_database(concepts, create_table_prompt, insert_values_prompt, db_name,
                                                        validate_func=validate_func, verbose=verbose)
    
            if not concepts_db:
                db_generator.logger.critical(f'FAILED {config_type} {item_idx}')

    elif args.ambig_type == 'scope':
        all_prompt_names = list(all_create_tbl_prompts.keys())

        for prompt_name in all_prompt_names:
            create_table_template = all_create_tbl_prompts[prompt_name]
            insert_values_template = all_insert_vals_prompts[prompt_name]

            create_table_prompt = create_prompt(create_table_template, concepts, domain=domain)
            insert_values_prompt = create_prompt(insert_values_template, concepts, domain=domain)

            db_name =  f"{args.ambig_type}_{concepts.entities}_{concepts.components}".lower()
            db_name = '_'.join(db_name.split(' '))

            concepts_db = db_generator.generate_database(concepts, create_table_prompt, insert_values_prompt, db_name,
                                                        validate_func=validate_scope, verbose=verbose)
            
            if not concepts_db:
                db_generator.logger.critical(f'FAILED {config_type} {item_idx}')

    elif args.ambig_type == 'vague':
        for prompt_name, validate_func in zip(list(all_create_tbl_prompts.keys()), [validate_vague_2cols, validate_vague_2tabs]):
            create_table_template = all_create_tbl_prompts[prompt_name]
            insert_values_template = all_insert_vals_prompts[prompt_name]

            create_table_prompt = create_prompt(create_table_template, concepts, domain=domain)
            insert_values_prompt = create_prompt(insert_values_template, concepts, domain=domain)

            focus = '_'.join(concepts.focus.split(' '))
            config_type = prompt_name.split('_')[-1]
            db_name = f"{args.ambig_type}_{config_type}_{focus}".lower()

            concepts_db = db_generator.generate_database(concepts, create_table_prompt, insert_values_prompt, db_name,
                                                        validate_func=validate_func, verbose=verbose)
            
            if not concepts_db:
                db_generator.logger.critical(f'FAILED {prompt_name} {item_idx}')


def process_domain(domain, all_concepts, args):
    db_dir = os.path.join(args.db_dir, domain)
    if not os.path.exists(db_dir):
        os.mkdir(db_dir)
    
    log_path = os.path.join(db_dir, f'{domain}_log.txt')
    checkpoint_path = os.path.join(db_dir, f'{domain}_checkpoint.json')
    logger = setup_logger(log_path)
    print(f'Create databases for {domain}')
    logger.info(f'Loaded {len(all_concepts)} ambig items for domain {domain}')

    db_generator = DatabaseGenerator(args.model, args.api_url, db_dir, logger, 
                                     budget=args.budget, 
                                     num_attempts=args.num_attempts, 
                                     top_p=args.top_p,
                                     top_k=args.top_k,
                                     temperature=args.temperature)
    
    start_index = 0
    if os.path.exists(checkpoint_path):
        with open(checkpoint_path, 'r') as f:
            checkpoint = json.load(f)
            start_index = checkpoint.get('last_processed_index', -1) + 1
            if args.types_of_tables != checkpoint.get('types_of_tables', None):
                start_index = 0

    try:
        for item_idx, concepts in enumerate(tqdm(all_concepts[start_index: ])):
            generate_all_databases(args, db_generator, concepts, domain, start_index + item_idx, verbose=args.verbose)
            # Update and save checkpoint
            with open(checkpoint_path, 'w') as f:
                json.dump({'last_processed_index': start_index + item_idx, 'types_of_tables': args.types_of_tables}, f)

    except Exception as e:
        logger.critical(traceback.format_exc())
    return f'Database created for {domain}'

def parallel_process_domains(domain_concepts, args, max_workers=4):
    # for domain, all_concepts in domain_concepts.items():
    #     process_domain(domain, all_concepts, args)
        
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_domain, domain, concepts, args) 
                   for domain, concepts in domain_concepts.items()]
        for future in concurrent.futures.as_completed(futures):
            print(future.result())

if __name__ == "__main__":  
    args = _parse_args()

    domain_concepts = load_concepts(args.concepts_dir, args.ambig_type, args.concepts_file_prefix)
    if not os.path.exists(args.data_dir):
        os.mkdir(args.data_dir)

    args.db_dir = os.path.join(args.data_dir, args.ambig_type)
    if not os.path.exists(args.db_dir):
        os.mkdir(args.db_dir)

    # Load prompts
    all_create_tbl_prompts, all_insert_vals_prompts = load_prompts(args.prompt_dir, args.ambig_type)

    # Parallel processing
    parallel_process_domains(domain_concepts, args, max_workers=args.num_workers)