import os
import re
import json

import argparse

import numpy as np
import torch
from openai import OpenAI

from key_concepts import AttachmentConcepts, ScopeConcepts, VagueConcepts

import random
random.seed(42)
np.random.seed(42)
torch.manual_seed(42)

def _parse_args():
    parser = argparse.ArgumentParser(description="Generate key concepts and relations for a given ambiguity type")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output")

    parser.add_argument("--data_dir", type=str, default="data/key_concepts_relations/scope", help="Directory where to save the results")
    parser.add_argument("--file_prefix", type=str, default="concepts", help="Prefix of the file name where to save the results")

    parser.add_argument("--prompt_file", type=str, default="src/prompts/db_generation/scope/generate_key_concepts_relations", help="Name of the file with the prompt")
    parser.add_argument("--domain_file", type=str, default="data/domains/domains_scope", help="Name of the file with the list of domains")
    parser.add_argument("--ambig_type", type=str, choices=['scope', 'attachment', 'vague'], default="scope", help="Type of ambiguity: 'scope', 'attachment', or 'vague'")

    parser.add_argument("--num_attempts", type=int, default=5, help="Number of attempts to try")
    
    parser.add_argument("--model", type=str, default="openchat_3.5", help="Model name to use")
    parser.add_argument("--api_url", type=str, help="API URL to connect to")

    parser.add_argument("--temperature", type=float, default=0.6, help="Sampling temperature")
    parser.add_argument("--top_p", type=float, default=0.95, help="Nucleus sampling probability")
    parser.add_argument("--top_k", type=int, default=1, help="Top-k sampling value")

    return parser.parse_args()

def remove_number_dot(text):
    return re.sub(r'^\d+\.\s*', '', text)

def extract_enumerated_sentences(text):
    pattern = r"\d+\.\s+(.*?)\n"
    matches = re.findall(pattern, text, re.DOTALL)
    return matches

def parse_attachment(text):
    # [Class 1] and [Class 2] are subclasses of [General Class]. All [Entities of Class 1] and [Entities of Class 2] have the property "[Common Property]". There might be a [Entity of Class 1] and a [Entity of Class 2] that both have "[Common Property]" equal to "[Common Value]".
    # Class 1: [Class 1]
    # Class 2: [Class 2]
    # General Class: [General Class]
    # Common Property: [Common Property]
    # Common Value: [Common Value]

    splitted_text = text.split('\n')
    if len(splitted_text) != 6:
        print(f"Not enought lines {text}")
        return
    
    if not splitted_text[1].startswith('Class 1: '):
        print(f"Failed to topic {text}")
        return
    item1 = splitted_text[1][len('Class 1: '):]
    
    if not splitted_text[2].startswith('Class 2: '):
        print(f"Failed to topic {text}")
        return
    item2 = splitted_text[2][len('Class 2: '):]

    if not splitted_text[3].startswith('General Class: '):
        print(f"Failed to topic {text}")
        return
    class_name = splitted_text[3][len('General Class: '):]

    if not splitted_text[4].startswith('Common Property: '):
        print(f"Failed to topic {text}")
        return
    common_property = splitted_text[4][len('Common Property: '):]

    if not splitted_text[5].startswith('Common Value: '):
        print(f"Failed to topic {text}")
        return
    common_value = splitted_text[5][len('Common Value: '):]

    return AttachmentConcepts(class_name, item1, item2, template=remove_number_dot(splitted_text[0]), common_property=common_property, common_value=common_value)

def parse_scope(text):
    patterns = [
                r'Each (.+?) has many different (.+?)\. Among them, (.+?) is common to many (.+?)\.',
                r'Each (.+?) has many different (.+?)\. Among them, (.+?) are common to many (.+?)\.'
                ]   

    entities, components, specific_component = None, None, None
    
    for pattern in patterns:
        match = re.search(pattern, text)

        # Check if a match is found
        if match:
            entities = match.group(1)
            components = match.group(2)
            specific_component = match.group(3)
    if entities is None or components is None or specific_component is None:
        print(f"Failed to parse {text}")
        return
    return ScopeConcepts(entities, components, specific_component, template=text)

def parse_vague(text):
    # Question: [Who / What / How / When / Where ...]?
    # Subject of Inquiry: [Subject of Inquiry]
    # Focus: [Focus]
    # Possible answer types:
    # 1. [General Category 1]
    # 2. [General Category 2]

    splitted_text = text.split('\n')
    if len(splitted_text) != 6:
        print(f"Not enought lines {text}")
        return
    
    if not splitted_text[0].startswith('Question: '):
        print(f"Failed to question {text}")
        return
    
    if not splitted_text[1].startswith('Subject of Inquiry: '):
        print(f"Failed to Subject of Inquiry {text}")
        return
    subject = splitted_text[1][len('Subject of Inquiry: '):]

    if not splitted_text[2].startswith('Focus:'):
        print(f"Failed to Answer types {text}")
        return
    
    focus = splitted_text[2][len('Focus: '):]
    
    if not splitted_text[3].startswith('Possible answer types:'):
        print(f"Failed to Answer types {text}")
        return

    if not splitted_text[4].startswith('1. '):
        print(f"Failed to answer1 {text}")
        return
    if not splitted_text[5].startswith('2. '):
        print(f"Failed to answer2 {text}")
        return
    general_category1 = splitted_text[4][len('1. '):]
    general_category2 = splitted_text[5][len('2. '):]
    if not general_category1 or not general_category2:
        print(f"Failed to parse {text}")
        return

    return VagueConcepts(subject, general_category1, general_category2, focus, template=remove_number_dot(text))

def generate_items(args, ambig_file):
    print(f"Generating key concepts and relations for {args.ambig_type}")

    if args.ambig_type == "attachment":
        parse_func = parse_attachment
    elif args.ambig_type == "scope":
        parse_func = parse_scope
    elif args.ambig_type == "vague":
        parse_func = parse_vague

    domains = ["random"]
    if args.domain_file:
        with open(os.path.join(args.domain_file), 'r') as f:
            domains = f.readlines()

    with open(os.path.join(args.prompt_file), 'r') as f:
        prompt = f.read()

    client = OpenAI(api_key="dockerllmapikey", base_url=args.api_url)  

    for domain in domains:
        if domain != 'random':
            domain = domain.replace('\n', '')
            cur_prompt = prompt.replace('DOMAIN', domain)
            print(cur_prompt.split('\n')[0])

        attempt_num = 0
        all_concepts_in_one_domain = []
        while len(all_concepts_in_one_domain) < 35 and attempt_num < args.num_attempts:
            # Generate
            response = client.chat.completions.create(
                model=args.model,
                messages= [{"role": "user", "content": cur_prompt}],
                temperature=args.temperature,
                top_p=args.top_p,
                seed=42
            )

            outputs = response.choices[0].message.content

            # Parse
            generated_concepts = []
            if args.ambig_type == 'vague' or args.ambig_type == 'attachment':
                result = outputs.split('\n\n')
                if len(result) == 0:
                    print(outputs)
                print(f"Generated {len(result)}")

                for row in result:
                    parsed_concepts = parse_func(row)
                    if parsed_concepts:
                        generated_concepts.append(parsed_concepts)
                        if args.verbose:
                            print(row)
                            print(parsed_concepts)
                            print()
            else:
                if outputs.startswith('1'):
                    result = extract_enumerated_sentences(outputs)
                
                else:
                    result = outputs.split('\n')
                print(f"Generated {len(result)}")
                if len(result) == 0:
                    print(outputs)

                for row in result:

                    parsed_concepts = parse_func(row)
                    
                    if parsed_concepts:
                        generated_concepts.append(parsed_concepts)
                        if args.verbose:
                            print(row)
                            print(parsed_concepts)
                            print()
            all_concepts_in_one_domain += generated_concepts
            attempt_num += 1
            all_concepts_in_one_domain = list(set(all_concepts_in_one_domain))

        print(f"Size for {domain}: {len(all_concepts_in_one_domain)}")

        json.dump([concepts.dump_json() for concepts in all_concepts_in_one_domain], open(ambig_file.replace('DOMAIN', domain), 'w'), indent=4)
        print(f"Saved to {ambig_file.replace('DOMAIN', domain)}")

if __name__ == "__main__":  
    args = _parse_args()
    if not os.path.exists(args.data_dir):
        os.mkdir(args.data_dir)
    ambig_file = os.path.join(args.data_dir, f"{args.ambig_type}_{args.file_prefix}_DOMAIN")

    generate_items(args, ambig_file)