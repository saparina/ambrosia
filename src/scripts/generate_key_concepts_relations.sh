#!/bin/bash

# generate key concepts and relations for scope
python src/db_generation/generate_key_concepts_relations.py --prompt_file src/prompts/db_generation/scope/generate_key_concepts_relations  --ambig_type scope --data_dir data/key_concepts_relations/scope --domain_file data/domains/domains_scope  --api_url "http://localhost:18888/v1/"


# generate key concepts and relations for attachment
python src/db_generation/generate_key_concepts_relations.py --prompt_file src/prompts/db_generation/attachment/generate_key_concepts_relations  --ambig_type attachment --data_dir data/key_concepts_relations/attachment --domain_file data/domains/domains_attachment  --api_url "http://localhost:18888/v1/"

# generate key concepts and relations for vague
python src/db_generation/generate_key_concepts_relations.py --prompt_file src/prompts/db_generation/vague/generate_key_concepts_relations  --ambig_type vague --data_dir data/key_concepts_relations/vague --domain_file data/domains/domains_vague  --api_url "http://localhost:18888/v1/"