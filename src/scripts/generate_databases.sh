#!/bin/bash
python src/db_generation/generate_databases.py --data_dir data/ --ambig_type scope --api_url "http://localhost:18888/v1/"
python src/db_generation/generate_databases.py --data_dir data/ --ambig_type vague --api_url "http://localhost:18888/v1/"
python src/db_generation/generate_databases.py --data_dir data/ --ambig_type attachment --api_url "http://localhost:18888/v1/"