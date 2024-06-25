import os
import json
import random
import argparse

import concurrent.futures

import numpy as np

from evaluation_utils import Dataset, EvaluatorConfig, generate
from format_prompts import write_icl_prompt, format_prompt, merge_all_insert_statements
from metrics import evaluate_predicted_statements

def parse_args():
    parser = argparse.ArgumentParser(description="Evaluate a model on AMBROSIA.")

    parser.add_argument("--verbose", action="store_true", help="Enable verbose output")

    parser.add_argument("--prompt_file", type=str, default="src/prompts/evaluation/prompt", help="File with the prompt")
    parser.add_argument("--croissant_file", type=str, default="data/ambrosia_croissant.json", help="Dataset in croissant format")
    parser.add_argument("--experiment_name", type=str, default="", help="Optional name of experiment")

    parser.add_argument("--icl_pairs", type=int, default=0, help="Number of ICL examples")

    parser.add_argument("--type_of_questions", type=str, choices=["ambig", "unambig"], help="Type of questions: 'ambig' or 'unambig'")

    parser.add_argument("--model_name", type=str, help="Full model name (as in HuggingFace Hub)")

    parser.add_argument("--ambig_detection", action="store_true", help="Evaluate detection of ambiguity")

    parser.add_argument("--use_openchat_api", action="store_true", help="Use OpenChat server")
    parser.add_argument("--use_tgi", action="store_true", help="Use TGI server")
    parser.add_argument("--use_openai", action="store_true", help="Use OpenAI API")
    parser.add_argument("--use_vllm", action="store_true", help="Use VLLM")
    parser.add_argument("--use_transformers_beam", action="store_true", help="Use Transformers for beam search")

    parser.add_argument("--api_url", type=str, help="API URL to connect to")
    parser.add_argument("--auth_token", type=str, default="", help="Auth token for Transformers")
    parser.add_argument("--api_key", type=str, default="", help="API key")
    parser.add_argument("--transformers_cache", type=str, help="Transformers cache dir")

    parser.add_argument("--num_return_sequences", type=int, default=1)
    parser.add_argument("--temperature", type=float, default=0.5, help="Sampling temperature")
    parser.add_argument("--top_p", type=float, help="Nucleus sampling probability")
    parser.add_argument("--top_k", type=int, help="Top-k sampling value")
    parser.add_argument("--repetition_penalty", type=float, default=1.0, help="Repetition penalty")
    parser.add_argument("--max_new_tokens", type=int, default=2048, help="Maximum tokens to generate")

    parser.add_argument("--seed", type=int, default=42, help="Random seed")

    return parser.parse_args()

def init_seed(seed):
    random.seed(seed)
    np.random.seed(seed)

def setup_generation(args, generator, dataset, tokenizer=None):
    all_metrics = {'vague': {}, 'attachment': {}, 'scope': {}}
    if not args.ambig_detection:
        for amb_type in all_metrics.keys():
            all_metrics[amb_type]['recall'] = []
            all_metrics[amb_type]['precision'] = []
            all_metrics[amb_type]['f1_score'] = []
            all_metrics[amb_type]['num_queries'] = []
            all_metrics[amb_type]['num_unique_queries'] = []
            all_metrics[amb_type]['unique_results'] = []
            all_metrics[amb_type]['unique_results_filtered'] = []
            all_metrics[amb_type]['all_found'] = []
    else:
        for amb_type in all_metrics.keys():
            all_metrics[amb_type]['is_ambiguous'] = []

    if not os.path.exists('eval_logs'):
        os.mkdir('eval_logs')

    eval_config = EvaluatorConfig(args, generator, dataset, tokenizer)

    if args.icl_pairs:
        write_icl_prompt(args, eval_config.prompt_template, dataset.df_few_shot_examples)

    return eval_config, all_metrics

def evaluate(df_one_db_examples, eval_config):
    file_name = df_one_db_examples['db_file'].iloc[0]
    db_dump = df_one_db_examples['db_dump'].iloc[0]
    db_file = df_one_db_examples["db_file"].iloc[0]
    is_ambiguous = df_one_db_examples['is_ambiguous'].iloc[0]

    db_dump = merge_all_insert_statements(db_file, db_dump)

    metrics, results = [], []
    for _, row in df_one_db_examples.iterrows():
        example = row.to_dict()

        question = example['question']
        gold_queries = example["gold_queries"]

        cur_prompt = format_prompt(eval_config.args, eval_config.prompt_template, db_dump, question, eval_config.tokenizer)

        # Generate
        outputs = generate(eval_config.args, eval_config.generator, cur_prompt)

        if eval_config.args.use_vllm:
            statements = []
            for choice in outputs:
                one_stat = eval_config.parse_statements(choice)
                statements += one_stat
            outputs = "\n\n".join(outputs)
        else:
            statements = eval_config.parse_statements(outputs)

        if eval_config.args.ambig_detection:
            cor_res = "yes" if eval_config.args.type_of_questions == 'ambig' else "no"
            is_ambiguous = statements == cor_res
            metrics.append({'is_ambiguous': is_ambiguous})
            results.append({'question': question, 'db_file': file_name, 'predictions': statements,'is_ambiguous': is_ambiguous})
        else:
            try:
                if not statements:
                    if 'select' in outputs.lower():
                        # Could not find SQL query...
                        continue
            
                    continue
                else:
                    local_metrics = evaluate_predicted_statements(file_name, statements, gold_queries)
            except Exception as e:
                continue

            metrics.append(local_metrics)

            local_metrics['question'] = question
            local_metrics['db_file'] = file_name
            local_metrics['predictions'] = statements
            results.append(local_metrics)
    
    metrics_grouped = {}
    for res_dict in metrics:
        for type, value in res_dict.items():
            if type not in metrics_grouped:
                metrics_grouped[type] = []
            metrics_grouped[type].append(value)
    return metrics_grouped, results

def evaluate_one_type(eval_config, df_one_type):
    if not eval_config.args.ambig_detection:
        metrics_one_type = {
                            'recall': [],
                            'precision': [],
                            'f1_score': [],
                            'num_queries': [],
                            'num_unique_queries': [],
                            'unique_results': [],
                            'unique_results_filtered': [],
                            'all_found': []
                        }
    else:
        metrics_one_type = {
                            'is_ambiguous': []
                            }
    results_one_type = []

    grouped_df = df_one_type.groupby('db_file')

    if eval_config.args.use_openai:
        for _, df_one_db_examples in grouped_df:
            one_db_metrics, one_db_results = evaluate(df_one_db_examples, eval_config)
            for metric_type, res in one_db_metrics.items():
                if metric_type in metrics_one_type:
                    metrics_one_type[metric_type] += res
            results_one_type += one_db_results

    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
            result_futures = [executor.submit(evaluate, df_one_db_examples, eval_config) for _, df_one_db_examples in grouped_df]
        for future in result_futures:
            one_db_metrics, one_db_results = future.result()
            for metric_type, res in one_db_metrics.items():
                if metric_type in metrics_one_type:
                    metrics_one_type[metric_type] += res
            results_one_type += one_db_results

    return metrics_one_type, results_one_type

def run_evaluation(args, generator, tokenizer=None):
    dataset = Dataset(args.croissant_file)
    if args.type_of_questions:
        is_ambiguous = args.type_of_questions == 'ambig'
        dataset.df_test = dataset.df_test[dataset.df_test['is_ambiguous'] == is_ambiguous]

    eval_config, all_metrics = setup_generation(args, generator, dataset, tokenizer)

    all_results = []
    for ambig_type in dataset.df_test['ambig_type'].unique():
        dataset_one_type = dataset.df_test[dataset.df_test['ambig_type'] == ambig_type]
        metrics_of_all_datasets, results_of_all_datasets = evaluate_one_type(eval_config, dataset_one_type)
        
        for metric, values in metrics_of_all_datasets.items():
            all_metrics[ambig_type][metric] += values
        all_results += results_of_all_datasets

    save_results_to_file(args, all_metrics, all_results)


def save_results_to_file(args, all_metrics, all_results):
    if not os.path.exists("experiment_results"):
        os.mkdir("experiment_results")

    path_to_res = os.path.join("experiment_results", f"{args.model}")
    if not os.path.exists(path_to_res):
        os.mkdir(path_to_res)

    if args.experiment_name:
        args.experiment_name = args.prompt_file.split('/')[-1] + f"_{args.experiment_name}"
    else:
        args.experiment_name = args.prompt_file.split('/')[-1]

    suffix = f"_{args.icl_pairs}" if args.icl_pairs else ""
    suffix += "_detect"  if args.ambig_detection else ""
    suffix += f"_rs{args.seed}" if args.seed else ""

    agg_all_metrics = {}
    micro_average_metrics = {}

    for amb_type in all_metrics.keys():
        agg_all_metrics[amb_type] = {}
        print(f'Metrics for {amb_type}')
        for res_type, all_res in all_metrics[amb_type].items():
            agg_all_metrics[amb_type][res_type] = np.mean(all_res)
            print(res_type, ",", agg_all_metrics[amb_type][res_type])
            if res_type not in micro_average_metrics:
                micro_average_metrics[res_type] = []
            micro_average_metrics[res_type] += all_res
        print()

    print(f'Micro-average metrics')
    for res_type, all_res in micro_average_metrics.items():
        micro_average_metrics[res_type] = np.mean(all_res)
        print(res_type, ",", micro_average_metrics[res_type])

    agg_all_metrics['micro-average'] = micro_average_metrics

    metrics_file_name = f"{path_to_res}/metrics_{args.model}_{args.type_of_questions}_{args.experiment_name}{suffix}.json"
    res_file_name = f"{path_to_res}/predictions_{args.model}_{args.type_of_questions}_{args.experiment_name}{suffix}.json"
    json.dump({'metrics': agg_all_metrics, 'parameters': vars(args)}, open(metrics_file_name, 'w'), indent=4)
    json.dump(all_results, open(res_file_name, 'w'), indent=4)
    print(f'Results saved to {res_file_name}')
    print(f'Metrics saved to {metrics_file_name}')