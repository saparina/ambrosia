#!/bin/bash

# Check if the correct number of arguments are given
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <model_name> <command_option>"
    echo "command_option: --vllm, --transformers"
    exit 1
fi

model="$1"
command_option="$2"
model_name=$(basename "$model")

if [ ! -d "logs" ]; then
    mkdir -p "logs"
fi

types_of_questions=("ambig" "unambig")

for type_of_questions in "${types_of_questions[@]}"; do
    log_file="logs/log_${model_name}_beam_${type_of_questions}_rs1.log"

    case "$command_option" in
        --vllm)
            python3 src/evaluation/evaluate_model_openai_server.py  \
                --prompt_file src/prompts/evaluation/beam \
                --use_vllm \
                --api_url 'http://localhost:8000/v1' \
                --api_key 'EMPTY' \
                --model_name "${model}" \
                --type_of_questions "${type_of_questions}" \
                --seed 1 \
                2>&1 | tee "${log_file}"
            ;;
        --transformers)
            python3 src/evaluation/evaluate_model_tgi.py \
                --prompt_file src/prompts/evaluation/beam \
                --use_transformers_beam \
                --model_name "${model}" \
                --type_of_questions "${type_of_questions}" \
                --seed 1 \
                2>&1 | tee "${log_file}"
            ;;
        *)
                echo "Error: Invalid command option. Use --vllm or --transformers."
                exit 1
                ;;
        esac

    echo "Executed: model=${model_name}, type_of_questions=${type_of_questions}, prompt_name=beam, log_file=${log_file}"
done
echo "All experiments completed."