#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <model_name> <openai_api_key>"
    exit 1
fi

model="$1"
openai_api_key="$2"
model_name=$(basename "$model")

if [ ! -d "logs" ]; then
    mkdir -p "logs"
fi

seeds=(1 2 3)
types_of_questions=("ambig" "unambig")

for seed in "${seeds[@]}"; do
    for type_of_questions in "${types_of_questions[@]}"; do
        log_file="logs/log_${model_name}_prompt_few_shot_1_${type_of_questions}_rs${seed}.log"

            python src/evaluation/evaluate_model_openai_server.py \
            --prompt_file src/prompts/evaluation/prompt_few_shot \
            --use_openai \
            --api_key "${openai_api_key}" \
            --temperature 0 \
            --icl_pairs 1 \
            --model_name "${model}" \
            --type_of_questions "${type_of_questions}" \
            --seed "${seed}" \
            2>&1 | tee "${log_file}"

            echo "Executed: strategy=${strategy}, seed=${seed}, pairs=1, model=${model_name}, type_of_questions=${type_of_questions}, prompt_name=${prompt_name}, log_file=${log_file}"
        done
    done
done

echo "All experiments completed."