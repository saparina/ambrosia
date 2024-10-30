#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <model_name>"
    exit 1
fi

model="$1"
model_name=$(basename "$model")

if [ ! -d "logs" ]; then
    mkdir -p "logs"
fi

seeds=(1 2 3 4 5)
types_of_questions=("ambig" "unambig")

for seed in "${seeds[@]}"; do
    for type_of_questions in "${types_of_questions[@]}"; do
        log_file="logs/log_${model_name}_prompt_few_shot_definitions_${type_of_questions}_rs${seed}.log"

        python src/evaluation/evaluate_model_tgi.py \
            --prompt_file src/prompts/evaluation/prompt_few_shot \
            --icl_pairs 1 \
            --icl_strategy "all_ambig_types" \
            --use_tgi \
            --api_url 'http://0.0.0.0/' \
            --model_name "${model}" \
            --type_of_questions "${type_of_questions}" \
            --seed "${seed}" \
            2>&1 | tee "${log_file}"

        echo "Executed: seed=${seed}, model=${model_name}, type_of_questions=${type_of_questions}, prompt_name=prompt_few_shot (with definitions), log_file=${log_file}"
    done
done

echo "All experiments completed."