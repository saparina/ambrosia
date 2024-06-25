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
        log_file="logs/log_${model_name}_detect_ambig_${type_of_questions}_rs${seed}.log"

        python src/evaluation/evaluate_model_tgi.py \
            --prompt_file src/prompts/evaluation/detect_ambig \
            --use_tgi \
            --api_url 'http://0.0.0.0/' \
            --model_name "${model}" \
            --type_of_questions "${type_of_questions}" \
            --seed "${seed}" \
            --ambig_detection \
            2>&1 | tee "${log_file}"

        echo "Executed: seed=${seed}, model=${model_name}, type_of_questions=${type_of_questions}, prompt_name=detect_ambig, log_file=${log_file} detect"
    done
done

echo "All experiments completed."
