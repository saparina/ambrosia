#!/bin/bash
if [ "$#" -lt 2 ]; then
    echo "Usage: $0 <model_name> <command_option> [openai_api_key]"
    echo "command_option: --tgi, --openchat_server, or --openai_api"
    exit 1
fi

model="$1"
command_option="$2"
openai_api_key="$3"
model_name=$(basename "$model")

if [ ! -d "logs" ]; then
    mkdir -p "logs"
fi

seeds=(1 2 3 4 5)
types_of_questions=("ambig" "unambig")

for seed in "${seeds[@]}"; do
    for type_of_questions in "${types_of_questions[@]}"; do
        log_file="logs/log_${model_name}_prompt_${type_of_questions}_rs${seed}.log"

        case "$command_option" in
            --tgi)
                python src/evaluation/evaluate_model_tgi.py \
                    --prompt_file src/prompts/evaluation/prompt \
                    --use_tgi \
                    --api_url 'http://0.0.0.0/' \
                    --model_name "${model}" \
                    --type_of_questions "${type_of_questions}" \
                    --seed "${seed}" \
                    2>&1 | tee "${log_file}"
                ;;
            --openchat_server)
                python src/evaluation/evaluate_model_openai_server.py \
                    --prompt_file src/prompts/evaluation/prompt \
                    --use_openchat_api \
                    --api_url "http://localhost:18888/v1/" \
                    --api_key "dockerllmapikey" \
                    --model_name "${model}" \
                    --type_of_questions "${type_of_questions}" \
                    --seed "${seed}" \
                    --top_p 1.0 \
                    2>&1 | tee "${log_file}"
                ;;
            --openai_api)
                if [ -z "$openai_api_key" ]; then
                    echo "Error: OPENAI_API_KEY must be provided when using --openai_api"
                    exit 1
                fi
                python src/evaluation/evaluate_model_openai_server.py \
                    --prompt_file "src/prompts/evaluation/prompt" \
                    --use_openai \
                    --temperature 0 \
                    --model_name "${model}" \
                    --type_of_questions "${type_of_questions}" \
                    --api_key "${openai_api_key}" \
                    --seed 42 \
                    2>&1 | tee "${log_file}"
                ;;
            *)
                echo "Error: Invalid command option. Use --tgi, --openchat_server, or --openai_api."
                exit 1
                ;;
        esac

        echo "Executed: seed=${seed}, model=${model_name}, type_of_questions=${type_of_questions}, prompt_name=prompt, log_file=${log_file}"
    done
done

echo "All experiments completed."
