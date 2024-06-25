from openai import OpenAI
from eval import parse_args, init_seed, run_evaluation

if __name__ == "__main__":  
    args = parse_args()
    init_seed(args.seed)

    args.model =  args.model_name.split('/')[-1]

    if args.use_vllm or args.use_openchat_api:
        generator = OpenAI(base_url=args.api_url, api_key=args.api_key)
    elif args.use_openai:
        generator = OpenAI(api_key=args.api_key)

    run_evaluation(args, generator)