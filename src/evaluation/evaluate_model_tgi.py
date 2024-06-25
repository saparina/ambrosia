import torch 
import transformers
from transformers import AutoTokenizer, AutoModelForCausalLM
from huggingface_hub import InferenceClient

from eval import parse_args, init_seed, run_evaluation

def create_generator(args):
    if args.use_tgi:
        client = InferenceClient(model=args.api_url)
        return client
    else:
        device = torch.device('cuda')
        tokenizer = AutoTokenizer.from_pretrained(args.model_name, token=args.auth_token, cache_dir=args.transformers_cache)
        model = AutoModelForCausalLM.from_pretrained(args.model_name, token=args.auth_token, cache_dir=args.transformers_cache)
        model.to(device)
        if args.use_transformers_beam:
            pipeline = transformers.pipeline("text-generation", 
                                        model=model,
                                        tokenizer=tokenizer,       
                                        device=device,
                                        num_beams=5,
                                        num_return_sequences=5,
                                        max_new_tokens=500)
        else:
            pipeline = transformers.pipeline("text-generation", 
                                    model=model,
                                    tokenizer=tokenizer,       
                                    device=device,
                                    do_sample=True,
                                    temperature=args.temperature, 
                                    top_p=args.top_p,
                                    max_new_tokens=500)
        return pipeline


if __name__ == "__main__":  
    args = parse_args()
    init_seed(args.seed)
    torch.manual_seed(args.seed)

    args.model =  args.model_name.split('/')[-1]

    tokenizer = AutoTokenizer.from_pretrained(args.model_name, cache_dir=args.transformers_cache)  
    generator = create_generator(args)
    run_evaluation(args, generator, tokenizer)