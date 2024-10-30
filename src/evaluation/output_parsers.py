import re
import sqlparse

def parse_statements_openchat(text):
    text = text.replace("```sql\n", "").replace("```", "")
    statements = sqlparse.split(text)

    statements = list(dict.fromkeys(statements))
    if statements:
        new_statements = []
        for stat in statements:
            new_statements.append(stat.replace("\\", "").replace("```sql\n", "").replace("```", ""))
        statements = new_statements

    new_statements = []
    for code in statements:
        if "\n\n" in code or ";" in code:
            split = re.split(r';|\n\n', code)
            new_statements += [
                x.strip() for x in split 
                if x.strip() and (x.strip().lower().startswith("select") or x.strip().lower().startswith("with"))
            ]
    if new_statements:
        statements = new_statements
    return statements

def parse_statements_mistral(text):
    if "```" in text:
        pattern = r"```(\w+)\s+(.*?)```"
        matches = re.findall(pattern, text, re.DOTALL)
        statements = [code for _, code in matches] 
    elif "\n\n" in text or ";" in text:
        split = re.split(r';|\n\n', text)
        statements = [x.strip() for x in split if x.strip() and x.strip().lower().startswith("select")]
    elif text.strip().lower().startswith("select"):
        statements = [text.strip()]
    else:
        print(f'Something wrong: {text}')
        statements = []
    
    new_statements = []
    for stat in statements:
        # assert "```" not in stat and "\n\n" not in stat and stat.lower().startswith("select"), stat
        new_statements.append(stat.replace("\\", ""))
    statements = new_statements

    return statements

def parse_statements_mixtral(text):
    if "```" in text:
        pattern = r"```(\w+)\s+(.*?)```"
        matches = re.findall(pattern, text, re.DOTALL)
        statements = [code for _, code in matches] 
    elif "\n\n" in text or ";" in text:
        split = re.split(r';|\n\n', text)
        statements = [x.strip() for x in split if x.strip() and (x.strip().lower().startswith("select") or x.strip().lower().startswith("with"))]
    elif text.strip().lower().startswith("select"):
        statements = [text.strip()]
    else:
        print(f'Something wrong: {text}')
        statements = []
    
    new_statements = []
    for stat in statements:
        new_statements.append(stat.replace("\\", ""))
    statements = new_statements

    return statements

def parse_single_statement(text):
    text = text.replace("SQL query(s):", "")
    text = text.strip()
    if text.lower().startswith("assistant\n\n"):
        text = text[len("assistant\n\n"):].strip()
    
    # Extract code blocks within triple backticks
    pattern = r"```(?:\w+)?\s+(.*?)```"
    matches = re.findall(pattern, text, re.DOTALL)

    if matches:
        text = "\n\n\n".join(matches)

    text = '\n'.join([row.strip() for row in text.split('\n')])
    text = text.replace('<step>', '')

    statements = re.split(r'\d+\.\s*', text)
    if statements:
        text = "\n\n\n".join(statements)

    text = text.replace("```sql\n", "").replace("```", "")
    statements = sqlparse.split(text)
    
    statements = list(dict.fromkeys(statements))
    if statements:
        new_statements = []

        for stat in statements:
            new_statements.append(stat.replace("\\", "").replace("```sql\n", "").replace("```", ""))
        statements = new_statements
        text = "\n\n\n".join(statements)

    return text.split("\n\n\n")

def parse_statements_llama(text):
    statements = []
    # Strip initial "assistant\n\n" if present
    text = text.replace("SQL query(s):", "")
    text = text.strip()
    if text.lower().startswith("assistant\n\n"):
        text = text[len("assistant\n\n"):].strip()
    
    # Extract code blocks within triple backticks
    pattern = r"```(?:\w+)?\s+(.*?)```"
    matches = re.findall(pattern, text, re.DOTALL)
    
    if matches:
        statements = matches  # Directly use the matches list
    else:
        statements = [text]  # Use the whole input if no code blocks are found

    new_statements = []
    for code in statements:
        if "\n\n" in code or ";" in code:
            # Split on newlines or semicolons
            split = re.split(r';|\n\n', code)
            new_statements += [
                x.strip() for x in split 
                if x.strip() and (x.strip().lower().startswith("select") or x.strip().lower().startswith("with"))
            ]
    if new_statements:
        statements = new_statements
    
    if not statements and (text.strip().lower().startswith("select") or text.strip().lower().startswith("with")):
        statements = [text.strip()]

    if not statements:
        print(f'Something wrong: {text}')
        statements = []
    
    # Final cleanup
    new_statements = [stat.replace("\\", "") for stat in statements]
    statements = new_statements

    return statements

def parse_statements_codellama(text):
    text = '\n'.join([row.strip() for row in text.split('\n')])
    text = text.replace('<step>', '')

    statements = []
    # Strip initial "assistant\n\n" if present
    text = text.replace("SQL query(s):", "")
    text = text.strip()
    if text.lower().startswith("assistant\n\n"):
        text = text[len("assistant\n\n"):].strip()

    statements = re.split(r'\d+\.\s*', text)

    if not statements:
    
        # Extract code blocks within triple backticks
        pattern = r"```(?:\w+)?\s+(.*?)```"
        matches = re.findall(pattern, text, re.DOTALL)
        
        if matches:
            statements = matches  # Directly use the matches list
        else:
            statements = [text]  # Use the whole input if no code blocks are found

    new_statements = []
    for code in statements:
        if "\n\n" in code or ";" in code:
            # Split on newlines or semicolons
            split = re.split(r';|\n\n', code)
            new_statements += [
                x.strip() for x in split 
                if x.strip() and (x.strip().lower().startswith("select") or x.strip().lower().startswith("with"))
            ]
    if new_statements:
        statements = new_statements
    

    if not statements and (text.strip().lower().startswith("select") or text.strip().lower().startswith("with")):
        statements = [text.strip()]

    if not statements:
        print(f'Something wrong: {text}')
        statements = []
    
    # Final cleanup
    new_statements = [stat.replace("\\", "") for stat in statements]
    statements = new_statements

    return statements

def parse_ambig_detection(text):
    text = text.strip()
    if text.lower().startswith("assistant\n\n"):
        text = text[len("assistant\n\n"):].strip()

    if "yes" in text.lower():
        return "yes"
    elif "no" in text.lower():
        return "no"
    else:
        return 'n/a'