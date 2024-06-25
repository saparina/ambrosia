from collections import Counter
import sqlite3

from exceptions import DublicatesError, MetricError, GoldQueryExecutionError, PredQueryExecutionError

def sort_key(x):
    if x is None:
        return (0, '')  # Treat None as the smallest value
    elif isinstance(x, (int, float)):
        return (1, float(x))  # Handle numerical types uniformly
    else:
        return (2, str(x))  # Convert all other types to string for consistent comparison

def sort_with_different_types(arr):
    sorted_arr = sorted(arr, key=sort_key)
    return sorted_arr

def compare_query_results(predicted_results, gold_results, order_by=False):
    if not predicted_results:
        return False

    if order_by:
        if len(gold_results) != len(predicted_results):
            return False
    
        if any(len(row) != len(gold_results[0]) for row in gold_results + predicted_results):
            return False
    
        for gold_row, predicted_row in zip(gold_results, predicted_results):
            if tuple(sort_with_different_types(gold_row)) != tuple(sort_with_different_types(predicted_row)):
                return False
        return True
    else:
        flat_gold = Counter(item for row in gold_results for item in row)
        flat_predicted = Counter(item for row in predicted_results for item in row)

        return flat_gold == flat_predicted

def duplicate_exact(results):
    # Convert each result set into a tuple of tuples to make them hashable
    hashable_results = [tuple(map(tuple, result)) for result in results]
    
    # Use a set to identify duplicates
    seen = set()
    for idx, result in enumerate(hashable_results):
        if result in seen:
            return True, result, idx + 1
        seen.add(result)
    return False, None, None

def count_unique_results(results):
    # Use a set to identify duplicates
    seen = set()
    
    for result in results:
        if isinstance(result, PredQueryExecutionError):
            seen.add(None)
        else:
            hashable_result = tuple(map(tuple, result))
            seen.add(hashable_result)
    
    return len(seen)

def remove_duplicate_results(all_pred_exec_outputs):
    # List of keys to remove
    keys_to_remove = []

    # List of queries already processed
    processed_queries = list(all_pred_exec_outputs.keys())
    
    for i, query1 in enumerate(processed_queries):
        if query1 in keys_to_remove:
            continue  # Skip if this key is already marked for removal
        
        result1 = all_pred_exec_outputs[query1]

        if isinstance(result1, PredQueryExecutionError):
            result1 = None
        
        for query2 in processed_queries[i+1:]:
            if query2 in keys_to_remove:
                continue  # Skip if this key is already marked for removal
            
            result2 = all_pred_exec_outputs[query2]
            if isinstance(result2, PredQueryExecutionError):
                result2 = None

            order_by = 'order by' in query1.lower() or 'order by' in query2.lower()
            
            if compare_query_results(result1, result2, order_by):
                # Mark query2 for removal if it's a duplicate of query1
                keys_to_remove.append(query2)
    
    # Remove the marked keys from the dictionary
    for key in keys_to_remove:
        del all_pred_exec_outputs[key]
    
    return all_pred_exec_outputs


def evaluate_predicted_statements(file_name, pred_statements, gold_sql_queries, remove_duplicates_predictions=False, verbose=False):
    conn = sqlite3.connect(file_name)
    cursor = conn.cursor()

    all_gold_exec_outputs = {}
    exec_acc_per_gold_queries = {}
    for query in gold_sql_queries:
        try:
            cursor.execute(query)
            all_gold_exec_outputs[query] = cursor.fetchall()
            exec_acc_per_gold_queries[query] = False
        except sqlite3.DatabaseError as e:
            raise GoldQueryExecutionError(query, e)

    has_duplicates, duplicates, duplicate_idx = duplicate_exact(list(all_gold_exec_outputs.values()))
    if has_duplicates:
        raise DublicatesError(duplicates, duplicate_idx)

    all_pred_exec_outputs = {}
    num_queries = len(pred_statements)
    pred_statements = list(set(pred_statements))
    execution_errors = []
    for query in pred_statements:
        try:
            cursor.execute(query)
            all_pred_exec_outputs[query] = cursor.fetchall()
        except sqlite3.DatabaseError as e:
            all_pred_exec_outputs[query] = PredQueryExecutionError(query, e)
            execution_errors.append(PredQueryExecutionError(query, e).to_dict())

            error_message = str(e).lower()
            ignore_errors = ["no such table", "no such column", "ambiguous"]
            
            if verbose and not any(ignore in error_message for ignore in ignore_errors):
                print(f'\nCannot execute {query}\nError: {e}\n{file_name}\n\n')

    if remove_duplicates_predictions:
        all_pred_exec_outputs = remove_duplicate_results(all_pred_exec_outputs)

    exec_acc_per_pred_queries = {query: False for query in pred_statements} 
    for pred_query, pred_exec_output in all_pred_exec_outputs.items():
        if isinstance(pred_exec_output, PredQueryExecutionError):
            continue
        for gold_query, gold_exec_output in all_gold_exec_outputs.items():
            try:
                if 'order by' in gold_query.lower():
                    is_same = compare_query_results(pred_exec_output, gold_exec_output, order_by=True)
                else:
                    is_same = compare_query_results(pred_exec_output, gold_exec_output, order_by=False)
                if is_same:
                    exec_acc_per_gold_queries[gold_query] = True
                    exec_acc_per_pred_queries[pred_query] = True
            except Exception as e:
                raise MetricError(pred_exec_output, gold_exec_output, pred_query, gold_query, e)
                
    recall = sum(exec_acc_per_gold_queries.values()) / len(gold_sql_queries)
    all_found =  sum(exec_acc_per_gold_queries.values()) == len(gold_sql_queries)
    precision = sum(exec_acc_per_pred_queries.values()) / len(pred_statements) if pred_statements else 0
    f1_score = 2 * precision * recall / (precision + recall) if precision + recall > 0 else 0

    if remove_duplicates_predictions:
        all_pred_exec_outputs_wo_duplicates = all_pred_exec_outputs
    else:
        all_pred_exec_outputs_wo_duplicates = remove_duplicate_results(all_pred_exec_outputs)
    unique_results = count_unique_results(list(all_pred_exec_outputs.values()))
    unique_results_filtered = count_unique_results(list(all_pred_exec_outputs_wo_duplicates.values()))

    metrics = {
                'recall': recall,
                'precision': precision,
                'f1_score': f1_score,
                'num_queries': num_queries,
                'num_unique_queries': len(pred_statements),
                'unique_results': unique_results,
                'unique_results_filtered': unique_results_filtered,
                'execution_errors': execution_errors,
                'all_found': all_found
            }
    
    return metrics