class DublicatesError(Exception):
    def __init__(self, duplicates, duplicate_idx):
        self.duplicates = duplicates
        self.duplicate_idx = duplicate_idx
        super().__init__(f"Duplicates - {duplicate_idx} is the same: {duplicates}")

    def __str__(self):
        return f"Duplicates - {self.duplicate_idx} is the same: {self.duplicates}"

class MetricCheckError(Exception):
    def __init__(self, gold_res, gold_query, original_exception):
        self.gold_res = gold_res
        self.gold_query = gold_query
        self.original_exception = original_exception
        super().__init__((f"An error:\n"
                          f"{self.original_exception}\n"
                          f"occurred when comparing execution result of these queries:\n"
                          f"Gold: {self.gold_query}\n"
                          f"Gold Res: {self.gold_res}\n"
                          ))

    def __str__(self):
        return (f"An error:\n"
                f"{self.original_exception}\n"
                f"occurred when comparing execution result of these queries:\n"
                f"Gold: {self.gold_query}\n"
                f"Gold Res: {self.gold_res}\n"
                )

class MetricError(Exception):
    def __init__(self, pred_res, gold_res, pred_query, gold_query, original_exception):
        self.pred_res = pred_res
        self.gold_res = gold_res
        self.pred_query = pred_query
        self.gold_query = gold_query
        self.original_exception = original_exception
        super().__init__((f"An error:\n"
                          f"{self.original_exception}\n"
                          f"occurred when comparing execution result of these queries:\n"
                          f"Gold: {self.gold_query}\nPred: {self.pred_query}\n"
                          f"Gold Res: {self.gold_res}\nPred Res: {self.pred_res}"
                          ))

    def __str__(self):
        return (f"An error:\n"
                f"{self.original_exception}\n"
                f"occurred when comparing execution result of these queries:\n"
                f"Gold: {self.gold_query}\nPred: {self.pred_query}\n"
                f"Gold Res: {self.gold_res}\nPred Res: {self.pred_res}"
                )

class GoldQueryExecutionError(Exception):
    def __init__(self, query, original_exception):
        self.query = query
        self.original_exception = original_exception
        super().__init__(f"An error occurred when executing the query: {query}")

    def __str__(self):
        return f"{self.original_exception} occurred during the execution of query: {self.query}"

class EmptyGoldQueryExecutionError(Exception):
    def __init__(self, query):
        self.query = query
        super().__init__(f"Empty result of executing the query: {query}")

    def __str__(self):
        return f"Empty result of executing the query: {self.query}"
    
class DuplicatesTableScopeError(Exception):
    def __init__(self, table):
        self.table = table
        super().__init__(f"Duplicates in table: {table}")

    def __str__(self):
        return f"Duplicates in table: {self.table}"

class PredQueryExecutionError(Exception):
    def __init__(self, query, original_exception):
        self.query = query
        self.original_exception = original_exception
        super().__init__(f"{self.original_exception} error occurred when executing the query: {query}")

    def __str__(self):
        return f"{self.original_exception} occurred during the execution of query: {self.query}"

    def to_dict(self):
        """Convert exception data to a dictionary, which can then be easily serialized to JSON."""
        return {
            'query': self.query,
            'original_exception': f"{self.original_exception}"
        }