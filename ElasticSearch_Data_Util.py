


from elasticsearch import Elasticsearch
import pandas as pd
import os
from datetime import datetime

def query_elasticsearch_incremental(es, index_name, query, batch_size=1000):
    query['size'] = batch_size
    page = None
    scroll_id = None
    hits = None
    total_docs = 0
    
    while True:
        if page is None:
            page = es.search(index=index_name, body=query, scroll='2m')
            scroll_id = page['_scroll_id']
            hits = page['hits']['hits']
        else:
            page = es.scroll(scroll_id=scroll_id, scroll='2m')
            scroll_id = page['_scroll_id']
            hits = page['hits']['hits']
        
        if len(hits) == 0:
            break
        
        total_docs += len(hits)
        print(f"Processing {total_docs} documents from index '{index_name}'")
        
        data = [hit['_source'] for hit in hits]
        df = pd.DataFrame(data)
        
        yield df

def save_dataframe_incremental(df, file_path, is_first_batch):
    mode = 'w' if is_first_batch else 'a'
    header = is_first_batch
    df.to_csv(file_path, mode=mode, header=header, index=False)

# TODO: Change to Elasticsearch prod URL
es = Elasticsearch(['http://localhost:9200'])

# Define your indices and queries
indices_and_queries = {
    'cex-position-stats': {
        "query": {"match_all": {}}
    },
    'vault-position-stats': {
        "query": {"match_all": {}}
    },
    'global-data': {
        "query": {"match_all": {}}
    },
}

# Create 'data' directory if it doesn't exist
os.makedirs('data', exist_ok=True)

# Process each index
for index_name, query in indices_and_queries.items():
    print(f"Processing index: {index_name}")
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = f'data/{index_name}_{timestamp}.csv'
    
    is_first_batch = True
    total_docs = 0
    
    for df_batch in query_elasticsearch_incremental(es, index_name, query):
        save_dataframe_incremental(df_batch, file_path, is_first_batch)
        is_first_batch = False
        total_docs += len(df_batch)
    
    print(f"Total {total_docs} documents from index '{index_name}' saved to: {file_path}")
    print("\n")

print("Data export completed.")