# This script is used to insert data into the database
import os
import json
#from dotenv import load_dotenv
from datasets import load_dataset
import pandas as pd
from utils import fast_pg_insert

# TODO: Read the embedding files
directory = "../embedding"
#df = movielens.load_pandas_df(size="100k")

embeddings = pd.DataFrame()
for root, dirs, files in os.walk(directory):
    for file_name in files:
        embeddings = pd.concat([embeddings, pd.read_json(f"{directory}/{file_name}", lines=True)])

print(embeddings['response'].apply(lambda x: x['response']['embeddings']))
#print("Dataframes: ", embeddings)
#final_embeddings = embeddings['custom_id']
#embeddings_columns = pd.json_normalize(embeddings['response'])['body.data']
#print(embeddings_columns)
#final_embeddings[embeddings] = pd.json_normalize(pd.json_normalize(embeddings['response'], max_level=0)['body'], max_level=0)


'''directory = "../documents"

documents = pd.DataFrame()
for root, dirs, files in os.walk(directory):
    for file_name in files:
        documents = pd.concat([documents, pd.read_json(f"../documents/{file_name}", lines=True)])

#print("Dataframs: ", documents)

documents_columns = documents[['custom_id','input','metadata']]
print(documents_columns)'''


# TODO: Read documents files

# HINT: In addition to the embedding and document files you likely need to load the raw data via the hugging face datasets library
ds = load_dataset("Whispering-GPT/lex-fridman-podcast")


# TODO: Insert into postgres
# HINT: use the recommender.utils.fast_pg_insert function to insert data into the database
# otherwise inserting the 800k documents will take a very, very long time