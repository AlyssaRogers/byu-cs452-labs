import os
import json
import shutil
#from dotenv import load_dotenv
from datasets import load_dataset
import pandas as pd
from utils import fast_pg_insert
from tqdm import tqdm

# TODO: Read the embedding files
#df = movielens.load_pandas_df(size="100k")

CONNECTION = "postgres://tsdbadmin:kwmtav03ndd501ym@mqeecwb2xv.l2ecxhvt0m.tsdb.cloud.timescale.com:39779/tsdb?sslmode=require"
embed_dir = "../embedding"
batch_dir = "../documents"

batch_ids = []

for f in os.listdir(embed_dir):
    batch_ids.append(f[:-6])

batch_files = []
for batch_file in os.listdir(batch_dir):
    if not batch_file.endswith(".jsonl"):
        continue
    batch_files.append(batch_file)

batch_dict = dict(zip(batch_ids, batch_files))

emded_dict = {}
for embed_file in os.listdir(embed_dir):
    if not embed_file.endswith(".jsonl"):
        continue
    embed_id = embed_file.split(".")[0]
    emded_dict[embed_id] = embed_file

print("reading data:")
documents = []
embeddings = []
for batch_id, batch_file in tqdm(batch_dict.items()):
    with open(os.path.join(batch_dir, batch_file), "r") as f:
        documents.extend([json.loads(line) for line in f.readlines()])
    with open(os.path.join(embed_dir, emded_dict[batch_id]), "r") as f:
        embeddings.extend([json.loads(line) for line in f.readlines()])
num_docs = len(documents)
assert num_docs == len(embeddings)
print("complete")

print("Copying batch request files")
if not os.path.exists("output_batch"):
    os.mkdir("output_batch")

for batch_id, batch_file in tqdm(batch_dict.items()):
    shutil.copy(
        os.path.join(batch_dir, batch_file),
        os.path.join("output_batch", f"batch_request_{batch_id}.jsonl")
    )

print("Data read successfully!")

ds = load_dataset("Whispering-GPT/lex-fridman-podcast")

podcast_table_insert = []
num_podcasts = len(ds['train'])
print("Collecting podcasts")
for i in tqdm(range(num_podcasts)):
    podcast_id = ds['train'][i]['id']
    title = ds['train'][i]['title']
    podcast_table_insert.append((podcast_id, title))


segment_table_insert = []
print("Collecting segments")
for i in tqdm(range(num_docs)):
    podcast_id = documents[i]['body']['metadata']['podcast_id']
    start_time = documents[i]['body']['metadata']['start_time']
    stop_time = documents[i]['body']['metadata']['stop_time']

    seg_id = documents[i]['custom_id']
    doc = documents[i]['body']['input']
    e = json.dumps(embeddings[i]['response']['body']['data'][0]['embedding'])


    segment_table_insert.append((seg_id, start_time, stop_time, doc, e, podcast_id))

print("Inserting into podcast table")

podcast_df = pd.DataFrame(podcast_table_insert, columns=["id", "title"])
fast_pg_insert(
    df=podcast_df,
    connection=CONNECTION,
    table_name="podcast",
    columns=["id", "title"]
)
print("Values inserted successfully!")

segment_df = pd.DataFrame(segment_table_insert, columns=["id", "start_time", "end_time", "content", "embedding", "podcast_id"])
fast_pg_insert(
    df=segment_df,
    connection=CONNECTION,
    table_name="podcast_segment",
    columns=["id", "start_time", "end_time", "content", "embedding", "podcast_id"]
)

print("Values inserted successfully!")

'''# This script is used to insert data into the database
import os
import json
#from dotenv import load_dotenv
from datasets import load_dataset
import pandas as pd
import tqdm
from utils import fast_pg_insert

# TODO: Read the embedding files
directory = "../embedding"
#df = movielens.load_pandas_df(size="100k")

f = []

for root, dirs, files in os.walk(directory):
    for file_name in files:
        f.append(f)

batch_dict = defaultdict(f)

documents = []
embeddings = []

for batch_id, batch_file in batch_dict.items():
    with open(os.path.join(batch_dir, batch_file), "r") as f:
        documents.extend([json.loads(line) for line in f.readlines()])
    with open(os.path.join(embed_dir, emded_dict[batch_id]), "r") as f:
        embeddings.extend([json.loads(line) for line in f.readlines()])
num_docs = len(documents)

print(json.dumps(embeddings))

for root, dirs, files in os.walk(directory):
    for file_name in files:
        with open(f"{directory}/{file_name}", 'r') as file:
            for line in file:
                embeddings.append(line)
                
        #embeddings = pd.concat([embeddings, pd.read_json(f"{directory}/{file_name}", lines=True)])

#final_embeddings = embeddings['custom_id']
embeddings_columns = pd.json_normalize(embeddings['response'])
new_embeddings = pd.DataFrame(data=embeddings_columns)
new_embeddings = new_embeddings['body.data']

for i in embeddings:
    idStartIndex = i.find("\", \"custom_id")
    idEndIndex = i.find("\", \"response\": {\"st")
    #print(i[(idStartIndex):(idEndIndex)])

    embeddingsStartIndex = i.find("0, \"embedding\": [") 
    if counter == 0:
        print(i[embeddingsStartIndex+17:])
        counter = 1

#print("DUMPS: ", json.dumps(finalLine))

#json.dumps, table.append?


directory = "../documents"

documents = pd.DataFrame()
for root, dirs, files in os.walk(directory):
    for file_name in files:
        documents = pd.concat([documents, pd.read_json(f"../documents/{file_name}", lines=True)])

#print("Dataframs: ", documents)

documents_columns = documents[['custom_id','input','metadata']]
print(documents_columns)


# TODO: Read documents files

# HINT: In addition to the embedding and document files you likely need to load the raw data via the hugging face datasets library
ds = load_dataset("Whispering-GPT/lex-fridman-podcast")


# TODO: Insert into postgres
# HINT: use the recommender.utils.fast_pg_insert function to insert data into the database
# otherwise inserting the 800k documents will take a very, very long time'''