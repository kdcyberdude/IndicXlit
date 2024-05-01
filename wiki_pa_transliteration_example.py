import pandas as pd
import sys
sys.path.append('./app')
from ai4bharat.transliteration import XlitEngine
import datasets
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

now = time.time()

def process_data(i, doc):
    # check how much time has passed since now
    print(f'{i} - Time elapsed: ', time.strftime("%H:%M:%S", time.gmtime(time.time() - now)))
    y = e.translit_sentence(doc, 'pa')
    return {i: y}

if not os.path.exists('../pb_test/pa_dataset'):
    print('Downloading pa dataset')
    dataset = datasets.load_dataset('wikimedia/wikipedia', '20231101.pa')
    dataset.save_to_disk('../pb_test/pa_dataset')
else:
    print('Loading pa dataset')
    dataset = datasets.load_from_disk('../pb_test/pa_dataset')
df = pd.DataFrame(dataset['train'])
print(df.shape)
df.rename(columns={'text': 'text_pb'}, inplace=True)
df.rename(columns={'title': 'title_pb'}, inplace=True)
df.reset_index(inplace=True)
#create a dict of index and text_pb
data = dict(zip(df.index, df.text_pb))


e = XlitEngine(beam_width=4, src_script_type='indic', lang2use='pa', batch_size = 1024)

futures = []
with ThreadPoolExecutor(max_workers=65) as executor:
    for i, doc in data.items():
        futures.append(executor.submit(process_data, i, doc))

results = [future.result() for future in as_completed(futures)]

for result in results:
    for key, value in result.items():
        df.loc[key, 'transliterated_text'] = value

df.to_csv('wiki_pa_transliteration.csv', index=True)

# save where transliterated_text is not None
dfx = df[df['transliterated_text'].notnull()]
dfx.to_csv('wiki_pat_transliteration_not_null.csv', index=True)

# for i, doc in enumerate(data):
#     print(i)
#     y = e.translit_sentence(doc, 'pa')
#     result.append(y)
    
