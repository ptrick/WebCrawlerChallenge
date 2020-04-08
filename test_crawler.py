# Run using pytest

import crawler
import pandas as pd

def test_extract_history_section_text():
  test_doc = """
    <!DOCTYPE html>
    <html>
      <head>
        <title>test</title>
      </head>
      <body>
        <p>This should not be in text</p>
        <h2>
          <span id="History">History</span>
        </h2>
        <div>Skip this!</div>
        <p>Some entry</p>
        <p>Another entry</p>
        <p>More words...</p>
        <h2>Now we are in another section</h2>
      </body>
    </html>
  """
  bts = test_doc.encode('utf-8')
  data = crawler.extract_history_section_text(bts)
  assert(len(data) == 3)
  vals = data.values
  assert(vals[0] == 'Some entry')

def test_transform_data():
  test_data = pd.Series([
    'Words words words',
    'I like words',
    'I could really use some WORDS right now.'
  ])
  output = crawler.transform_data(test_data)
  assert(len(output) == 10)
  vals = output.values
  assert(vals[0][0] == 'Words')
  assert(vals[0][1] == 5)

def test_filter_data():
  test_data = pd.Series(['Once', 'Once', 'Once', 'Upon', 'Upon', 'Upon', 'A', 'Time'])
  counts = test_data.value_counts() \
  .rename_axis('Words') \
  .reset_index(name='# of Occurences')

  filtered_data = crawler.filter_data(counts, ['Once', 'A', 'Notthere'])
  assert(len(filtered_data) == 2)

  vals = filtered_data.values
  assert(vals[0][0] == 'Upon')
  assert(vals[0][1] == 3)