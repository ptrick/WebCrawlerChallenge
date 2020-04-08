################################################
# Web Crawler Challenge
# April 7, 2020
# Patrick S. Worthey
# hello@pworthey.net
# 509-339-3369
################################################

# Hello! Welcome to my crawler assignment!
# I've made a few web crawlers in the past that were done using Chromium.
# More specifically, I used CefSharp with C# on those projects.
# So why did I decide to write this one in Python then?
# Well, a few reasons:
# - My other projects involved http request payloads that
#   were constructed with complex javascript logic. It could only
#   be done with something like Chromium. This project has none of 
#   that, so the lighter the weight, the better for brevity.
# - I want to promote the data science portion of this in particular.
#   Although I'm limited in what I can (or should) do here data 
#   pipeline-wise, the sky is the limit for doing things like making charts.
#   During a less ad-hoc situation, I would probably split the projects between
#   Webscrape data collection, data pipeline management, and data science.
#   More specifically, I would run scheduled C# webscrape jobs to write
#   cloud storage blobs (I have a lot of experience doing this for webscrapers), 
#   which would allow for the subsequent ETL processes and with no data loss.
#   When it comes time to do the data analysis, Data scientists would have 
#   their pick of data science tools, including something that resembles 
#   the bottom half of this Python script.

from bs4 import BeautifulSoup
from typing import List, Tuple, Union

import json
import pandas as pd
import requests
import time

# These are globals; runtime config is in config.json file.
wiki_url = 'https://en.wikipedia.org/wiki/Microsoft'
err_code_success = 0
err_code_fail = 1
config_file = 'config.json'
full_output_file = 'full_output.csv'

def load_config(filename: str) -> Tuple[bool, Union[object, None]]:
  """Loads the config file.
  Contains some basic error logging. For further validation,
  VSCode will automatically validate the config file as you
  write it (based on the config.schema.json Json schema file).

  :param filename: The filename to read
  :returns: A tuple containing a success flag (True=success, False=fail), and
  the json object.
  """
  success = False
  obj = None
  try:
    with open(filename, 'r') as fp:
      obj = json.load(fp)
      success = True
  except IOError:
    print('Failed to open config file.')
  except ValueError:
    print('Failed to parse config file. (JSON)')

  return (success, obj)

def request_with_retry(url: str) -> Tuple[bool, Union[bytes, None]]:
  """Sends a GET request (without additional headers) to the specified URL
  with an exponential backoff retry policy. Upon request failure, the function
  will sleep (stall thread) a certain amount of seconds. Each subsequent failure
  will exponentially increase the sleep time up to 4 times. The purpose of this
  is to give the program a high probability of recovering from any communication mishap.

  :param url: The http request url.
  :returns: A tuple containing a success flag (True=success, False=fail), and
  a bytes array object on success, None if fail.
  """
  num_tries = 5
  retry_durations_in_seconds = [2, 4, 8, 16]
  success = False
  content = None
  for n in range(num_tries):
    req = None
    requestSuccess = False
    try:
      req = requests.get(url=url)
      requestSuccess = True
    except requests.exceptions.RequestException as e:
      print('Request failed with a connection error: %s.' % (str(e)))

    if requestSuccess:
      if req.status_code == 200:
        success = True
        content = req.content
        break
      else:
        print('Request failed with status: %d.' % (req.status_code))

    last_retry = n == num_tries - 1
    if not last_retry:
      next_duration = retry_durations_in_seconds[n]
      print('Retrying in %ds...' % (next_duration))
      time.sleep(next_duration)
    else:
      print('Retries exhausted.')
  
  return (success, content)

def extract_history_section_text(html_content: bytes) -> pd.Series:
  """This will extract only words from the section 'history' as specified
    in the assignment.

    :param html_content: The html content, in bytes, of the webpage.
    :returns: A Pandas series constructed from the text in each html element.
  """
  page = BeautifulSoup(html_content, 'html.parser')
  span = page.find(id='History')
  current_element = span.parent.find_next_sibling()
  current_element = current_element.find_next_sibling() # skip "Main articles" text
  sections = []
  # This all seems to work as of now (4/8/2020)...
  # This could easily be invalidated with a wiki edit or redesign
  while current_element and current_element.name != 'h2' and current_element.name != 'h1':
    sections.append(current_element.get_text())
    current_element = current_element.find_next_sibling()

  # We're putting the text into Pandas at the earliest opportunity
  # so we can do all the data crunching in C++-written code behind 
  # the scenes for that perf boost
  return pd.Series(sections)

def transform_data(text_data: pd.Series) -> pd.Series:
  """Transforms text data into a word count series.

  :param text_data: A Pandas series with rows of text to mine.
  :returns: A Pandas series containing the word counts.
  """

  # A bit of a simplification made:
  # Words with differing capitalization will count as same word:
  # For instance: us, Us, and even US (which could be a problem)
  # Will use str.title() for formatting instead of lower() because
  # it looks nicer to display
  words_split = text_data \
    .str.strip() \
    .str.title() \
    .str.replace('[0-9]', '') \
    .str.split('[\\W_]+')

  # Pandas has many strengths, but this is not one of them.
  # I have a bunch of rows with word arrays, but now I need
  # one big massive series with all those words in it...
  # This is the most efficient way to do it I could find.
  words_exploded = pd.DataFrame(words_split.to_list()) \
    .stack() \
    .reset_index(drop=True) # No need to keep track of paragraph element indices

  # Now count up the values
  counts = words_exploded.value_counts(sort=True, ascending=False) \
    .rename_axis('Words') \
    .reset_index(name='# of Occurences')

  return counts

def filter_data(data: pd.Series, filter_words: List[str]) -> pd.Series:
  """Filters out rows in the data according to the filter word list.

  :param data: The data to filter
  :param filter_words: A list of words to filter out of the data.
  :returns: A subset of the input data
  """

  matching_caps = pd.Series(filter_words).str.title() # Make the capitalization the same
  return data[~data['Words'].isin(matching_caps)] # Filter

def main() -> None:
  """The main entrypoint of the program.
  """
  success, obj = load_config(config_file)
  if not success:
    print('Config load failed. Exiting.')
    exit(err_code_fail)

  row_count = obj['console_output_line_count']
  words_to_discard = obj['words_to_discard']

  success, content = request_with_retry(wiki_url)
  if not success:
    print('Request exceeded maximum retries. Exiting.')
    exit(err_code_fail)

  text_data = extract_history_section_text(content)

  counts = transform_data(text_data)
  
  filtered_counts = filter_data(counts, words_to_discard)
  filtered_counts.to_csv(full_output_file)

  print(filtered_counts.head(row_count).to_string(index=False, min_rows=row_count))

  exit(err_code_success)

if __name__ == "__main__":
  main()