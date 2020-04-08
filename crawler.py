from bs4 import BeautifulSoup
import pandas as pd
import requests
import time
  
wiki_url = 'https://en.wikipedia.org/wiki/Microsoft'
err_code_success = 0
err_code_fail = 1
row_count = 10

def request_with_retry(url):
  # Retry 5 times with exponential backoff
  num_retries = 5
  retry_durations_in_seconds = [2, 4, 8, 16, 32]
  success = False
  content = None
  for n in range(num_retries):
    req = requests.get(url=url)
    if req.status_code == 200:
      success = True
      content = req.content
      break
    else:
      next_duration = retry_durations_in_seconds[n]
      print('Request failed with status: %d. Retrying in %ds...' % (req.status_code, next_duration))
      time.sleep(next_duration)
  
  return (success, content)

def extract_history_section_text(html_content):
  page = BeautifulSoup(content, 'html.parser')
  span = page.find(id='History')
  current_element = span.parent.find_next_sibling()
  current_element = current_element.find_next_sibling() # skip "Main articles" text
  sections = []
  while current_element and current_element.name != 'h2' and current_element.name != 'h1':
    sections.append(current_element.get_text())
    current_element = current_element.find_next_sibling()
  return pd.DataFrame({'Section': sections})

success, content = request_with_retry(wiki_url)
if not success:
  print('Request exceeded maximum retries. Exiting.')
  exit(err_code_fail)

text_data = extract_history_section_text(content)

words_split = text_data.Section.str.strip().str.title().str.replace('[0-9]', '').str.split('[\\W_]+')

words_exploded = pd.DataFrame(words_split.to_list()).stack().reset_index(drop=True)

counts = words_exploded.value_counts(sort=True, ascending=False).rename_axis('Words').reset_index(name='# of Occurences')

counts.to_csv('full_output.csv')

print(counts.head(row_count).to_string(index=False, min_rows=row_count))

exit(err_code_success)