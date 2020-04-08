# Web Crawler Challenge

> Patrick S. Worthey | hello@pworthey.net | 509-339-3369

## Requirements

- Python 3.8 (although it would probably work if you curbed this requirement, it's just not tested) and an install of pipenv

## Basic Usage

To intall dependencies and activate virtual environment:
```bash
pipenv install
pipenv shell
```

To run crawler:
```bash
python crawler.py
```

Examine console output, and possibly the newly generated full_output.csv file if so inclined. To filter out words, modify config.json with new words.

## Testing

Run:
```bash
pytest
```