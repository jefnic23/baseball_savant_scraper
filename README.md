# Baseball Savant Scraper

Python script that scrapes all data from Baseball Savant into .csv files.

## Installation

Download the repo using **Code > Download ZIP**, or clone the repo using git bash.

```bash
git clone https://github.com/jefnic23/baseball_savant_scraper.git
```

## Setup

Open a console inside the *baseball_savant_scraper* directory and activate the virtual environment.

```bash
venv\scripts\activate
```

Then install dependencies.

```bash
pip install -r requirements.txt
```

## Usage

Once everything is setup and dependencies are installed, run the script from the console. Files are saved to the *data* directory.

```bash
python savant_scraper.py
```

The script will take roughly 120-150 minutes to run. A progress bar will keep track of how far along in the process things are.

If you only need a small subset of data you can edit the start and/or end dates
in the *savant_scraper.py* file on lines 17, 19, and 20, or the years on line 13.
