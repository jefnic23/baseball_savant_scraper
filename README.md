# BaseballSavant Scraper

Python script that scrapes data from [BaseballSavant](https://baseballsavant.mlb.com/) into a database.

## Requirements

- Python 3.X
- One of these databases:
  - PostgreSQL
  - MySQL/MariaDB
  - SQLite
  - SQL Server
  - Oracle

## Installation

Download the repo using **Code > Download ZIP**, or clone the repo using git bash.

```bash
git clone https://github.com/jefnic23/baseball_savant_scraper.git
```

## Setup

Open a console inside the `baseball_savant_scraper` directory and create a virtual environment.

```bash
# cd path/to/baseball_savant_scraper
python.exe -m venv venv
```

Activate the virtual environment.

```bash
venv\scripts\activate
```

Then install dependencies.

```bash
pip install -r requirements.txt
```

## Usage

Once everything is setup and dependencies are installed, create a ```.env``` file in the root directory.

```bash
type NUL > .env
```

Open the file in a text editor and set the CONNECTION_STRING variable for your database.

```env
CONNECTION_STRING=''
```

Finally, run the script from the console.

```bash
python savant_scraper.py
```

If you're running the script for the first time, it may take up to a half hour to gather all the data; subsequent updates will check for the most recent date in the database and gather data since then. The script uses `tqdm` to generate progress bars for each season and week loop, so you'll know how long to expect the process to take.

### Related

- [wES](https://github.com/jefnic23/wES)
- [hEV](https://github.com/jefnic23/hEV)
- [SBot](https://github.com/jefnic23/SBot)
