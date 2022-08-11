# BaseballSavant Scraper

Python script that scrapes data from [BaseballSavant](https://baseballsavant.mlb.com/) into a SQL database.

## Requirements

- Python 3.X
- PostgreSQL

## Installation

Download the repo using **Code > Download ZIP**, or clone the repo using git bash.

```bash
git clone https://github.com/jefnic23/baseball_savant_scraper.git
```

## Setup

Open a console inside the `baseball_savant_scraper` directory and create a virtual enviroment. 

```bash
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

Open the file in a text editor and copy these five variables, then assign them the appropriate value (make sure to enter the values between the quotation marks).

```
USER=''
PSWD=''
HOST=''
PORT=''
NAME=''
```

Finally, run the script from the console.

```bash
python savant_scraper.py
```

If you're running the script for the first time, it will take over an hour to gather all the data; subsequent updates will check for the most recent date in the database and gather data since then. The script uses tqdm to generate progress bars for each season and week loop, so you'll know how long to expect the process to take.

#### Related

- [wES](https://github.com/jefnic23/wES)
- [hEV](https://github.com/jefnic23/hEV)
- [SBot](https://github.com/jefnic23/SBot)