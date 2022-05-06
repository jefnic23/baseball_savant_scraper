# Baseball Savant Scraper

Python script that scrapes data from Baseball Savant into .csv files.

## Requirements

- Python 3.X

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

Once everything is setup and dependencies are installed, run the script from the console and follow the prompts.

```bash
python savant_scraper.py
```

Files are saved to the `data` directory.

The script can take anywhere from a few to several minutes to run, depending on how large the date range is. A progress bar will keep track of how far along in the process things are.

#### Related

- [wES](https://github.com/jefnic23/wES)
- [hEV](https://github.com/jefnic23/hEV)
- [SBot](https://github.com/jefnic23/SBot)