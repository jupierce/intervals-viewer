# Intervals Viewer

## Installation

## System Dependencies
- On Linux, install `xclip` through dnf or your distribution's package manager.
- Requires Python3.6 or later.

## Installation
```bash
$ git clone https://github.com/jupierce/intervals-viewer.git
$ cd intervals-viewer

# Create a venv in the cloned directory.
intervals-viewer$ python -m venv venv

# Activate the virtual environment ("venv\Scripts\activate" on Windows).
intervals-viewer$ source venv/bin/activate

# Install all dependencies
(venv) intervals-viewer$ pip install -r requirements.txt
```

## Run
```bash
# If you have not activated the env, do so (("venv\Scripts\activate" on Windows). 
intervals-viewer$ source venv/bin/activate

# Run the application.
(venv) intervals-viewer$ python main.py
```