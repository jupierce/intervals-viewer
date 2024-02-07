# Intervals Viewer

## Installation
Requires Python3.6 or later.

## System Dependencies
Fedora/RHEL quick paste: `sudo dnf install xclip libjpeg-devel zlib-devel`
 
On Linux, install the following through dnf or your distribution's package manager.
  - `xlcip` - Required for clipboard support.
  - [Python Pillow Dependencies](https://pillow.readthedocs.io/en/latest/installation.html)
  
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