# About this repo

This project started as a script meant to organize Google Photos exports. Google Photos uses a feature 'Takeout' to export files. This became necessary as I kept hitting the storage limit. 

There's one problem though, the exports contain all files up to that point in time meaning that they will contain duplicate files. For example, say that you make an export and then delete 

a number of files to free up space. So far so good, but what if two months later, you hit the storage limit again. You can redo the steps, export and delete but there will be  duplicate 

files in the new export (all files that have not been deleted since the last cleanup). 

These scripts aim to fix this by grouping all files into directories (each directory containing unique files taken in a given year) which has the benefit of being storage efficient (only 

media files are saved and their respective config files (jsons). 

And, it's also multithreded :) . 

## Usage 

`git clone 'https://github.com/msebi/google-photos-takeout-manager'
cd google-photos-takeout-manager
# create venv python >= 3.6
python -m venv /path/to/new/virtual/environment
(venv) pip install -r requirements.txt`

and run:

`(venv) python manage_google_photos.py --in_dirs path/to/takeout --out_dir images-backup --verbose --progress_bar --threads 8`


