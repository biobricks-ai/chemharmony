import os, shutil, pathlib

def mk_empty_dir(dir_path):
    if os.path.exists(dir_path):
        shutil.rmtree(dir_path)
    path = pathlib.Path(dir_path)
    path.mkdir(parents=True)
    return path