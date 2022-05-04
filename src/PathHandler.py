import os
import pathlib


class PathHandler:

    def __init__(self):
        self.__dir_script_run = pathlib.Path(__file__).parent.resolve()
        self.__dir_working_directory = os.path.abspath(os.getcwd())
        self.__dir_parent_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

    def get_script_run_dir(self):
        return self.__dir_script_run

    def get_working_dir(self):
        return self.__dir_working_directory

    def get_parent_dir(self):
        return self.__dir_parent_directory


if __name__ == '__main__':
    dir = PathHandler()
    script = dir.get_script_run_dir()
    working_dir = dir.get_working_dir()
    parent_dir = dir.get_parent_dir()
    print(script)
    print(working_dir)
    print(parent_dir)
