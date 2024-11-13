from datetime import datetime
from pathlib import Path


class Args:
    C_ARG_TYPES = {
        'VALUE': 0,
        'LIST': 1,
        'NO_VALUE': 2
    }

    C_ALL_ARGS = {
        '-i': '--in_dirs',
        '--in_dirs': '-i',
        '-o': '--out_dir',
        '--out_dir': '-o',
        '-v': '--verbose',
        '--verbose': '-v',
        '-p': '--progress_bar',
        '--progress_bar': '-p',
        '-s': '--show_steps',
        '--show_steps': '-s',
        '-t': '--threads',
        '--threads': '-t'
    }

    class Arg:
        current_index = 0

        def __init__(self, c_name, arg_type, value=None):
            self.C_NAME = c_name
            self.value = value
            self.is_set = False
            self.arg_type = arg_type

        def is_arg(self, arg):
            return arg in self.C_NAME

        def get_args(self, args):
            can_start_fetching_args = False
            # second iteration, corner case
            is_second_loop = False
            for i in range(len(args)):
                self.current_index = i
                if self.is_arg(args[i]) and can_start_fetching_args is False:
                    can_start_fetching_args = True
                    is_second_loop = True
                    # arguments without a value e.g. --verbose, --progress_bar, etc.
                    if self.arg_type is Args.C_ARG_TYPES['NO_VALUE']:
                        self.is_set = True
                        self.current_index = self.current_index + 1
                        return
                    continue
                if self.is_arg(args[i]) and can_start_fetching_args is True:
                    # e.g. -i --input_dirs // use the same arg twice in succession
                    self.current_index = -1
                    return None
                if i == len(args) and is_second_loop:
                    # e.g. -i // last arg in cmd line but no actual values
                    self.current_index = -1
                    return None
                if args[i] in Args.C_ALL_ARGS and is_second_loop:
                    # reached second argument in cmd line break
                    break
                if can_start_fetching_args and i == 1:
                    # arg is not specified e.g. --in_dirs -v // input dirs not specified
                    if args[i] in Args.C_ALL_ARGS:
                        self.current_index = -1
                        return None
                if can_start_fetching_args and self.arg_type is Args.C_ARG_TYPES['VALUE'] and i == 1:
                    # arg is a single value but there is no new arg name after it
                    # and arg is not the last argument in the command line. Basically a list
                    # when the arg is only a value
                    self.value = args[i]
                    if i + 1 < len(args) and (args[i + 1] not in Args.C_ALL_ARGS):
                        self.current_index = -1
                        return None
                    self.current_index = self.current_index + 1
                    return

                if (can_start_fetching_args and self.arg_type is Args.C_ARG_TYPES['LIST'] and
                        args[i] not in Args.C_ALL_ARGS):
                    # fetch args until the next arg name
                    # e.g. --in_dir dir1 dir 2 --out_dir stop at --out_dir
                    self.value.append(args[i])
                    # case when there's only one list argument given
                    # index has to be incremented +1
                    if self.current_index + 1 == len(args):
                        self.current_index = self.current_index + 1

            self.is_set = True

    # args
    C_IN_DIRS = ['-i', '--in_dirs']

    def get_in_dirs(self):
        return self.attr_in_dirs.value

    def set_in_dirs(self, in_dirs):
        self.attr_in_dirs = in_dirs

    C_OUT_DIR = ['-o', '--out_dir']

    def get_out_dir(self):
        return self.attr_out_dir.value

    C_VERBOSE = ['-v', '--verbose']

    def get_verbose(self):
        return self.attr_verbose.is_set

    C_PROGRESS_BAR = ['-p', '--progress_bar']

    def get_progress_bar(self):
        return self.attr_progress_bar.is_set

    C_SHOW_STEPS = ['-s', '--show_steps']

    def get_show_steps(self):
        return self.attr_show_steps.is_set

    C_N_THREADS = ['-n', '--threads']

    def get_n_threads(self):
        if not self.attr_n_threads.value:
            return 1
        return int(self.attr_n_threads.value)

    def get_script_wd(self):
        return self.cwd

    are_all_args_valid = True
    are_all_mandatory_args_set = True

    def __init__(self, cmd_line=''):
        self.args = {}
        self.cmd_line = cmd_line

        self.cwd = ''
        if cmd_line:
            path = Path(cmd_line[0])
            self.cwd = str(path.parent)

        self.attr_in_dirs = Args.Arg(Args.C_IN_DIRS, Args.C_ARG_TYPES['LIST'], value=[])
        self.args[self.C_IN_DIRS[0]] = self.attr_in_dirs
        self.args[self.C_IN_DIRS[1]] = self.attr_in_dirs

        c_default_out_dir = 'out_dir_'
        dt = datetime.now()
        ts = dt.strftime("%Y%b%d_%H%M%S_%f")
        default_out_dir = c_default_out_dir + ts
        self.attr_out_dir = Args.Arg(self.C_OUT_DIR, self.C_ARG_TYPES['VALUE'], value=default_out_dir)
        self.args[self.C_OUT_DIR[0]] = self.attr_out_dir
        self.args[self.C_OUT_DIR[1]] = self.attr_out_dir

        self.attr_verbose = self.Arg(self.C_VERBOSE, self.C_ARG_TYPES['NO_VALUE'])
        self.args[self.C_VERBOSE[0]] = self.attr_verbose
        self.args[self.C_VERBOSE[1]] = self.attr_verbose

        self.attr_progress_bar = self.Arg(self.C_PROGRESS_BAR, self.C_ARG_TYPES['NO_VALUE'])
        self.args[self.C_PROGRESS_BAR[0]] = self.attr_progress_bar
        self.args[self.C_PROGRESS_BAR[1]] = self.attr_progress_bar

        self.attr_show_steps = self.Arg(self.C_SHOW_STEPS, self.C_ARG_TYPES['NO_VALUE'])
        self.args[self.C_SHOW_STEPS[0]] = self.attr_show_steps
        self.args[self.C_SHOW_STEPS[1]] = self.attr_show_steps

        self.attr_n_threads = self.Arg(self.C_N_THREADS, self.C_ARG_TYPES['VALUE'])
        self.args[self.C_N_THREADS[0]] = self.attr_n_threads
        self.args[self.C_N_THREADS[1]] = self.attr_n_threads

        if self.cmd_line and len(cmd_line):
            self.process_cmdline_args(self.cmd_line)

    def process_cmdline_args(self, param_args):
        n_args = len(param_args)
        if n_args == 1:
            self.are_all_args_valid = False
            self.are_all_mandatory_args_set = False
            return

        #  0 is the script
        i = 1
        while i < n_args:
            cur_arg = self.args[param_args[i]]
            cur_arg.get_args(param_args[i:])
            if cur_arg.current_index < 0:
                self.are_all_args_valid = False
                break

            i = i + cur_arg.current_index

    def are_args_correct(self):
        if not self.are_all_args_valid and not self.are_all_mandatory_args_set:
            return False

        if self.get_in_dirs() is None or len(self.get_in_dirs()) == 0:
            self.are_all_mandatory_args_set = False

        return self.are_all_mandatory_args_set and self.are_all_args_valid
