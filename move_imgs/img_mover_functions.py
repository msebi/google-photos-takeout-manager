import errno
import hashlib
import ntpath
import os
import logging
import pathlib
import re
import shutil
import sys

logging.basicConfig(level=logging.NOTSET)
logger = logging.getLogger(__name__)

SHOW_DEBUG_MSGS = False


def rem_file(file_path):
    try:
        os.remove(file_path)
    except OSError as e:
        print("Error: %s - %s." % (e.filename, e.strerror))
        logger.error("Error: %s - %s." % (e.filename, e.strerror))


def user_said_yes(question, default="yes"):
    valid = {'yes': True, 'y': True, 'no': False, 'n': False}
    if default is None:
        prompt = '[y/n]'
    elif default == 'yes':
        prompt = '[Y/n]'
    elif default == 'no':
        prompt = '[y/N]'
    else:
        raise ValueError('Invalid default answer: \'%s\'' % default)

    while True:
        sys.stdout.write(question + prompt)
        choice = input().lower()
        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write('Please enter \'yes\' or \'no\' ' '(or \'y\' or \'n\').\n')


def get_file_extension(file_path):
    head, tail = ntpath.split(file_path)
    file_name = tail or ntpath.basename(tail)
    return os.path.splitext(file_name)[1]


def get_file_name(file_path):
    head, tail = ntpath.split(file_path)
    return tail or ntpath.basename(tail)


def get_file_path(file_path):
    head, tail = ntpath.split(file_path)
    return head or ntpath.basename(head)


def cp_recursive_overwrite(src, dest, ignore=None):
    if os.path.isdir(src):
        if not os.path.isdir(dest):
            os.makedirs(dest)
        files = os.listdir(src)
        if ignore is not None:
            ignored = ignore(src, files)
        else:
            ignored = set()
        for f in files:
            if f not in ignored:
                cp_recursive_overwrite(os.path.join(src, f),
                                       os.path.join(dest, f),
                                       ignore)
    else:
        shutil.copyfile(src, dest)


def rm_empty_dirs(directory, is_verbose=False):
    if os.path.isdir(directory):
        files = os.listdir(directory)
        for f in files:
            rm_empty_dirs(os.path.join(directory, f))
    try:
        os.rmdir(directory)
        if is_verbose:
            logger.info('Deleted empty directory \n\n\t{0}\n\n\t'.format(directory))
    except OSError as ex:
        if ex.errno == errno.ENOTEMPTY:
            # print('Dir not empty')
            return


def cp_media_object_to_dir(target_dir, media_object):
    """
    :param target_dir:
    :param media_object:
    :return:

    moves an image to a directory. Creates it if it doesn't exist
    """
    if os.path.exists(target_dir):
        shutil.copy(media_object.image_path, target_dir)
        path_parts = os.path.normpath(media_object.image_path).split(os.path.sep)
        media_object.image_path = os.path.join(target_dir, path_parts[len(path_parts) - 1])
        return

    # create dir, mv img
    pathlib.Path(target_dir).mkdir(parents=True, exist_ok=True)
    shutil.copy(media_object.image_path, target_dir)
    path_parts = os.path.normpath(media_object.image_path).split(os.path.sep)
    media_object.image_path = os.path.join(target_dir, path_parts[len(path_parts) - 1])


def mv_media_object_to_dir(target_dir, media_object):
    """
    :param target_dir:
    :param media_object:
    :return:

    moves an image to a directory. Creates it if it doesn't exist
    """
    if os.path.exists(target_dir):
        path_parts = os.path.normpath(media_object.image_path).split(os.path.sep)
        target_dir_str = os.path.join(target_dir, path_parts[len(path_parts) - 1])
        if media_object.image_path == target_dir_str:
            return
        shutil.move(media_object.image_path, target_dir)
        path_parts = os.path.normpath(media_object.image_path).split(os.path.sep)
        media_object.image_path = os.path.join(target_dir, path_parts[len(path_parts) - 1])
        return

    # create dir, mv img
    pathlib.Path(target_dir).mkdir(parents=True, exist_ok=True)
    shutil.move(media_object.image_path, target_dir)
    path_parts = os.path.normpath(media_object.image_path).split(os.path.sep)
    media_object.image_path = os.path.join(target_dir, path_parts[len(path_parts) - 1])


def is_video_or_image_file(file_path, extensions):
    extension = get_file_extension(file_path)

    # flatten extensions (1 level deep lists)
    abc = extensions
    flattened_extensions = [a for ab in abc for a in ab]

    is_media_file = False
    if extension in flattened_extensions:
        is_media_file = True

    return is_media_file


def compute_sha256_of_file(file_path):
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        # read and update hash string value in blocks of 4K
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256.update(byte_block)
        sha256_hex = sha256.hexdigest()

    return sha256_hex


def is_file_in_print_order_dir(file_path, print_order_regex):
    path_parts = os.path.normpath(file_path).split(os.path.sep)
    for p in path_parts:
        m = re.search(print_order_regex, p)
        if m:
            return True
    return False


img_file_ext = ['.apng', '.png', '.avif', '.gif', '.jpg', '.jpeg', '.jfif', '.pjpeg',
                '.pjp', '.svg', '.webp', '.bmp', '.ico', '.tiff']

vid_file_ext = ['.mp3', '.mp4', '.avi']

pdf_file_ext = ['.pdf']

json_file_ext = ['.json']


def get_files_in_dir(directory, extensions=None):
    if extensions is None:
        extensions = [img_file_ext,
                      vid_file_ext,
                      pdf_file_ext,
                      json_file_ext]

    media_files_in_dir = []
    for root, dirs, files in os.walk(directory):
        for file in files:

            if not is_video_or_image_file(file, extensions):
                continue

            media_files_in_dir.append(file)

    return media_files_in_dir


def get_media_files_in_dir(directory):
    # skip print orders
    print_order_regex = 'Print Order \\d{21}$'
    m = re.search(print_order_regex, directory)
    if m:
        return []

    media_files_in_dir = get_files_in_dir(directory)

    return media_files_in_dir


def get_print_orders_in_dir(directory):
    # skip print orders
    print_order_regex = 'Print Order \\d{21}$'
    m = re.search(print_order_regex, directory)
    if not m:
        return []

    media_files_in_dir = get_files_in_dir(directory)

    return media_files_in_dir


def is_file_in_correct_out_dir(files, out_dir):
    """
    :param files:
    :param out_dir:
    :return:

    file is a list of file names without their paths

    returns a list of tuples where the first value is the name of the file and
    the second is a bool value (whether it's been moved to the correct dir)
    """
    res = []
    for f in files:
        extension = get_file_extension(f)
        abc = [img_file_ext, vid_file_ext]
        extensions = [a for ab in abc for a in ab]

        # non-pdf files
        if extension in extensions:
            dir_prefix = 'Photos from '
            timestamp_reg = '[0-9]{8}'
            m = re.search(timestamp_reg, f)
            # there should be one match normally. If more, first
            # from left to r is taken
            timestamp = m.group(0)
            timestamp = timestamp[0:4]
            file_out_path = os.path.join(out_dir, dir_prefix + timestamp, f)
            if os.path.isfile(file_out_path):
                res.append([f, True])
                continue
            res.append([f, False])
    return res


def set_up_test_cp_dir(src_dir, dst_dir):
    """

    :param src_dir:
    :param dst_dir:
    :return:

    function adds media files in dir as list
    """
    os.makedirs(dst_dir, exist_ok=True)
    cp_recursive_overwrite(src_dir, dst_dir)
    media_files_in_dir = get_media_files_in_dir(dst_dir)

    return media_files_in_dir


def set_up_test_cp_file(src_file, dst_dir):
    """
    :param src_file:
    :param dst_dir:
    :return:

    function adds media file as list. Normally this isn't used except for setting up tests.
    """
    os.makedirs(dst_dir, exist_ok=True)
    shutil.copy(src_file, dst_dir)
    head, tail = ntpath.split(src_file)
    file_name = tail or ntpath.basename(tail)

    json_file_name = file_name + json_file_ext[0]
    json_file_name_path = os.path.join(head, json_file_name)
    if os.path.isfile(json_file_name_path):
        shutil.copy(json_file_name_path, dst_dir)

    return [file_name]


class PrintOrder:
    def __init__(self, print_order_file_path, sha256):
        self.print_order_file_path = print_order_file_path
        self.sha256 = sha256


def get_files_that_have_not_been_moved(all_media_files, print_orders, out_dir):
    # check if all files are in the expected directories
    unmoved_media_files = []
    have_all_files_been_moved = True
    err_message = ''
    for entry in all_media_files:
        res = is_file_in_correct_out_dir(entry, out_dir)
        for r in res:
            if not r[1]:
                unmoved_media_files.append(r[0])
                have_all_files_been_moved = False

    # create print order objects from out_dir and compare with those in src
    print_orders_in_out_dir = []
    print_order_regex = 'Print Order \\d{21}$'
    first_lvl_dirs_out_dir = os.listdir(out_dir)
    for d in first_lvl_dirs_out_dir:
        m = re.search(print_order_regex, d)
        if m:
            print_order_file = os.path.join(out_dir, d, 'print-order.pdf')
            sha256 = compute_sha256_of_file(print_order_file)
            print_orders_in_out_dir.append(PrintOrder(print_order_file, sha256))

    for p_o in print_orders:
        is_p_o_in_out_dir = False
        for p_o_out in print_orders_in_out_dir:
            if p_o.sha256 == p_o_out.sha256:
                is_p_o_in_out_dir = True
                break
        if not is_p_o_in_out_dir:
            unmoved_media_files.append(p_o.file_path)

    return unmoved_media_files


def get_leaf_dirs_in_dir(directory, leafs, extensions=None):
    """
    :param directory:
    :param leafs:
    :param extensions:
    :return:

    gets the leaf directories in directory and stores them as a list
    in leaf_dirs
    """
    if extensions is None:
        extensions = [img_file_ext,
                      vid_file_ext,
                      pdf_file_ext,
                      json_file_ext]

    if os.path.isdir(directory):
        files = os.listdir(directory)
        for f in files:
            get_leaf_dirs_in_dir(os.path.join(directory, f), leafs)
    else:
        # directory is a file at this point
        file = directory
        if is_video_or_image_file(file, extensions):
            leafs.append(file)


def split_leaf_files_into_sub_lists(directory, n_threads):
    leafs = []
    get_leaf_dirs_in_dir(directory, leafs)
    sub_lists = []

    if len(leafs) <= n_threads:
        for leaf in leafs:
            sub_lists.append([leaf])
        return sub_lists

    sub_list_size = len(leafs) // n_threads
    remainder = len(leafs) % n_threads
    tmp = []

    for i in range(len(leafs)):
        if not i % sub_list_size and i != 0:
            sub_lists.append(tmp)
            if i / sub_list_size != n_threads:
                tmp = []
        tmp.append(leafs[i])

    if not remainder:
        sub_lists.append(tmp)

    return sub_lists


def create_work_units(work, n_threads):
    """
    :param work:
    :param n_threads:
    :return:

    splits the list into n_threads sublists. The last list is len(work_list) % n_threads longer
    than the rest. Work can be a list of a dict
    """

    is_work_dict = False
    if isinstance(work, dict):
        work_list = []
        for x, y in work.items():
            work_list.append([x, y])
        work = work_list
        is_work_dict = True

    thread_work_units = []
    if len(work) <= n_threads:
        for i_o_dir in work:
            thread_work_units.append(i_o_dir)
        if not is_work_dict:
            return thread_work_units
        if is_work_dict:
            # convert back to dict
            work_dict = {}
            for k, v in work:
                work_dict[k] = v
            return work_dict

    thread_work_units_size = len(work) // n_threads
    remainder = len(work) % n_threads
    tmp = []

    # a more even distribution would be to use buckets of work and add an item to the least filled bucket at
    # a time
    k_work_units = 1
    for i in range(len(work)):
        if k_work_units != n_threads:
            if not i % thread_work_units_size and i != 0:
                thread_work_units.append(tmp)
                k_work_units = k_work_units + 1
                tmp = []
        tmp.append(work[i])

    if tmp:
        thread_work_units.append(tmp)

    if not is_work_dict:
        return thread_work_units
    else:
        # convert back to a list of dicts
        work_dict_list = []

        for t_w in thread_work_units:
            d = {}
            for k, v in t_w:
                d[k] = v
            work_dict_list.append(d)

        return work_dict_list


def copy_print_order_thread_function(in_out_dirs):
    for src_dir, dst_dir in in_out_dirs:
        cp_recursive_overwrite(src_dir, dst_dir)


def copy_print_order_thread_function_progress_bar(arg_object):
    in_out_dirs, bar_fun = arg_object
    for src_dir, dst_dir in in_out_dirs:
        cp_recursive_overwrite(src_dir, dst_dir)
        bar_fun()


def print_list_of_files(list_of_files, tab_indent_count=1):
    is_list_nested = any(isinstance(i, list) for i in list_of_files)

    flattened_list_of_files = None
    if is_list_nested:
        abc = list_of_files
        flattened_list_of_files = [a for ab in abc for a in ab]
    else:
        flattened_list_of_files = list_of_files

    for file in flattened_list_of_files:
        tab_str = ''
        while tab_indent_count >= 0:
            tab_str = tab_str + '\t'
            tab_indent_count = tab_indent_count - 1

        logger.info('{0}{1}\n'.format(tab_str, file))
