import concurrent
import json
import os
import re
import shutil
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Lock

from alive_progress import alive_bar

from get_args.args import Args
import sys
import logging

from datetime import datetime
from time import sleep

from move_imgs.img_mover_functions import user_said_yes, cp_recursive_overwrite, cp_media_object_to_dir, \
    mv_media_object_to_dir, rem_file, is_video_or_image_file, compute_sha256_of_file, is_file_in_print_order_dir, \
    create_work_units, copy_print_order_thread_function, split_leaf_files_into_sub_lists, \
    get_file_name, get_file_path, SHOW_DEBUG_MSGS, copy_print_order_thread_function_progress_bar, print_list_of_files

logging.basicConfig(level=logging.NOTSET)
logger = logging.getLogger(__name__)


class ImageMover:
    args = None

    class ImageContextManager:
        in_dirs = []
        config_file = ''
        config_obj = {}
        media_objects = []
        conf = {}

        DEBUG = True

        C_PROGRESS_BAR_SLEEP_MS = 0

        C_IMG_EXTENSIONS_JSON_KEY = 'image_file_extensions'
        C_VID_EXTENSIONS_JSON_KEY = 'video_file_extensions'
        C_JSON_EXTENSION_JSON_KEY = 'json_file_extension'

        C_SRC_ROOT_DIR_NAME_JSON_KEY = 'src_root_dir_name'
        C_PHOTO_DIR_NAME_JSON_KEY = 'photo_dir_name'
        C_PHOTO_DIR_NAME_REGEX_JSON_KEY = 'photo_dir_name_regex'
        C_PHOTOS_FROM_DIR_NAME_JSON_KEY = 'photos_from_dir'
        C_PHOTOS_FROM_DIR_NAME_REGEX_JSON_KEY = 'photos_from_dir_regex'
        C_PRINT_ORDER_JSON_KEY = 'print_order_dir'
        C_PRINT_ORDER_JSON_KEY_REGEX = 'print_order_dir_regex'
        photo_dirs = []

        C_PRINT_ORDER_DIR_JSON_KEY = 'print_order_dir'
        C_PRINT_ORDER_DIR_REGEX_JSON_KEY = 'print_order_dir_regex'
        print_order_dirs = []

        C_TRASH_REGEX_JSON_KEY = 'trash_dir_regex'
        trash_dirs = []

        C_UNTITLED_DIR_REGEX_JSON_KEY = 'untitled'
        untitled_dirs = []

        def __init__(self, args, config_file='conf.json'):
            self.args = args
            self.in_dirs = args.get_in_dirs()
            self.config_file = config_file

            if not os.path.isfile(config_file):
                script_wd = self.args.get_script_wd()
                if os.path.isfile(os.path.join(script_wd, config_file)):
                    config_file = os.path.join(script_wd, config_file)

            with open(config_file) as conf_json:
                self.conf = json.load(conf_json)
                conf_json.close()

            self.media_objects = {}

        def rm_duplicate_media_files_in_dir_and_create_media_objects(self, directory, is_in_dir=False):
            """
            :param directory:
            :param is_in_dir:
            :return:

            returns a dictionary of media objects where each object points to a unique file. All duplicate files in
            'directory' are removed
            """
            if self.args.get_verbose():
                if not is_in_dir:
                    logger.info('Removing duplicate images in out dir \'{0}\' ...'.format(directory))
                if is_in_dir:
                    logger.info('Removing duplicate images in in dir \'{0}\' ...'.format(directory))

            media_objects = {}
            for root, dirs, files in os.walk(directory):
                for file in files:

                    full_file_path = os.path.join(root, file)

                    # skip print orders
                    if is_file_in_print_order_dir(full_file_path, self.conf[self.C_PRINT_ORDER_DIR_REGEX_JSON_KEY]):
                        continue

                    if not is_video_or_image_file(file, [self.conf[self.C_IMG_EXTENSIONS_JSON_KEY],
                                                         self.conf[self.C_VID_EXTENSIONS_JSON_KEY],
                                                         self.conf[self.C_JSON_EXTENSION_JSON_KEY]]):
                        continue

                    # get file path & sha
                    sha256_hex = compute_sha256_of_file(full_file_path)

                    # determine the year the image was taken in (by image name)
                    # the image directory is not important. Even images from trash
                    # are moved into a dir of type 'Photos from xxxx'
                    # get it using regex since file names may vary
                    timestamp_reg = '[0-9]{8}'
                    m = re.search(timestamp_reg, file)
                    # there should be one match normally. If more, first
                    # from left to r is taken
                    if not m:
                        # there are metadata.json files in directories like Untitled
                        # we skip these files
                        continue
                    timestamp = m.group(0)

                    if not timestamp:
                        # if there is no timestamp, create one
                        dt = datetime.now()
                        ts = dt.strftime("%Y%b%d_%H%M%S_%f")
                        extension = os.path.splitext(file)[1]
                        new_file = ts + '_' + extension
                        # rename file
                        shutil.copy(os.path.join(root, new_file), os.path.join(root, file))
                        file = new_file

                    # the year is contained in the timestamp which does
                    # not have a fixed position in the file name
                    img_year = timestamp[0:4]

                    curr_file_path = os.path.join(root, file)
                    if sha256_hex in media_objects:
                        logger.warning('Duplicate file found %s. Deleting ...' % curr_file_path)
                        rem_file(curr_file_path)
                        continue

                    if is_in_dir:
                        media_objects[sha256_hex] = (
                            self.MediaObj(curr_file_path, sha256_hex, img_year, directory, True))

                    if not is_in_dir:
                        media_objects[sha256_hex] = (
                            self.MediaObj(curr_file_path, sha256_hex, img_year, directory, False))

            if self.args.get_verbose():
                if not is_in_dir:
                    if len(media_objects):
                        logger.info('Found {0} media files in output directory:\n\n\t {1}\n\n\t'
                                    .format(len(media_objects), directory))

                if is_in_dir:
                    if not media_objects or not len(media_objects):
                        logger.info('No media files found in input directory:\n\n\t {0}'
                                    .format(directory))
                        return
                    logger.info('Found {0} media files in input directory:\n\n\t {1}'
                                .format(len(media_objects), directory))

            return media_objects

        def mv_media_object_to_correct_dir(self, media_object):
            """
            :param media_object:
            :return:

            this method ensures that an image is in its right place depending upon its location. After
            calling  all media files will be moved to dirs:

            /out_dir/Photos from xxxx/photo.jpg
            /out_dir/Photos from xxxx/vid.mp3

            media_object, the argument, is updated
            """
            if not media_object.is_in_dir:
                # out_dir
                photo_dir_prefix = self.conf[self.C_PHOTOS_FROM_DIR_NAME_JSON_KEY]
                photo_dir = os.path.join(media_object.image_dir, photo_dir_prefix + media_object.image_year)

            if media_object.is_in_dir:
                # in_dir
                # e.g. Takeout\Google Photos
                photo_dir_prefix = os.path.join(
                    self.conf[self.C_SRC_ROOT_DIR_NAME_JSON_KEY],
                    self.conf[self.C_PHOTO_DIR_NAME_JSON_KEY],
                    self.conf[self.C_PHOTOS_FROM_DIR_NAME_JSON_KEY])
                photo_dir = os.path.join(media_object.image_dir, photo_dir_prefix + media_object.image_year)

            path_parts = os.path.normpath(media_object.image_path).split(os.path.sep)

            # is media file already in photo_dir
            img_path_in_photo_dir = os.path.join(photo_dir, path_parts[len(path_parts) - 1])
            if not Path(img_path_in_photo_dir).exists():
                mv_media_object_to_dir(photo_dir, media_object)

        def mv_media_object_to_correct_dir_multi_threaded(self, media_object_list):
            """
            :param media_object_list:
            :return:

            same as mv_media_object_to_correct_dir() but moves a list of files. This is not really multithreading
            """
            for media_object in media_object_list:
                self.mv_media_object_to_correct_dir(media_object)

        def flatten_dirs_and_mv_files_to_correct_dirs(self, media_objects_dict):
            """
            :param root_dir_prefix:
            :param media_objects_dict:
            :return:

            media_objects contains all image and video files in a directory. These are expected to be unique
            there's a chance that the out_dir (as well as the in_dirs) look like this:
            /out_dir/another_dir/some_dir/Photos from 2014/20230112.jpg
            /out_dir/Photos from 2023/some_dir/Photos from 2014/20230112.jpg
            /out_dir/Photos from 2023/20230212.jpg
            /out_dir/Photos from 2023/20110212.jpg
            /out_dir/20100212.jpg

            there can be more directories in between such as:
            /<dirs>/out_dir/<dirs>/Photos from 2023/some_dir/Photos from 2014/20230112.jpg

            out_dir needs to be flattened so the deepest paths are at most one e.g. Images from xxxx
            and the images have to be in the right subdirs; 20110212.jpg can't be in Images from 2023

            If there is no dir 'Images from 2011', one is created
            """
            if self.args.get_progress_bar():
                with alive_bar(len(media_objects_dict), title='Moving media objects to their correct dirs') as bar:
                    for k, m_o in media_objects_dict.items():
                        self.mv_media_object_to_correct_dir(m_o)
                        sleep(self.C_PROGRESS_BAR_SLEEP_MS)
                        bar()
                return

            # no progress bar
            for k, m_o in media_objects_dict.items():
                self.mv_media_object_to_correct_dir(m_o)

        def cp_files_from_in_dir_to_out_dir(self, arg_object):

            if SHOW_DEBUG_MSGS:
                logger.info('cp_files_from_in_dir_to_out_dir() arg_object = {0}\n\n'.format(
                    arg_object))

            media_objects_in_in_dir_dict, media_objects_in_out_dir_dict = arg_object

            if SHOW_DEBUG_MSGS:
                logger.info('cp_files_from_in_dir_to_out_dir() media_objects_in_in_dir_dict = {0}\n\n'.format(
                    media_objects_in_in_dir_dict))
                logger.info('cp_files_from_in_dir_to_out_dir() media_objects_in_out_dir_dict = {0}\n\n'.format(
                    media_objects_in_out_dir_dict))

            # TODO: delme?
            # if len(media_objects_in_in_dir_dict) and self.args.get_verbose():
            #     logger.info('Copying files from \n\n\t{0}\n\n\t to \n\n\t{1}\n\n\t'
            #                 .format(self.args.get_in_dirs(), self.args.get_out_dir()))

            if SHOW_DEBUG_MSGS:
                logger.info('cp_files_from_in_dir_to_out_dir media_objects_in_in_dir_dict = {0}\n\n'.format(
                    media_objects_in_in_dir_dict))
                logger.info('cp_files_from_in_dir_to_out_dir media_objects_in_out_dir_dict = {0}\n\n'.format(
                    media_objects_in_out_dir_dict))

            if not media_objects_in_out_dir_dict:
                media_objects_in_out_dir_dict = {}

            out_dir = self.args.get_out_dir()
            for k, v in media_objects_in_in_dir_dict.items():
                if k not in media_objects_in_out_dir_dict:
                    out_dir_year = (self.conf[self.C_PHOTOS_FROM_DIR_NAME_JSON_KEY] +
                                    media_objects_in_in_dir_dict[k].image_year)
                    path_parts = os.path.normpath(media_objects_in_in_dir_dict[k].image_path).split(os.path.sep)
                    image_name = path_parts[len(path_parts) - 1]
                    out_file_path = os.path.join(out_dir, out_dir_year)
                    out_image_path = os.path.join(out_file_path, image_name)
                    media_objects_in_out_dir_dict[k] = self.MediaObj(media_objects_in_in_dir_dict[k].image_path, k,
                                                                     media_objects_in_in_dir_dict[k].image_year,
                                                                     out_dir, False)
                    cp_media_object_to_dir(out_file_path, media_objects_in_out_dir_dict[k])
                    media_objects_in_out_dir_dict[k].image_path = out_image_path

        def cp_files_from_in_dir_to_out_dir_progress_bar(self, arg_object):

            if SHOW_DEBUG_MSGS:
                logger.info('cp_files_from_in_dir_to_out_dir() arg_object = {0}\n\n'.format(
                    arg_object))

            media_objects_in_in_dir_dict, media_objects_in_out_dir_dict, bar = arg_object

            if SHOW_DEBUG_MSGS:
                logger.info('cp_files_from_in_dir_to_out_dir() media_objects_in_in_dir_dict = {0}\n\n'.format(
                    media_objects_in_in_dir_dict))
                logger.info('cp_files_from_in_dir_to_out_dir() media_objects_in_out_dir_dict = {0}\n\n'.format(
                    media_objects_in_out_dir_dict))

            # TODO: delme?
            # if len(media_objects_in_in_dir_dict) and self.args.get_verbose():
            #     logger.info('Copying files from \n\n\t{0}\n\n\t to \n\n\t{1}\n\n\t'
            #                 .format(self.args.get_in_dirs(), self.args.get_out_dir()))

            if SHOW_DEBUG_MSGS:
                logger.info('cp_files_from_in_dir_to_out_dir media_objects_in_in_dir_dict = {0}\n\n'.format(
                    media_objects_in_in_dir_dict))
                logger.info('cp_files_from_in_dir_to_out_dir media_objects_in_out_dir_dict = {0}\n\n'.format(
                    media_objects_in_out_dir_dict))

            if not media_objects_in_out_dir_dict:
                media_objects_in_out_dir_dict = {}

            out_dir = self.args.get_out_dir()
            for k, v in media_objects_in_in_dir_dict.items():
                if k not in media_objects_in_out_dir_dict:
                    out_dir_year = (self.conf[self.C_PHOTOS_FROM_DIR_NAME_JSON_KEY] +
                                    media_objects_in_in_dir_dict[k].image_year)
                    path_parts = os.path.normpath(media_objects_in_in_dir_dict[k].image_path).split(os.path.sep)
                    image_name = path_parts[len(path_parts) - 1]
                    out_file_path = os.path.join(out_dir, out_dir_year)
                    out_image_path = os.path.join(out_file_path, image_name)
                    media_objects_in_out_dir_dict[k] = self.MediaObj(media_objects_in_in_dir_dict[k].image_path, k,
                                                                     media_objects_in_in_dir_dict[k].image_year,
                                                                     out_dir, False)
                    cp_media_object_to_dir(out_file_path, media_objects_in_out_dir_dict[k])
                    media_objects_in_out_dir_dict[k].image_path = out_image_path
                    bar()

        def cp_print_orders(self, in_dir, out_dir):
            for root, dirs, files in os.walk(in_dir):
                for directory in dirs:
                    m = re.search(self.conf[self.C_PRINT_ORDER_JSON_KEY_REGEX], directory)
                    if m:
                        full_path_src_dir = os.path.join(root, directory)
                        full_path_dst_dir = os.path.join(out_dir, directory)
                        cp_recursive_overwrite(full_path_src_dir, full_path_dst_dir)

        def cp_print_orders_progress_bar(self, in_dir, out_dir):
            with alive_bar(title='Copying print orders') as bar:
                for root, dirs, files in os.walk(in_dir):
                    for directory in dirs:
                        m = re.search(self.conf[self.C_PRINT_ORDER_JSON_KEY_REGEX], directory)
                        if m:
                            full_path_src_dir = os.path.join(root, directory)
                            full_path_dst_dir = os.path.join(out_dir, directory)
                            cp_recursive_overwrite(full_path_src_dir, full_path_dst_dir)
                            bar()

        def process_in_dir_single_threaded(self, in_dir):
            out_dir = self.args.get_out_dir()
            does_out_dir_exist = False
            if os.path.isdir(out_dir):
                q = 'Destination directory:\n\n \t\'{0}\' \n\nalready exists. Continue? '.format(out_dir)
                if not user_said_yes(q):
                    sys.exit('Output directory already exists. Quitting...')
                does_out_dir_exist = True
                logger.info('\n')

            if not os.path.isdir(out_dir):
                Path(out_dir).mkdir(parents=True, exist_ok=True)
                does_out_dir_exist = True
            self.cp_print_orders(in_dir, out_dir)

            media_objects_in_out_dir_dict = {}
            if does_out_dir_exist:
                # duplicate files can only exist in the out_dir on the first run of the script. It removes
                # all dupes and does not copy duplicate files
                media_objects_in_out_dir_dict = self.rm_duplicate_media_files_in_dir_and_create_media_objects(out_dir)

            self.flatten_dirs_and_mv_files_to_correct_dirs(media_objects_in_out_dir_dict)

            media_objects_in_in_dir_dict = (
                self.rm_duplicate_media_files_in_dir_and_create_media_objects(in_dir, is_in_dir=True))
            # for the src dir, the default dir layout is 'Takeout/Google Photos'
            self.flatten_dirs_and_mv_files_to_correct_dirs(media_objects_in_in_dir_dict)

            if not media_objects_in_in_dir_dict or not len(media_objects_in_in_dir_dict):
                logging.warning('No media objects found in input directory: \'{0}\''.format(
                    media_objects_in_in_dir_dict)
                )
                return

            # both src and dst dirs are now flattened and all files are in their respective directory (given their
            # name and the time when they were taken). Time to copy them
            arg_object = [media_objects_in_in_dir_dict, media_objects_in_out_dir_dict]
            if self.args.get_progress_bar():
                with alive_bar(len(media_objects_in_in_dir_dict), title='Copying files to out dir...') as bar:
                    arg_object = [media_objects_in_in_dir_dict, media_objects_in_out_dir_dict, bar]
                    self.cp_files_from_in_dir_to_out_dir_progress_bar(arg_object)
            else:
                self.cp_files_from_in_dir_to_out_dir(arg_object)

        def cp_print_orders_multithreading(self, in_dir, out_dir):
            if self.args.get_verbose():
                logger.info('Copying print orders from {0} to {1}'.format(in_dir, out_dir))
            in_out_dirs = []
            for root, dirs, files in os.walk(in_dir):
                for directory in dirs:
                    m = re.search(self.conf[self.C_PRINT_ORDER_JSON_KEY_REGEX], directory)
                    if m:
                        full_path_src_dir = os.path.join(root, directory)
                        full_path_dst_dir = os.path.join(out_dir, directory)
                        in_out_dirs.append([full_path_src_dir, full_path_dst_dir])

            # split the work and start threads
            n_threads = self.args.get_n_threads()

            if SHOW_DEBUG_MSGS:
                logger.info('cp_print_orders_multithreading() in_out_dirs = {0}\n\n'.format(in_out_dirs))

            thread_work_in_out_dirs = create_work_units(in_out_dirs, n_threads)

            if SHOW_DEBUG_MSGS:
                logger.info('cp_print_orders_multithreading() thread_work_in_out_dirs = {0}\n\n'.format(
                    thread_work_in_out_dirs))

            # start threads
            with concurrent.futures.ThreadPoolExecutor(max_workers=n_threads) as executor:
                executor.map(copy_print_order_thread_function, thread_work_in_out_dirs)
            if self.args.get_verbose():
                logger.info('Done copying print orders from {0} to {1}'.format(in_dir, out_dir))

        def cp_print_orders_multithreading_progress_bar(self, in_dir, out_dir):
            if self.args.get_verbose():
                logger.info('Copying print orders from {0} to {1}'.format(in_dir, out_dir))
            in_out_dirs = []

            with alive_bar(title='Copying print orders (multi threaded)') as bar:
                for root, dirs, files in os.walk(in_dir):
                    for directory in dirs:
                        m = re.search(self.conf[self.C_PRINT_ORDER_JSON_KEY_REGEX], directory)
                        if m:
                            full_path_src_dir = os.path.join(root, directory)
                            full_path_dst_dir = os.path.join(out_dir, directory)
                            in_out_dirs.append([full_path_src_dir, full_path_dst_dir])
                            bar()

            # split the work and start threads
            n_threads = self.args.get_n_threads()

            if SHOW_DEBUG_MSGS:
                logger.info('cp_print_orders_multithreading() in_out_dirs = {0}\n\n'.format(in_out_dirs))

            thread_work_in_out_dirs = create_work_units(in_out_dirs, n_threads)

            if SHOW_DEBUG_MSGS:
                logger.info('cp_print_orders_multithreading() thread_work_in_out_dirs = {0}\n\n'.format(
                    thread_work_in_out_dirs))

            with alive_bar(title='Copying print orders multi threaded') as bar:
                thread_work_in_out_dirs_with_progress_bar = []
                for tw in thread_work_in_out_dirs:
                    thread_work_in_out_dirs_with_progress_bar.append([tw, bar])
                # start threads
                with concurrent.futures.ThreadPoolExecutor(max_workers=n_threads) as executor:
                    futures = {
                        executor.submit(
                            copy_print_order_thread_function_progress_bar, tw):
                            tw for tw in thread_work_in_out_dirs_with_progress_bar
                    }
                if self.args.get_verbose():
                    logger.info('Done copying print orders from {0} to {1}'.format(in_dir, out_dir))

        def thread_task_rm_duplicate_media_files(self, arg_object):
            """
            :param arg_object: this contains the list of file paths, a lock, a flag and  a dictionary
            :return:

            Updates the dictionary with unique file paths and deletes duplicate paths. The dictionary and the paths
            are synced with a lock. The flag indicates whether the dir is a src dir
            """
            # if SHOW_DEBUG_MSGS:
            #     logger.info('thread_task_rm_duplicate_media_files() arg_object = {0}\n\n'.format(arg_object))

            list_of_file_paths, lock, is_in_dir, dict_media_objects = arg_object

            # if SHOW_DEBUG_MSGS:
            #     logger.info('thread_task_rm_duplicate_media_files() arg_object = {0}\n\n'.format(arg_object))

            for full_file_path in list_of_file_paths:
                file = get_file_name(full_file_path)
                # skip print orders
                if is_file_in_print_order_dir(full_file_path, self.conf[self.C_PRINT_ORDER_DIR_REGEX_JSON_KEY]):
                    continue

                # at this point it can no longer be a print order
                if not is_video_or_image_file(file, [self.conf[self.C_IMG_EXTENSIONS_JSON_KEY],
                                                     self.conf[self.C_VID_EXTENSIONS_JSON_KEY],
                                                     self.conf[self.C_JSON_EXTENSION_JSON_KEY]]):
                    continue

                # get file path & sha
                sha256_hex = compute_sha256_of_file(full_file_path)

                # determine the year the image was taken in (by image name)
                # the image directory is not important. Even images from trash
                # are moved into a dir of type 'Photos from xxxx'
                # get it using regex since file names may vary
                timestamp_reg = '[0-9]{8}'
                m = re.search(timestamp_reg, file)
                # there should be one match normally. If more, first
                # from left to r is taken
                if not m:
                    # there are metadata.json files in directories like Untitled
                    # we skip these files
                    continue
                timestamp = m.group(0)

                root = get_file_path(full_file_path)

                with lock:
                    if not timestamp:
                        # the timestamp might not be granular enough. Keep looping
                        # so that files with similar names from threads are not overwritten
                        is_duplicate_file = True
                        while is_duplicate_file:
                            # if there is no timestamp, create one
                            dt = datetime.now()
                            ts = dt.strftime("%Y%b%d_%H%M%S_%f")
                            extension = os.path.splitext(file)[1]
                            new_file = ts + '_' + extension
                            # rename file
                            new_file_full_path = os.path.join(root, new_file)
                            full_file_path = os.path.join(root, file)
                            if not os.path.isfile(new_file_full_path):
                                shutil.copy(os.path.join(root, new_file), os.path.join(root, file))
                                file = new_file
                                full_file_path = new_file_full_path
                                is_duplicate_file = False
                            else:
                                sleep(0.05)

                # the year is contained in the timestamp which does
                # not have a fixed position in the file name
                img_year = timestamp[0:4]

                with lock:
                    if sha256_hex in dict_media_objects:
                        logger.warning('Duplicate file found %s. Deleting ...' % full_file_path)
                        m_o = dict_media_objects[sha256_hex]
                        # this should never be the case
                        if m_o.image_path == full_file_path:
                            continue
                        if os.path.isfile(full_file_path):
                            rem_file(full_file_path)
                            continue

                if is_in_dir:
                    with lock:
                        dict_media_objects[sha256_hex] = (
                            self.MediaObj(full_file_path, sha256_hex, img_year, dict_media_objects, True))

                if not is_in_dir:
                    with lock:
                        dict_media_objects[sha256_hex] = (
                            self.MediaObj(full_file_path, sha256_hex, img_year, dict_media_objects, False))

            return

        def thread_task_rm_duplicate_media_files_progress_bar(self, arg_object):
            # the progress bar function is packed into the arg_object
            list_of_file_paths, lock, is_in_dir, dict_media_objects, bar = arg_object

            for full_file_path in list_of_file_paths:
                file = get_file_name(full_file_path)
                # skip print orders
                if is_file_in_print_order_dir(full_file_path, self.conf[self.C_PRINT_ORDER_DIR_REGEX_JSON_KEY]):
                    continue

                # at this point it can no longer be a print order
                if not is_video_or_image_file(file, [self.conf[self.C_IMG_EXTENSIONS_JSON_KEY],
                                                     self.conf[self.C_VID_EXTENSIONS_JSON_KEY],
                                                     self.conf[self.C_JSON_EXTENSION_JSON_KEY]]):
                    continue

                # get file path & sha
                sha256_hex = compute_sha256_of_file(full_file_path)

                timestamp_reg = '[0-9]{8}'
                m = re.search(timestamp_reg, file)
                if not m:
                    # there are metadata.json files in directories like Untitled
                    # we skip these files
                    continue
                timestamp = m.group(0)

                root = get_file_path(full_file_path)

                with lock:
                    if not timestamp:
                        # the timestamp might not be granular enough. Keep looping
                        # so that files with similar names from threads are not overwritten
                        is_duplicate_file = True
                        while is_duplicate_file:
                            # if there is no timestamp, create one
                            dt = datetime.now()
                            ts = dt.strftime("%Y%b%d_%H%M%S_%f")
                            extension = os.path.splitext(file)[1]
                            new_file = ts + '_' + extension
                            # rename file
                            new_file_full_path = os.path.join(root, new_file)
                            full_file_path = os.path.join(root, file)
                            if not os.path.isfile(new_file_full_path):
                                shutil.copy(os.path.join(root, new_file), os.path.join(root, file))
                                file = new_file
                                full_file_path = new_file_full_path
                                is_duplicate_file = False
                            else:
                                sleep(0.05)

                # the year is contained in the timestamp which does
                # not have a fixed position in the file name
                img_year = timestamp[0:4]
                bar()

                with lock:
                    if sha256_hex in dict_media_objects:
                        logger.warning('Duplicate file found %s. Deleting ...' % full_file_path)
                        m_o = dict_media_objects[sha256_hex]
                        # this should never be the case
                        if m_o.image_path == full_file_path:
                            continue
                        if os.path.isfile(full_file_path):
                            rem_file(full_file_path)
                            continue

                if is_in_dir:
                    with lock:
                        dict_media_objects[sha256_hex] = (
                            self.MediaObj(full_file_path, sha256_hex, img_year, dict_media_objects, True))

                if not is_in_dir:
                    with lock:
                        dict_media_objects[sha256_hex] = (
                            self.MediaObj(full_file_path, sha256_hex, img_year, dict_media_objects, False))

            return

        def flatten_dirs_and_mv_files_to_correct_dirs_multi_threading(self, media_objects_dict):
            """
            :param media_objects_dict:
            :return:

            similar as the single threaded version
            """

            if SHOW_DEBUG_MSGS:
                logger.info('flatten_dirs_and_mv_files_to_correct_dirs_multi_threading media_objects_dict = {0}\n\n'.
                            format(media_objects_dict))

            if not media_objects_dict or not len(media_objects_dict):
                return

            media_objects_list = []
            for k, m_o in media_objects_dict.items():
                media_objects_list.append(m_o)

            media_object_thread_work_lists = create_work_units(media_objects_list, self.args.get_n_threads())

            with ThreadPoolExecutor(max_workers=len(media_object_thread_work_lists)) as executor:
                # no progress bar
                executor.map(self.mv_media_object_to_correct_dir_multi_threaded,
                             media_object_thread_work_lists)

        def rm_duplicate_media_files_in_dir_and_create_media_objects_multi_threading(self,
                                                                                     thread_work_list_of_files,
                                                                                     directory,
                                                                                     is_in_dir=False):
            """
            :param thread_work_list_of_files: this is a list of list of full file paths. Each thread processes a list
            :param directory:
            :param is_in_dir:
            :return:

            returns a dictionary of media objects where each object points to a unique file. All duplicate files in
            'directory' are removed
            """
            if self.args.get_verbose():
                if not is_in_dir:
                    logger.info('Removing duplicate images in out dir \'{0}\' ...'.format(directory))
                if is_in_dir:
                    logger.info('Removing duplicate images in in dir \'{0}\' ...'.format(directory))

            media_objects = {}
            lock = Lock()
            work_units = [[x, lock, is_in_dir, media_objects] for x in thread_work_list_of_files]
            abc = thread_work_list_of_files
            # needed to set the number of iterations for the progress bar
            flattened_list = [a for ab in abc for a in ab]
            flattened_list_len = len(flattened_list)
            flattened_list = []

            if SHOW_DEBUG_MSGS:
                logger.info('rm_duplicate_media_files_in_dir_and_create_media_objects_multi_threading()\n\n '
                            'work_units: {0}\n\n\n'.format(work_units))

            if not self.args.get_progress_bar():
                with ThreadPoolExecutor(max_workers=len(thread_work_list_of_files)) as executor:
                    executor.map(self.thread_task_rm_duplicate_media_files, work_units)
            else:
                with ThreadPoolExecutor(max_workers=len(thread_work_list_of_files)) as executor:
                    with alive_bar(flattened_list_len) as bar:
                        work_units = [[x, lock, is_in_dir, media_objects, bar] for x in thread_work_list_of_files]
                        futures = {
                            executor.submit(self.thread_task_rm_duplicate_media_files_progress_bar, wu):
                                wu for wu in work_units
                        }
                        for future in concurrent.futures.as_completed(futures):
                            completed = futures[future]
                            try:
                                # don't use this we need it only to check for exceptions
                                data = future.result()
                            except Exception as exc:
                                logger.error('Exception occurred: {0}\n\n'.format(exc))

            for w_u in work_units:
                # get media objects for each work unit
                m_os = w_u[3]
                for k, v in m_os.items():
                    media_objects[k] = v

            if self.args.get_verbose():
                if not is_in_dir:
                    if not media_objects or not len(media_objects):
                        logger.info('No media files found in output directory:\n\n\t {0}\n\n\t'
                                    .format(directory))
                        return
                    logger.info('Found {0} media files in output directory:\n\n\t {1}\n\n\t'
                                .format(len(media_objects), thread_work_list_of_files))

                if is_in_dir:
                    if not media_objects or not len(media_objects):
                        logger.info('rm_duplicate_media_files_in_dir_and_create_media_objects_multi_threading() '
                                    'No media files found in input directory:\n\n\t {0}'
                                    .format(directory))
                        return
                    logger.info('rm_duplicate_media_files_in_dir_and_create_media_objects_multi_threading() Found {0} '
                                'media files in input directory:\n\n'
                                .format(len(media_objects)))
                    print_list_of_files(thread_work_list_of_files)

            return media_objects

        def cp_files_from_in_dir_to_out_dir_multi_threaded(self,
                                                           media_objects_in_in_dir_dict,
                                                           media_objects_in_out_dir_dict):

            if SHOW_DEBUG_MSGS:
                logger.info(
                    'cp_files_from_in_dir_to_out_dir_multi_threaded() media_objects_in_in_dir_dict = {0}\n\n'.format(
                        media_objects_in_in_dir_dict))
                logger.info(
                    'cp_files_from_in_dir_to_out_dir_multi_threaded() media_objects_in_out_dir_dict = {0}\n\n'.format(
                        media_objects_in_out_dir_dict))

            # TODO: delme?
            # if len(media_objects_in_in_dir_dict) and self.args.get_verbose():
            #     logger.info('\nCopying files from \n\n\t{0}\n\n\t to \n\n\t{1}\n\n\t'
            #                 .format(self.args.get_in_dirs(), self.args.get_out_dir()))

            n_threads = self.args.get_n_threads()
            media_objects_in_in_dir_thread_work = create_work_units(media_objects_in_in_dir_dict, n_threads)

            if SHOW_DEBUG_MSGS:
                logger.info(
                    'cp_files_from_in_dir_to_out_dir_multi_threaded() media_objects_in_in_dir_thread_work = {0}\n\n'.
                    format(media_objects_in_in_dir_thread_work))

            # we also need the media objects in out dir. Create a list of pairs, in_dir work units along
            # with the out_dir media objects
            work_for_threads = [[x, media_objects_in_out_dir_dict] for x in media_objects_in_in_dir_thread_work]

            if SHOW_DEBUG_MSGS:
                logger.info(
                    'cp_files_from_in_dir_to_out_dir_multi_threaded() work_for_threads = {0}\n\n'.format(
                        work_for_threads))

            if not self.args.get_progress_bar():
                with concurrent.futures.ThreadPoolExecutor(max_workers=n_threads) as executor:
                    future_cp_files = {
                        executor.submit(self.cp_files_from_in_dir_to_out_dir, work): work for work in work_for_threads
                    }

                    for future in concurrent.futures.as_completed(future_cp_files):
                        result = future_cp_files[future]
                        try:
                            function_result = future.result()
                        except Exception as exc:
                            logger.error(
                                'cp_files_from_in_dir_to_out_dir_multi_threaded() generated exception {0}\n\n'.format(
                                    exc))
                        else:
                            if SHOW_DEBUG_MSGS:
                                logger.info(
                                    'cp_files_from_in_dir_to_out_dir_multi_threaded() cp result {0}\n\n'.format(result))
            else:
                with ThreadPoolExecutor(max_workers=n_threads) as executor:
                    with alive_bar(len(media_objects_in_in_dir_dict)) as bar:
                        work_for_threads = [[x, media_objects_in_out_dir_dict, bar] for x in
                                            media_objects_in_in_dir_thread_work]

                        if SHOW_DEBUG_MSGS:
                            logger.info('cp_files_from_in_dir_to_out_dir_multi_threaded() prog bar work units {0}\n\n'.
                                        format(work_for_threads))

                        futures = {executor.submit(self.cp_files_from_in_dir_to_out_dir_progress_bar, work):
                                       work for work in work_for_threads}
                        for future in concurrent.futures.as_completed(futures):
                            function_result = future.result()
                            try:
                                data = future.result()
                            except Exception as exc:
                                logger.error(
                                    'Exception occurred {0}\n\n'.format(exc))

            if SHOW_DEBUG_MSGS:
                logger.info(
                    'cp_files_from_in_dir_to_out_dir_multi_threaded() After copying files\n\n')

        def process_in_dir_multi_threaded(self, in_dir):
            out_dir = self.args.get_out_dir()
            does_out_dir_exist = False
            if os.path.isdir(out_dir):
                q = 'Destination directory:\n\n \t\'{0}\' \n\nalready exists. Continue? '.format(out_dir)
                if not user_said_yes(q):
                    sys.exit('Output directory already exists. Quitting...')
                does_out_dir_exist = True
                logger.info('\n')

            if not os.path.isdir(out_dir):
                Path(out_dir).mkdir(parents=True, exist_ok=True)
                does_out_dir_exist = True

            self.cp_print_orders_multithreading(in_dir, out_dir)

            thread_work_list_of_files_out_dir = split_leaf_files_into_sub_lists(out_dir, self.args.get_n_threads())

            if SHOW_DEBUG_MSGS:
                logger.info('process_in_dir_multi_threaded() thread_work_list_of_files_out_dir = {0}\n\n'.format(
                    thread_work_list_of_files_out_dir))

            media_objects_in_out_dir_dict = {}
            if does_out_dir_exist:
                # duplicate files can only exist in the out_dir on the first run of the script. It removes
                # all dupes and does not copy duplicate files
                media_objects_in_out_dir_dict = (
                    self.rm_duplicate_media_files_in_dir_and_create_media_objects_multi_threading(
                        thread_work_list_of_files_out_dir, out_dir))

            if SHOW_DEBUG_MSGS:
                logger.info('media_objects_in_out_dir_dict = {0}\n\n'.format(media_objects_in_out_dir_dict))

            self.flatten_dirs_and_mv_files_to_correct_dirs_multi_threading(media_objects_in_out_dir_dict)

            if SHOW_DEBUG_MSGS:
                logger.info(
                    'media_objects_in_out_dir_dict post flatten = {0}\n\n'.format(media_objects_in_out_dir_dict))

            thread_work_list_of_files_in_dir = split_leaf_files_into_sub_lists(in_dir, self.args.get_n_threads())

            if SHOW_DEBUG_MSGS:
                logger.info('process_in_dir_multi_threaded() in dir thread_work_list_of_files_in_dir = {0}\n\n'.format(
                    thread_work_list_of_files_in_dir))

            media_objects_in_in_dir_dict = (
                self.rm_duplicate_media_files_in_dir_and_create_media_objects_multi_threading(
                    thread_work_list_of_files_in_dir, in_dir, True))

            if SHOW_DEBUG_MSGS:
                logger.info('process_in_dir_multi_threaded() media_objects_in_in_dir_dict = {0}\n\n'.format(
                    media_objects_in_in_dir_dict))

            # for the src dir, the default dir layout is 'Takeout/Google Photos'
            self.flatten_dirs_and_mv_files_to_correct_dirs_multi_threading(media_objects_in_in_dir_dict)

            if SHOW_DEBUG_MSGS:
                logger.info('media_objects_in_in_dir_dict post flatten = {0}\n\n'.format(media_objects_in_in_dir_dict))

            self.cp_files_from_in_dir_to_out_dir_multi_threaded(media_objects_in_in_dir_dict,
                                                                media_objects_in_out_dir_dict)

        def determine_image_obj_and_path(self):
            for in_dir in self.in_dirs:
                self.process_in_dir_single_threaded(in_dir)
            return

        class MediaObj:
            image_path = ''
            image_dir = ''
            is_in_dir = ''
            image_SHA = ''
            has_been_moved = False
            is_duplicate = False

            def __init__(self, image_path, image_sha, image_year, image_dir='', is_in_dir=False):
                self.image_path = image_path
                self.image_dir = image_dir
                self.is_in_dir = is_in_dir
                self.image_SHA = image_sha
                self.image_year = image_year
                self.has_been_moved = False
                self.is_duplicate = False

    def __init__(self, cmd_line):
        self.args = Args(cmd_line)
        self.image_context_manager = None

    def mv_files(self):
        is_single_threaded = True
        if self.args.get_n_threads() > 1:
            is_single_threaded = False

        if is_single_threaded:
            self.mv_files_single_threaded()

    def mv_files_single_threaded(self):
        if self.args.get_verbose():
            logger.info('Using a single thread to copy files')
        # out_dir cannot be in in_dir
        # /a/ can be in_dir
        # /   can be out_dir
        out_dir_path = Path(self.args.get_out_dir())
        in_dirs = self.args.get_in_dirs()
        for d in in_dirs:
            in_dir_path = Path(d)

            if out_dir_path in in_dir_path.parents:
                logger.error('out_dir cannot be in any of the in_dirs. \n\tout_dir: {0}'
                             '\n\n\tin_dir: {1}\n\nQuitting...'.format(self.args.get_out_dir(), d))
                sys.exit('out_dir cannot be in any of the in_dirs. \n\tout_dir: {0}'
                         '\n\n\tin_dir: {1}\n\nQuitting...'.format(self.args.get_out_dir(), d))

        logger.info('Moving images from directories \n\n\t{0}\n\n\t to destination directory \n\n\t{1}\n\n'.format(
            self.args.get_in_dirs(),
            self.args.get_out_dir()))

        self.image_context_manager = self.ImageContextManager(self.args)

        if self.args.get_n_threads() == 1:
            for d in self.args.get_in_dirs():
                self.image_context_manager.process_in_dir_single_threaded(d)
            return

    def mv_files_multi_threaded(self):
        if self.args.get_verbose():
            logger.info('Using {0} threads to copy files'.format(self.args.get_n_threads()))
        # out_dir cannot be in in_dir
        # /a/ can be in_dir
        # /   can be out_dir
        out_dir_path = Path(self.args.get_out_dir())
        in_dirs = self.args.get_in_dirs()
        for d in in_dirs:
            in_dir_path = Path(d)

            if out_dir_path in in_dir_path.parents:
                logger.error('out_dir cannot be in any of the in_dirs. \n\tout_dir: {0}'
                             '\n\n\tin_dir: {1}\n\nQuitting...'.format(self.args.get_out_dir(), d))
                sys.exit('out_dir cannot be in any of the in_dirs. \n\tout_dir: {0}'
                         '\n\n\tin_dir: {1}\n\nQuitting...'.format(self.args.get_out_dir(), d))

        logger.info('Moving images from directories \n\n\t{0}\n\n\t to destination directory \n\n\t{1}\n\n'.format(
            self.args.get_in_dirs(),
            self.args.get_out_dir()))

        self.image_context_manager = self.ImageContextManager(self.args)

        for d in self.args.get_in_dirs():
            self.image_context_manager.process_in_dir_multi_threaded(d)

    def mv_files(self):
        if self.args.get_n_threads() > 1:
            self.mv_files_multi_threaded()
            return
        self.mv_files_single_threaded()