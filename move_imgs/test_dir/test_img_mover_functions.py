import concurrent
import os
import random
import unittest

from move_imgs.img_mover import ImageMover
from move_imgs.img_mover_functions import get_leaf_dirs_in_dir, split_leaf_files_into_sub_lists, create_work_units, \
    cp_recursive_overwrite, get_print_orders_in_dir, get_file_extension, pdf_file_ext, compute_sha256_of_file, \
    PrintOrder, set_up_test_cp_dir, set_up_test_cp_file, get_files_that_have_not_been_moved, \
    copy_print_order_thread_function


class TestImgMoverFunctions(unittest.TestCase):
    directory = 'C:\\Disk_D_Backup\\ImagesBackup\\images-managed\\script\\move_imgs\\test_dir\\dir_to_get_leaf_files'
    C_NO_OF_FILES = 16
    C_N_THREADS_EVEN = 4
    C_N_THREADS_ODD = 5

    l_size = 0
    d_size = 0
    n_threads = 0

    def test_get_leaf_files_from_dir(self):
        leafs = []
        get_leaf_dirs_in_dir(self.directory, leafs)
        self.assertTrue(len(leafs) == self.C_NO_OF_FILES)

    def test_split_leaf_files_into_batches(self):
        leafs = []
        get_leaf_dirs_in_dir(self.directory, leafs)
        sub_lists = split_leaf_files_into_sub_lists(self.directory, self.C_N_THREADS_EVEN)

        abc = sub_lists
        flattened_sub_lists = [a for ab in abc for a in ab]

        self.assertTrue(leafs == flattened_sub_lists)

        sub_lists = split_leaf_files_into_sub_lists(self.directory, self.C_N_THREADS_ODD)

        abc = sub_lists
        flattened_sub_lists = [a for ab in abc for a in ab]

        self.assertTrue(leafs == flattened_sub_lists)

    def test_thread_work_helper_function(self):
        l = [x for x in range(self.l_size)]
        d = {}
        for i in range(self.d_size):
            d[chr(97 + i)] = i

        is_less_work_than_threads = False
        if len(l) <= self.n_threads:
            is_less_work_than_threads = True

        l_work = create_work_units(l, self.n_threads)
        self.assertTrue(len(l_work) == min(len(l), self.n_threads),
                        'The list has not been split into the correct number of chunks')
        l_flattened = []
        if not is_less_work_than_threads:
            abc = l_work
            l_flattened = [a for ab in abc for a in ab]
        else:
            l_flattened = l_work
        self.assertTrue(l == l_flattened, 'The thread work list differs from the original one')

        d_work = create_work_units(d, self.n_threads)
        self.assertTrue(len(d_work) == min(len(d), self.n_threads),
                        'The dict has not been split into the correct number of chunks')

    # TODO: uncomment
    # def test_create_thread_work_units_function(self):
    #     # more work than threads
    #     self.n_threads = 4
    #     for i in range(5000):
    #         self.l_size = random.randrange(self.n_threads + 1, 10000, 1)
    #         self.d_size = random.randrange(self.n_threads + 1, 10000, 1)
    #         self.test_thread_work_helper_function()
    #
    #     # less work than threads
    #     self.l_size = 2
    #     self.d_size = 3
    #     self.n_threads = 4
    #     self.test_thread_work_helper_function()
    #
    #     # equal amount of work to threads
    #     self.l_size = 4
    #     self.d_size = 4
    #     self.n_threads = 4
    #     self.test_thread_work_helper_function()

    def test_cp_print_orders_multi_threaded(self):
        dirs = [['C:\\Disk_D_Backup\\ImagesBackup\\images-managed\\script\\move_imgs\\test_dir'
                                    '\\dir_with_imgs_in_random_dirs\\takeout-20240914-001\\Takeout\\Print Order '
                                    '401028576039896696299',
                                    'C:\\Disk_D_Backup\\ImagesBackup\\images-managed\\script\\move_imgs\\test_dir'
                                    '\\dir_with_imgs_in_random_dirs\\out_dir\\Print Order 401028576039896696299'],
                                   ['C:\\Disk_D_Backup\\ImagesBackup\\images-managed\\script\\move_imgs\\test_dir'
                                    '\\dir_with_imgs_in_random_dirs\\takeout-20240914-001\\Takeout\\Print Order '
                                    '401784216297609585955',
                                    'C:\\Disk_D_Backup\\ImagesBackup\\images-managed\\script\\move_imgs\\test_dir'
                                    '\\dir_with_imgs_in_random_dirs\\out_dir\\Print Order 401784216297609585955'],
                                   ['C:\\Disk_D_Backup\\ImagesBackup\\images-managed\\script\\move_imgs\\test_dir'
                                    '\\dir_with_imgs_in_random_dirs\\takeout-20240914-001\\Takeout\\Print Order '
                                    '402059780286512303683',
                                    'C:\\Disk_D_Backup\\ImagesBackup\\images-managed\\script\\move_imgs\\test_dir'
                                    '\\dir_with_imgs_in_random_dirs\\out_dir\\Print Order 402059780286512303683'],
                                   ['C:\\Disk_D_Backup\\ImagesBackup\\images-managed\\script\\move_imgs\\test_dir'
                                    '\\dir_with_imgs_in_random_dirs\\takeout-20240914-001\\Takeout\\Print Order '
                                    '405803964936218374699',
                                    'C:\\Disk_D_Backup\\ImagesBackup\\images-managed\\script\\move_imgs\\test_dir'
                                    '\\dir_with_imgs_in_random_dirs\\out_dir\\Print Order 405803964936218374699'],
                                   ['C:\\Disk_D_Backup\\ImagesBackup\\images-managed\\script\\move_imgs\\test_dir'
                                    '\\dir_with_imgs_in_random_dirs\\takeout-20240914-001\\Takeout\\Print Order '
                                    '410612548961450013336',
                                    'C:\\Disk_D_Backup\\ImagesBackup\\images-managed\\script\\move_imgs\\test_dir'
                                    '\\dir_with_imgs_in_random_dirs\\out_dir\\Print Order 410612548961450013336'],
                                   ['C:\\Disk_D_Backup\\ImagesBackup\\images-managed\\script\\move_imgs\\test_dir'
                                    '\\dir_with_imgs_in_random_dirs\\takeout-20240914-001\\Takeout\\Print Order '
                                    '412083293552693825021',
                                    'C:\\Disk_D_Backup\\ImagesBackup\\images-managed\\script\\move_imgs\\test_dir'
                                    '\\dir_with_imgs_in_random_dirs\\out_dir\\Print Order 412083293552693825021'],
                                   ['C:\\Disk_D_Backup\\ImagesBackup\\images-managed\\script\\move_imgs\\test_dir'
                                    '\\dir_with_imgs_in_random_dirs\\takeout-20240914-001\\Takeout\\Print Order '
                                    '412598128807565587499',
                                    'C:\\Disk_D_Backup\\ImagesBackup\\images-managed\\script\\move_imgs\\test_dir'
                                    '\\dir_with_imgs_in_random_dirs\\out_dir\\Print Order 412598128807565587499'],
                                   ['C:\\Disk_D_Backup\\ImagesBackup\\images-managed\\script\\move_imgs\\test_dir'
                                    '\\dir_with_imgs_in_random_dirs\\takeout-20240914-001\\Takeout\\Print Order '
                                    '413797421927160753096',
                                    'C:\\Disk_D_Backup\\ImagesBackup\\images-managed\\script\\move_imgs\\test_dir'
                                    '\\dir_with_imgs_in_random_dirs\\out_dir\\Print Order 413797421927160753096'],
                                   ['C:\\Disk_D_Backup\\ImagesBackup\\images-managed\\script\\move_imgs\\test_dir'
                                    '\\dir_with_imgs_in_random_dirs\\takeout-20240914-001\\Takeout\\Print Order '
                                    '414185331160016676170',
                                    'C:\\Disk_D_Backup\\ImagesBackup\\images-managed\\script\\move_imgs\\test_dir'
                                    '\\dir_with_imgs_in_random_dirs\\out_dir\\Print Order 414185331160016676170'],
                                   ['C:\\Disk_D_Backup\\ImagesBackup\\images-managed\\script\\move_imgs\\test_dir'
                                    '\\dir_with_imgs_in_random_dirs\\takeout-20240914-001\\Takeout\\Print Order '
                                    '414451411745298844941',
                                    'C:\\Disk_D_Backup\\ImagesBackup\\images-managed\\script\\move_imgs\\test_dir'
                                    '\\dir_with_imgs_in_random_dirs\\out_dir\\Print Order 414451411745298844941'],
                                   ['C:\\Disk_D_Backup\\ImagesBackup\\images-managed\\script\\move_imgs\\test_dir'
                                    '\\dir_with_imgs_in_random_dirs\\takeout-20240914-001\\Takeout\\Print Order '
                                    '417309802818609634891',
                                    'C:\\Disk_D_Backup\\ImagesBackup\\images-managed\\script\\move_imgs\\test_dir'
                                    '\\dir_with_imgs_in_random_dirs\\out_dir\\Print Order 417309802818609634891']]

        n_threads = 8

        thread_work_in_out_dirs = create_work_units(dirs, n_threads)

        with concurrent.futures.ThreadPoolExecutor(max_workers=n_threads) as executor:
            executor.map(copy_print_order_thread_function, thread_work_in_out_dirs)


class SetupTest:
    all_media_files = []

    print_order_ids = ['401028576039896696299', '401784216297609585955', '402059780286512303683',
                       '405803964936218374699', '410612548961450013336', '412083293552693825021',
                       '412598128807565587499', '413797421927160753096', '414185331160016676170',
                       '414451411745298844941', '417309802818609634891']

    print_orders = []

    def set_up_test_cp_print_order_dir(self, src_dir, dst_dir):
        """
        :param src_dir:
        :param dst_dir:
        :return:

        function adds media files in dir as list
        """
        os.makedirs(dst_dir, exist_ok=True)
        cp_recursive_overwrite(src_dir, dst_dir)
        media_files_in_dir = get_print_orders_in_dir(dst_dir)
        for f in media_files_in_dir:
            extension = get_file_extension(f)
            if extension == pdf_file_ext[0]:
                file_path = os.path.join(src_dir, f)
                sha256 = compute_sha256_of_file(file_path)
                self.print_orders.append(PrintOrder(file_path, sha256))

    def __init__(self, out_dir, in_dir, root_dst_dir, root_src_dir, print_order_root_dir, print_order_dst_root_dir):
        self.img_mover = None
        self.in_dir = in_dir
        self.out_dir = out_dir
        self.root_dst_dir = root_dst_dir
        self.root_src_dir = root_src_dir
        self.print_order_root_dir = print_order_root_dir
        self.print_order_dst_root_dir = print_order_dst_root_dir

    def set_up(self, n_threads=1):
        # copy images
        cmd_line = ['./manage_google_photos.py',
                    '--in_dirs',
                    self.in_dir,
                    '--out_dir',
                    self.out_dir,
                    '--verbose',
                    '--progress_bar',
                    '--show_steps',
                    '--threads',
                    str(n_threads)]

        self.img_mover = ImageMover(cmd_line)

        root_dst_dir = ('C:\\Disk_D_Backup\\ImagesBackup\\images-managed\\'
                        'script\\move_imgs\\test_dir\\dir_with_imgs_in_random_dirs\\takeout-20240914-001'
                        '\\Takeout')
        dst_dir = os.path.join(root_dst_dir, 'Photos from 2018')
        root_src_dir = 'C:\\Disk_D_Backup\\ImagesBackup\\takeout-20240914T123646Z-001\\Takeout\\Google Photos\\'
        src_dir = os.path.join(root_src_dir, 'Photos from 2018')
        media_files_in_dir = set_up_test_cp_dir(src_dir, dst_dir)
        self.all_media_files.append(media_files_in_dir) if len(media_files_in_dir) > 0 else False

        src_dir = os.path.join(root_src_dir, 'Photos from 2019')
        dst_dir = os.path.join(root_dst_dir, 'Photos from 2019')
        media_files_in_dir = set_up_test_cp_dir(src_dir, dst_dir)
        self.all_media_files.append(media_files_in_dir) if len(media_files_in_dir) > 0 else False

        src_file = os.path.join(root_src_dir, 'Photos '
                                              'from 2021\\20210129_174216.jpg')
        dst_dir = os.path.join(root_dst_dir, 'Photos from 2024\\some\\dir')
        file_name = set_up_test_cp_file(src_file, dst_dir)
        self.all_media_files.append(file_name)

        src_file = os.path.join(root_src_dir, 'Photos '
                                              'from 2023\\20240101_000124.mp4')
        dst_dir = os.path.join(root_dst_dir, 'Photos from 2024\\some\\random\\dir')
        file_name = set_up_test_cp_file(src_file, dst_dir)
        self.all_media_files.append(file_name)

        src_file = os.path.join(root_src_dir, 'Photos '
                                              'from 2023\\20230402_130947.jpg')
        dst_dir = root_dst_dir
        file_name = set_up_test_cp_file(src_file, dst_dir)
        self.all_media_files.append(file_name)

        src_file = os.path.join(root_src_dir, 'Photos '
                                              'from 2024\\20240114_165621.jpg')
        dst_dir = os.path.join(root_dst_dir, 'Photos from 10000\\the\\future')
        file_name = set_up_test_cp_file(src_file, dst_dir)
        self.all_media_files.append(file_name)

        src_file = os.path.join(root_src_dir, 'Photos '
                                              'from 2024\\20240128_133920.jpg')
        dst_dir = os.path.join(root_dst_dir, 'a\\b\\b')
        file_name = set_up_test_cp_file(src_file, dst_dir)
        self.all_media_files.append(file_name)

        trash_dir = os.path.join(root_src_dir, 'Trash')
        dst_dir = os.path.join(root_dst_dir, 'Trash')
        media_files_in_dir = set_up_test_cp_dir(trash_dir, dst_dir)
        self.all_media_files.append(media_files_in_dir) if len(media_files_in_dir) > 0 else False

        untitled_dir = os.path.join(root_src_dir, 'Untitled')
        dst_dir = os.path.join(root_dst_dir, 'Untitled')
        media_files_in_dir = set_up_test_cp_dir(untitled_dir, dst_dir)
        self.all_media_files.append(media_files_in_dir) if len(media_files_in_dir) > 0 else False

        untitled_dir_1 = os.path.join(root_src_dir, 'Untitled(1)')
        dst_dir = os.path.join(root_dst_dir, 'Untitled(1)')
        media_files_in_dir = set_up_test_cp_dir(untitled_dir_1, dst_dir)
        self.all_media_files.append(media_files_in_dir) if len(media_files_in_dir) > 0 else False

        archive_dir = os.path.join(root_src_dir, 'Archive')
        dst_dir = os.path.join(root_dst_dir, 'Archive')
        media_files_in_dir = set_up_test_cp_dir(archive_dir, dst_dir)
        self.all_media_files.append(media_files_in_dir) if len(media_files_in_dir) > 0 else False

        # cp print orders
        print_order_root_dir = ('C:\\Disk_D_Backup\\ImagesBackup\\takeout-20240914T123646Z-001\\Takeout\\Google '
                                'Photos')
        dst_root_dir = ('C:\\Disk_D_Backup\\ImagesBackup\\images-managed\\script\\move_imgs\\test_dir'
                        '\\dir_with_imgs_in_random_dirs\\takeout-20240914-001\\Takeout')

        for po_id in self.print_order_ids:
            print_order = 'Print Order ' + po_id
            print_order_dir = os.path.join(print_order_root_dir, print_order)
            dst_dir = os.path.join(dst_root_dir, print_order)
            self.set_up_test_cp_print_order_dir(print_order_dir, dst_dir)

    def have_all_files_been_moved_single_threaded(self):
        self.img_mover.mv_files_single_threaded()

        unmoved_media_files = get_files_that_have_not_been_moved(self.all_media_files, self.print_orders, self.out_dir)

        return unmoved_media_files

    def have_all_files_been_moved_multi_threaded(self):
        self.img_mover.mv_files_multi_threaded()

        unmoved_media_files = get_files_that_have_not_been_moved(self.all_media_files, self.print_orders, self.out_dir)

        return unmoved_media_files
