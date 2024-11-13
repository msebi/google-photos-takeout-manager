import os.path
import shutil
import unittest

from move_imgs.test_dir.test_img_mover_functions import SetupTest


class TestImgMoverSingleThreaded(unittest.TestCase):

    out_dir = ('C:\\Disk_D_Backup\\ImagesBackup\\images-managed\\script\\move_imgs\\test_dir'
               '\\dir_with_imgs_in_random_dirs\\out_dir')
    in_dir = ('C:\\Disk_D_Backup\\ImagesBackup\\images-managed\\script\\move_imgs\\test_dir'
              '\\dir_with_imgs_in_random_dirs\\takeout-20240914-001')

    root_dst_dir = ('C:\\Disk_D_Backup\\ImagesBackup\\images-managed\\'
                    'script\\move_imgs\\test_dir\\dir_with_imgs_in_random_dirs\\takeout-20240914-001'
                    '\\Takeout')
    root_src_dir = 'C:\\Disk_D_Backup\\ImagesBackup\\takeout-20240914T123646Z-001\\Takeout\\Google Photos\\'

    print_order_root_dir = ('C:\\Disk_D_Backup\\ImagesBackup\\takeout-20240914T123646Z-001\\Takeout\\Google '
                            'Photos')
    print_order_dst_root_dir = ('C:\\Disk_D_Backup\\ImagesBackup\\images-managed\\script\\move_imgs\\test_dir'
                                '\\dir_with_imgs_in_random_dirs\\takeout-20240914-001\\Takeout')

    def run_setup(self):
        self.setup_test = SetupTest(self.out_dir,
                                    self.in_dir,
                                    self.root_dst_dir,
                                    self.root_src_dir,
                                    self.print_order_root_dir,
                                    self.print_order_dst_root_dir)
        self.setup_test.set_up(n_threads=8)

    def test_mv_files_in_correct_dirs_multi_threaded_battery(self, n_iter=10):
        """
        :param n_iter:
        :return:

        Deletes the destination directory before performing a backup and then checks
        if all files have been moved. This is one iteration. The idea is to check
        whether there are any bugs in the multi-threaded implementation
        """

        for i in range(n_iter):
            if os.path.isdir(self.out_dir):
                shutil.rmtree(self.out_dir)
            self.run_setup()

            unmoved_media_files = self.setup_test.have_all_files_been_moved_multi_threaded()
            have_all_files_been_moved = True
            err_message = 'All files have been moved successfully!'

            if len(unmoved_media_files) > 0:
                err_message = 'Files \n\n\t{0}\n\n\t have not been moved'.format(unmoved_media_files)
                have_all_files_been_moved = False

            self.assertTrue(have_all_files_been_moved, err_message)


if __name__ == '__main__':
    unittest.main()
