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

    def setUp(self):
        # out_dir, in_dir, root_dst_dir, root_src_dir, print_order_root_dir, print_order_dst_root_dir
        self.setup_test = SetupTest(self.out_dir,
                                    self.in_dir,
                                    self.root_dst_dir,
                                    self.root_src_dir,
                                    self.print_order_root_dir,
                                    self.print_order_dst_root_dir)
        self.setup_test.set_up()

    def test_mv_files_in_correct_dirs_single_threaded(self):

        unmoved_media_files = self.setup_test.have_all_files_been_moved_single_threaded()
        have_all_files_been_moved = True
        err_message = 'All files have been moved successfully!'

        if len(unmoved_media_files) > 0:
            err_message = 'Files \n\n\t{0}\n\n\t have not been moved'.format(unmoved_media_files)
            have_all_files_been_moved = False

        self.assertTrue(have_all_files_been_moved, err_message)


if __name__ == '__main__':
    unittest.main()
