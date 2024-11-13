import os
import shutil
import unittest
from move_imgs.img_mover import ImageMover
from move_imgs.img_mover_functions import cp_recursive_overwrite, set_up_test_cp_dir, set_up_test_cp_file, \
    get_files_that_have_not_been_moved


class TestImgMoverSingleThreaded(unittest.TestCase):
    all_media_files = []

    out_dir = ('C:\\Disk_D_Backup\\ImagesBackup\\images-managed\\script\\move_imgs\\test_dir'
               '\\dir_with_imgs_in_correct_dirs\\out_dir')
    in_dir = ('C:\\Disk_D_Backup\\ImagesBackup\\images-managed\\script\\move_imgs\\test_dir'
              '\\dir_with_imgs_in_correct_dirs\\takeout-20240914-001')

    def setUp(self):
        # copy images
        cmd_line = ['./manage_google_photos.py',
                    '--in_dirs',
                    self.in_dir,
                    '--out_dir',
                    self.out_dir,
                    '--verbose', '--progress_bar', '--show_steps']

        self.img_mover = ImageMover(cmd_line)

        root_dst_dir = ('C:\\Disk_D_Backup\\ImagesBackup\\images-managed\\'
                        'script\\move_imgs\\test_dir\\dir_with_imgs_in_correct_dirs\\takeout-20240914-001'
                        '\\Takeout\\Google Photos')
        dst_dir = os.path.join(root_dst_dir, 'Photos from 2018')
        root_src_dir = 'C:\\Disk_D_Backup\\ImagesBackup\\takeout-20240914T123646Z-001\\Takeout\\Google Photos\\'
        src_dir = os.path.join(root_src_dir, 'Photos from 2018')

        media_files_in_dir = set_up_test_cp_dir(src_dir, dst_dir)
        self.all_media_files.append(media_files_in_dir) if len(media_files_in_dir) > 0 else False

        src_file = os.path.join(root_src_dir, 'Photos '
                                              'from 2021\\20210129_174216.jpg')
        dst_dir = os.path.join(root_dst_dir, 'Photos from 2021')
        file_name = set_up_test_cp_file(src_file, dst_dir)
        self.all_media_files.append(file_name)

        src_file = os.path.join(root_src_dir, 'Photos '
                                              'from 2021\\20210129_174216.jpg')
        dst_dir = os.path.join(root_dst_dir, 'Photos from 2021')
        file_name = set_up_test_cp_file(src_file, dst_dir)
        self.all_media_files.append(file_name)

        src_file = os.path.join(root_src_dir, 'Photos '
                                              'from 2023\\20240101_000124.mp4')
        dst_dir = os.path.join(root_dst_dir, 'Photos from 2023')
        file_name = set_up_test_cp_file(src_file, dst_dir)
        self.all_media_files.append(file_name)

        src_file = os.path.join(root_src_dir, 'Photos '
                                              'from 2023\\20230402_130947.jpg')
        dst_dir = os.path.join(root_dst_dir, 'Photos from 2023')
        file_name = set_up_test_cp_file(src_file, dst_dir)
        self.all_media_files.append(file_name)

        src_file = os.path.join(root_src_dir, 'Photos '
                                              'from 2023\\20230402_130947.jpg')
        dst_dir = os.path.join(root_dst_dir, 'Photos from 2023')
        file_name = set_up_test_cp_file(src_file, dst_dir)
        self.all_media_files.append(file_name)

        src_file = os.path.join(root_src_dir, 'Photos '
                                              'from 2024\\20240114_165621.jpg')
        dst_dir = os.path.join(root_dst_dir, 'Photos from 2024')
        file_name = set_up_test_cp_file(src_file, dst_dir)
        self.all_media_files.append(file_name)

        src_file = os.path.join(root_src_dir, 'Photos '
                                              'from 2024\\20240128_133920.jpg')
        dst_dir = os.path.join(root_dst_dir, 'Photos from 2024')
        file_name = set_up_test_cp_file(src_file, dst_dir)
        self.all_media_files.append(file_name)

        trash_dir = os.path.join(root_src_dir, 'Trash')
        dst_dir = os.path.join(root_dst_dir, 'Trash')
        media_files_in_dir = set_up_test_cp_dir(trash_dir, dst_dir)
        self.all_media_files.append(media_files_in_dir) if len(media_files_in_dir) > 0 else False

    def test_mv_files_in_correct_dirs_single_threaded(self):
        self.img_mover.mv_files_single_threaded()

        unmoved_media_files = get_files_that_have_not_been_moved(self.all_media_files, [], self.out_dir)
        have_all_files_been_moved = True
        err_message = 'All files have been moved successfully!'

        if len(unmoved_media_files) > 0:
            err_message = 'Files \n\n\t{0}\n\n\t have not been moved'.format(unmoved_media_files)
            have_all_files_been_moved = False

        self.assertTrue(have_all_files_been_moved, err_message)


if __name__ == '__main__':
    unittest.main()
