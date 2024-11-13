import unittest
from get_args.args import Args


class TestCmdLineArgsParser(unittest.TestCase):

    def test_valid_all_params_long_names_default_order(self):
        cmd_line = ['./manage_google_photos.py', '--in_dirs', 'images-2018', 'images-2019', '--out_dir', 'unique_images'
                    , '--verbose', '--progress_bar', '--show_steps', '--threads', '2']
        args_obj = Args(cmd_line)

        self.assertEqual(args_obj.get_in_dirs(), ['images-2018', 'images-2019'])
        self.assertEqual(args_obj.get_out_dir(), 'unique_images')
        self.assertTrue(args_obj.get_verbose())
        self.assertTrue(args_obj.get_show_steps())
        self.assertTrue(args_obj.get_progress_bar())
        self.assertAlmostEqual(args_obj.get_n_threads(), 2)
        self.assertTrue(args_obj.are_args_correct())

    def test_valid_all_params_long_names_mixed_order(self):
        cmd_line = ['./manage_google_photos', '--out_dir', 'unique_images', '--threads', '2',
                    '--verbose', '--in_dirs', 'images-2018', 'images-2019', '--progress_bar', '--show_steps']
        args_obj = Args(cmd_line)

        self.assertEqual(args_obj.get_in_dirs(), ['images-2018', 'images-2019'])
        self.assertEqual(args_obj.get_out_dir(), 'unique_images')
        self.assertTrue(args_obj.get_verbose())
        self.assertTrue(args_obj.get_show_steps())
        self.assertTrue(args_obj.get_progress_bar())
        self.assertAlmostEqual(args_obj.get_n_threads(), 2)
        self.assertTrue(args_obj.are_args_correct())

    def test_valid_mandatory_params_long_names_default_order_one_arg(self):
        cmd_line = ['./manage_google_photos', '--in_dirs', 'images-2018']
        args_obj = Args(cmd_line)

        self.assertEqual(args_obj.get_in_dirs(), ['images-2018'])
        self.assertTrue(args_obj.are_args_correct())

    def test_valid_mandatory_params_short_names_default_order_one_arg(self):
        cmd_line = ['./manage_google_photos', '-i', 'images-2018']
        args_obj = Args(cmd_line)

        self.assertEqual(args_obj.get_in_dirs(), ['images-2018'])
        self.assertTrue(args_obj.are_args_correct())

    def test_valid_mandatory_params_long_names_default_order_more_args(self):
        cmd_line = ['./manage_google_photos', '--in_dirs', 'images-2018', 'images-2020']
        args_obj = Args(cmd_line)

        self.assertEqual(args_obj.get_in_dirs(), ['images-2018', 'images-2020'])
        self.assertTrue(args_obj.are_args_correct())

    def test_valid_mandatory_params_short_names_default_order_more_args(self):
        cmd_line = ['./manage_google_photos', '-i', 'images-2018', 'images-2020']
        args_obj = Args(cmd_line)

        self.assertEqual(args_obj.get_in_dirs(), ['images-2018', 'images-2020'])
        self.assertTrue(args_obj.are_args_correct())


if __name__ == '__main__':
    unittest.main()
