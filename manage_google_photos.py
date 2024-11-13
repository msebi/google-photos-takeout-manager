# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import sys

from colorama import Fore
from colorama import Style
from colorama import init as colorama_init
import logging

from get_args.args import Args
from move_imgs.img_mover import ImageMover

colorama_init()


def print_usage():
    print(f"{Fore.CYAN}Usage {Style.RESET_ALL}: ./manage_google_photos {Fore.CYAN}-i{Style.RESET_ALL} <dir1> "
          f"<dir2> ... <dirN> {Fore.CYAN}-o{Style.RESET_ALL} out_dir")

    print("Looks into all source directories and copies all files once into the destination directory. It \n"
          "specifically looks for all images exported in the google takeout format. These images are, by default\n"
          "timestamped, the timestamp being included in the image name. All images are considered regardless of\n"
          "the dir they're in. The script creates directories prefixed by 'images-xxxx' where xxxx is the year the\n"
          "images were taken in.\n"
          "To make sure there are no collisions, a check sum is computed for each image and compared against the\n"
          "checksum of the images in the directory the image is to be moved to.\n")

    print("\nArguments:")

    print(f"\t{Fore.CYAN}-i --in_dirs {Style.RESET_ALL}\t\t Input directories")

    print(f"\t{Fore.CYAN}-o --out_dir {Style.RESET_ALL}\t\t Input directories")

    print(f"\t{Fore.CYAN}-v --verbose {Style.RESET_ALL}\t\t Displays every step made by the script as well as "
          f"progress bar, the current image that is being moved, etc.")

    print(f"\t{Fore.CYAN}-p --progress_bar {Style.RESET_ALL}\t "
          f"Displays a progress bar showing the number of files that have already been moved.")

    print(f"\t{Fore.CYAN}-s --show_steps {Style.RESET_ALL}\t "
          f"Show all steps the script is taking.")

    print(f"\t{Fore.CYAN}-t --threads {Style.RESET_ALL}\t\t "
          f"Number of threads the scripts should use. Defaults to 1.")


logger = logging.getLogger(__name__)

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    logging.basicConfig(filename='image_mover.log', level=logging.INFO)
    logger.info('Starting...')
    if len(sys.argv) == 1:
        print_usage()
        exit(0)

    args_obj = Args(sys.argv)

    if not args_obj.are_args_correct():
        print_usage()
        exit(1)

    img_mover = ImageMover(sys.argv)

    img_mover.mv_files()

