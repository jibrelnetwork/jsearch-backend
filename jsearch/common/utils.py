import subprocess


def get_git_revesion_num():
    label = subprocess.check_output(['git', 'describe', '--always']).strip()
    return label.decode()
