"""
Scan directories to see if they have files modified in the last x days
"""
import argparse
from pathlib import Path
from datetime import datetime, timedelta
import subprocess


def was_modified_after(path: Path, after: datetime):
    """
    Return true if path or descendents were modified after given datetime
    """
    after_ts = after.timestamp()

    stat = path.stat()

    if stat.st_mtime >= after_ts:
        return True

    # Check files first before recursing into subdirectories
    # We only check files, symlinks with valid targets and directories
    return any(c.stat().st_mtime >= after_ts for c in path.iterdir() if (c.is_file() or (c.is_symlink() and c.exists()))) or \
        any(was_modified_after(c, after) for c in path.iterdir() if c.is_dir())

def get_dir_size(path: Path):
    """
    Use `du` to get size of directory

    Faster than us walking through every file in python
    """
    output = subprocess.check_output(['du', '--bytes', '--max-depth=0', str(path)]).decode()
    return int(output.split()[0])

def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument(
        "root_dir", help="Root directory containing user home directories", type=Path
    )
    argparser.add_argument(
        "days_ago",
        type=int,
        help="If a user directory was last touched this many days ago, it is considered inactive",
    )

    args = argparser.parse_args()

    root_dir: Path = args.root_dir

    cutoff_date = datetime.now() - timedelta(days=args.days_ago)

    inactive_count = 0
    active_count = 0
    inactive_space = 0

    for p in root_dir.iterdir():
        if p.is_dir():
            if was_modified_after(p, cutoff_date):
                print(f'Active -> {p.name}')
                active_count += 1
            else:
                size = get_dir_size(p)
                print(f'Inactive -> {p.name:16} -> {(size / 1024 / 1024):.2f}mb')
                inactive_count +=1
                inactive_space += size
    print(f'Active: {active_count}, Inactive: {inactive_count}, Inactive Size: {(inactive_space / 1024 / 1024 / 1024):.2f}')


if __name__ == '__main__':
    main()
