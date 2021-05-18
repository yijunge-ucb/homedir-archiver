"""
Scan directories to see if they have files modified in the last x days
"""
import argparse
from pathlib import Path
from datetime import datetime, timedelta


def was_modified_after(path: Path, after: datetime):
    """
    Return True, None if path or descendents were modified after given datetime

    If they were *not*, return False, size_of_dir_in_bytes.

    This somewhat ugly design lets us walk the file tree just once, and skip large
    parts of it that are active. Since we only want to detect inactive directories
    and report their size, this works ok.
    """
    after_ts = after.timestamp()

    stat = path.stat()
    total_size = stat.st_size

    if stat.st_mtime >= after_ts:
        return True, None

    # Check files first before recursing into subdirectories
    # We only check files, symlinks with valid targets and directories
    for c in path.iterdir():
        if c.is_file() or (c.is_symlink() and c.exists()):
            cstat = c.stat()
            if cstat.st_mtime >= after_ts:
                return True, None
            total_size += cstat.st_size
        elif c.is_dir():
            was_modified, size = was_modified_after(c, after)
            if was_modified:
                return True, None
            total_size += size
    return False, total_size


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
            is_active, dirsize = was_modified_after(p, cutoff_date)
            if is_active:
                print(f'Active -> {p.name}')
                active_count += 1
            else:
                print(f'Inactive -> {p.name:16} -> {(dirsize / 1024 / 1024):.2f}mb')
                inactive_count += 1
                inactive_space += dirsize
    print(f'Active: {active_count}, Inactive: {inactive_count}, Inactive Size: {(inactive_space / 1024 / 1024 / 1024):.2f}')


if __name__ == '__main__':
    main()
