"""
Scan directories to see if they have files modified in the last x days
"""
import argparse
from pathlib import Path
from datetime import datetime


def walk(path: Path):
    stat = path.stat()
    total_size = stat.st_size
    newest_modified = stat.st_mtime

    for c in path.iterdir():
        # Only operate on files, non-broken symlinks and directories
        if c.is_file() or (c.is_symlink() and c.exists()):
            fstat = c.stat()
            total_size += fstat.st_size
            if fstat.st_mtime > newest_modified:
                newest_modified = fstat.st_mtime
        elif c.is_dir():
            size, modified = walk(c)
            total_size += size
            if modified > newest_modified:
                newest_modified = modified
    return total_size, newest_modified

def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument(
        "root_dir", help="Root directory containing user home directories", type=Path
    )
    argparser.add_argument(
        "days_since",
        type=int,
        help="Number of days a directory needs to be untouched to count as inactive",
    )

    args = argparser.parse_args()

    root_dir: Path = args.root_dir

    init_date = datetime.now()

    total_size = 0
    active_size = 0
    inactive_size = 0
    for p in root_dir.iterdir():
        size, newest_ts = walk(p)
        newest_mtime = datetime.fromtimestamp(newest_ts)
        last_modified = init_date - newest_mtime
        is_active = last_modified.days < args.days_since
        total_size += size
        if is_active:
            active_size += size
        else:
            inactive_size += size
        size_in_mb = size / 1024 / 1024
        print(f'{p.name:24} -> {size_in_mb:>12.2f} -> {is_active} -> {last_modified.days}')

    print(f"active: {(active_size / 1024 / 1024 / 1024):.2f}gb inactive: {(inactive_size / 1024 / 1024 / 1024):.2f}gb")
    print(f"| {(total_size / 1024 / 1024 / 1024):.2f} | {(inactive_size / total_size * 100):.2f}% |")

if __name__ == '__main__':
    main()
