"""
Scan directories to see if they have files modified in the last x days
"""
import argparse
from pathlib import Path
import subprocess
from datetime import datetime, timedelta
import tempfile
import hashlib


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


def md5sum(filename: Path):
    """
    Return md5 of given file.

    Google Cloud Storage supports md5 to validate integrity of upload, so
    we use it https://cloud.google.com/storage/docs/hashes-etags
    """
    return subprocess.check_output(["md5sum", str(filename)]).decode().split()[0]

def archive_dir(dir_path: Path, out_path: Path):
    """
    Archive given directory reproducibly to out_path
    """
    cmd = [
        "tar",
        "--sort=name",
        "--numeric-owner",
        "--create", "--xz",
        f"--file={out_path}",
        dir_path,
    ]
    subprocess.check_call(cmd)


def process_inactive_dir(dir_path: Path):
    with tempfile.TemporaryDirectory() as d:
        target_file = Path(d) / (dir_path.name + ".tar.gz")
        archive_dir(dir_path, target_file)
        return (target_file.stat().st_size, md5sum(target_file))


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
    inactive_compressed_space = 0

    for p in root_dir.iterdir():
        if p.is_dir():
            is_active, dirsize = was_modified_after(p, cutoff_date)
            if is_active:
                print(f"Active -> {p.name}")
                active_count += 1
            else:
                compressed_size, md5 = process_inactive_dir(p)
                print(f"Inactive -> {p.name:16} -> {(dirsize / 1024 / 1024):.2f}mb -> compressed {(compressed_size / 1024 / 1024):.2f}mb")
                inactive_count += 1
                inactive_space += dirsize
                inactive_compressed_space += compressed_size
    print(
        f"Active: {active_count}, Inactive: {inactive_count}, Inactive Uncompressed Size: {(inactive_space / 1024 / 1024 / 1024):.2f}, Inactive Compressed Size: {(inactive_compressed_space / 1024 / 1024 / 1024):.2f}gb"
    )


if __name__ == "__main__":
    main()
