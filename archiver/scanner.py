"""
Scan directories to see if they have files modified in the last x days
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse
from pathlib import Path
import subprocess
from datetime import datetime, timedelta
import tempfile
import re
import os
from contextlib import contextmanager
import sys
import shutil


NOTICE_CONTENT_TEMPLATE = """
Your files have been archived due to inactivity.

Send an email to ds-infrastructure@lists.berkeley.edu from your berkeley.edu
email address to get a copy of your files. You must include the following
text to help us retrieve your file:

{object_id}
"""


def was_modified_after(path: Path, after: datetime, ignored_filenames: list):
    """
    Return True, None if path or descendents have files modified after given datetime

    If they were *not*, return False, size_of_dir_in_bytes.

    This somewhat ugly design lets us walk the file tree just once, and skip large
    parts of it that are active. Since we only want to detect inactive directories
    and report their size, this works ok.

    filenames in ignored_filenames will be ignored when testing the recentness
    of a file.

    Directory mtimes are ignored, primarily so that us putting in `notice` files about
    where to find your archived directories does not mark a directory as 'active'. New
    directory creations or file deletions in a directory will also no longer mark
    a directory as active - a new file must be created or modified. This seems
    acceptable.
    """
    after_ts = after.timestamp()

    stat = path.stat()
    total_size = stat.st_size

    # Check files first before recursing into subdirectories
    # Only files are checked for freshness, symlinks
    # and other kinds of special files are ignored. This only affects
    # what is checked for freshness - `tar` copies everything anyway
    for c in path.iterdir():
        if c.name in ignored_filenames:
            continue
        if c.is_file():
            cstat = c.stat()
            if cstat.st_mtime >= after_ts:
                return True, None
            total_size += cstat.st_size
        elif c.is_dir():
            was_modified, size = was_modified_after(c, after, ignored_filenames)
            if was_modified:
                return True, None
            total_size += size
    return False, total_size


def md5sum_local(filename: Path):
    """
    Return base64-encoded md5 of given file.

    Google Cloud Storage supports md5 to validate integrity of upload, so
    we use it https://cloud.google.com/storage/docs/hashes-etags. GCS
    prefers dealing with md5 in base64 format so we use that instead of the
    more common hex format.
    """
    output = subprocess.check_output([
        'gsutil', '-q', 'hash', '-m',
        str(filename)
    ]).decode()
    match = re.search(r'Hash \(md5\):\s*(.*)\n', output)
    return match.group(1)


def md5sum_gcs(object_path: str):
    """
    Return base64-encoded md5 of object_path on GCS
    """
    try:
        output = subprocess.check_output([
            'gsutil', '-q', 'ls', '-L', object_path
        ]).decode()
        match = re.search(r'Hash \(md5\):\s*(.*)\n', output)
        return match.group(1)
    except Exception as e:
        print(e)
        return None


@contextmanager
def archive_dir(dir_path: Path, ignored_filenames: list):
    """
    Archive given directory reproducibly to out_path
    """

    with tempfile.TemporaryDirectory() as d:
        target_file = Path(d) / (dir_path.name + ".tar.gz")
        cmd = [
            "tar",
            f"--directory={dir_path}",
            "--sort=name",
            "--numeric-owner",
            "--create",
            "--gzip",
            f"--file={target_file}",
        ] +  [f'--exclude={ignored_file}' for ignored_file in ignored_filenames] + [ '.']
        try:
            # Capture output and fail explicitly on non-0 error code
            # Primarily to get rid of tar: Removing leading `/' from member names
            subprocess.check_output(cmd, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            print(f"Executing {e.cmd} failed with code {e.returncode}", file=sys.stderr)
            print(f"stdout: {e.stdout}", file=sys.stderr)
            print(f"stderr: {e.stderr}", file=sys.stderr)
            sys.exit(1)

        yield target_file


def upload_to_gcs(file_path: Path, target_path: str):
    """
    Upload file_path to target_path on GCS
    """
    md5 = md5sum_local(file_path)
    subprocess.check_call(["gsutil", "-q", "-h", f"Content-MD5={md5}", "cp", str(file_path), target_path])


def process_dir(p, cutoff_date, ignored_filenames, object_prefix, notice_file_name, delete):
    print(f'{p.name:32} -> ', end='')
    is_active, dirsize = was_modified_after(p, cutoff_date, ignored_filenames)
    if is_active:
        print(f'{"Active":16} -> Skipped')
        return {
            'active': True,
            'uncompressed_size': dirsize,
            'compressed_size': None
        }
    else:
        with archive_dir(p, ignored_filenames) as target_file:
            size = target_file.stat().st_size
            # print(f'Archiving {(size / 1024 / 1024):5.1f}mb -> ', end='')
            target_object_path = f'{object_prefix}/{target_file.name}'
            local_md5 = md5sum_local(target_file)
            remote_md5 = md5sum_gcs(target_object_path)
            if local_md5 != remote_md5:
                upload_to_gcs(target_file, target_object_path)
                print('Uploaded! -> ', end='')
            else:
                print('Validated! -> ', end='')
            if delete:
                # DELETE THE DIRECTORY
                notice_content = NOTICE_CONTENT_TEMPLATE.format(object_id=target_object_path)
                notice_file = p / notice_file_name
                with open(notice_file, 'w') as f:
                    f.write(notice_content)
                print('Notice printed! -> ', end='')

                for subchild in p.iterdir():
                    if subchild.name in ignored_filenames:
                        continue
                    if subchild.is_dir():
                        shutil.rmtree(subchild)
                    else:
                        os.remove(subchild)
                print('-> Deleted!', end='')
            print()
            return {
                'active': False,
                'uncompressed_size': dirsize,
                'compressed_size': size
            }

def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument(
        'action', choices=['validate', 'upload'],
        help='Validate already uploaded files, or upload new files'
    )
    argparser.add_argument(
        "root_dir", help="Root directory containing user home directories", type=Path
    )
    argparser.add_argument(
        "days_ago",
        type=int,
        help="If a user directory was last touched this many days ago, it is considered inactive",
    )
    argparser.add_argument(
        "object_prefix",
        help="GCS Prefix (gs://<bucket-name>/prefix/) to upload archived user directories to",
    )
    argparser.add_argument(
        "--delete",
        help="Delete uploaded objects",
        action='store_true'
    )
    argparser.add_argument(
        '--notice-file-name',
        help='Name of file to create with instructions on how to retrieve your archive',
        default='WHERE-ARE-MY-FILES.txt'
    )

    argparser.add_argument(
        '--user',
        help='Only perform action for this user'
    )

    args = argparser.parse_args()

    root_dir: Path = args.root_dir
    object_prefix: str = args.object_prefix.rstrip("/")
    ignored_filenames = [args.notice_file_name]

    cutoff_date = datetime.now() - timedelta(days=args.days_ago)

    inactive_count = 0
    active_count = 0
    inactive_space = 0
    inactive_compressed_space = 0

    pool = ThreadPoolExecutor(max_workers=64)
    futures = []

    if args.user:
        dirs = [root_dir / args.user]
    else:
        dirs = [p for p in root_dir.iterdir() if p.is_dir()]
    for p in dirs:
        future = pool.submit(process_dir,
            p, cutoff_date, ignored_filenames, object_prefix, args.notice_file_name, args.delete
        )
        futures.append(future)

    for future in as_completed(futures):
        result = future.result()
        if result['active']:
            active_count += 1
        else:
            inactive_count += 1
            inactive_space += result['uncompressed_size']
            inactive_compressed_space += result['compressed_size']

    print(
        f"Active: {active_count}, Inactive: {inactive_count}, Inactive Uncompressed Size: {(inactive_space / 1024 / 1024 / 1024):.2f}, Inactive Compressed Size: {(inactive_compressed_space / 1024 / 1024 / 1024):.2f}gb"
    )


if __name__ == "__main__":
    main()
