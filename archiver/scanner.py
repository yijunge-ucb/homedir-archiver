"""
Archive directories that haven't been modified to object storage.

When users have not logged into their JupyterHub session for a long time,
we want to move their home directories to object storage to save cost.
This script can be run periodically to find such users, and archive their
home directories to object storage.

WHen run, this script will:

1. Scan all user home directories (assumed to be subdirectories of the root provided
   to the script) for *staleness*. A user home directory is considered stale if there
   is not a single regular file (we do not currently count directories) that has been
   created or modified since the cutoff date we give it. We also pass a list of file
   names that are ignored when testing for staleness - this is primarily so that the
   file we drop with instructions on how to retrieve your files doesn't mark the whole
   home directory at unstale. At the end of this step, we have a list of user home
   directories that are ready to be archived.
2. Use `tar` to make a compressed archive of the stale home directory, and upload it
   to object storage *if necessary*. So the script can be run multiple times, and it
   will not do unnecessary uploads.
3. If the --delete flag is passed, drop a note telling users where they can get their
   files back from, and then *delete their files*. This is a destructive action!

The idea is that you can run this script once to do all the uploads, and run it
again with --delete to validate your uploads *and* delete existing home directories.
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

If you want to retrieve a copy of your files, please
open a Data Archival Request via github here:
https://github.com/berkeley-dsep-infra/datahub/issues/new?assignees=&labels=support&template=data_archival_request.yml

The following text is the link to your datahub folder,
it must be included with your request:

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
            return True, None
        elif c.is_file():
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
        # Object does not exist
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
        # The user home directory isn't stale, so let's ignore it
        print(f'{"Active":16} -> Skipped')
        return {
            'active': True,
            'uncompressed_size': dirsize,
            'compressed_size': None
        }
    elif int(dirsize) >= 100000000000:
        print(f'Too Big {dirsize} -> Skipped')
        return {
            'active': True,
            'uncompressed_size': dirsize,
            'compressed_size': None
        }
    else:
        # Compress the user home directory, but ignore files we don't want
        with archive_dir(p, ignored_filenames) as target_file:
            size = target_file.stat().st_size
            # print(f'Archiving {(size / 1024 / 1024):5.1f}mb -> ', end='')
            target_object_path = f'{object_prefix}/{target_file.name}'

            # Run a reconciliation loop here to ensure we do an upload here.
            # 1. If there is an object currently in storage, and it has the same md5
            #    as our local freshly created archive, we do nothing.
            # 2. If there is *no* object currently in storage, we upload the freshly
            #    created archive.
            # 3. If there is already an existing object in storage, we raise an error
            #    and exit. This shouldn't happen because gsutil should fail if it does
            #    not verify the object fully uploaded.
            #
            # This lets us run the script multiple times idempotently, and it will deal with
            # any file corruption as needed.
            local_md5 = md5sum_local(target_file)
            remote_md5 = md5sum_gcs(target_object_path)
            if remote_md5 is None:
                # object doesn't exist on object storage
                upload_to_gcs(target_file, target_object_path)
                print('Uploaded! -> ', end='')
            elif local_md5 != remote_md5:
                print(f'Remote object exists and does not match what we wanted! local: {local_md5}, remote: {remote_md5}')
                sys.exit(-1)
            else:
                # object exists in gcs and is the same as local file
                print('Validated! -> ', end='')

            if delete:
                # DELETE THE USER HOME DIRECTORY!
                # This is a destructive action, and we require a special flag for it
                # We drop the text file with the notice on how to retrieve your files, and
                # then delete all other files.
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
            # print an empty newline to make output format look nice
            print()
            return {
                'active': False,
                'uncompressed_size': dirsize,
                'compressed_size': size
            }

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

    # tarring is CPU bound, so we can parallelize trivially.
    # FIXME: This should be tuneable, or at least default to some multiple of number of cores on the system
    pool = ThreadPoolExecutor(max_workers=30)
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