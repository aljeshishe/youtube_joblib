import socket
import subprocess
from datetime import datetime
import random
import logging
from pathlib import Path
import tarfile

import cache

log = logging.getLogger(__name__)


def as_str(self, max_len=1024):
    stdout = self.stdout if self.stdout else ''
    stderr = self.stderr if self.stderr else ''

    result = f'''args: {self.args},
returncode: {self.returncode}
stdout: {safe_str(stdout.strip(), max_len=max_len)}
stderr: {safe_str(stderr.strip(), max_len=max_len)}'''
    return result


subprocess.CompletedProcess.__str__ = as_str
subprocess.CompletedProcess.as_str = as_str


def run(cmd):
    log.debug(f'Executing: {cmd}')
    proc = subprocess.run(cmd, stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT, shell=True, encoding='utf-8')
    if proc.returncode and proc.returncode != -2:
        log.error(f'Executing error: {proc}\n{proc}')
        raise subprocess.CalledProcessError(
            proc.returncode, proc.args, proc.stdout, proc.stderr)
    return proc


def uniq_file_name(prefix='', postfix=''):
    the_time = datetime.now().strftime("%d%m%y_%H%M%S")
    return Path('%s_%s_%s%s' % (prefix, the_time, random.randint(1000, 9999), postfix))


def safe_str(obj, max_len=160):
    """
    Helper function to truncate long strings and ignore short
    :param max_len: max string len. Any string longer will be truncated
    :return: str -- trunkated string or string not modified
    """
    result = str(obj)
    if len(result) > max_len:
        return result[:max_len] + ' ...[cut %s chars]' % (len(result) - max_len)
    return result


def start_debug(port=55555):
    def start_client_hack(host, port):
        log_func(1, "Connecting to ", host, ":", str(port))
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(1)
        s.connect((host, port))
        log_func(1, "Connected.")
        return s

    import pydevd
    log_func = pydevd.pydevd_log
    pydevd.start_client = start_client_hack

    try:
        pydevd.settrace('localhost',
                        port=port,
                        stdoutToServer=True,
                        stderrToServer=True,
                        suspend=False)
    except socket.error as e:
        msg = 'Unable to connect to debugging server: %s' % str(e)
        # log.warning(msg)


@cache.cache
def convert(file_path, extension, samplerate=16000):
    path = Path(f'/tmp/cprc/coverted/')
    path.mkdir(exist_ok=True, parents=True)

    hash = cache.hash(file_path)
    tmp_file_name = path / f'{hash}{extension}'

    cmd = f'ffmpeg -i "{file_path}" -vn -ac 1 -sample_fmt s16 -ar {samplerate} "{tmp_file_name}"'
    run(cmd)
    return tmp_file_name


def add_ffmpeg_to_path():
    # making ffmpeg accesible for pydub an utils.convert
    # TODO: Resolve path if called in genrule as binary param
    import os
    os.environ['PATH'] = '{}:{}'.format(
        Path().cwd() / 'external/ffmpeg/bin', os.environ['PATH'])


def add_sox_to_path():
    # making sox accesible for pysox
    # TODO: Resolve path if called in genrule as binary param
    import os
    os.environ['PATH'] = '{}:{}'.format(
        Path().cwd() / 'external/sox/bin', os.environ['PATH'])


def add_sph2pipe_to_path():
    # making sox accesible for pysox
    # TODO: Resolve path if called in genrule as binary param
    import os
    os.environ['PATH'] = '{}:{}'.format(
        Path().cwd() / 'external/sph2pipe/bin', os.environ['PATH'])


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""

    from google.cloud import storage
    from google.oauth2 import service_account

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)
    logging.info("Uploaded {}".format(blob.crc32c))
    return blob


def download_datasets(datasets):
    datadirs = []
    for dataset in datasets:
        name = dataset.name.split(':')[0]
        version = dataset.name.split(":")[-1]
        dataset_versioned = dataset.name.replace(':', '_')
        datadir = Path(f'data/datasets/{dataset_versioned}')
        if not datadir.exists():
            logging.info(f'Extracting dataset {dataset}')
            tar_filename = list(Path(dataset.download()).glob("*.tar.gz"))[0]   
            logging.info(f'Dataset dir {tar_filename}')                
            tar = tarfile.open(str(tar_filename))
            tar.extractall(path=datadir)
            logging.info(f'Archive extracted to {datadir}')
        datadirs.append(datadir)
    return datadirs
