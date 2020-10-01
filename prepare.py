import argparse
import logging
import os
import tarfile
from pathlib import Path

import wandb

import utils

# Path manipulaton
utils.add_ffmpeg_to_path()
utils.add_sox_to_path()

import work_base
from youtube import YouTube

log = logging.getLogger(__name__)


def main():
    utils.start_debug()

    parser = argparse.ArgumentParser(description='Create a dataset for wav2letter from rc_meetings')
    parser.add_argument('-p', '--path', type=str, help='Location of the dataset')
    parser.add_argument('-d', '--dataset', type=str, help='Dataset')
    parser.add_argument('--platform', type=str, default='cpu', help='Platform - cuda or cpu')
    parser.add_argument('--workers', type=int, default=os.cpu_count(), help='Number of workers')
    params = parser.parse_args()

    if params.dataset.startswith('youtube_'):
        WorkType = YouTube 
    else:
        assert f'unsupported dataset: {params.dataset}'
   
    work_path = work_base.new_work_path()
    handler = logging.StreamHandler()
    handlers = [handler, logging.FileHandler(utils.uniq_file_name(prefix=f'{work_path}/log_', postfix='.log'))]
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s|%(levelname)-4.4s|%(thread)s|%(filename)-10.10s|%(funcName)-10.10s|%(message)s',
                        handlers=handlers)
    logging.getLogger('sox').setLevel(logging.WARNING)

    log.info(f'work_path={work_path}')
    log.info(f'dataset={params.dataset}')
    
    work = WorkType(work_path=Path(work_path), dataset_path=Path(params.path),
                    dataset=params.dataset, platform=params.platform, workers=params.workers)
    work.run()
    upload(params.dataset)


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


def upload(name):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '/home/comp/git/gke_service_account.json'
    os.environ["WANDB_API_KEY"] = '5c5f03d42e16ce3df7aaabb404480128adef6719'

    api = wandb.Api()
    try:
        existing = api.artifact(f"cprc/asr/{name}:latest")
        version = int(existing.name.split(":")[-1][1:]) + 1
    except wandb.apis.CommError as e:
        if 'does not contain artifact' in str(e):
            version = 0
        else:
            raise e
    new_name = f"{name}_v{version}.tar"
    with tarfile.open(new_name, "w:gz") as tar:
        tar.add(name, arcname=os.path.basename(name))

    upload_blob(bucket_name="cprc-dataset-bucket",
                source_file_name=new_name,
                destination_blob_name=f"datasets/{new_name}")
    run = wandb.init(job_type="create-dataset",
                     tags=["dataset_creation"],
                     group="dataset",
                     name=f"create-{name}",
                     project="asr",
                     entity="cprc")

    artifact = wandb.Artifact(type='dataset', name=name)
    artifact.add_reference(uri=f"gs://cprc-dataset-bucket/datasets/{new_name}", name=new_name)
    run.log_artifact(artifact)


if __name__ == '__main__':
    main()
