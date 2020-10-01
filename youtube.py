import logging
import os
import re
from datetime import datetime


import utils
from subtitles import Subtitles
from work_base import WorkBase, prepare_mapping
from pathlib import Path
from episode import Episode
log = logging.getLogger(__name__)


import itertools


def pairwise(iterable):
    "s -> (s0,s1), (s1,s2), (s2, s3), ..."
    a, b = itertools.tee(iterable)
    next(b, None)
    return zip(a, b)


class YouTube(WorkBase):
    def _files_generator(self):
        processed_urls = set()
        with Path(self.dataset_path).open() as fp:
            for url in fp:
                if url and url not in processed_urls:
                    processed_urls.add(url)
                    yield url.strip('\n')

    def _process(self, url):
        log.info(f'Processing {url}')
        episode = Episode.cached(url)
        if not episode:
            return []
        tmp_audio_path = utils.convert(file_path=episode.audio_path, extension='.flac')

        subtitles = Subtitles.from_srt(episode.captions_path)

        results = []
        start = None
        accum_text = []
        for line, next_line in pairwise(subtitles.lines):
            # log.info(f'Processing {line}')
            if start is None:
                start = line.start
            accum_text.append(line.text)
            if line.end - start > 10 * 1000:
                self._save_part(start, line.end, tmp_audio_path, ' '.join(accum_text), results)
                accum_text = []
                start = next_line.start

        log.info(f'Processed {url}')
        return results
        # shutil.rmtree(ep_folder, ignore_errors=True)  # remove episode folder and files
