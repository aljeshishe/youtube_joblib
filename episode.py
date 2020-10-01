import requests
from pytube import YouTube, Playlist
import pytube
import os
import logging
import shutil
from pathlib import Path

import cache

logging.getLogger('pytube').setLevel(logging.WARNING)
log = logging.getLogger(__name__)

#
# def f():
#     print('executing  f')
#     time.sleep(1)
#     return
#
# @memory.cache
# def do(url):
#     print(f'executing do1 {url}')
#     f()
#     time.sleep(1)
#
#     return requests.get(url)
#
#
#
# response = do(url='https://joblib.readthedocs.io/en/latest/memory.html')
# print(response)


class Episode:

    @classmethod
    @cache.cache
    def cached(cls, url):
        try:
            return cls(url)
        except Exception:
            log.exception(f'Got exception while processing {url}')
            return None

    def __init__(self, url):
        self.url = url
        self.video_info = self._video_info(self.url)

        filtered_audios = self.video_info.streams.filter(only_audio=True, audio_codec="opus").order_by('abr').desc()
        if not filtered_audios:
            raise Exception(f'Video has no opus audio streams {self.url}')
        self.audio_info = filtered_audios[0]

        self.captions_info = self._get_captions(self.video_info)
        _, _, ep_name = self.url.partition('=')

        ep_folder = Path('/tmp/cprc/downloads') / ep_name
        ep_folder.mkdir(parents=True, exist_ok=True)

        self.captions_path = self.captions_info.download(title=ep_name, output_path=ep_folder)
        self.audio_path = self.audio_info.download(filename=ep_name, output_path=ep_folder)

    @staticmethod
    def _video_info(url, attempts=3):
        """Loading info with several attempts"""
        for i in range(attempts):
            try:
                log.info(f"Getting info for: {url}")
                return YouTube(url)
            except Exception:
                log.exception(f'Got exception while loading {url}')
                if i == attempts - 1:
                    raise

    def _get_captions(self, video):
        log.info(video.captions)
        for lang in ('en', 'en-US', 'en-GB'):
            captions = video.captions.get(lang)
            if captions is not None:
                return captions
        raise Exception(f"Lack of en subtitles. Url:{self.url}")


if __name__ == '__main__':
    import logging

    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s.%(msecs)03d|%(levelname)-4.4s|%(thread)-6.6s|%(funcName)-10.10s|%(message)s',
                        handlers=[logging.StreamHandler()])

    print(Episode.cached('https://www.youtube.com/watch?v=UiEaWkf3r9A'))
    print(Episode.cached('https://www.youtube.com/watch?v=GniyQkgGlUA'))