import logging
import os
import re
import shutil
import socket
from abc import ABC, abstractmethod
# from multiprocessing.pool import Pool
from multiprocessing.dummy import Pool

import sox
from typing import Dict, Iterable, Tuple, Generator, List
from pathlib import Path
import utils

log = logging.getLogger(__name__)


def new_work_path() -> Path:
    """Create work path and return it"""
    cur_dir = Path().cwd()
    work_path = utils.uniq_file_name(prefix=f'{cur_dir}/work_path')
    work_path.mkdir(parents=True, exist_ok=True)
    return work_path


def prepare_mapping(file_paths: Iterable[Path], filename_regexp: str) -> Dict[str, Path]:
    """Create mapping id -> Path where is extracted from Path with filename_regexp"""
    result = dict()
    for file in file_paths:
        m = re.match(filename_regexp, file.name)
        id = m.group(1)
        result[id] = file

    return result


class WorkBase(ABC):
    """This class contains all base dataset preprocessing logic.
    Preprocessing consists of following stages:
    1. Collect pairs of audio/text files and return them via _files_generator method(implement it in subclass).
    2. For each pair call process method(implement it in subclass).
        It should split input audio file into samples and save them to output/audio dir
        It also should create and return pair of temporary transcript.lst and text.txt files
    3. Merge above temporary files into output/transcript.lst output/text.txt files

    transcript.lst contents example:
        14_1channel.ogg.000012727-000013727.flac audio/14_1channel.ogg.000012727-000013727.flac 1.00 right
        14_1channel.ogg.000026716-000028428.flac audio/14_1channel.ogg.000026716-000028428.flac 1.71 yeah yep
        14_1channel.ogg.000035422-000036422.flac audio/14_1channel.ogg.000035422-000036422.flac 1.00 yeah
    text.txt contents example:
        here right
        okay
        and you can see how this one says processing right
        mm hmm

    """

    def __init__(self, work_path: Path, dataset_path: Path, dataset: str, platform: str, workers:int = os.cpu_count()) -> None:
        self.dataset_path = dataset_path
        self.work_path = work_path
        self.output_path = Path().cwd() / dataset
        if self.output_path.exists():
            shutil.rmtree(str(self.output_path))
        self.output_audio_path = self.output_path / 'audio'
        self.output_audio_path.mkdir(parents=True, exist_ok=True)
        self.platform = platform
        self.workers = workers
        socket.setdefaulttimeout(30)

    def run(self) -> None:
        """Run dataset preprocessing"""

        def cb(exc):
            log.error(exc)
            # executor.terminate()

        results = []
        with Pool(self.workers) as executor:
            for item in self._files_generator():
                item = item if isinstance(item, tuple) else (item,)  # _files_generator can return tuple or single item
                result = executor.apply_async(self.process, args=item, error_callback=cb)
                results.append(result)
            executor.close()
            executor.join()

        log.info('Merging results')
        # transcript_path = self.output_path / 'transcript.lst'
        # text_path = self.output_path / 'text.txt'
        # with transcript_path.open('w') as transcript_fp, text_path.open('w') as text_fp:
        #     for result in results:
        #         try:
        #             lines = result.get(timeout=5*60)
        #             for fields in lines:
        #                 transcript_fp.write(fields[0])
        #                 text_fp.write(fields[1])
        #         except Exception:
        #             log.exception(f'Got exception while processing result')
        log.info('Finished')

    def process(self, *args, **kwargs):
        try:
            return self._process(*args, **kwargs)
        except Exception as e:
            log.exception(f'Got exception while processing {args} {kwargs}')
            raise

    def _process_allignment(self, alignment):
        results = []
        audio_path = alignment.get_audio_file()
        log.info(f'Processing {audio_path}')
        audio_duration = sox.file_info.duration(audio_path) * 1000
        accum_duration = 0
        if alignment.get_wer() >= 90:
            log.error(f'WER {alignment.get_wer()} for {audio_path}')
            return results
        prev_start = 0
        current_sample_text = []
        for alignment_word in alignment.get_words():
            word = alignment_word.get_word()
            start = alignment_word.get_start()
            log.info(f'For {audio_path.name} got word {word} at {start}')
            if start and start - prev_start > 20 * 1000:
                # Pad by 200ms, because VAD starting word is generaly in middle of the word
                start -= 200
                accum_duration += self._save_part(prev_start, start, audio_path,
                                                  ' '.join(current_sample_text),
                                                  results)
                prev_start = start
                current_sample_text = []
            current_sample_text.append(word)
        if len(current_sample_text) > 0:
            if audio_duration - prev_start > 20 * 1000:
                log.error(f'Big last segment: {audio_duration - prev_start}')
            else:
                accum_duration += self._save_part(prev_start, audio_duration, audio_path,
                                                  ' '.join(current_sample_text), results)
        if accum_duration != audio_duration:
            log.error(f'Accum duration and audio duration missmatch {accum_duration}!={audio_duration}')
        log.info(f'Processed {audio_path}')
        return results

    def _save_part(self, start, end, audio_path, text, results):
        sample_name = re.sub('[^\d-]', '', f'{start}-{end}')
        sample_name = f'{audio_path.name}-{sample_name}.flac'
        sample_path = self.output_audio_path / sample_name
        log.info(f'Saving part: {sample_path.name}')
        text = re.sub(r'\s+', ' ', text)
        tfm = sox.Transformer()
        tfm.trim(start / 1000.0, end / 1000.0)
        tfm.build(audio_path.as_posix(), sample_path.as_posix())

        duration = (end - start)
        rms_amp = sox.file_info.stat(str(sample_path))['RMS     amplitude']
        # Filter out mostly silent samples
        if rms_amp > 0.01:
            transcript_result = f'{sample_path.name} audio/{sample_path.name} {duration:.2f} {text} \n'
            text_result = f'{text}\n'
            results.append((transcript_result, text_result))
        else:
            log.warn(f'Skipped silent file {sample_name} with RMS {rms_amp}')
            sample_path.unlink()
        return duration

    @abstractmethod
    def _process(self, audio_path: Path, txt_path: Path) -> Tuple[Path, Path]:
        """Process a pair of audio and txt file
        Should return a alignment"""
        raise NotImplementedError()

    @abstractmethod
    def _files_generator(self) -> Generator[Tuple[Path, Path], None, None]:
        """Should be implemented in subclasses
        Method should generate pairs of audio/txt files
        """
        raise NotImplementedError()
