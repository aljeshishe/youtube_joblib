import pysrt
import datetime

from collections import namedtuple

line = namedtuple('line', 'start end duration text')


def time_to_total_seconds(time):
    return datetime.timedelta(
        hours=time.hour,
        minutes=time.minute,
        seconds=time.second,
        milliseconds=time.microsecond/1000.0).total_seconds()


class Subtitles:
    """Subtiters wrapper"""

    def __init__(self, lines: list):
        self.lines = lines

    @staticmethod
    def from_srt(file_path: str):  # TODO: from_lst
        subs = pysrt.open(file_path)
        lines = [Subtitles.line_from_sub(s) for s in subs]
        return Subtitles(lines)

    @staticmethod
    def line_from_sub(sub):
        start = int(time_to_total_seconds(sub.start.to_time()) * 1000)
        end = int(time_to_total_seconds(sub.end.to_time()) * 1000)
        duration = end - start
        text = sub.text
        return line(start, end, duration, text)

    def get_overlap(self) -> float:
        """Calculates overlap between adjacent lines. [0.0, 1.0]"""
        subs_time = 0.0
        clear_time = 0.0
        last_end = 0.0
        for i in range(len(self.lines)):
            line = self.lines[i]
            subs_time += line.end - line.start
            clear_time += line.end - max(last_end, line.start)
            last_end = line.end
        return 1.0 - clear_time / subs_time

    def join_lines(self, max_duration=15.0):
        """Joins lines without braks"""
        source_df = dc.srt_to_df(self.lines)
        df = jh.join_lines(source_df, max_duration)
        new_lines = list([line(row["start"], row["end"], row["duration"], row["text"]) for index, row in df.iterrows()])
        return Subtitles(new_lines)








