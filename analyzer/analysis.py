import arcade
import pandas
import pandas as pd
import traceback
from pandas.core.groupby import DataFrameGroupBy
from datetime import datetime, timedelta
from typing import Optional, Tuple, Union, List, Dict, Iterable
from .intervals import IntervalClassification, IntervalClassifications, IntervalCategory

NANOSECONDS_PER_SECOND = 1000000000


def seconds_between(pd_datetime_from, pd_datatime_to) -> float:
    try:
        return float((pd_datatime_to - pd_datetime_from).to_timedelta64()) / NANOSECONDS_PER_SECOND
    except:
        print(f'Error process {pd_datetime_from} -> {pd_datatime_to}')
        return 0.0


def get_interval_duration(row: pd.Series):
    return seconds_between(row['from'], row['to'])


def enhance_interval_data_with_classification(interval_dict: Dict):
    """
    Given an interval entry from JSON, decorate it with additional metadata for grouping/indexing/etc.
    """
    for classification in IntervalClassifications:
        if classification.value.matches(interval_dict):
            classification.value.decorate_interval(interval_dict)
            break


def get_interval_color(row: pd.Series) -> Union[arcade.Color, Tuple[int, int, int, int]]:
    classification: IntervalClassification = row['classification']
    return classification.color


class Details:

    def __init__(self, ei: "EventsInspector"):
        self.ei = ei
        self.mouse_over_datetime: datetime


class EventsInspector:

    def __init__(self):
        self.events_df = pd.DataFrame()
        self.last_known_mouse_location: Tuple[int, int] = (0, 0)

        # No data yet, so setup arbitrary start and stop for absolute timeline extents
        self.absolute_timeline_start: pd.Timestamp = pd.Timestamp.now()
        self.absolute_timeline_stop: pd.Timestamp = pd.Timestamp.now() + timedelta(minutes=60)
        self.zoom_timeline_start: pd.Timestamp = self.absolute_timeline_start
        self.zoom_timeline_stop: pd.Timestamp = self.absolute_timeline_stop
        self.selected_rows = self.events_df

        # The number of horizontal pixels available to draw each timeline row
        self.current_timeline_width = 0
        self.current_pixels_per_second_in_timeline = 0
        # The number of seconds each time is expected to represent
        self.current_zoom_timeline_seconds: Optional[float] = 1

        # Will store the currently selected rows, grouped by a tuple key (category, locator).
        # Each group is, in effective, all the intervals that should be rendered for a
        # timeline row.
        self.grouped_intervals: Optional[DataFrameGroupBy] = None

        self.details: Details = Details(self)
        self.last_filter_query: Optional[str] = None

    def add_interval_data(self, intervals: Iterable[Dict]):
        import cProfile, pstats, io
        from pstats import SortKey
        pr = cProfile.Profile()
        pr.enable()
        for interval_dict in intervals:
            # Classify an interval. The classification implies category and color for later decoration of the interval row.
            enhance_interval_data_with_classification(interval_dict)
        pr.disable()
        s = io.StringIO()
        sortby = SortKey.CUMULATIVE
        ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
        ps.print_stats()
        print(s.getvalue())

        new_events = pd.DataFrame.from_dict(pd.json_normalize(intervals), orient='columns')

        # Set 'to' equal to 'from' where 'to' is null
        new_events.loc[new_events['to'].isnull(), 'to'] = new_events['from']
        # Ensure the 'to' and 'from' columns are parsed as datetime
        new_events['to'] = pd.to_datetime(new_events['to'], format="%Y-%m-%dT%H:%M:%SZ")
        new_events['from'] = pd.to_datetime(new_events['from'], format="%Y-%m-%dT%H:%M:%SZ")

        # Filter out intervals that are not important to render
        new_events = new_events[new_events['tempStructuredMessage.reason'] != 'DisruptionEnded']
        # self.events_df = self.events_df[-((self.events_df['tempSource'] == 'E2ETest') & ((self.events_df['tempStructuredMessage.annotations.status'] == 'Passed') | (self.events_df['tempStructuredMessage.annotations.status'] == 'Skipped')))]  # Notice the '-', which inverts the criteria
        # self.events_df = self.events_df[-(self.events_df['tempStructuredMessage.annotations.interesting'] == 'false')]  # Notice the '-', which inverts the criteria

        # Pre-calculate the duration, in seconds, of all intervals
        new_events['duration'] = new_events.apply(get_interval_duration, axis=1)

        self.events_df = pd.concat([self.events_df, new_events], ignore_index=True, sort=False)

        # Reassess the data for max/min
        self.absolute_timeline_start: pd.Timestamp = (self.events_df['from'].min()).floor('min')  # Get the earliest interval start and round down to nearest minute
        self.absolute_timeline_stop: pd.Timestamp = (self.events_df['to'].max()).ceil('min')

        # Order rows by category, timeline, then make sure all rows are in chronological order by start time
        self.events_df = self.events_df.sort_values(['category', 'timeline_id', 'from'], ascending=True)

        self.zoom_timeline_start: pd.Timestamp = self.absolute_timeline_start
        self.zoom_timeline_stop: pd.Timestamp = self.absolute_timeline_stop
        self.selected_rows = self.events_df

        self.set_filter_query(self.last_filter_query)  # Re-apply the filter to apply to combined data
        self.notify_of_timeline_date_range_change()

    def notify_of_timeline_date_range_change(self):
        """
        Call whenever the selected rows have been modified
        """
        df = self.selected_rows
        filtered_df = df[(df['to'] >= self.zoom_timeline_start) & (df['from'] <= self.zoom_timeline_stop)]
        self.grouped_intervals = filtered_df.groupby(['category', 'timeline_id'])

    def set_filter_query(self, query: Optional[str] = None):
        if query:
            try:
                self.selected_rows = self.events_df.query(query)
            except:
                traceback.print_exc()
                raise
        else:
            self.selected_rows = self.events_df
        self.last_filter_query = query
        self.notify_of_timeline_date_range_change()

    def on_zoom_resize(self, timeline_width):
        self.current_timeline_width = timeline_width
        # Number of seconds which must be displayed in the timeline
        self.current_zoom_timeline_seconds = seconds_between(self.zoom_timeline_start, self.zoom_timeline_stop)
        self.current_pixels_per_second_in_timeline: float = self.calculate_pixels_per_second(self.current_timeline_width)

    def zoom_to_dates(self, from_dt: datetime, to_dt: datetime):

        # If the from and to are in reserve chronological order, correct it
        if from_dt > to_dt:
            t_dt = to_dt
            to_dt = from_dt
            from_dt = t_dt

        # Ensure the timeline will show at least 10 seconds of time.
        if to_dt - from_dt < timedelta(seconds=10):
            to_dt = from_dt + timedelta(seconds=10)

        if to_dt > self.absolute_timeline_stop:
            to_dt = self.absolute_timeline_stop

        if from_dt < self.absolute_timeline_start:
            from_dt = self.absolute_timeline_start

        self.zoom_timeline_start = from_dt
        self.zoom_timeline_stop = to_dt
        self.notify_of_timeline_date_range_change()
        self.on_zoom_resize(self.current_timeline_width)

    def get_current_timeline_width(self):
        return self.current_timeline_width

    def calculate_pixels_per_second(self, timeline_width: int) -> float:
        """
        :param timeline_width: Calculate the answer as if this many pixels where available for the entire timeline bar.
        :return: Given the span of time the current zoomed in graph is supposed to cover,
                    how many pixels should be drawn for each second of an interval's duration.
        """
        return timeline_width / self.current_zoom_timeline_seconds

    def calculate_interval_width(self, timeline_width: int, pd_interval_row: pandas.Series) -> float:
        duration = pd_interval_row['duration']
        return max(1.0, duration * self.calculate_pixels_per_second(timeline_width))

    def current_interval_width(self, pd_interval_row: pandas.Series) -> float:
        return self.calculate_interval_width(
            timeline_width=self.current_timeline_width,
            pd_interval_row=pd_interval_row
        )

    def calculate_left_offset(self, timeline_width: int, pd_interval_row: pandas.Series) -> float:
        from_dt = pd_interval_row['from']
        if self.zoom_timeline_start > from_dt:
            from_dt = self.zoom_timeline_start
        return seconds_between(self.zoom_timeline_start, from_dt) * self.calculate_pixels_per_second(timeline_width)

    def current_interval_left_offset(self, pd_interval_row: pandas.Series) -> float:
        """
        How far from the left side of the beginning of the zoom timeline should the
        interval begin to render.
        :param pd_interval_row: A row indicating an interval
        """
        return seconds_between(self.zoom_timeline_start, pd_interval_row['from']) * self.current_pixels_per_second_in_timeline

    def left_offset_from_datetime(self, timeline_start: pd.Timestamp, position_dt: pd.Timestamp, timeline_width: int) -> float:
        return seconds_between(timeline_start, position_dt) * self.calculate_pixels_per_second(
            timeline_width)

    def left_offset_to_datetime(self, left_offset) -> datetime:
        """
        Given an offset from the left side of the zoom timeline, what datetime is
        the location approximating?
        :param left_offset: distance in pixels from the start of the zoom timeline.
        """
        return self.zoom_timeline_start + timedelta(seconds=int(left_offset / self.current_pixels_per_second_in_timeline))


