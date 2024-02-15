import arcade
import numpy as np
import pandas
import pandas as pd
import traceback
from pandas.core.groupby import DataFrameGroupBy
from datetime import datetime, timedelta
from typing import Optional, Tuple, Union, List, Dict, Iterable, Any
from .intervals import IntervalClassification, IntervalClassifications, IntervalCategories, IntervalCategory
from collections import OrderedDict

NANOSECONDS_PER_SECOND = 1000000000


def seconds_between(pd_datetime_from: pandas.Timestamp, pd_datatime_to:pandas.Timestamp) -> float:
    """
    Calculates the number of seconds between to pandas Timestamps.
    """
    try:
        return float((pd_datatime_to - pd_datetime_from).to_timedelta64()) / NANOSECONDS_PER_SECOND
    except:
        print(f'Error process {pd_datetime_from} -> {pd_datatime_to}')
        return 0.0


def left_offset_from_datetime(baseline_dt: pd.Timestamp, position_dt: pd.Timestamp, pixels_per_second: float) -> float:
    """
    Given two timestamps (from, to), calculate the number of pixels which would represent the visual spacing
    between the two times.
    Args:
        baseline_dt: Baseline time.
        position_dt: Time to assess offset from baseline.
        pixels_per_second: The number of pixels per second presently represented on the screen (may be <1.0)
    """
    return seconds_between(baseline_dt, position_dt) * pixels_per_second


def left_offset_to_datetime(baseline_dt: pd.Timestamp, left_offset_px: float, pixels_per_second: float) -> datetime:
    """
    Given a baseline datetime, determine what datetime is represented by a pixel offset from that baseline.
    Args:
        baseline_dt: The baseline datetime.
        left_offset_px: The number of pixels away the date to determine is from that baseline.
        pixels_per_second: The number of pixels per second presently represented on the screen (may be <1.0)
    Returns:
        An approximate datetime that the pixel offset represents.
    """
    return baseline_dt + timedelta(microseconds=int(left_offset_px / pixels_per_second * 1000000))


def get_interval_duration(row: pd.Series):
    return seconds_between(row['from'], row['to'])


def get_interval_width_px(pd_interval_row: pandas.Series, pixels_per_second: float) -> float:
    duration = pd_interval_row['duration']
    return max(3.0, duration * pixels_per_second)  # Give even the smallest interval several pixels to ensure it can be hovered over easily.


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

        # Contains the filtered intervals of the pandas dataframe.
        self.selected_rows = self.events_df

        # The number of horizontal pixels available to draw each timeline row
        self.current_timeline_width = 0
        self.current_pixels_per_second_in_timeline = 0
        # The number of seconds each time is expected to represent
        self.current_zoom_timeline_seconds: Optional[float] = 1

        # Stores all timelines from all available pandas data.
        self.all_timelines: OrderedDict[Any, pd.DataFrame] = OrderedDict()

        # Will store the currently selected rows, grouped by a tuple key (category, timeline_id).
        # Each group is, in effective, all the intervals that should be rendered for a
        # timeline row.
        self.selected_timelines: OrderedDict[Any, pd.DataFrame] = dict()
        self.selected_timeline_keys: List[Tuple] = list()  # keeps an ordered list of timeline keys (same order that exist in self.selected_timelines

        self.details: Details = Details(self)
        self.last_filter_query: Optional[str] = None

    def add_logs_data(self, file_path):
        new_events: pandas.DataFrame = pandas.read_json(file_path, lines=True)  # Read as jsonl
        new_events['classification'] = None  # Initialize classification to null for all rows

        # Requires string columns
        for required_column in ('category_str', 'category_str_lower', 'classification_str_lower', 'timeline_diff'):
            new_events[required_column] = ''

        new_events['from'] = pd.to_datetime(new_events['requestReceivedTimestamp'], format="%Y-%m-%dT%H:%M:%S.%fZ")
        new_events = new_events.assign(to=lambda row: row['from'] + timedelta(seconds=1))
        new_events['tempStructuredLocator.keys.requestURI'] = new_events['requestURI']
        new_events['tempStructuredLocator.keys.auditID'] = new_events['auditID']
        new_events['tempStructuredLocator.keys.verb'] = new_events['verb']
        new_events.rename(columns={
            'requestURI': 'locator',
            'kind': 'tempSource',
            'auditID': 'key.auditID'
        }, inplace=True)

        for classifier in IntervalClassifications:
            new_events = classifier.value.apply(new_events)

        new_events = new_events.assign(timeline_id=lambda row: row['locator'] + '-' + row['timeline_diff'])
        # Pre-calculate the duration, in seconds, of all intervals
        new_events['duration'] = new_events.apply(get_interval_duration, axis=1)
        self.add_intervals(new_events)

    def add_interval_dicts(self, intervals: Iterable[Dict]):
        new_events = pd.DataFrame.from_dict(pd.json_normalize(intervals), orient='columns')
        new_events['classification'] = None  # Initialize classification to null for all rows

        # Requires string columns
        for required_column in ('category_str', 'category_str_lower', 'classification_str_lower', 'timeline_diff'):
            new_events[required_column] = ''

        # Provide each classification an opportunity to choose the rows it represents. IntervalClassifications
        # will enumerate in order, and the first classification to claim a row will keep it.
        for classifier in IntervalClassifications:
            new_events = classifier.value.apply(new_events)

        new_events = new_events.assign(timeline_id=lambda row: row['locator'] + '-' + row['timeline_diff'])

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
        self.add_intervals(new_events)

    def add_intervals(self, new_events: pandas.DataFrame):
        self.events_df = pd.concat([self.events_df, new_events], ignore_index=True, sort=False)

        # Order rows by category, timeline, then make sure all rows are in chronological order by start time
        self.events_df = self.events_df.sort_values(['category_str', 'timeline_id', 'from'], ascending=True)

        self.rebuild_all_timelines()

        self.zoom_timeline_start: pd.Timestamp = self.absolute_timeline_start
        self.zoom_timeline_stop: pd.Timestamp = self.absolute_timeline_stop
        self.selected_rows = self.events_df

        self.set_filter_query(self.last_filter_query)  # Re-apply the filter to apply to combined data

    def rebuild_all_timelines(self):
        """
        Call whenever the pandas intervals have changed (e.g. new data) - meaning there are
        potentially new timeline groups to create / a new order in which to display them.
        """

        # Reassess the data for max/min
        self.absolute_timeline_start: pd.Timestamp = (self.events_df['from'].min()).floor('min')  # Get the earliest interval start and round down to nearest minute
        self.absolute_timeline_stop: pd.Timestamp = (self.events_df['to'].max()).ceil('min')

        # This is tricky. We could collect up the dataframes associated with each timeline
        # with a simple df.groupby(['category_str', 'timeline_id']). However, when viewing the
        # timelines on the screen, we sometimes want the order of the timelines WITHIN A GROUP to be
        # ordered by the earliest interval in that timeline. For e2etests, for example,
        # it ensures that a visualization that looks like a waterfall of ordered test executions
        # vertically, vs the random row ordering we would have otherwise.
        # For other groups, we want the timelines to be displayed in 'locator' order so that
        # activities in namespaces/pods/containers are grouped visually.
        category_groups = self.events_df.groupby('category_str')
        self.all_timelines = OrderedDict()
        for category_key, category_timelines_data in category_groups:
            category: IntervalCategory = category_timelines_data.iloc[0]['category']
            timelines_by_category: Dict[Any, pd.DataFrame] = category_timelines_data.groupby('timeline_id')
            timelines_to_order: List[Tuple[Any, pd.DataFrame]] = [(key, timeline) for key, timeline in timelines_by_category]   # Create a list of tuples (group_name, timeline dataframe) so that we can sort them by earliest interval
            ordered_timelines = timelines_to_order  # by default, keep the same order, which will be based on timeline_id sort performed while loading data.
            if category.order_timelines_by_earliest_from:
                ordered_timelines = sorted(timelines_to_order, key=lambda key_df: key_df[1]['from'].min())
            for key, timeline in ordered_timelines:
                self.all_timelines[(category_key, key,)] = timeline

    def rebuild_selected_timelines_with(self, selected_rows):
        """
        Populated self.timelines with timelines including data from the selected rows.
        """
        grouped = selected_rows.groupby(['category_str', 'timeline_id'])  # Find all timeline_id tuples for which we should retain data.
        self.selected_timelines = OrderedDict()
        for key, pd_intervals in self.all_timelines.items():
            if key in grouped.groups.keys():
                self.selected_timelines[key] = pd_intervals
        self.update_selected_timeline_keys()

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
        self.rebuild_selected_timelines_with(self.selected_rows)

    def on_zoom_resize(self, timeline_width_px):
        self.current_timeline_width = timeline_width_px
        # Number of seconds which must be displayed in the timeline
        self.current_zoom_timeline_seconds = seconds_between(self.zoom_timeline_start, self.zoom_timeline_stop)
        self.current_pixels_per_second_in_timeline: float = self.calculate_pixels_per_second(self.current_timeline_width)

    def apply_collapse_filter(self):
        """
        Reduces selected rows to timelines which have an interval active in the current zoom time window.
        """
        df = self.selected_rows
        filtered_df = df[(df['to'] >= self.zoom_timeline_start) & (df['from'] <= self.zoom_timeline_stop)]
        # Group the intervals which were detected. Each group key represents a timeline id whose data
        # should be retained. We retain all the intervals in the selected timelines because we will need
        # that data if the user scrolls left or right in the resulting view.
        grouped = filtered_df.groupby(['category_str', 'timeline_id'])
        self.selected_timelines = OrderedDict()
        for key, pd_intervals in self.all_timelines.items():
            if key in grouped.groups.keys():
                self.selected_timelines[key] = pd_intervals
        self.update_selected_timeline_keys()

    def update_selected_timeline_keys(self):
        self.selected_timeline_keys = list(self.selected_timelines.keys())

    def zoom_to_dates(self, from_dt: datetime, to_dt: datetime, refilter_based_on_date_range=False):

        # If the from and to are in reserve chronological order, correct it
        if from_dt > to_dt:
            t_dt = to_dt
            to_dt = from_dt
            from_dt = t_dt

        desired_visible_timedelta = to_dt - from_dt

        # Ensure the timeline will show at least 10 seconds of time.
        if desired_visible_timedelta < timedelta(seconds=10):
            to_dt = from_dt + timedelta(seconds=10)
            desired_visible_timedelta = to_dt - from_dt

        if from_dt < self.absolute_timeline_start:
            from_dt = self.absolute_timeline_start
            to_dt = from_dt + desired_visible_timedelta

        if to_dt > self.absolute_timeline_stop:
            to_dt = self.absolute_timeline_stop
            from_dt = to_dt - desired_visible_timedelta
            if from_dt < self.absolute_timeline_start:
                from_dt = self.absolute_timeline_start

        self.zoom_timeline_start = from_dt
        self.zoom_timeline_stop = to_dt
        if refilter_based_on_date_range:
            self.apply_collapse_filter()
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

    def calculate_interval_pixel_width(self, timeline_width: int, pd_interval_row: pandas.Series) -> float:
        duration = pd_interval_row['duration']
        return max(3.0, duration * self.calculate_pixels_per_second(timeline_width))  # Give even the smallest interval several pixels to ensure it can be hovered over easily.

    def current_interval_width(self, pd_interval_row: pandas.Series) -> float:
        return self.calculate_interval_pixel_width(
            timeline_width=self.current_timeline_width,
            pd_interval_row=pd_interval_row
        )

    def zoom_left_offset_to_datetime(self, left_offset_px) -> datetime:
        """
        Given an offset from the left side of the zoom timeline, what datetime is
        the location approximating?
        :param left_offset_px: distance in pixels from the start of the zoom timeline.
        """
        return left_offset_to_datetime(self.zoom_timeline_start, left_offset_px, pixels_per_second=self.current_pixels_per_second_in_timeline)
