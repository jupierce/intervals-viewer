import arcade
import pandas
import pandas as pd
from pandas.core.groupby import DataFrameGroupBy
from datetime import datetime, timedelta
from typing import Optional, Tuple, Union, List, Callable, Set, Dict
from enum import Enum

NANOSECONDS_PER_SECOND = 1000000000


class IntervalCategory(Enum):
    Alert = 'Alert'
    KubeEvent = 'KubeEvent'
    KubeletLog = 'KubeletLog'
    NodeState = 'NodeState'
    OperatorState = 'OperatorState'
    Pod = 'Pod'
    E2ETest = 'E2ETest'
    Disruption = 'Disruption'
    ClusterState = 'ClusterState'
    PodLog = 'PodLog'

    # Not a real category for display, but serves to classify & color when a classification
    # is not specific to one category.
    Any = '*'

    Unclassified = 'Unclassified'


SingleStrOrSet = Optional[Union[str, Set[str]]]


class SimpleIntervalMatcher:
    """
    Provides an easy way to specify how to classify an interval by
    identifying values for attributes which must be set (and to what values, if desired).
    """
    def __init__(self, temp_source: SingleStrOrSet = None,
                 locator_type: SingleStrOrSet = None,
                 locator_keys_exist: Optional[Set[str]] = None,
                 locator_keys_match: Optional[Dict[str, str]] = None,
                 reason: SingleStrOrSet = None,
                 cause: SingleStrOrSet = None,
                 annotations_exist: Optional[Set[str]] = None,
                 annotations_match: Optional[Dict[str, str]] = None,
                 message_contains: Optional[str] = None,
                 ):
        self.temp_source = temp_source
        self.locator_type = locator_type
        self.locator_keys_exist: Optional[Set[str]] = locator_keys_exist
        self.locator_keys_match: Optional[Dict[str, str]] = locator_keys_match
        self.reason: Optional[str] = reason
        self.cause: Optional[str] = cause
        self.annotations_exist: Optional[Set[str]] = annotations_exist
        self.annotations_match: Optional[Dict[str, str]] = annotations_match
        self.message_contains: Optional[str] = message_contains

    def matches(self, interval: pd.Series) -> bool:

        def matches_any(actual_value, options: SingleStrOrSet):
            if isinstance(options, str):
                return actual_value == options
            else:
                return actual_value in options  # Treat as a Set

        if self.temp_source:
            if not matches_any(IntervalAnalyzer.get_series_column_value(interval, 'tempSource'), self.temp_source):
                return False

        if self.locator_type:
            if not matches_any(IntervalAnalyzer.get_locator_attr(interval, 'type'), self.locator_type):
                return False

        if self.reason:
            if not matches_any(IntervalAnalyzer.get_message_attr(interval, 'reason'), self.reason):
                return False

        if self.cause:
            if not matches_any(IntervalAnalyzer.get_message_attr(interval, 'cause'), self.cause):
                return False

        if self.locator_keys_exist:
            for locator_key in self.locator_keys_exist:
                if IntervalAnalyzer.get_locator_key(interval, locator_key) is None:
                    return False

        if self.message_contains:
            message = IntervalAnalyzer.get_series_column_value(interval, 'message')
            if not message or self.message_contains not in message:
                return False

        def required_value_matches(actual_value: str, required_value: str):
            if actual_value:
                actual_value = actual_value.lower()
            if required_value:
                required_value = required_value.lower()
            return actual_value == required_value

        if self.locator_keys_match:
            for locator_key, required_value in self.locator_keys_match.items():
                return required_value_matches(IntervalAnalyzer.get_locator_key(interval, locator_key), required_value)

        if self.annotations_exist:
            for annotation_name in self.annotations_exist:
                if IntervalAnalyzer.get_message_annotation(interval, annotation_name) is None:
                    return False

        if self.annotations_match:
            for annotation_name, required_value in self.annotations_match.items():
                return required_value_matches(IntervalAnalyzer.get_message_annotation(interval, annotation_name), required_value)

        return True


class IntervalClassification:

    def __init__(self, category: IntervalCategory, color: Optional[arcade.Color] = arcade.color.GRAY,
                 series_matcher: Optional[Callable[[pd.Series], bool]] = None,
                 simple_series_matcher: Optional[SimpleIntervalMatcher] = None):
        self.category = category
        self.color = color
        self.does_series_match = series_matcher
        self.simple_series_matcher = simple_series_matcher

    def matches(self, interval: pd.Series) -> bool:
        if self.simple_series_matcher:
            return self.simple_series_matcher.matches(interval)
        if self.does_series_match:
            return self.does_series_match(interval)
        return False


def hex_to_color(hex_color_code) -> arcade.Color:
    # Remove '#' if present
    hex_color_code = hex_color_code.lstrip('#')

    # Extract RGB and optionally alpha
    if len(hex_color_code) == 6:
        color_tuple = tuple(int(hex_color_code[i:i+2], 16) for i in (0, 2, 4)) + (255,)  # Add alpha=255 if not provided
    elif len(hex_color_code) == 8:
        color_tuple = tuple(int(hex_color_code[i:i+2], 16) for i in (0, 2, 4, 6))
    else:
        raise ValueError("Invalid hex color code length")

    return color_tuple


class IntervalClassifications(Enum):

    # KubeEvents
    PathologicalKnown = IntervalClassification(
        IntervalCategory.KubeEvent, color=hex_to_color('#0000ff'),
        simple_series_matcher=SimpleIntervalMatcher(
            temp_source='KubeEvent',
            annotations_match={
                'interesting': 'true',
                'pathological': 'true',
            }
        )
    )

    InterestingEvent = IntervalClassification(
        IntervalCategory.KubeEvent, color=hex_to_color('#6E6E6E'),
        simple_series_matcher=SimpleIntervalMatcher(
            temp_source='KubeEvent',
            annotations_match={
                'interesting': 'true'
            }
        )
    )
    PathologicalNew = IntervalClassification(
        # PathologicalKnown will capture if interesting=true. New will fall through
        # and be captured here.
        IntervalCategory.KubeEvent, color=hex_to_color('#d0312d'),
        simple_series_matcher=SimpleIntervalMatcher(
            temp_source='KubeEvent',
            annotations_match={
                'pathological': 'true',
            }
        )
    )

    # Alerts
    AlertPending = IntervalClassification(
        IntervalCategory.Alert, color=hex_to_color('#fada5e'),
        simple_series_matcher=SimpleIntervalMatcher(
            temp_source='Alert',
            annotations_match={
                'pending': 'true',
            }
        )
    )
    AlertInfo = IntervalClassification(
        IntervalCategory.Alert, color=hex_to_color('#fada5e'),
        simple_series_matcher=SimpleIntervalMatcher(
            temp_source='Alert',
            annotations_match={
                'severity': 'info',
            }
        )
    )
    AlertWarning = IntervalClassification(
        IntervalCategory.Alert, color=hex_to_color('#ffa500'),
        simple_series_matcher=SimpleIntervalMatcher(
            temp_source='Alert',
            annotations_match={
                'severity': 'warning',
            }
        )
    )
    AlertCritical = IntervalClassification(
        IntervalCategory.Alert, color=hex_to_color('#d0312d'),
        simple_series_matcher=SimpleIntervalMatcher(
            temp_source='Alert',
            annotations_match={
                'severity': 'critical',
            }
        )
    )

    # Operator
    OperatorUnavailable = IntervalClassification(
        IntervalCategory.OperatorState, color=hex_to_color('#d0312d'),
        simple_series_matcher=SimpleIntervalMatcher(
            temp_source='OperatorState',
            annotations_match={
                'condition': 'Available',
                'status': 'false',
            }
        )
    )
    OperatorDegraded = IntervalClassification(
        IntervalCategory.OperatorState, color=hex_to_color('#ffa500'),
        simple_series_matcher=SimpleIntervalMatcher(
            temp_source='OperatorState',
            annotations_match={
                'condition': 'Degraded',
                'status': 'true',
            }
        )
    )
    OperatorProgressing = IntervalClassification(
        IntervalCategory.OperatorState, color=hex_to_color('#fada5e'),
        simple_series_matcher=SimpleIntervalMatcher(
            temp_source='OperatorState',
            annotations_match={
                'condition': 'Progressing',
                'status': 'true',
            }
        )
    )

    # Node
    NodeDrain = IntervalClassification(
        IntervalCategory.NodeState, color=hex_to_color('#4294e6'),
        simple_series_matcher=SimpleIntervalMatcher(
            locator_type='Node',
            annotations_match={
                'phase': 'Drain',
            }
        )
    )
    NodeReboot = IntervalClassification(
        IntervalCategory.NodeState, color=hex_to_color('#6aaef2'),
        simple_series_matcher=SimpleIntervalMatcher(
            locator_type='Node',
            annotations_match={
                'phase': 'Reboot',
            }
        )
    )
    NodeOperatingSystemUpdate = IntervalClassification(
        IntervalCategory.NodeState, color=hex_to_color('#96cbff'),
        simple_series_matcher=SimpleIntervalMatcher(
            locator_type='Node',
            annotations_match={
                'phase': 'OperatingSystemUpdate',
            }
        )
    )
    NodeUpdate = IntervalClassification(
        IntervalCategory.NodeState, color=hex_to_color('#1e7bd9'),
        simple_series_matcher=SimpleIntervalMatcher(
            locator_type='Node',
            annotations_match={
                'reason': 'NodeUpdate',
            }
        )
    )
    NodeNotReady = IntervalClassification(
        IntervalCategory.NodeState, color=hex_to_color('#fada5e'),
        simple_series_matcher=SimpleIntervalMatcher(
            locator_type='Node',
            annotations_match={
                'reason': 'NotReady',
            }
        )
    )

    # Tests
    TestPassed = IntervalClassification(
        IntervalCategory.E2ETest, color=hex_to_color('#3cb043'),
        simple_series_matcher=SimpleIntervalMatcher(
            temp_source='E2ETest',
            annotations_match={
                'status': 'Passed',
            }
        )
    )
    TestSkipped = IntervalClassification(
        IntervalCategory.E2ETest, color=hex_to_color('#ceba76'),
        simple_series_matcher=SimpleIntervalMatcher(
            temp_source='E2ETest',
            annotations_match={
                'status': 'Skipped',
            }
        )
    )
    TestFlaked = IntervalClassification(
        IntervalCategory.E2ETest, color=hex_to_color('#ffa500'),
        simple_series_matcher=SimpleIntervalMatcher(
            temp_source='E2ETest',
            annotations_match={
                'status': 'Flaked',
            }
        )
    )
    TestFailed = IntervalClassification(
        IntervalCategory.E2ETest, color=hex_to_color('#d0312d'),
        simple_series_matcher=SimpleIntervalMatcher(
            temp_source='E2ETest',
            annotations_match={
                'status': 'Failed',
            }
        )
    )

    # Pods
    PodCreated = IntervalClassification(
        IntervalCategory.Pod, color=hex_to_color('#96cbff'),
        simple_series_matcher=SimpleIntervalMatcher(
            temp_source='PodState',
            annotations_match={
                'reason': 'Created',
            }
        )
    )
    PodScheduled = IntervalClassification(
        IntervalCategory.Pod, color=hex_to_color('#1e7bd9'),
        simple_series_matcher=SimpleIntervalMatcher(
            temp_source='PodState',
            annotations_match={
                'reason': 'Scheduled',
            }
        )
    )
    PodTerminating = IntervalClassification(
        IntervalCategory.Pod, color=hex_to_color('#ffa500'),
        simple_series_matcher=SimpleIntervalMatcher(
            temp_source='PodState',
            annotations_match={
                'reason': 'GracefulDelete',
            }
        )
    )
    ContainerWait = IntervalClassification(
        IntervalCategory.Pod, color=hex_to_color('#ca8dfd'),
        simple_series_matcher=SimpleIntervalMatcher(
            temp_source='PodState',
            locator_keys_exist={'container'},
            annotations_match={
                'reason': 'ContainerWait',
            }
        )
    )
    ContainerStart = IntervalClassification(
        IntervalCategory.Pod, color=hex_to_color('#9300ff'),
        simple_series_matcher=SimpleIntervalMatcher(
            temp_source='PodState',
            locator_keys_exist={'container'},
            annotations_match={
                'reason': 'ContainerStart',
            }
        )
    )
    ContainerNotReady = IntervalClassification(
        IntervalCategory.Pod, color=hex_to_color('#fada5e'),
        simple_series_matcher=SimpleIntervalMatcher(
            temp_source='PodState',
            locator_keys_exist={'container'},
            annotations_match={
                'reason': 'NotReady',
            }
        )
    )
    ContainerReady = IntervalClassification(
        IntervalCategory.Pod, color=hex_to_color('#3cb043'),
        simple_series_matcher=SimpleIntervalMatcher(
            temp_source='PodState',
            locator_keys_exist={'container'},
            annotations_match={
                'reason': 'Ready',
            }
        )
    )
    ContainerReadinessFailed = IntervalClassification(
        IntervalCategory.Pod, color=hex_to_color('#d0312d'),
        simple_series_matcher=SimpleIntervalMatcher(
            temp_source='PodState',
            locator_keys_exist={'container'},
            annotations_match={
                'reason': 'ReadinessFailed',
            }
        )
    )
    ContainerReadinessErrored = IntervalClassification(
        IntervalCategory.Pod, color=hex_to_color('#d0312d'),
        simple_series_matcher=SimpleIntervalMatcher(
            temp_source='PodState',
            locator_keys_exist={'container'},
            annotations_match={
                'reason': 'ReadinessErrored',
            }
        )
    )
    StartupProbeFailed = IntervalClassification(
        IntervalCategory.Pod, color=hex_to_color('#c90076'),
        simple_series_matcher=SimpleIntervalMatcher(
            temp_source='PodState',
            annotations_match={
                'reason': 'StartupProbeFailed',
            }
        )
    )

    # Disruption
    CIClusterDisruption = IntervalClassification(
        IntervalCategory.Disruption, color=hex_to_color('#96cbff'),
        simple_series_matcher=SimpleIntervalMatcher(
            temp_source='Disruption',
            message_contains='likely a problem in cluster running tests',
        )
    )
    Disruption = IntervalClassification(
        IntervalCategory.Disruption, color=hex_to_color('#d0312d'),
        simple_series_matcher=SimpleIntervalMatcher(
            temp_source='Disruption',
        )
    )

    # Cargo cult from HTML. Cluster state? Not sure how to match.
    Degraded = IntervalClassification(IntervalCategory.ClusterState, color=hex_to_color('#b65049'))
    Upgradeable = IntervalClassification(IntervalCategory.ClusterState, color=hex_to_color('#32b8b6'))
    StatusFalse = IntervalClassification(IntervalCategory.ClusterState, color=hex_to_color('#ffffff'))
    StatusUnknown = IntervalClassification(IntervalCategory.ClusterState, color=hex_to_color('#bbbbbb'))

    # PodLog
    PodLogWarning = IntervalClassification(
        IntervalCategory.PodLog, color=hex_to_color('#fada5e'),
        simple_series_matcher=SimpleIntervalMatcher(
            temp_source={'PodLog', 'EtcdLog'},
            annotations_match={
                'severity': 'warning',
            }
        )
    )
    PodLogError = IntervalClassification(
        IntervalCategory.PodLog, color=hex_to_color('#d0312d'),
        simple_series_matcher=SimpleIntervalMatcher(
            temp_source={'PodLog', 'EtcdLog'},
            annotations_match={
                'severity': 'error',
            }
        )
    )
    PodLogInfo = IntervalClassification(
        IntervalCategory.PodLog, color=hex_to_color('#96cbff'),
        simple_series_matcher=SimpleIntervalMatcher(
            temp_source={'PodLog', 'EtcdLog'},
            annotations_match={
                'severity': 'info',
            }
        )
    )
    PodLogOther = IntervalClassification(
        IntervalCategory.PodLog, color=hex_to_color('#96cbff'),
        simple_series_matcher=SimpleIntervalMatcher(
            temp_source={'PodLog', 'EtcdLog'},
        )
    )

    # Enums enumerate in the order of their elements, so this
    # is guaranteed to execute last and sweep in anything not already
    # matched.
    UnknownClassification = IntervalClassification(
        IntervalCategory.Unclassified, color=arcade.color.GRAY,
        series_matcher=lambda interval: True
    )


class IntervalAnalyzer:

    STRUCTURED_LOCATOR_PREFIX = 'tempStructuredLocator.'
    STRUCTURED_LOCATOR_KEY_PREFIX = 'tempStructuredLocator.keys.'

    STRUCTURED_MESSAGE_PREFIX = 'tempStructuredMessage.'
    STRUCTURED_MESSAGE_ANNOTATION_PREFIX = 'tempStructuredMessage.annotations.'

    @classmethod
    def get_series_column_value(cls, interval: pd.Series, column_name: str) -> Optional[str]:
        try:
            val = interval[column_name]
            if pd.isnull(val):
                return None
            return val
        except KeyError:
            # The column does not exist in the JSON structure; ignore it.
            return None

    @classmethod
    def get_locator_attr(cls, interval: pd.Series, attr_name: str) -> Optional[str]:
        return IntervalAnalyzer.get_series_column_value(interval, f'{IntervalAnalyzer.STRUCTURED_LOCATOR_PREFIX}{attr_name}')

    @classmethod
    def get_locator_key(cls, interval: pd.Series, key_name: str) -> str:
        return IntervalAnalyzer.get_series_column_value(interval, f'{IntervalAnalyzer.STRUCTURED_LOCATOR_KEY_PREFIX}{key_name}')

    @classmethod
    def get_message_attr(cls, interval: pd.Series, attr_name: str) -> Optional[str]:
        return IntervalAnalyzer.get_series_column_value(interval, f'{IntervalAnalyzer.STRUCTURED_MESSAGE_PREFIX}{attr_name}')

    @classmethod
    def get_message_annotation(cls, interval: pd.Series, annotation_name: str) -> str:
        return IntervalAnalyzer.get_series_column_value(interval, f'{IntervalAnalyzer.STRUCTURED_MESSAGE_ANNOTATION_PREFIX}{annotation_name}')

    @classmethod
    def get_column_names(cls, interval: pd.Series, column_name_prefix: str) -> List[str]:
        selected_col_names: List[str] = list()
        prefix_length = len(column_name_prefix)
        for column_name in interval.index.tolist():
            if str(column_name).startswith(column_name_prefix):
                column_name = column_name[prefix_length:]
                selected_col_names.append(column_name)
        return sorted(selected_col_names)

    @classmethod
    def get_message_annotation_names(cls, interval: pd.Series):
        return IntervalAnalyzer.get_column_names(interval, IntervalAnalyzer.STRUCTURED_MESSAGE_ANNOTATION_PREFIX)

    @classmethod
    def get_locator_key_names(cls, interval: pd.Series):
        return IntervalAnalyzer.get_column_names(interval, IntervalAnalyzer.STRUCTURED_LOCATOR_KEY_PREFIX)


def get_interval_category(row: pd.Series):
    classification: IntervalClassification = row['classification']
    return classification.category.value


def seconds_between(pd_datetime_from, pd_datatime_to) -> float:
    try:
        return float((pd_datatime_to - pd_datetime_from).to_timedelta64()) / NANOSECONDS_PER_SECOND
    except:
        print(f'Error process {pd_datetime_from} -> {pd_datatime_to}')
        return 0.0


def get_interval_duration(row: pd.Series):
    return seconds_between(row['from'], row['to'])


def get_interval_classification(interval: pd.Series) -> IntervalClassifications:
    classifications: List[IntervalClassification] = [e.value for e in IntervalClassifications]
    for classification in classifications:
        if classification.matches(interval):
            return classification
    return IntervalClassifications.UnknownClassification


def get_interval_color(row: pd.Series) -> Union[arcade.Color, Tuple[int, int, int, int]]:
    classification: IntervalClassification = row['classification']
    return classification.color


class Details:

    def __init__(self, ei: "EventsInspector"):
        self.ei = ei
        self.mouse_over_datetime: datetime


class EventsInspector:

    def __init__(self, events_df: pd.DataFrame):
        self.events_df = events_df
        self.last_known_mouse_location: Tuple[int, int] = (0, 0)

        # Set 'to' equal to 'from' where 'to' is null
        self.events_df.loc[self.events_df['to'].isnull(), 'to'] = self.events_df['from']
        # Ensure the 'to' and 'from' columns are parsed as datetime
        self.events_df['to'] = pd.to_datetime(self.events_df['to'], format="%Y-%m-%dT%H:%M:%SZ")
        self.events_df['from'] = pd.to_datetime(self.events_df['from'], format="%Y-%m-%dT%H:%M:%SZ")

        # Filter out intervals that are not important to render
        self.events_df = self.events_df[self.events_df['tempStructuredMessage.reason'] != 'DisruptionEnded']
        # self.events_df = self.events_df[-((self.events_df['tempSource'] == 'E2ETest') & ((self.events_df['tempStructuredMessage.annotations.status'] == 'Passed') | (self.events_df['tempStructuredMessage.annotations.status'] == 'Skipped')))]  # Notice the '-', which inverts the criteria
        # self.events_df = self.events_df[-(self.events_df['tempStructuredMessage.annotations.interesting'] == 'false')]  # Notice the '-', which inverts the criteria

        # Classify an interval. The classification implies category and color for later decoration of the interval row.
        self.events_df['classification'] = self.events_df.apply(get_interval_classification, axis=1)

        # Create a new row called category that will be used as the first grouping level for the
        # data. In the graph area, category for each timeline is shown on the left.
        self.events_df['category'] = self.events_df.apply(get_interval_category, axis=1)
        # Populate the color the interval should be rendered with.
        self.events_df['color'] = self.events_df.apply(get_interval_color, axis=1)

        # Pre-calculate the duration, in seconds, of all intervals
        self.events_df['duration'] = self.events_df.apply(get_interval_duration, axis=1)

        self.absolute_timeline_start: pd.Timestamp = (events_df['from'].min()).floor('min')  # Get the earliest interval start and round down to nearest minute
        self.absolute_timeline_stop: pd.Timestamp = (events_df['to'].max()).ceil('min')

        # Order rows by category, locator, then make sure all rows are in chronological order
        self.events_df = self.events_df.sort_values(['category', 'locator', 'to'], ascending=True)

        self.total_timeline_ns = int((self.absolute_timeline_start - self.absolute_timeline_start).to_timedelta64())

        self.zoom_timeline_start: pd.Timestamp = self.absolute_timeline_start
        self.zoom_timeline_stop: pd.Timestamp = self.absolute_timeline_stop

        self.selected_rows = self.events_df
        self.visible_groups_by_category = self.events_df.groupby(['category'])

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
        self.notify_of_timeline_date_range_change()

    def notify_of_timeline_date_range_change(self):
        """
        Call whenever the selected rows have been modified
        """
        df = self.selected_rows
        filtered_df = df[(df['to'] >= self.zoom_timeline_start) & (df['from'] <= self.zoom_timeline_stop)]
        self.grouped_intervals = filtered_df.groupby(['category', 'locator'])

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


