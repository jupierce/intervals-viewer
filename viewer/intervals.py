import pandas as pd
from enum import Enum
from typing import Optional, Union, List, Callable, Set, Dict
import arcade


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
