import pandas as pd
from enum import Enum
from typing import Optional, Union, List, Callable, Set, Dict
import arcade
import pandas.errors

from .util import prioritized_sort


class IntervalAnalyzer:

    STRUCTURED_LOCATOR_ATTR_NAME = 'tempStructuredLocator'
    STRUCTURED_LOCATOR_PREFIX = f'{STRUCTURED_LOCATOR_ATTR_NAME}.'
    STRUCTURED_LOCATOR_KEY_PREFIX = f'{STRUCTURED_LOCATOR_PREFIX}keys.'

    STRUCTURED_MESSAGE_ATTR_NAME = 'tempStructuredMessage'
    STRUCTURED_MESSAGE_PREFIX = f'{STRUCTURED_MESSAGE_ATTR_NAME}.'
    STRUCTURED_MESSAGE_ANNOTATION_PREFIX = f'{STRUCTURED_MESSAGE_PREFIX}annotations.'

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
        selected_col_names: Set[str] = set()
        prefix_length = len(column_name_prefix)
        for column_name in interval.index.tolist():
            if str(column_name).startswith(column_name_prefix):
                column_name = column_name[prefix_length:]
                selected_col_names.add(column_name)
        return sorted(selected_col_names)

    @classmethod
    def get_message_annotation_names(cls, interval: pd.Series):
        return IntervalAnalyzer.get_column_names(interval, IntervalAnalyzer.STRUCTURED_MESSAGE_ANNOTATION_PREFIX)

    @classmethod
    def get_locator_key_names(cls, interval: pd.Series):
        key_names = IntervalAnalyzer.get_column_names(interval, IntervalAnalyzer.STRUCTURED_LOCATOR_KEY_PREFIX)
        return sorted(key_names, key=prioritized_sort)


class IntervalCategory:

    def __init__(self, display_name: str, order_timelines_by_earliest_from=False):
        """
        Args:
            display_name:
            order_timelines_by_earliest_from: Instead of being based on the locator, order timelines in this category based on earliest interval.
        """
        self.display_name = display_name
        self.order_timelines_by_earliest_from = order_timelines_by_earliest_from


class IntervalCategories(Enum):
    Alert = IntervalCategory('Alert')
    KubeEvent = IntervalCategory('KubeEvent')
    KubeletLog = IntervalCategory('KubeletLog')
    NodeState = IntervalCategory('NodeState')
    OperatorState = IntervalCategory('OperatorState')
    Pod = IntervalCategory('Pod')
    E2ETest = IntervalCategory('E2ETest', order_timelines_by_earliest_from=True)
    Disruption = IntervalCategory('Disruption')
    ClusterState = IntervalCategory('ClusterState')
    PodLog = IntervalCategory('PodLog')

    # Not a real category for display, but serves to classify & color when a classification
    # is not specific to one category.
    Any = IntervalCategory('*')

    Unclassified = IntervalCategory('Unclassified')


SingleStrOrSet = Union[str, Set[str]]
OptionalSingleStrOrSet = Optional[SingleStrOrSet]


class SimpleIntervalMatcher:
    """
    Provides an easy way to specify how to classify an interval by
    identifying values for attributes which must be set (and to what values, if desired).
    """
    def __init__(self, temp_source: OptionalSingleStrOrSet = None,
                 locator_type: OptionalSingleStrOrSet = None,
                 locator_keys_exist: Optional[Set[str]] = None,
                 locator_keys_match: Optional[Dict[str, str]] = None,
                 reason: OptionalSingleStrOrSet = None,
                 cause: OptionalSingleStrOrSet = None,
                 annotations_exist: Optional[Set[str]] = None,
                 annotations_match: Optional[Dict[str, str]] = None,
                 message_contains: Optional[str] = None,
                 ):

        and_exprs: List[str] = list()

        def add__and_equal(column_name: str, is_in: OptionalSingleStrOrSet, column_name_prefix: Optional[str] = ''):
            if is_in is None:
                return
            if isinstance(is_in, str):
                and_exprs.append(f'`{column_name_prefix}{column_name}` == "{is_in}"')
            else:
                quoted = ', '.join([f'"{to_quote}"' for to_quote in is_in])
                and_exprs.append(f'`{column_name_prefix}{column_name}`.isin([{quoted}])')

        def add__and_not_null(column_names: Optional[Set[str]], column_name_prefix: Optional[str] = ''):
            if column_names:
                for column_name in column_names:
                    and_exprs.append(f'(not `{column_name_prefix}{column_name}`.isnull())')

        def add__and_all_equal(column_vals: Optional[Dict[str, str]], column_name_prefix: Optional[str] = ''):
            if column_vals:
                for column_name, val in column_vals.items():
                    add__and_equal(column_name, val, column_name_prefix)

        and_exprs.append('classification.isnull()')  # Only set classification if nothing has already set it.
        add__and_equal('tempSource', temp_source)
        add__and_equal('type', locator_type, IntervalAnalyzer.STRUCTURED_LOCATOR_PREFIX)
        add__and_not_null(locator_keys_exist, IntervalAnalyzer.STRUCTURED_LOCATOR_KEY_PREFIX)
        add__and_all_equal(locator_keys_match, IntervalAnalyzer.STRUCTURED_LOCATOR_KEY_PREFIX)
        add__and_equal('reason', reason, IntervalAnalyzer.STRUCTURED_MESSAGE_PREFIX)
        add__and_equal('cause', cause, IntervalAnalyzer.STRUCTURED_MESSAGE_PREFIX)
        add__and_not_null(annotations_exist, IntervalAnalyzer.STRUCTURED_MESSAGE_ANNOTATION_PREFIX)
        add__and_all_equal(annotations_match, IntervalAnalyzer.STRUCTURED_MESSAGE_ANNOTATION_PREFIX)
        if message_contains:
            and_exprs.append(f'message.str.contains("{message_contains}")')

        self.targeting_query = ' & '.join(and_exprs)
        pass


class IntervalClassification:

    def __init__(self, display_name: str,
                 category: IntervalCategories, color: Optional[arcade.Color] = arcade.color.GRAY,
                 simple_interval_matcher: SimpleIntervalMatcher = None,
                 timeline_differentiator: str = '', ):
        self.display_name = display_name
        self.category: IntervalCategories = category
        self.color = color
        self.series_matcher = simple_interval_matcher
        # If this interval classification should cause records with the same locator
        # to appear on different timelines, differentiate the timeline with an additional string.
        # For example, ContainerLifecycle and ContainerReadiness
        self.timeline_differentiator = timeline_differentiator

    def apply(self, events_df: pd.DataFrame) -> pd.DataFrame:
        if self.series_matcher:  # If there is no matcher specified, it can't match anything.
            try:
                # For any classification that has not already been set, set fields in rows with matching criteria
                events_df.loc[events_df.eval(self.series_matcher.targeting_query), ['category', 'category_str_lower', 'classification', 'classification_str_lower', 'timeline_diff']] = self.category.value.display_name, self.category.value.display_name.lower(), self, self.display_name.lower(), self.timeline_differentiator
            except pandas.errors.UndefinedVariableError:
                print(f'warning: no keys in json available to allow classification of type: {self.display_name}')
        return events_df


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
        display_name='PathologicalKnown',
        category=IntervalCategories.KubeEvent,
        color=hex_to_color('#0000ff'),
        simple_interval_matcher=SimpleIntervalMatcher(
            temp_source='KubeEvent',
            annotations_match={
                'interesting': 'true',
                'pathological': 'true',
            }
        )
    )

    InterestingEvent = IntervalClassification(
        display_name='InterestingEvent',
        category=IntervalCategories.KubeEvent,
        color=hex_to_color('#6E6E6E'),
        simple_interval_matcher=SimpleIntervalMatcher(
            temp_source='KubeEvent',
            annotations_match={
                'interesting': 'true'
            }
        )
    )
    PathologicalNew = IntervalClassification(
        # PathologicalKnown will capture if interesting=true. New will fall through
        # and be captured here.
        display_name='PathologicalNew',
        category=IntervalCategories.KubeEvent,
        color=hex_to_color('#d0312d'),
        simple_interval_matcher=SimpleIntervalMatcher(
            temp_source='KubeEvent',
            annotations_match={
                'pathological': 'true',
            }
        )
    )

    # Alerts
    AlertPending = IntervalClassification(
        display_name='AlertPending',
        category=IntervalCategories.Alert, color=hex_to_color('#fada5e'),
        simple_interval_matcher=SimpleIntervalMatcher(
            temp_source='Alert',
            annotations_match={
                'alertstate': 'pending',
            }
        )
    )
    AlertInfo = IntervalClassification(
        display_name='AlertInfo',
        category=IntervalCategories.Alert, color=hex_to_color('#fada5e'),
        simple_interval_matcher=SimpleIntervalMatcher(
            temp_source='Alert',
            annotations_match={
                'severity': 'info',
            }
        )
    )
    AlertWarning = IntervalClassification(
        display_name='AlertWarning',
        category=IntervalCategories.Alert, color=hex_to_color('#ffa500'),
        simple_interval_matcher=SimpleIntervalMatcher(
            temp_source='Alert',
            annotations_match={
                'severity': 'warning',
            }
        )
    )
    AlertCritical = IntervalClassification(
        display_name='AlertCritical',
        category=IntervalCategories.Alert, color=hex_to_color('#d0312d'),
        simple_interval_matcher=SimpleIntervalMatcher(
            temp_source='Alert',
            annotations_match={
                'severity': 'critical',
            }
        )
    )

    # Operator
    OperatorUnavailable = IntervalClassification(
        display_name='OperatorUnavailable',
        category=IntervalCategories.OperatorState, color=hex_to_color('#d0312d'),
        simple_interval_matcher=SimpleIntervalMatcher(
            temp_source='OperatorState',
            annotations_match={
                'condition': 'Available',
                'status': 'false',
            }
        )
    )
    OperatorDegraded = IntervalClassification(
        display_name='OperatorDegraded',
        category=IntervalCategories.OperatorState, color=hex_to_color('#ffa500'),
        simple_interval_matcher=SimpleIntervalMatcher(
            temp_source='OperatorState',
            annotations_match={
                'condition': 'Degraded',
                'status': 'true',
            }
        )
    )
    OperatorProgressing = IntervalClassification(
        display_name='OperatorProgressing',
        category=IntervalCategories.OperatorState, color=hex_to_color('#fada5e'),
        simple_interval_matcher=SimpleIntervalMatcher(
            temp_source='OperatorState',
            annotations_match={
                'condition': 'Progressing',
                'status': 'true',
            }
        )
    )

    # Node
    NodeDrain = IntervalClassification(
        display_name='NodeDrain',
        category=IntervalCategories.NodeState, color=hex_to_color('#4294e6'),
        simple_interval_matcher=SimpleIntervalMatcher(
            locator_type='Node',
            annotations_match={
                'phase': 'Drain',
            }
        )
    )
    NodeReboot = IntervalClassification(
        display_name='NodeReboot',
        category=IntervalCategories.NodeState, color=hex_to_color('#6aaef2'),
        simple_interval_matcher=SimpleIntervalMatcher(
            locator_type='Node',
            annotations_match={
                'phase': 'Reboot',
            }
        )
    )
    NodeOperatingSystemUpdate = IntervalClassification(
        display_name='NodeOperatingSystemUpdate',
        category=IntervalCategories.NodeState, color=hex_to_color('#96cbff'),
        simple_interval_matcher=SimpleIntervalMatcher(
            locator_type='Node',
            annotations_match={
                'phase': 'OperatingSystemUpdate',
            }
        )
    )
    NodeUpdate = IntervalClassification(
        display_name='NodeUpdate',
        category=IntervalCategories.NodeState, color=hex_to_color('#1e7bd9'),
        simple_interval_matcher=SimpleIntervalMatcher(
            locator_type='Node',
            annotations_match={
                'reason': 'NodeUpdate',
            }
        )
    )
    NodeNotReady = IntervalClassification(
        display_name='NodeNotReady',
        category=IntervalCategories.NodeState, color=hex_to_color('#fada5e'),
        simple_interval_matcher=SimpleIntervalMatcher(
            locator_type='Node',
            annotations_match={
                'reason': 'NotReady',
            }
        )
    )

    # Tests
    TestPassed = IntervalClassification(
        display_name='TestPassed',
        category=IntervalCategories.E2ETest, color=hex_to_color('#3cb043'),
        simple_interval_matcher=SimpleIntervalMatcher(
            temp_source='E2ETest',
            annotations_match={
                'status': 'Passed',
            }
        )
    )
    TestSkipped = IntervalClassification(
        display_name='TestSkipped',
        category=IntervalCategories.E2ETest, color=hex_to_color('#ceba76'),
        simple_interval_matcher=SimpleIntervalMatcher(
            temp_source='E2ETest',
            annotations_match={
                'status': 'Skipped',
            }
        )
    )
    TestFlaked = IntervalClassification(
        display_name='TestFlaked',
        category=IntervalCategories.E2ETest, color=hex_to_color('#ffa500'),
        simple_interval_matcher=SimpleIntervalMatcher(
            temp_source='E2ETest',
            annotations_match={
                'status': 'Flaked',
            }
        )
    )
    TestFailed = IntervalClassification(
        display_name='TestFailed',
        category=IntervalCategories.E2ETest, color=hex_to_color('#d0312d'),
        simple_interval_matcher=SimpleIntervalMatcher(
            temp_source='E2ETest',
            annotations_match={
                'status': 'Failed',
            }
        )
    )

    # Pods
    PodCreated = IntervalClassification(
        display_name='PodCreated',
        category=IntervalCategories.Pod, color=hex_to_color('#96cbff'),
        simple_interval_matcher=SimpleIntervalMatcher(
            temp_source='PodState',
            annotations_match={
                'reason': 'Created',
            }
        )
    )
    PodScheduled = IntervalClassification(
        display_name='PodScheduled',
        category=IntervalCategories.Pod, color=hex_to_color('#1e7bd9'),
        simple_interval_matcher=SimpleIntervalMatcher(
            temp_source='PodState',
            annotations_match={
                'reason': 'Scheduled',
            }
        )
    )
    PodTerminating = IntervalClassification(
        display_name='PodTerminating',
        category=IntervalCategories.Pod, color=hex_to_color('#ffa500'),
        simple_interval_matcher=SimpleIntervalMatcher(
            temp_source='PodState',
            annotations_match={
                'reason': 'GracefulDelete',
            }
        )
    )
    ContainerWait = IntervalClassification(
        display_name='ContainerWait',
        category=IntervalCategories.Pod, color=hex_to_color('#ca8dfd'),
        simple_interval_matcher=SimpleIntervalMatcher(
            temp_source='PodState',
            locator_keys_exist={'container'},
            annotations_match={
                'reason': 'ContainerWait',
            }
        ),
        timeline_differentiator='container-lifecycle'
    )
    ContainerStart = IntervalClassification(
        display_name='ContainerStart',
        category=IntervalCategories.Pod, color=hex_to_color('#9300ff'),
        simple_interval_matcher=SimpleIntervalMatcher(
            temp_source='PodState',
            locator_keys_exist={'container'},
            annotations_match={
                'reason': 'ContainerStart',
            }
        ),
        timeline_differentiator='container-lifecycle'
    )
    ContainerNotReady = IntervalClassification(
        display_name='ContainerNotReady',
        category=IntervalCategories.Pod, color=hex_to_color('#fada5e'),
        simple_interval_matcher=SimpleIntervalMatcher(
            temp_source='PodState',
            locator_keys_exist={'container'},
            annotations_match={
                'reason': 'NotReady',
            }
        ),
        timeline_differentiator='container-readiness'
    )
    ContainerReady = IntervalClassification(
        display_name='ContainerReady',
        category=IntervalCategories.Pod, color=hex_to_color('#3cb043'),
        simple_interval_matcher=SimpleIntervalMatcher(
            temp_source='PodState',
            locator_keys_exist={'container'},
            annotations_match={
                'reason': 'Ready',
            }
        ),
        timeline_differentiator='container-readiness'
    )
    ContainerReadinessFailed = IntervalClassification(
        display_name='ContainerReadinessFailed',
        category=IntervalCategories.Pod, color=hex_to_color('#d0312d'),
        simple_interval_matcher=SimpleIntervalMatcher(
            temp_source='PodState',
            locator_keys_exist={'container'},
            annotations_match={
                'reason': 'ReadinessFailed',
            }
        ),
        timeline_differentiator='container-readiness'
    )
    ContainerReadinessErrored = IntervalClassification(
        display_name='ContainerReadinessErrored',
        category=IntervalCategories.Pod, color=hex_to_color('#d0312d'),
        simple_interval_matcher=SimpleIntervalMatcher(
            temp_source='PodState',
            locator_keys_exist={'container'},
            annotations_match={
                'reason': 'ReadinessErrored',
            }
        ),
        timeline_differentiator='container-readiness'
    )
    StartupProbeFailed = IntervalClassification(
        display_name='StartupProbeFailed',
        category=IntervalCategories.Pod, color=hex_to_color('#c90076'),
        simple_interval_matcher=SimpleIntervalMatcher(
            temp_source='PodState',
            annotations_match={
                'reason': 'StartupProbeFailed',
            }
        ),
        timeline_differentiator='container-readiness'
    )

    # Disruption
    CIClusterDisruption = IntervalClassification(
        display_name='CIClusterDisruption',
        category=IntervalCategories.Disruption, color=hex_to_color('#96cbff'),
        simple_interval_matcher=SimpleIntervalMatcher(
            temp_source='Disruption',
            message_contains='likely a problem in cluster running tests',
        )
    )
    Disruption = IntervalClassification(
        display_name='Disruption',
        category=IntervalCategories.Disruption, color=hex_to_color('#d0312d'),
        simple_interval_matcher=SimpleIntervalMatcher(
            temp_source='Disruption',
        )
    )

    # Cargo cult from HTML. Cluster state? Not sure how to match.
    Degraded = IntervalClassification(
        display_name='Degraded',
        category=IntervalCategories.ClusterState, color=hex_to_color('#b65049')
    )
    Upgradeable = IntervalClassification(
        display_name='Upgradeable',
        category=IntervalCategories.ClusterState, color=hex_to_color('#32b8b6')
    )
    StatusFalse = IntervalClassification(
        display_name='StatusFalse',
        category=IntervalCategories.ClusterState, color=hex_to_color('#ffffff')
    )
    StatusUnknown = IntervalClassification(
        display_name='StatusUnknown',
        category=IntervalCategories.ClusterState, color=hex_to_color('#bbbbbb')
    )

    # PodLog
    PodLogWarning = IntervalClassification(
        display_name='PodLogWarning',
        category=IntervalCategories.PodLog, color=hex_to_color('#fada5e'),
        simple_interval_matcher=SimpleIntervalMatcher(
            temp_source={'PodLog', 'EtcdLog'},
            annotations_match={
                'severity': 'warning',
            }
        )
    )
    PodLogError = IntervalClassification(
        display_name='PodLogError',
        category=IntervalCategories.PodLog, color=hex_to_color('#d0312d'),
        simple_interval_matcher=SimpleIntervalMatcher(
            temp_source={'PodLog', 'EtcdLog'},
            annotations_match={
                'severity': 'error',
            }
        )
    )
    PodLogInfo = IntervalClassification(
        display_name='PodLogInfo',
        category=IntervalCategories.PodLog, color=hex_to_color('#96cbff'),
        simple_interval_matcher=SimpleIntervalMatcher(
            temp_source={'PodLog', 'EtcdLog'},
            annotations_match={
                'severity': 'info',
            }
        )
    )
    PodLogOther = IntervalClassification(
        display_name='PodLogOther',
        category=IntervalCategories.PodLog, color=hex_to_color('#96cbff'),
        simple_interval_matcher=SimpleIntervalMatcher(
            temp_source={'PodLog', 'EtcdLog'},
        )
    )

    # Enums enumerate in the order of their elements, so this
    # is guaranteed to execute last and sweep in anything not already
    # matched.
    UnknownClassification = IntervalClassification(
        display_name='Unknown',
        category=IntervalCategories.Unclassified, color=arcade.color.GRAY,
        simple_interval_matcher=SimpleIntervalMatcher(),  # Match everything that is not already classified
    )
