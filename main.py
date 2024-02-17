import datetime
from collections import OrderedDict
from functools import lru_cache
import pandas as pd
import json
import arcade
import os
import copy
import math

from typing import Optional, List, Tuple, Dict, Set, Iterable

from intervaltree import IntervalTree, Interval

from analyzer import SimpleRect, EventsInspector, humanize_timedelta, seconds_between, left_offset_to_datetime, left_offset_from_datetime, get_interval_width_px
from analyzer.intervals import IntervalAnalyzer, IntervalCategories, IntervalClassification, IntervalClassifications
from ui import FilteringView, ImportTimelineView
from ui.layout import Theme, Layout
from analyzer.util import extract_and_process_tar
import pyglet


def get_max_resolution():
    screens = pyglet.canvas.get_display().get_screens()
    max_width = max(screen.width for screen in screens)
    max_height = max(screen.height for screen in screens)
    return max_width, max_height


# For performance reasons, we need caches that can contain all
# visible rows on a screen.
MAX_TIMELINES_TO_DISPLAY_AT_ONCE = max(*get_max_resolution())


INIT_SCREEN_WIDTH = 500
INIT_SCREEN_HEIGHT = 500
SCREEN_TITLE = "Intervals Analysis"


class LineInfo:
    def __init__(self, start: arcade.Point, stop: arcade.Point, color: arcade.Color, line_width: int = 1):
        self.start = start
        self.end = stop
        self.color = color
        self.line_width = line_width

    def __eq__(self, other):
        if isinstance(other, LineInfo):
            return self.start == other.start and self.end == other.end and self.color == other.color and self.line_width == other.line_width
        return NotImplemented

    def __hash__(self):
        return hash((self.start, self.end, self.color, self.line_width))


class IntervalsTimelineEntry:
    def __init__(self, pd_interval: pd.Series, rect_offset: int):
        self.pd_interval = pd_interval
        self.rect_offset = rect_offset


class IntervalsTimeline:

    def __init__(self,
                 group_id: Tuple,
                 pd_interval_rows: pd.DataFrame,
                 timeline_row_height: int,
                 timeline_absolute_start: pd.Timestamp,
                 pixels_per_second: float):
        self.lower_layer_decorations_cache: Optional[Tuple[List[LineInfo], List[arcade.Shape]]] = None
        self.first_interval_row = pd_interval_rows.iloc[0]
        self.group_id = group_id
        self.pd_interval_rows = pd_interval_rows

        self.timeline_absolute_start = timeline_absolute_start
        self.pixels_per_second = pixels_per_second
        self._interval_tree = IntervalTree()  # Efficient data structure used to detect when mouse is over a given interval

        # The rect_list contains a list of points [ (r1x1, r1y1), (r1x2, r1y2), (r1x3, r1y3), (r1x4, r1y4), (r2x1, r2y1), (r2x2, r1y2)...
        # representing different rectangle corners (the example above are the corners for two rectangles, r1/r2).
        # There is a rectangle for every interval on the timeline. The coordinates of these rectangles are relative to x=0 being the absolute
        # start time of the timeline. y=0 is the bottom of the timeline and y=timeline_row_height is the top.
        self.rect_list: List = list()

        # A lightweight object representing a specific interval in the timeline. There is one entry here
        # for each 4 points in the rect_list.
        self.entry_list: List[IntervalsTimelineEntry] = list()

        # Timeline rects are included in a ShapeElementList for the entire visible graph area. Since
        # rect_list is all based on y=0, we need those rects moved horizontally on the screen to
        # account for where the user has scrolled.
        self.transformed_rect_list: arcade.PointList = list()

        # Each rectangle point can have its own color. So there are four colors per interval.
        # If these colors are different per rectangle, there will be gradients in the rectangle.
        self.color_list: List[arcade.Color] = list()

        self.timeline_row_height = timeline_row_height
        self.set_size()
        self.last_transform_y = 0

        # If a series within this timeline is determined to be under the mouse,
        # this value will be set to the interval's row. When the mouse
        # leaves the interval's area, this value will be unset.
        self.interval_under_mouse: Optional[pd.Series] = None

    def set_size(self):
        """
        Populates an element list with the rendered timeline. on_draw uses this shape element list.
        """""
        self.rect_list = list()
        self.color_list = list()
        self.entry_list = list()

        # pd_interval_rows contains a list of intervals specific to this timeline.
        # iterate through them all and draw the relevant lines in a shape element list.
        offset = 0
        for _, interval_row in self.pd_interval_rows.iterrows():
            absolute_interval_line_start_x, absolute_interval_line_end_x = self.get_absolute_interval_horizontal_extents(interval_row)

            top_left = [absolute_interval_line_start_x, self.timeline_row_height]
            top_right = [absolute_interval_line_end_x, self.timeline_row_height]
            # Make bottom 1-based so as to leave some space between timelines when they are displayed
            bottom_right = [absolute_interval_line_end_x, 1]
            bottom_left = [absolute_interval_line_start_x, 1]

            # Add the pixels this rectangle occupies to a datastructure that can efficiently find the interval under a given x
            # coordinate. Use floor/ceil because x coordinate from mouse will be int.
            self._interval_tree.add(Interval(math.floor(absolute_interval_line_start_x), math.ceil(absolute_interval_line_end_x), offset))

            self.rect_list.extend(
                (
                    top_left,
                    top_right,
                    bottom_right,
                    bottom_left,
                )
            )
            self.entry_list.append(IntervalsTimelineEntry(interval_row, offset))
            color = interval_row['classification'].color
            self.color_list.extend(
                (
                    color,
                    color,
                    color,
                    color,
                )
            )
            offset += 1

    def get_absolute_interval_horizontal_extents(self, interval_row: pd.Series) -> Tuple[float, float]:
        """
        Returns the starting x and ending x of an interval within the absolute timeline. The
        x values are relative to the beginning of the timeline - not relative to the screen.
        :param interval_row: An interval on the timeline.
        :return: (start_x, end_x)
        """
        interval_absolute_left_offset = left_offset_from_datetime(self.timeline_absolute_start, interval_row['from'], self.pixels_per_second)
        interval_width = get_interval_width_px(interval_row, self.pixels_per_second)
        interval_line_absolute_start_x = interval_absolute_left_offset
        interval_line_absolute_end_x = interval_absolute_left_offset + interval_width
        return interval_line_absolute_start_x, interval_line_absolute_end_x

    def apply_transform(self, by_y: float) -> Tuple[arcade.PointList, List[arcade.Color]]:
        """
        Returns rectangle coordinates for the rectangles in this timeline after shifting
        them vertically by the specified offset.
        Prior to a transform, rectangles are offset by (0, 0), where x=0 would be the absolute
        timeline starting position.
        """
        if by_y == self.last_transform_y:
            # If the values match, we've already computed the transform
            return self.transformed_rect_list, self.color_list

        self.last_transform_y = by_y
        self.transformed_rect_list = copy.deepcopy(self.rect_list)
        for point in self.transformed_rect_list:
            point[1] += by_y  # A y coordinate

        return self.transformed_rect_list, self.color_list

    def get_timeline_entries_at_x(self, absolute_x: float) -> Optional[List[IntervalsTimelineEntry]]:
        """
        Given an absolute x offset from the start of the timeline, returns
        an interval (only one if there is more than one) the x position overlaps with.
        If multiple intervals are found, the list will be sorted in chronological order.
        """
        matching_intervaltree_intervals = self._interval_tree[int(absolute_x)]
        if matching_intervaltree_intervals:
            ordered_intervaltree_intervals = sorted(matching_intervaltree_intervals, key=lambda interval: interval.begin)
            matching_timeline_entries: List[IntervalsTimelineEntry] = list()
            for intervaltree_interval in ordered_intervaltree_intervals:
                matching_timeline_entries.append(self.entry_list[intervaltree_interval.data])
            return matching_timeline_entries
        return None

    def draw(self, check_for_mouse_over_interval=False):
        global detail_section_ref

        # mouse_x, mouse_y = self.ei.last_known_mouse_location
        # row_bottom = self.shape_element_list.center_y
        # row_top = self.shape_element_list.center_y + self.timeline_row_height
        # mouse_is_over_this_timeline = mouse_y >= row_bottom and mouse_y < row_top
        #
        # # See if the mouse is over a timeline that shares something in common with us (e.g. namespace).
        # # if it does, render one or more yellow lines to indicate how much of a match we are.
        # mouse_over_intervals = detail_section_ref.mouse_over_intervals
        # if mouse_over_intervals is not None:
        #     first_interval_of_mouse_over_intervals = mouse_over_intervals.iloc[0]
        #     match_level = 0
        #     for match_attr in ('namespace', 'pod', 'container', 'uid'):
        #         over_attr = IntervalAnalyzer.get_locator_key(first_interval_of_mouse_over_intervals, match_attr)
        #         if over_attr and IntervalAnalyzer.get_locator_key(self.first_interval_row, match_attr) == over_attr:
        #             match_level += 1
        #         else:
        #             break
        #
        #     for level in range(match_level):
        #         arcade.draw_line(
        #             start_x=Layout.CATEGORY_BAR_RIGHT,
        #             start_y=self.last_set_bottom + self.timeline_row_height / 2,
        #             end_x=self.window.width - Layout.VERTICAL_SCROLL_BAR_RIGHT_OFFSET,
        #             end_y=self.last_set_bottom + self.timeline_row_height / 2,
        #             line_width=self.timeline_row_height,
        #             color=(255, 255, 0, 160 // (5 - level)),
        #         )

        # if self.interval_under_mouse is not None:
        #     # There is an interval under a recent mouse position --
        #     # enhance the size of the interval visually. Rows are painted
        #     # from top to bottom. If we increase size downward, the next row
        #     # will paint over it. So increase size upward.
        #     start_x, end_x = self.get_interval_extents(self.interval_under_mouse)
        #
        #     # First, draw a bright white rectangle (just a wide line) that is
        #     # slightly offset from the selected interval. A few of these white pixels
        #     # will be left around the border of the interval when it renders in.
        #     arcade.draw_line(
        #         start_x=self.last_set_left + start_x - 2,
        #         start_y=self.last_set_bottom + self.timeline_row_height / 2 + 4,
        #         end_x=self.last_set_left + end_x,
        #         end_y=self.last_set_bottom + self.timeline_row_height / 2 + 4,
        #         line_width=self.timeline_row_height + 2,
        #         color=arcade.color.WHITE,
        #     )
        #
        #     arcade.draw_line(
        #         start_x=self.last_set_left + start_x,
        #         start_y=self.last_set_bottom + self.timeline_row_height / 2 + 2,
        #         end_x=self.last_set_left + end_x,
        #         end_y=self.last_set_bottom + self.timeline_row_height / 2 + 2,
        #         line_width=self.timeline_row_height + 2,
        #         color=self.interval_under_mouse['classification'].color
        #     )

        # if detail_section_ref:
        #
        #     def clear_my_interval_detail():
        #         if self.interval_under_mouse is not None:
        #             # If we set the interval being displayed by the detail section, clear it
        #             if self.interval_under_mouse.equals(detail_section_ref.mouse_over_interval):
        #                 detail_section_ref.set_mouse_over_interval(None)
        #             self.interval_under_mouse = None
        #
        #     # See if the mouse is over this particular timeline row visualization
        #     if mouse_is_over_this_timeline:
        #
        #         # Draw the horizontal cross hair line
        #         arcade.draw_line(
        #             start_x=Layout.CATEGORY_BAR_WIDTH + Layout.CATEGORY_BAR_LEFT,
        #             start_y=self.last_set_bottom + 1,
        #             end_x=Layout.CATEGORY_BAR_WIDTH + Layout.CATEGORY_BAR_LEFT + self.timeline_row_width,
        #             end_y=self.last_set_bottom + 1,
        #             line_width=1,
        #             color=Theme.COLOR_CROSS_HAIR_LINES
        #         )
        #
        #         if check_for_mouse_over_interval:
        #             detail_section_ref.set_mouse_over_intervals(self.pd_interval_rows)
        #             # Determine whether the mouse is over a date selecting an interval in this timeline
        #             if detail_section_ref.mouse_over_time_dt:
        #                 df = self.pd_interval_rows
        #                 moment = detail_section_ref.mouse_over_time_dt
        #                 over_intervals_df = df[(moment >= df['from']) & (moment <= df['to'])]
        #                 if not over_intervals_df.empty:
        #                     # The mouse is over one or more intervals in the timeline.
        #                     # Select the last one in the list since the analysis module sorts
        #                     # based on from, the last one in the list should be the one that started
        #                     # most closely to the mouse point and the one that visually occupies the
        #                     # screen. i.e. if there are overlapping intervals in the timeline, the
        #                     # newer one will paint over the order as rendering progresses left->right
        #                     # in the timeline draw.
        #                     over_interval = over_intervals_df.iloc[-1]
        #                     detail_section_ref.set_mouse_over_interval(over_interval)
        #                     self.interval_under_mouse = over_interval
        #                 else:
        #                     clear_my_interval_detail()
        #     else:
        #         # If the mouse is not over the timeline, it is definitely not over an interval
        #         # If we set the interval, clear it
        #         clear_my_interval_detail()

    @lru_cache(1)
    def get_category_name(self) -> str:
        return self.first_interval_row['category_str']

    @lru_cache(1)
    def get_locator_value(self) -> str:
        return self.first_interval_row['locator']

    @lru_cache(1)
    def get_timeline_id(self) -> str:
        return self.first_interval_row['timeline_id']

    def _lower_highlight_color_for_match_level(self, level: int):
        return (255, 255, 0, 160 // (5 - level))

    def _get_lower_layer_decorations(self, mouse_over_intervals_timeline: Optional["IntervalsTimeline"], mouse_over_intervals_timeline_entry: Optional[IntervalsTimelineEntry], absolute_timeline_pixel_width: float) -> List[LineInfo]:
        safe_width = absolute_timeline_pixel_width + Layout.CATEGORY_BAR_RIGHT  # Sufficient to cover the entire absolute timeline width
        if mouse_over_intervals_timeline and len(mouse_over_intervals_timeline.pd_interval_rows.index) > 0 is not None:
            first_interval_of_mouse_over_intervals = mouse_over_intervals_timeline.pd_interval_rows.iloc[0]
            match_level = 0
            if self != mouse_over_intervals_timeline:
                # If the mouse is over an interval timeline which is NOT this, see if this timeline shares any characteristics
                # with the timeline under the mouse. Draw a dim yellow color under the timeline if so, an increase color
                # intensity by the strength of the match.
                for match_attr in ('namespace', 'pod', 'container', 'uid'):
                    over_attr = IntervalAnalyzer.get_locator_key(first_interval_of_mouse_over_intervals, match_attr)
                    if over_attr and IntervalAnalyzer.get_locator_key(self.first_interval_row, match_attr) == over_attr:
                        match_level += 1
                    else:
                        break

                if match_level > 0:
                    return [
                        LineInfo((0, self.last_transform_y + self.timeline_row_height//2),
                                 (safe_width, self.last_transform_y + self.timeline_row_height//2),
                                 self._lower_highlight_color_for_match_level(match_level),
                                 line_width=self.timeline_row_height)
                    ]
            else:
                # This timeline is under the mouse.
                return [
                    LineInfo((0, self.last_transform_y + self.timeline_row_height // 2),
                             (safe_width, self.last_transform_y + self.timeline_row_height // 2),
                             (211, 155, 203, 120),
                             line_width=self.timeline_row_height)
                ]

        return []

    def _convert_to_shapes(self, lines: List[LineInfo]) -> List[arcade.Shape]:
        shapes: List[arcade.Shape] = list()
        for line in lines:
            shapes.append(arcade.create_line(
                start_x=line.start[0],
                start_y=line.start[1],
                end_x=line.end[0],
                end_y=line.end[1],
                color=line.color,
                line_width=line.line_width
            ))
        return shapes

    @lru_cache(maxsize=MAX_TIMELINES_TO_DISPLAY_AT_ONCE)
    def get_lower_layer_decorations(self,
                                    mouse_over_intervals_timeline: Optional["IntervalsTimeline"],
                                    mouse_over_intervals_timeline_entry: Optional[IntervalsTimelineEntry],
                                    row_offset: int,  # While row_offset is not used for calculations, it is critical for caching as decorations need to move when a timeline is displayed on a different row
                                    absolute_timeline_pixel_width: float,  # The number of pixels in the absolute timeline (i.e. ignore current zoom)
                                    ) -> Tuple[List[arcade.Shape], bool]:
        new_lower_layer_decorations = self._get_lower_layer_decorations(mouse_over_intervals_timeline, mouse_over_intervals_timeline_entry, absolute_timeline_pixel_width)
        changed = False
        if self.lower_layer_decorations_cache is None or new_lower_layer_decorations != self.lower_layer_decorations_cache[0]:
            self.lower_layer_decorations_cache = (new_lower_layer_decorations, self._convert_to_shapes(new_lower_layer_decorations))
            changed = True
        return self.lower_layer_decorations_cache[1], changed


class MessageSection(arcade.Section):
    """
    Section for display long strings.
    """

    def __init__(self, ei: EventsInspector, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.background = SimpleRect(
            color=Theme.COLOR_MESSAGE_SECTION_BACKGROUND,
            border_color=arcade.color.LIGHT_GRAY,
            border_width=1
        )
        self.ei = ei
        self.long_text = arcade.Text(
            text='',
            start_x=Layout.MESSAGE_SECTION_LEFT + 2,
            start_y=0,
            multiline=True,
            width=self.window.width - 20,
            color=Theme.COLOR_MESSAGE_SECTION_FONT_COLOR,
            font_size=10,
            font_name=Theme.FONT_NAME
        )
        self.on_resize(self.window.width, self.window.height)
        self.set_message('')  # Trigger help message content

    def on_resize(self, window_width: int, window_height: int):
        self.background.position(
            left=Layout.MESSAGE_SECTION_LEFT,
            right=window_width - Layout.MESSAGE_SECTION_RIGHT_OFFSET,
            bottom=Layout.MESSAGE_SECTION_BOTTOM,
            top=Layout.MESSAGE_SECTION_TOP
        )
        self.long_text.width = self.background.width - 5
        self.long_text.y = self.background.top - 14

    def on_draw(self):
        self.background.draw()
        self.long_text.draw()

    def set_message(self, message: Optional[str]):
        if not message:
            self.long_text.text = '''
[R]=Reset Zoom  [+/-]=Timeline Height  [F1] Filtering  [C] Collapse  
[Home/End]=Scroll Top/Bottom  [PgUp/PgDown/\u2191/\u2193/\u2190/\u2192]=Scroll
[I] Import Additional Data            
'''.strip()  # Strip initial linefeed
        else:
            self.long_text.text = '\n'.join(message.splitlines()[:5])


class DetailSection(arcade.Section):
    """
    Section at the bottom of the layout which reports details about
    what the mouse is hovering over in the graph area.
    """

    def __init__(self, ei: EventsInspector, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.background = SimpleRect(
            color=Theme.COLOR_DETAIL_SECTION_BACKGROUND,
            border_color=arcade.color.LIGHT_GRAY,
            border_width=1,
        )
        self.ei = ei

        font_size = Theme.FONT_SIZE_DETAILS_SECTION
        self.mouse_from_time_dt: Optional[datetime.datetime] = None  # When the mouse is being dragged, the from time the mouse was pressed
        self.mouse_over_time_dt: Optional[datetime.datetime] = None  # Set when the mouse is over a known datetime.
        self.mouse_over_time_text: arcade.Text = arcade.Text('Wq',
                                                             start_x=2,
                                                             start_y=Layout.DETAIL_SECTION_HEIGHT - font_size - 2,
                                                             font_name=Theme.FONT_NAME,
                                                             font_size=font_size,
                                                             color=Theme.COLOR_DETAIL_SECTION_FONT_COLOR,
                                                             bold=True,
                                                             )

        # When the mouse is over a specific timeline, shows information pertinent to all intervals in that timeline (locator information)
        self.mouse_over_intervals_timeline: Optional[IntervalsTimeline] = None  # represents the timeline intervals the mouse is currently over
        self.mouse_over_timeline_text: arcade.Text = arcade.Text('Wq',
                                                                 start_x=2,
                                                                 start_y=self.mouse_over_time_text.y - self.mouse_over_time_text.content_height,  # Uses time_info as content height clue for own positioning
                                                                 multiline=True,
                                                                 width=2000,  # multiline requires width. Set to large width to avoid wrapping.
                                                                 font_name=Theme.FONT_NAME,
                                                                 font_size=font_size,
                                                                 color=Theme.COLOR_DETAIL_SECTION_FONT_COLOR
                                                                 )

        # When the mouse is over a specific interval, shows information pertinent to that interval
        self.timeline_entries_under_mouse: Optional[List[IntervalsTimelineEntry]] = None
        self.mouse_over_interval_text: arcade.Text = arcade.Text('Wq',
                                                                 start_x=600,
                                                                 start_y=self.mouse_over_time_text.y - self.mouse_over_time_text.content_height,  # Uses time_info as content height clue for own positioning
                                                                 multiline=True,
                                                                 width=2000,  # multiline requires width. Set to large width to avoid wrapping.
                                                                 font_name=Theme.FONT_NAME,
                                                                 font_size=font_size,
                                                                 color=Theme.COLOR_DETAIL_SECTION_FONT_COLOR
                                                                 )

        # We set 'Wq' to measure the content height. Clear it before we start rendering.
        self.mouse_over_time_text.text = ''
        self.mouse_over_timeline_text.text = ''
        self.mouse_over_interval_text.text = ''

        self.on_resize(self.window.width, self.window.height)

    def get_focused_timeline_entry_under_mouse(self) -> Optional[IntervalsTimelineEntry]:
        if self.timeline_entries_under_mouse:
            return self.timeline_entries_under_mouse[-1]
        return None

    def on_resize(self, window_width: int, window_height: int):
        self.mouse_over_interval_text.x = window_width // 2
        self.background.position(left=Layout.DETAIL_SECTION_LEFT, width=window_width,
                                 bottom=Layout.DETAIL_SECTION_BOTTOM, height=Layout.DETAIL_SECTION_HEIGHT)

    def on_draw(self):
        self.background.draw()
        self.mouse_over_time_text.draw()
        self.mouse_over_timeline_text.draw()

        # The mouse_over_timeline_text may have many characters which go beyond the
        # intended area and into the area of text meant for interval information.
        # This rectangle will erase the overage before drawing the interval information.
        left = Layout.DETAIL_SECTION_LEFT + self.mouse_over_interval_text.x
        arcade.draw_lrtb_rectangle_filled(
            left=left,
            right=max(left, self.window.width - Layout.DETAIL_SECTION_RIGHT_OFFSET - self.background.border_width),
            top=Layout.DETAIL_SECTION_HEIGHT - self.mouse_over_time_text.content_height,
            bottom=Layout.DETAIL_SECTION_BOTTOM,
            color=self.background.color,
        )
        self.mouse_over_interval_text.draw()

    def set_mouse_over_time(self, dt: datetime.datetime, from_dt: Optional[datetime.datetime]):
        self.mouse_from_time_dt = from_dt
        self.mouse_over_time_dt = dt
        self.refresh_mouse_over_time()

    def refresh_mouse_over_time(self):
        from_dt = self.mouse_from_time_dt
        dt = self.mouse_over_time_dt

        dt_str = dt.strftime("%Y-%m-%d %H:%M:%S.%f")
        text = f'Mouse[ ({dt_str}) {self.ei.last_known_mouse_location}'
        # from_dt is passed in if there is a mouse dragging operation. The click before the
        # drag began is used to calculate a time offset for the initial mouse position.
        if from_dt:
            duration = from_dt - dt
            if duration < datetime.timedelta(0):
                duration = -duration
            text += f'  from:({from_dt}), Δ:({humanize_timedelta(duration)})'
        text += ' ]'

        if self.timeline_entries_under_mouse is not None:
            # If the mouse is over an interval, describe the interval times
            for entry in self.timeline_entries_under_mouse:
                interval = entry.pd_interval
                delta = humanize_timedelta(datetime.timedelta(seconds=interval["duration"]))
                text += f'    Interval[ {interval["classification"].display_name} ({interval["from"]})  ->  ({interval["to"]})  Δ:({delta}) ]'

        self.mouse_over_time_text.text = text

    def set_mouse_over_interval_timeline(self, interval_timeline: IntervalsTimeline):
        self.mouse_over_intervals_timeline = interval_timeline
        pd_interval_rows = interval_timeline.pd_interval_rows
        interval = pd_interval_rows.iloc[0]  # get the first interval in the timeline
        lines: List[str] = list()

        def add_displayed_value(display_name: str, val: str):
            nonlocal lines
            if val:
                lines.append(f'{display_name:<{12}}: {val}')

        def add_non_empty_key_line(display_name: str, key_name: str):
            val = IntervalAnalyzer.get_locator_key(interval, key_name)
            add_displayed_value(display_name, val)

        common_keys = IntervalAnalyzer.get_locator_key_names(interval)

        # These keys are aggregated into a single line.
        id_keys = ['uid', 'hmsg']
        for id_key in id_keys:
            if id_key in common_keys:
                common_keys.remove(id_key)

        for common_key in common_keys:
            add_non_empty_key_line(common_key.capitalize(), common_key)

        id_str: str = ''
        for id_key in id_keys:
            val = IntervalAnalyzer.get_locator_key(interval, id_key)
            if val:
                id_str += f'[{id_key}={val}]'

        if id_str:
            add_displayed_value('IDs', id_str)

        self.mouse_over_timeline_text.text = '\n'.join(lines)

    def set_timeline_entries_under_mouse(self, timeline_entries: Optional[List[IntervalsTimelineEntry]]):
        self.timeline_entries_under_mouse = timeline_entries

        if not timeline_entries:  # Empty list or None, clear out information
            # The interval is being cleared. Clear the text in the detail section.
            self.mouse_over_interval_text.text = ''
            message_section_ref.set_message('')
            return

        interval = timeline_entries[-1].pd_interval
        lines: List[str] = list()

        def add_displayed_value(display_name: str, val: str):
            nonlocal lines
            if val:
                lines.append(f'{display_name:<{12}}: {val}')

        def add_message_attr_line(display_name: str, attr_name: str):
            val = IntervalAnalyzer.get_message_attr(interval, attr_name)
            add_displayed_value(display_name, val)

        def add_annotation_line(display_name: str, annotation_name: str):
            val = IntervalAnalyzer.get_message_annotation(interval, annotation_name)
            add_displayed_value(display_name, val)

        for common_message_attr in ('cause',):  # 'reason' is duplicated in annotations, so don't print here.
            add_message_attr_line(common_message_attr.capitalize(), common_message_attr)

        for common_annotation_keys in IntervalAnalyzer.get_message_annotation_names(interval):
            add_annotation_line(common_annotation_keys.capitalize(), common_annotation_keys)

        display_message = ''
        message_attr_val = IntervalAnalyzer.get_series_column_value(interval=interval, column_name='message')
        if message_attr_val:
            display_message = message_attr_val
        human_message_val = IntervalAnalyzer.get_message_attr(interval, 'humanMessage')
        if human_message_val and message_attr_val != human_message_val:
            if display_message:
                display_message += '\n'
            display_message += human_message_val
        message_section_ref.set_message(display_message)
        self.mouse_over_interval_text.text = '\n'.join(lines)

        # The interval that the mouse is over is displayed in the detail section.
        # Checking whether the mouse is over an interval occurs on a schedule instead of
        # on every mouse movement. So it is expected for this interval to be out of
        # sync with what the mouse is actually over. When the interval is set, therefore,
        # we need to refresh the mouse over date with the trued-up interval.
        self.refresh_mouse_over_time()


# Since different components all want to write different detail/messages,
# store a global reference once we initialize.
detail_section_ref: Optional[DetailSection] = None
message_section_ref: Optional[MessageSection] = None


class ColorLegendEntry(SimpleRect):

    def __init__(self, window: arcade.Window, classification: IntervalClassification):
        super().__init__(color=classification.color)
        self.window = window
        self.classification = classification
        self.font_size = 10
        self.category_text = arcade.Text(
            text=str(self.classification.display_name),
            start_x=0,
            start_y=0,
            color=arcade.color.BLACK,
            font_name=Theme.FONT_NAME,
            font_size=self.font_size,
        )
        self.text_width = self.category_text.content_width
        self.pos(0)

    def pos(self, left: int):
        self.position(
            left=left,
            right=left + self.text_width + 6,
            top=self.window.height - Layout.COLOR_LEGEND_BAR_TOP_OFFSET,
            height=Layout.COLOR_LEGEND_BAR_HEIGHT
        )
        self.category_text.x = left + 3
        self.category_text.y = self.bottom + 4

    def draw(self):
        super().draw()
        if detail_section_ref.timeline_entries_under_mouse:
            mouse_over_interval = detail_section_ref.timeline_entries_under_mouse[-1].pd_interval
            if mouse_over_interval is not None and mouse_over_interval['classification'] == self.classification:
                self.category_text.bold = True
            elif self.category_text.bold:
                self.category_text.bold = False
        self.category_text.draw()


class ColorLegendBar(SimpleRect):

    def __init__(self, graph_section: arcade.Section, ei: EventsInspector):
        super().__init__(color=arcade.color.BLACK)
        self.ei = ei
        self.graph_section = graph_section
        self.window = self.graph_section.window

        self.classifications: List[IntervalClassification] = [e.value for e in IntervalClassifications]

        self.legend_label = arcade.Text(
            text="Current Category Legend: ",
            start_x=Layout.ZOOM_DATE_RANGE_DISPLAY_BAR_LEFT,  # align with the start of the date range bar
            start_y=0,
            color=arcade.color.WHITE,
            font_name=Theme.FONT_NAME,
            font_size=10,
            bold=True,
        )

        self.legends: Dict[str, List[ColorLegendEntry]] = dict()
        x_offsets: Dict[str, int] = dict()
        for category in [e.value for e in IntervalCategories]:
            category_name = category.display_name
            self.legends[category_name] = list()
            x_offsets[category_name] = Layout.ZOOM_DATE_RANGE_DISPLAY_BAR_LEFT + self.legend_label.content_width + 4  # When a category is display, each entry needs to shift by an offset

        for classification in self.classifications:
            category_name = classification.category.value.display_name
            category_element_list = self.legends[category_name]
            x_offset = x_offsets[category_name]
            entry = ColorLegendEntry(self.window, classification)
            category_element_list.append(entry)
            entry.pos(x_offset)
            x_offset += entry.width + 4  # 4px between legend entries
            x_offsets[category_name] = x_offset  # Store the offset for the next entry's offset

        self.on_resize()

    def on_resize(self):
        self.position(
            left=Layout.COLOR_LEGEND_BAR_LEFT,
            right=self.window.width - Layout.COLOR_LEGEND_BAR_RIGHT_OFFSET,
            height=Layout.COLOR_LEGEND_BAR_HEIGHT,
            top=self.window.height - Layout.COLOR_LEGEND_BAR_TOP_OFFSET
        )
        self.legend_label.y = self.bottom + 4
        for legend_entries in self.legends.values():
            for entry in legend_entries:
                entry.pos(left=entry.left)  # Don't move left, but force a recalculation of Y position

    def draw(self):
        super().draw()
        self.legend_label.draw()  # message indicates that bar is the legend
        mouse_over_intervals_timeline = detail_section_ref.mouse_over_intervals_timeline
        if mouse_over_intervals_timeline is not None:
            first_interval = mouse_over_intervals_timeline.pd_interval_rows.iloc[0]
            category_name: str = first_interval['category_str']
            for entry in self.legends[category_name]:
                entry.draw()


class ZoomDateRangeDisplayBar(SimpleRect):
    DEFAULT_TICK = (arcade.color.ORANGE, 0.5, 3)
    TICK_DRAW_RULES = {
        # (color, hieght%, thickness)
        60 * 60: (arcade.color.BLACK, 1.0, 3),  # hour ticks
        60 * 30: (arcade.color.BLACK, 0.5, 3),  # half-hour
        60 * 10: (arcade.color.BLACK, 0.2, 3),  # ten minute
        60 * 5: (arcade.color.BLACK, 0.2, 1),  # 5 minute
        60: (arcade.color.WHITE, 0.2, 3),  # 1 minute
        30: (arcade.color.WHITE, 0.2, 3),  # 30 seconds
        10: (arcade.color.WHITE, 0.2, 3),  # 10 seconds
        5: (arcade.color.WHITE, 0.2, 1),  # 5 seconds
    }

    def __init__(self, graph_section: arcade.Section, ei: EventsInspector):
        super().__init__(color=arcade.color.LIGHT_GRAY)
        self.ei = ei
        self.graph_section = graph_section
        self.window = self.graph_section.window
        self.on_resize()

    def on_resize(self):
        self.position(
            left=Layout.ZOOM_DATE_RANGE_DISPLAY_BAR_LEFT,
            right=self.window.width - Layout.ZOOM_DATE_RANGE_DISPLAY_BAR_RIGHT_OFFSET,
            height=Layout.ZOOM_DATE_RANGE_DISPLAY_BAR_HEIGHT,
            top=self.window.height - Layout.ZOOM_DATE_RANGE_DISPLAY_BAR_TOP_OFFSET
        )

    def draw(self):
        super().draw()
        seconds_in_timeline = self.ei.current_zoom_timeline_seconds

        # Note that these values are rounded (down and up, respective) to the nearest minute by ei.
        start_time = self.ei.zoom_timeline_start
        stop_time = self.ei.zoom_timeline_stop

        jumping_unit_options = [24*60*60, 12*60*60, 6*60*60, 60*60, 60*30, 60*10, 60*5, 60, 30, 10, 5]

        tick_jumping_unit: int = jumping_unit_options[0]  # Start with maximum jumping unit
        for jumping_unit_to_test in jumping_unit_options:
            if seconds_in_timeline / jumping_unit_to_test > self.width * 0.10:
                # This jumping unit would result in more than a tick mark every 10 pixels
                break
            tick_jumping_unit = jumping_unit_to_test

        comfortable_number_of_labels = self.width // 100  # Don't position labels less than 100 pixels from each other

        label_jumping_unit: int = jumping_unit_options[0]  # Start with maximum jumping unit and narrow in on what will fit comfortably
        for jumping_unit_to_test in jumping_unit_options:
            if seconds_in_timeline / jumping_unit_to_test > comfortable_number_of_labels:
                # This jumping unit would result in more than a tick mark every 10 pixels
                break
            label_jumping_unit = jumping_unit_to_test

        jumping_unit = min(tick_jumping_unit, label_jumping_unit)
        pixels_per_jumping_unit = self.ei.calculate_pixels_per_second(self.width) * jumping_unit

        # Start looking for ticks & labels at hour before the timeline starts.
        # This ensures our jumps land on units aligned with the jumps.
        moving_timestamp = start_time.floor('h')
        moving_offset_x: Optional[float] = None
        while moving_timestamp < stop_time:

            def jump():
                nonlocal moving_timestamp
                moving_timestamp = moving_timestamp + datetime.timedelta(seconds=jumping_unit)

            if moving_timestamp < start_time:
                jump()
                continue

            elif moving_offset_x is None:
                initial_offset_x = left_offset_from_datetime(start_time, moving_timestamp, pixels_per_second=self.ei.current_pixels_per_second_in_timeline)
                moving_offset_x = initial_offset_x

            time_part = moving_timestamp.time()
            seconds_since_midnight = time_part.hour * 3600 + time_part.minute * 60 + time_part.second

            ticket_draw_rule: Optional[Tuple] = None
            for modder in jumping_unit_options:
                if seconds_since_midnight % modder == 0:
                    if modder in ZoomDateRangeDisplayBar.TICK_DRAW_RULES:
                        ticket_draw_rule = ZoomDateRangeDisplayBar.TICK_DRAW_RULES[modder]
                    else:
                        # If the tick marks are >hour, just draw the default
                        ticket_draw_rule = ZoomDateRangeDisplayBar.DEFAULT_TICK
                    break

            tick_color, tick_height_percent, tick_width = ticket_draw_rule
            tick_x = self.left + moving_offset_x - (tick_width//2)
            arcade.draw_line(
                start_x=tick_x,
                start_y=self.bottom,
                end_x=tick_x,
                end_y=self.bottom + self.height * tick_height_percent,
                line_width=tick_width,
                color=tick_color
            )

            if seconds_since_midnight % label_jumping_unit == 0:
                stamp = f'{time_part.hour:02}:{time_part.minute:02}'
                if time_part.second > 0:
                    stamp += f':{time_part.second:02}'
                arcade.draw_text(stamp,
                                 start_x=self.left + moving_offset_x + 2,
                                 start_y=self.top - 12,
                                 font_name=Theme.FONT_NAME,
                                 font_size=10,
                                 color=arcade.color.BLACK)

            jump()
            moving_offset_x += pixels_per_jumping_unit


class VerticalScrollBar(SimpleRect):

    def __init__(self, window: arcade.Window):
        super().__init__(color=arcade.color.GHOST_WHITE)
        self.handle = SimpleRect(arcade.color.DARK_GRAY, border_color=arcade.color.ALLOY_ORANGE, border_width=2)
        self.scroll_percent = 0.0
        self.window = window
        self.current_scrolled_y_rows = 0
        self.current_rows_per_page = 0
        self.scrollable_row_count = 1
        self.on_resize()

    def position_handle(self):
        if self.scrollable_row_count > 0:
            handle_height_px = min(self.height, int(self.current_rows_per_page / self.scrollable_row_count * self.height))  # the handle should shrink as the rows on the page represent a smaller fraction of available rows
            available_scroll_bar_height = self.height - handle_height_px
            handle_y_percentage = self.current_scrolled_y_rows / self.scrollable_row_count  # % distance from the top
            handle_y_offset = handle_y_percentage * available_scroll_bar_height
            self.handle.position(
                left=self.left,
                right=self.right,
                top=int(self.top - handle_y_offset),
                bottom=int(self.top - handle_y_offset - handle_height_px),
            )

    def on_scrolling_change(self, current_scrolled_y_rows: int, current_rows_per_page: int, available_row_count: int):
        self.current_scrolled_y_rows = current_scrolled_y_rows
        self.scrollable_row_count = max(1, available_row_count - current_rows_per_page + 2)  # prevent divide by zero
        self.current_rows_per_page = current_rows_per_page
        self.position_handle()

    def on_resize(self):
        self.position(
            right=self.window.width - Layout.VERTICAL_SCROLL_BAR_RIGHT_OFFSET,
            width=Layout.VERTICAL_SCROLL_BAR_WIDTH,
            bottom=Layout.VERTICAL_SCROLL_BAR_BOTTOM,
            top=self.window.height - Layout.VERTICAL_SCROLL_BAR_TOP_OFFSET
        )
        self.position_handle()

    def draw(self):
        super().draw()
        if self.scrollable_row_count > 0:
            self.handle.draw()

    def get_percent_scroll(self, y) -> float:
        """
        Given an offset y on the screen, if the top of the handle were to navigate to that y position,
        what percentage scrolled would the handle be?
        """
        _, offset_y = self.offset_of_xy(0, y)
        scrollable_pixels = max(self.height - self.handle.height, 1)
        distance_from_top = self.height - offset_y
        percent = min(distance_from_top / scrollable_pixels, 1.0)
        return max(0, percent)


class HorizontalScrollBar(SimpleRect):

    def __init__(self, window: arcade.Window, ei: EventsInspector):
        super().__init__(color=arcade.color.GHOST_WHITE)
        self.background_clearer = SimpleRect(arcade.color.BLACK)
        self.handle = SimpleRect(arcade.color.DARK_GRAY, border_width=2, border_color=arcade.color.ALLOY_ORANGE)
        self.ei = ei
        self.window = window
        self.width_percentage = 100.0
        self.left_offset_percentage = 0.0
        self.on_scrolling_change()
        self.on_resize()

    def on_scrolling_change(self):
        timedelta_displayed = self.ei.zoom_timeline_stop - self.ei.zoom_timeline_start
        timedelta_possible = self.ei.absolute_timeline_stop - self.ei.absolute_timeline_start
        self.width_percentage = (timedelta_displayed.total_seconds() / timedelta_possible.total_seconds())
        self.left_offset_percentage = (self.ei.zoom_timeline_start - self.ei.absolute_timeline_start).total_seconds() / timedelta_possible.total_seconds()

        handle_width = int(self.width_percentage * self.width)
        unoccupied_width = self.width - handle_width
        left_offset = int(self.width * self.left_offset_percentage)
        self.handle.position(
            left=Layout.HORIZONTAL_SCROLL_BAR_LEFT + left_offset,
            right=Layout.HORIZONTAL_SCROLL_BAR_LEFT + left_offset + handle_width,
            top=Layout.HORIZONTAL_SCROLL_BAR_TOP,
            bottom=Layout.HORIZONTAL_SCROLL_BAR_BOTTOM,
        )

    def get_percent_scroll(self, x) -> float:
        """
        Given an offset x on the screen, if the left of the handle were to navigate to that x position,
        what percentage scrolled would the handle be?
        """
        offset_x, _ = self.offset_of_xy(x, 0)
        avaialble_scrollable_pixels = self.width - self.handle.width
        percent = min(offset_x / avaialble_scrollable_pixels, 1.0)
        return max(0, percent)

    def on_resize(self):
        self.position(
            right=self.window.width - Layout.HORIZONTAL_SCROLL_BAR_RIGHT_OFFSET,
            left=Layout.HORIZONTAL_SCROLL_BAR_LEFT,
            bottom=Layout.HORIZONTAL_SCROLL_BAR_BOTTOM,
            top=Layout.HORIZONTAL_SCROLL_BAR_TOP
        )
        self.background_clearer.position(
            left=0,
            right=self.window.width,
            bottom=Layout.HORIZONTAL_SCROLL_BAR_BOTTOM,
            top=Layout.HORIZONTAL_SCROLL_BAR_TOP
        )

    def draw(self):
        self.background_clearer.draw()
        super().draw()
        self.handle.draw()


class CategoryBar(SimpleRect):

    def __init__(self, graph_section: arcade.Section, ei: EventsInspector):
        super().__init__(color=arcade.color.LIGHT_BLUE)
        self.ei = ei
        self.graph_section = graph_section
        self.window = self.graph_section.window
        self.on_resize()

    def on_resize(self):
        self.position(
            left=Layout.CATEGORY_BAR_LEFT,
            width=Layout.CATEGORY_BAR_WIDTH,
            bottom=Layout.CATEGORY_BAR_BOTTOM,
            top=self.window.height - Layout.CATEGORY_BAR_TOP_OFFSET
        )

    def draw_categories(self, category_label_row_count: OrderedDict[str, int], row_height_px: int):
        super().draw()  # Draw the background for the bar
        font_size = 10
        previous_rows_account_for = 0
        previous_category_divider_y = self.top
        for category_name, row_count in category_label_row_count.items():
            category_divider_y = self.top - (row_count * row_height_px) - (previous_rows_account_for * row_height_px)

            _, mouse_y = self.ei.last_known_mouse_location
            highlight_category = mouse_y < previous_category_divider_y and mouse_y >= category_divider_y

            approx_label_height = len(category_name) * font_size + 2
            category_rows_combined_height = row_count * row_height_px

            arcade.draw_line(
                start_x=self.left,
                start_y=category_divider_y,
                end_x=self.right + self.ei.current_visible_timeline_width,  # draw line across timeline view / between rows
                end_y=category_divider_y,
                color=arcade.color.DARK_GRAY
            )

            if highlight_category:
                arcade.draw_lrtb_rectangle_filled(
                    left=self.left,
                    right=self.right,
                    top=previous_category_divider_y,
                    bottom=category_divider_y,
                    color=Theme.COLOR_HIGHLIGHTED_CATEGORY_SECTION
                )

            if approx_label_height < category_rows_combined_height:
                # Only draw the label vertically if it will fit in the space available
                arcade.draw_text(text=category_name,
                                 start_x=self.right - 4,
                                 start_y=category_divider_y + 2,
                                 rotation=90,
                                 font_name=Theme.FONT_NAME,
                                 font_size=font_size,
                                 color=arcade.color.BLACK,
                                 bold=highlight_category,
                                 )
            else:
                # There is not enough space to draw the category name vertically,
                # so do our best to ensure it gets usefully on screen.

                horizontal_category_font_color = arcade.color.WHITE
                if highlight_category:
                    arcade.draw_xywh_rectangle_filled(
                        bottom_left_x=self.right - 4,
                        bottom_left_y=category_divider_y,
                        height=font_size + 2,
                        width=len(category_name) * font_size,
                        color=Theme.COLOR_HIGHLIGHTED_CATEGORY_SECTION
                    )
                    horizontal_category_font_color = arcade.color.BLACK

                arcade.draw_text(text=category_name,
                                 start_x=self.right,
                                 start_y=min(category_divider_y + 2, self.top - font_size - 2),
                                 font_name=Theme.FONT_NAME,
                                 font_size=font_size,
                                 color=horizontal_category_font_color,
                                 bold=highlight_category,
                                 )

            previous_rows_account_for += row_count
            previous_category_divider_y = category_divider_y


class GraphSection(arcade.Section):
    """
    The graph section consists of:
     - a date_range display bar at the top
     - a scroll bar to right
     - a "category bar" to the left
    Inside of the shape created by these bars, one or more interval timelines are rendered.
    Below this section, there is the timeline control allows users to change which area of
    the timeline the zoomed graph section is rendering.
    """

    PERIOD_BETWEEN_REACTION_TO_KEY_DOWN: float = 1 / 50

    def __init__(self, ei: EventsInspector, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Will contain a list of all rectangles to render in the current
        # visualization of selected / visible timelines. This includes rectangles
        # that are not presently displayed because of the zoom range
        # (e.g. scrolling left or right should not necessitate rebuilding this
        # list).
        self.buffered_graph: Optional[arcade.ShapeElementList] = None

        self.buffered_low_layer_decorations: Optional[arcade.ShapeElementList] = None

        self.fps = 0
        self.ei = ei
        self.color_legend_bar = ColorLegendBar(self, ei)
        self.zoom_date_range_display_bar = ZoomDateRangeDisplayBar(self, ei)
        self.category_bar = CategoryBar(self, ei)
        self.vertical_scroll_bar = VerticalScrollBar(self.window)
        self.horizontal_scroll_bar = HorizontalScrollBar(self.window, ei)

        # When rows are drawn, this list will be populated with the IntervalTimeline
        # objects representing those timelines.
        self.visible_interval_timelines: List[IntervalsTimeline] = list()

        self._row_height_px = 0
        self.row_height_px = 3
        self.rows_to_display = 0
        self._scroll_y_rows = 0  # How many rows we have scrolled past

        self.keys_down = set()
        # When this value is <= 0, the action desired by a key being held down will be
        # performed. The time will then be increased again to ensure the key press action
        # does not happen on every frame.
        self.time_until_next_process_keys_down = 0.0

        self.is_mouse_over_timeline_area = False
        # When the mouse is over the rendered interval timeline area,
        # these will be set to the offset relative to that area.
        self.mouse_timeline_area_offset_x: Optional[int] = None
        self.mouse_timeline_area_offset_from_top: Optional[int] = None

        # If a mouse button is being held down, the position it was pressed is
        # stored in the tuple [x, y, modifies] mapped to the button int.
        self.mouse_button_down: Dict[int, Tuple[int, int, int]] = dict()
        # If the mouse is moving with the button down, the tuple indicates
        # the x,y of the mouse during the drag.
        self.mouse_button_active_drag: Dict[int, Tuple[int, int]] = dict()
        # When a mouse button is released after a registered drag, then this
        # map is populated with two tuples from button_down and active_drag.
        self.mouse_button_finished_drag: Dict[int, Tuple[Tuple[int, int, int], Tuple[int, int]]] = dict()

        # In order to draw category labels in the category bar, we need to
        # know how large the area the label should cover on the screen -
        # which is how many rows are in the category * row_height. As we draw
        # the rows, keep a count of how many rows are in each category.
        # Since rows are drawn from top to bottom, the ordered dict will keep
        # the order of category labels to draw as well.
        self.category_label_row_count: OrderedDict[str, int] = OrderedDict()

        self.on_resize(self.window.width, self.window.height)

    def rebuild_buffered_shapes(self):
        self.buffered_graph = None
        self.buffered_low_layer_decorations = None

    @lru_cache(MAX_TIMELINES_TO_DISPLAY_AT_ONCE)
    def get_interval_timeline(self, group_id, absolute_timeline_start: pd.Timestamp, timeline_row_height_px: int, pixels_per_second: float) -> IntervalsTimeline:
        """
        :param group_id: A key for ei.timelines to find a DataFrame of interval rows for a specific timeline row.
        :param absolute_timeline_start: Generate rectangles offset from the left as if this absolutely timeline_start was x=0.
        :param timeline_row_height_px: The on-screen height (pixels), the timeline row should occupy when rendered.
        :param pixels_per_second: Instructs the timeline to generate rectangles with the specified pixels_per_second (may be <1.0)
        :return: IntervalsTimeline capable of drawing the timeline on screen.
        """
        intervals_for_row_df: pd.DataFrame = self.ei.selected_timelines[group_id]
        interval_timeline = IntervalsTimeline(
            group_id=group_id,
            pd_interval_rows=intervals_for_row_df,
            timeline_absolute_start=absolute_timeline_start,
            timeline_row_height=timeline_row_height_px,
            pixels_per_second=pixels_per_second,
        )
        return interval_timeline

    @property
    def row_height_px(self):
        return self._row_height_px

    @row_height_px.setter
    def row_height_px(self, value: int):
        self._row_height_px = value
        self.rebuild_buffered_shapes()

    @property
    def scroll_y_rows(self):
        return self._scroll_y_rows

    @scroll_y_rows.setter
    def scroll_y_rows(self, val):
        if val < 0:
            val = 0
        if val == self._scroll_y_rows:
            # Don't recompute anything if the value isn't actually changing.
            return
        self._scroll_y_rows = val
        self.rebuild_buffered_shapes()
        self.update_scroll_bars()

    def update_scroll_bars(self):
        self.vertical_scroll_bar.on_scrolling_change(
            current_scrolled_y_rows=self.scroll_y_rows,
            current_rows_per_page=self.calc_rows_to_display(),
            available_row_count=len(self.ei.selected_timelines)
        )
        self.horizontal_scroll_bar.on_scrolling_change()

    def scroll_down_by_rows(self, count: int):
        self.scroll_y_rows += count

    def on_mouse_scroll(self, x: int, y: int, scroll_x: int, scroll_y: int):
        self.scroll_down_by_rows(-1 * int(scroll_y // 2))

    def zoom_to_dates(self, start: datetime.datetime, end: datetime.datetime,  refilter_based_on_date_range: bool = False):
        # Store our scroll position prior to the zoom
        previous_scroll_position = self.scroll_y_rows

        material_change = self.ei.zoom_to_dates(start, end, refilter_based_on_date_range)

        # It should be noted that if refilter_based_on_date_range is True (i.e. Collapse), then the number of timelines may be reduced
        # significantly. In that, case, the old offset into the list is potentially meaningless.
        self.scroll_y_rows = previous_scroll_position
        if material_change:
            self.rebuild_buffered_shapes()
        self.update_scroll_bars()

    def process_keys_down(self, delay_until_next: Optional[float] = None):
        """
        Reacts to any keys that are presently being held down
        :param delay_until_next: If the caller wants a non-default time period to pass before this method
                    is called again by on_update, specify a value.
        """
        if arcade.key.HOME in self.keys_down:
            self.scroll_y_rows = 0

        if arcade.key.END in self.keys_down:
            self.scroll_y_rows = len(self.ei.selected_timelines.keys())

        if arcade.key.R in self.keys_down:
            self.zoom_to_dates(self.ei.absolute_timeline_start, self.ei.absolute_timeline_stop, refilter_based_on_date_range=True)
            self.scroll_y_rows = 0

        if arcade.key.C in self.keys_down:
            self.zoom_to_dates(self.ei.zoom_timeline_start, self.ei.zoom_timeline_stop, refilter_based_on_date_range=True)
            self.scroll_y_rows = 0

        if arcade.key.LEFT in self.keys_down or arcade.key.RIGHT in self.keys_down:
            # Move left or right by 10% of time on screen
            shift_by_seconds = datetime.timedelta(seconds=self.ei.current_zoom_timeline_seconds * 0.1)
            if arcade.key.LEFT in self.keys_down:
                shift_by_seconds = -shift_by_seconds
            self.zoom_to_dates(self.ei.zoom_timeline_start + shift_by_seconds,
                                  self.ei.zoom_timeline_stop + shift_by_seconds)

        if arcade.key.DOWN in self.keys_down:
            if arcade.key.LCTRL in self.keys_down:
                # Move in both directions by 10%
                zoom_out_by_seconds = datetime.timedelta(seconds=self.ei.current_zoom_timeline_seconds * 0.1)
                self.zoom_to_dates(self.ei.zoom_timeline_start + zoom_out_by_seconds,
                                      self.ei.zoom_timeline_stop - zoom_out_by_seconds)
            else:
                self.scroll_down_by_rows(1)

        if arcade.key.UP in self.keys_down:
            if arcade.key.LCTRL in self.keys_down:
                # Move in both directions by 10%
                zoom_out_by_seconds = datetime.timedelta(seconds=self.ei.current_zoom_timeline_seconds * 0.1)
                self.zoom_to_dates(self.ei.zoom_timeline_start - zoom_out_by_seconds,
                                      self.ei.zoom_timeline_stop + zoom_out_by_seconds)
            else:
                self.scroll_down_by_rows(-1)

        if arcade.key.PAGEDOWN in self.keys_down:
            self.scroll_down_by_rows(self.calc_rows_to_display())

        if arcade.key.PAGEUP in self.keys_down:
            self.scroll_down_by_rows(-1 * self.calc_rows_to_display())

        if arcade.key.ESCAPE in self.keys_down:
            if self.mouse_button_down:
                # Cancel any mouse clicks
                self.mouse_button_down = dict()
            if self.mouse_button_active_drag:
                # Cancel any active drag
                self.mouse_button_active_drag = dict()

        if arcade.key.NUM_ADD in self.keys_down or arcade.key.EQUAL in self.keys_down:  # non-numpad "+" is shift+= ; ignore if shift is not being held down
            self.row_height_px = min(self.row_height_px + 2, self.height // 4)
            self.update_scroll_bars()

        if arcade.key.NUM_SUBTRACT in self.keys_down or arcade.key.MINUS in self.keys_down:
            self.row_height_px = max(self.row_height_px - 2, 2)
            self.update_scroll_bars()

        if delay_until_next is None:
            self.time_until_next_process_keys_down = GraphSection.PERIOD_BETWEEN_REACTION_TO_KEY_DOWN
        else:
            self.time_until_next_process_keys_down = delay_until_next

    def on_key_press(self, key, modifiers):
        self.keys_down.add(key)
        self.process_keys_down(delay_until_next=0.5)  # Wait 0.5 seconds before starting to recognize key being held down

    def on_key_release(self, key, modifiers):
        if key in self.keys_down:
            self.keys_down.remove(key)

    def on_resize(self, width: int, height: int):
        self.color_legend_bar.on_resize()
        self.zoom_date_range_display_bar.on_resize()
        self.category_bar.on_resize()
        self.vertical_scroll_bar.on_resize()
        self.horizontal_scroll_bar.on_resize()
        self.update_scroll_bars()  # Trigger a scroll bar handle re-calculation
        self.rebuild_buffered_shapes()

    def calc_rows_to_display(self) -> int:
        """
        Returns the number of FULL rows that can be displayed in the timeline graphing area.
        """
        return min(MAX_TIMELINES_TO_DISPLAY_AT_ONCE, self.category_bar.height // self.row_height_px)

    @lru_cache(maxsize=1)  # Lazy abuse of lru_cache in order to only set center_x when things change. Pass in buffered_graph to invalidate cache if a new ShapeElementList is instantiated.
    def set_buffered_graph_center_x(self, buffered_graph: arcade.ShapeElementList, absolute_timeline_start, zoom_timeline_start, pixels_per_second):
        buffered_graph.center_x = -1 * left_offset_from_datetime(baseline_dt=absolute_timeline_start, position_dt=zoom_timeline_start, pixels_per_second=pixels_per_second)

    def on_draw(self):
        self.window.clear()

        self.color_legend_bar.draw()
        self.zoom_date_range_display_bar.draw()

        # If the minimum height of each row is ?px, then figure out how
        # many we can fit in the graph area.
        number_of_rows_to_display = self.calc_rows_to_display()
        available_timelines_ids = list(self.ei.selected_timelines.keys())

        # If the user has hit END or scrolled to the end of the data, show the last full page
        if self.scroll_y_rows > len(available_timelines_ids) - self.calc_rows_to_display():
            self.scroll_y_rows = max(0, len(available_timelines_ids) - self.calc_rows_to_display())

        pixels_per_second = self.ei.calculate_pixels_per_second(self.ei.current_visible_timeline_width)

        if not self.buffered_graph:
            row_id_tuples_to_render = available_timelines_ids[self.scroll_y_rows:self.scroll_y_rows + number_of_rows_to_display]

            self.buffered_graph = arcade.ShapeElementList()
            self.visible_interval_timelines = list()
            visual_row_number = 0
            rect_points = list()
            colors = list()
            self.category_label_row_count = OrderedDict()
            for row_id_tuple in row_id_tuples_to_render:
                interval_timeline = self.get_interval_timeline(
                    group_id=row_id_tuple,
                    absolute_timeline_start=self.ei.absolute_timeline_start,
                    timeline_row_height_px=self.row_height_px,
                    pixels_per_second=pixels_per_second
                )

                self.visible_interval_timelines.append(interval_timeline)

                # Track the number of rows in each category
                row_category = interval_timeline.get_category_name()
                if row_category not in self.category_label_row_count:
                    self.category_label_row_count[row_category] = 1
                else:
                    self.category_label_row_count[row_category] = self.category_label_row_count[row_category] + 1

                transformed_rect_list, color_list = interval_timeline.apply_transform(by_y=self.category_bar.top - (visual_row_number * self.row_height_px) - self.row_height_px)

                rect_points.extend(transformed_rect_list)
                colors.extend(interval_timeline.color_list)
                # interval_timeline.draw(check_for_mouse_over_interval=check_for_mouse_over_interval)
                visual_row_number += 1
            if rect_points:
                self.buffered_graph.append(arcade.create_rectangles_filled_with_colors(rect_points, colors))

        self.set_buffered_graph_center_x(self.buffered_graph, self.ei.absolute_timeline_start, self.ei.zoom_timeline_start, self.ei.current_pixels_per_second_in_timeline)

        overall_lower_decorations_change = False
        lower_decorations = list()
        for row_offset, interval_timeline in enumerate(self.visible_interval_timelines):
            decoration_shapes, changed = interval_timeline.get_lower_layer_decorations(
                mouse_over_intervals_timeline=detail_section_ref.mouse_over_intervals_timeline,
                mouse_over_intervals_timeline_entry=detail_section_ref.get_focused_timeline_entry_under_mouse(),
                row_offset=row_offset,
                absolute_timeline_pixel_width=self.ei.absolute_duration * self.ei.current_pixels_per_second_in_timeline
            )
            lower_decorations.extend(decoration_shapes)
            if changed:
                overall_lower_decorations_change = True

        if overall_lower_decorations_change or not self.buffered_low_layer_decorations:
            self.buffered_low_layer_decorations = arcade.ShapeElementList()
            for shape in lower_decorations:
                self.buffered_low_layer_decorations.append(shape)

        self.buffered_low_layer_decorations.draw()
        self.buffered_graph.draw()

        if arcade.MOUSE_BUTTON_LEFT in self.mouse_button_down and \
                arcade.MOUSE_BUTTON_LEFT in self.mouse_button_active_drag:
            from_x, from_y, with_mod = self.mouse_button_down[arcade.MOUSE_BUTTON_LEFT]
            to_x, to_y = self.mouse_button_active_drag[arcade.MOUSE_BUTTON_LEFT]
            arcade.draw_xywh_rectangle_filled(
                bottom_left_x=from_x,
                bottom_left_y=self.bottom,
                width=to_x-from_x,
                height=self.height-self.zoom_date_range_display_bar.height,
                color=(0, 0, 125, 100)
            )

        self.vertical_scroll_bar.draw()
        self.horizontal_scroll_bar.draw()
        self.category_bar.draw_categories(self.category_label_row_count, row_height_px=self.row_height_px)

        if self.is_mouse_over_timeline_area:
            # Draw a vertical line which follows the mouse while it is over the timeline draw area.
            arcade.draw_line(start_x=self.category_bar.right + self.mouse_timeline_area_offset_x,
                             start_y=self.category_bar.bottom,
                             end_x=self.category_bar.right + self.mouse_timeline_area_offset_x,
                             end_y=self.category_bar.top,
                             color=Theme.COLOR_CROSS_HAIR_LINES,
                             line_width=1)

            # Draw the horizontal cross hair line
            # arcade.draw_line(
            #     start_x=Layout.CATEGORY_BAR_WIDTH + Layout.CATEGORY_BAR_LEFT,
            #     start_y=self.zoom_date_range_display_bar.bottom - (self.mouse_timeline_area_offset_from_top // self.row_height_px * self.row_height_px),
            #     end_x=Layout.CATEGORY_BAR_WIDTH + Layout.CATEGORY_BAR_LEFT + self.ei.current_visible_timeline_width,
            #     end_y=self.zoom_date_range_display_bar.bottom - (self.mouse_timeline_area_offset_from_top // self.row_height_px * self.row_height_px),
            #     line_width=1,
            #     color=Theme.COLOR_CROSS_HAIR_LINES
            # )

            if detail_section_ref.timeline_entries_under_mouse:
                pass

        arcade.draw_text(f"FPS: {int(self.fps)}", self.category_bar.right + 2, self.horizontal_scroll_bar.top + 12, arcade.color.WHITE, 10)

    def on_mouse_leave(self, x: int, y: int):
        self.is_mouse_over_timeline_area = False

    def on_mouse_release(self, x: int, y: int, button: int, modifiers: int):
        if button in self.mouse_button_down and button in self.mouse_button_active_drag:
            self.mouse_button_finished_drag[button] = (
                self.mouse_button_down[button],
                self.mouse_button_active_drag[button]
            )

        self.mouse_button_down.pop(button, None)
        self.mouse_button_active_drag.pop(button, None)

    def track_scroll_y(self, y: int):
        """
        Trigger a vertical scroll based on a y position on the window.
        """
        rows = int((len(self.ei.selected_timelines) - self.calc_rows_to_display() + 2) * self.vertical_scroll_bar.get_percent_scroll(y))
        self.scroll_y_rows = rows

    def track_scroll_x(self, x: int):
        """
        Trigger a vertical scroll based on an x position on the window.
        """
        target_percent = self.horizontal_scroll_bar.get_percent_scroll(x)
        current_timedelta_in_zoom = int((self.ei.zoom_timeline_stop - self.ei.zoom_timeline_start).to_timedelta64())
        target_start_nanos = int(((self.ei.absolute_timeline_stop - self.ei.absolute_timeline_start ).to_timedelta64() - current_timedelta_in_zoom) * target_percent)
        target_start_time = self.ei.absolute_timeline_start + datetime.timedelta(seconds=target_start_nanos // 1000000000)
        self.zoom_to_dates(target_start_time, target_start_time + datetime.timedelta(seconds=current_timedelta_in_zoom // 1000000000))

    def on_mouse_press(self, x: int, y: int, button: int, modifiers: int):
        self.mouse_button_down[button] = (x, y, modifiers)

        if x >= self.vertical_scroll_bar.left:  # If you click anywhere on the right side of the screen, we'll try to get close
            self.track_scroll_y(y)
        elif y >= self.horizontal_scroll_bar.bottom and y <= self.horizontal_scroll_bar.top:
            self.track_scroll_x(x)

    def location_within_vertical_scroll_bar(self, x: int, y: int) -> bool:
        return self.vertical_scroll_bar.is_xy_within(x, y)

    def location_within_horizontal_scroll_bar(self, x: int, y: int) -> bool:
        return self.horizontal_scroll_bar.is_xy_within(x, y)

    def location_within_timeline_area(self, x: int, y: int) -> bool:
        """
        Returns: Returns True if the mouse coordinate is within the actual graphing area for timelines (False if over scroll bars / category / etc).
        """
        if x >= self.category_bar.right and x < self.vertical_scroll_bar.left and y < self.zoom_date_range_display_bar.bottom and y > self.horizontal_scroll_bar.top:
            return True
        else:
            return False

    def on_mouse_motion(self, x: int, y: int, dx: Optional[int] = None, dy: Optional[int] = None):
        global detail_section_ref

        if self.location_within_timeline_area(x, y):
            self.is_mouse_over_timeline_area = True
            self.mouse_timeline_area_offset_x = x - self.category_bar.right
            self.mouse_timeline_area_offset_from_top = self.category_bar.top - y
        else:
            self.is_mouse_over_timeline_area = False

        detail_section_ref.mouse_over_intervals_timeline = None
        if self.is_mouse_over_timeline_area:
            try:
                # Find which timeline the mouse is over and add that set of intervals to the detail section.
                timeline_row_offset = self.mouse_timeline_area_offset_from_top // self.row_height_px
                mouse_is_over_interval_timeline = self.visible_interval_timelines[timeline_row_offset]
                detail_section_ref.set_mouse_over_interval_timeline(mouse_is_over_interval_timeline)

                # Find which interval of the current timeline the mouse is over and set that into the detail section.
                absolute_x_offset = (-1 * self.buffered_graph.center_x) + x
                timeline_entries_under_mouse = mouse_is_over_interval_timeline.get_timeline_entries_at_x(absolute_x_offset)
                detail_section_ref.set_timeline_entries_under_mouse(timeline_entries_under_mouse)

            except:
                # If selected has changed and this offset no longer exists
                pass

        if arcade.MOUSE_BUTTON_LEFT in self.mouse_button_down:
            from_x, from_y, with_mod = self.mouse_button_down[arcade.MOUSE_BUTTON_LEFT]
            if self.location_within_vertical_scroll_bar(from_x, from_y):
                self.track_scroll_y(y)
            elif self.location_within_horizontal_scroll_bar(from_x, from_y):
                self.track_scroll_x(x)
            elif self.location_within_timeline_area(from_x, from_y):  # Only permit drag if the starting location is within the graphing area
                if abs(from_x - x) + abs(from_y - y) > 5:
                    # If the mouse has moved at least 5 pixels from where the button first went down
                    # create an active drag.
                    self.mouse_button_active_drag[arcade.MOUSE_BUTTON_LEFT] = (x, y)
                else:
                    # If there was an active drag, cancel it
                    self.mouse_button_active_drag.pop(arcade.MOUSE_BUTTON_LEFT, None)

        if self.is_mouse_over_timeline_area:
            from_dt = None  # will be set to the datetime associated with the origin of a mouse drag, if one is active
            if arcade.MOUSE_BUTTON_LEFT in self.mouse_button_down and \
                    arcade.MOUSE_BUTTON_LEFT in self.mouse_button_active_drag:
                from_x, _, _ = self.mouse_button_down[arcade.MOUSE_BUTTON_LEFT]
                from_dt = self.ei.zoom_left_offset_to_datetime(from_x)
            detail_section_ref.set_mouse_over_time(self.ei.zoom_left_offset_to_datetime(self.mouse_timeline_area_offset_x), from_dt)

    def on_update(self, delta_time: float):
        self.fps = 1 / delta_time
        self.time_until_next_process_keys_down -= delta_time
        if self.time_until_next_process_keys_down < 0:
            self.process_keys_down()

        if arcade.MOUSE_BUTTON_LEFT in self.mouse_button_finished_drag:
            (from_x, from_y, with_mod), (to_x, to_y) = self.mouse_button_finished_drag.pop(arcade.MOUSE_BUTTON_LEFT)
            from_dt = self.ei.zoom_left_offset_to_datetime(from_x - Layout.ZOOM_DATE_RANGE_DISPLAY_BAR_LEFT)
            to_dt = self.ei.zoom_left_offset_to_datetime(to_x - Layout.ZOOM_DATE_RANGE_DISPLAY_BAR_LEFT)
            self.zoom_to_dates(from_dt, to_dt)


class GraphInterfaceView(arcade.View):

    def __init__(self, ei: EventsInspector, window: arcade.Window):
        global detail_section_ref, message_section_ref
        super().__init__(window)
        self.ei = ei
        # All sections must have prevent_dispatch_view={False} so that the View gets on_mouse_motion
        self.detail_section = DetailSection(ei, 0, 0, 0, 0, prevent_dispatch_view={False})
        detail_section_ref = self.detail_section
        self.message_section = MessageSection(ei, 0, 0, 0, 0, prevent_dispatch_view={False})
        message_section_ref = self.message_section
        self.graph_section = GraphSection(ei, 0, 0, 0, 0, prevent_dispatch_view={False})
        self.adjust_section_positions(self.window.width, self.window.height)
        self.add_section(self.graph_section)  # Graph clears screen on draw, so make sure it is the first section added.
        self.add_section(self.detail_section)
        self.add_section(self.message_section)

    def adjust_section_positions(self, window_width, window_height):
        self.graph_section.width = window_width
        self.graph_section.height = window_height - Layout.GRAPH_SECTION_BOTTOM
        self.graph_section.left = 0
        self.graph_section.bottom = Layout.GRAPH_SECTION_BOTTOM

        self.detail_section.width = window_width

    def on_resize(self, window_width: int, window_height: int):
        self.ei.on_zoom_resize(window_width - Layout.VERTICAL_SCROLL_BAR_WIDTH - Layout.CATEGORY_BAR_WIDTH)
        self.adjust_section_positions(window_width, window_height)
        self.message_section.on_resize(window_width, window_height)
        self.graph_section.on_resize(window_width, window_height)
        self.detail_section.on_resize(window_width, window_height)

    def on_show_view(self):
        self.on_resize(self.window.width, self.window.height)
        arcade.set_background_color(Theme.COLOR_TIMELINE_BACKGROUND)

    def on_mouse_motion(self, x: int, y: int, dx: int, dy: int):
        self.ei.last_known_mouse_location = (x, y)

    def on_key_press(self, symbol: int, modifiers: int):
        self.window.on_key_press(symbol, modifiers)  # Pass key to window in case there are view changes desired


class MainWindow(arcade.Window):

    def __init__(self, width, height, title):
        super().__init__(width, height, title, resizable=True)
        arcade.set_background_color(arcade.color.WHITE)
        self.ei = EventsInspector()
        self.import_timeline_view = ImportTimelineView(self, load_data=lambda url, stream: self.on_data_load(url, stream))
        self.graph_view: Optional[GraphInterfaceView] = None
        self.filter_view: Optional[FilteringView] = None
        self.show_view(self.import_timeline_view)

    def on_data_load(self, url: str, stream_handle):
        if url.endswith('.json'):
            intervals_list = json.load(stream_handle)['items']
            self.ei.add_interval_dicts(intervals_list)
        elif url.endswith('.tar'):
            def process_extracted_tar_file_entry(file_path: str):
                if 'audit' in os.path.basename(file_path) and file_path.endswith('.log'):
                    print(f'Processing: {file_path}')
                    with open(file_path, mode='r') as audit_log_fp:
                        self.ei.add_logs_data(audit_log_fp)
            extract_and_process_tar(stream_handle, process_extracted_tar_file_entry)
        else:
            self.ei.add_logs_data(stream_handle)
        self.graph_view = GraphInterfaceView(self.ei, self)
        self.filter_view = FilteringView(self.ei, self, on_exit=lambda: self.show_view(self.graph_view))
        self.show_view(self.graph_view)

    def on_resize(self, width: int, height: int):
        """ This method is automatically called when the window is resized. """

        # Call the parent. Failing to do this will mess up the coordinates,
        # and default to 0,0 at the center and the edges being -1 to 1.
        super().on_resize(width, height)
        self.import_timeline_view.on_resize(width, height)
        if self.graph_view:
            self.graph_view.on_resize(width, height)
        if self.filter_view:
            self.filter_view.on_resize(width, height)

    def on_key_press(self, symbol: int, modifiers: int):
        if self.current_view != self.import_timeline_view:
            if symbol == arcade.key.F1 or symbol == arcade.key.SLASH and self.current_view == self.graph_view:
                self.show_view(self.filter_view)

            if self.current_view == self.filter_view and symbol == arcade.key.ESCAPE:
                self.show_view(self.graph_view)

            if self.current_view == self.import_timeline_view and symbol == arcade.key.ESCAPE and len(self.ei.selected_timelines) > 0:
                self.show_view(self.graph_view)

            if symbol == arcade.key.I and self.current_view == self.graph_view:
                self.show_view(self.import_timeline_view)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    # with open('e2e-timelines_everything_20240110-190423.json', mode='r') as json_fd:
    #     events_dict = json.load(json_fd)['items']

    # with open('small.json', mode='r') as json_fd:
    #     events_dict = json.load(json_fd)['items']

    mw = MainWindow(INIT_SCREEN_WIDTH, INIT_SCREEN_HEIGHT, SCREEN_TITLE)
    mw.maximize()
    arcade.run()
