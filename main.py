import datetime
from collections import OrderedDict
from functools import lru_cache
import pandas as pd
import json
import arcade

from typing import Optional, List, Tuple, Dict, Set, Iterable

from analyzer import SimpleRect, EventsInspector, humanize_timedelta
from analyzer.intervals import IntervalAnalyzer, IntervalCategory, IntervalClassification, IntervalClassifications
from ui import FilteringView, ImportTimelineView
from ui.layout import Theme, Layout

INIT_SCREEN_WIDTH = 500
INIT_SCREEN_HEIGHT = 500
SCREEN_TITLE = "Intervals Analysis"


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
            top=Layout.MESSAGE_SECTION_BOTTOM + Layout.MESSAGE_SECTION_HEIGHT
        )
        self.long_text.width = self.background.width - 5
        self.long_text.y = self.background.top - 14

    def on_draw(self):
        self.background.draw()
        self.long_text.draw()

    def set_message(self, message: Optional[str]):
        if not message:
            self.long_text.text = '''
[R]=Reset Zoom  [+/-]=Timeline Height  [F1] Filtering
[Home/End]=Scroll Top/Bottom  [PgUp/PgDown/\u2191/\u2193/\u2190/\u2192]=Scroll            
'''.strip()  # Strip initial linefeed
        else:
            self.long_text.text = message


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
        self.mouse_over_intervals: Optional[pd.DataFrame] = None  # represents the timeline intervals the mouse is currently over
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
        self.mouse_over_interval: Optional[pd.Series] = None
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

        text = f'Mouse[ ({dt})'
        # from_dt is passed in if there is a mouse dragging operation. The click before the
        # drag began is used to calculate a time offset for the initial mouse position.
        if from_dt:
            duration = from_dt - dt
            if duration < datetime.timedelta(0):
                duration = -duration
            text += f'  from:({from_dt}), Δ:({humanize_timedelta(duration)})'
        text += ' ]'

        if self.mouse_over_interval is not None:
            # If the mouse is over an interval, describe the interval times
            interval = self.mouse_over_interval
            delta = humanize_timedelta(datetime.timedelta(seconds=interval["duration"]))
            text += f'    Interval[ {interval["classification"].display_name} ({interval["from"]})  ->  ({interval["to"]})  Δ:({delta}) ]'

        self.mouse_over_time_text.text = text

    def set_mouse_over_intervals(self, pd_interval_rows: pd.DataFrame):
        self.mouse_over_intervals = pd_interval_rows
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

    def set_mouse_over_interval(self, interval: Optional[pd.Series]):
        self.mouse_over_interval = interval

        if interval is None:
            # The interval is being cleared. Clear the text in the detail section.
            self.mouse_over_interval_text.text = ''
            message_section_ref.set_message('')
            return

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
        if human_message_val:
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

        self.legends: Dict[IntervalCategory, List[ColorLegendEntry]] = dict()
        x_offsets: Dict[str, int] = dict()
        for category in [e.value for e in IntervalCategory]:
            self.legends[category] = list()
            x_offsets[category] = Layout.ZOOM_DATE_RANGE_DISPLAY_BAR_LEFT + self.legend_label.content_width + 4  # When a category is display, each entry needs to shift by an offset

        for classification in self.classifications:
            category_value = classification.category.value
            category_element_list = self.legends[category_value]
            x_offset = x_offsets[category_value]
            entry = ColorLegendEntry(self.window, classification)
            category_element_list.append(entry)
            entry.pos(x_offset)
            x_offset += entry.width + 4  # 4px between legend entries
            x_offsets[category_value] = x_offset  # Store the offset for the next entry's offset

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
        mouse_over_intervals = detail_section_ref.mouse_over_intervals
        if mouse_over_intervals is not None:
            first_interval = mouse_over_intervals.iloc[0]
            category: IntervalCategory = first_interval['category']
            for entry in self.legends[category]:
                entry.draw()


class ZoomDateRangeDisplayBar(SimpleRect):
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

        jumping_unit_options = [60*60, 60*30, 60*10, 60*5, 60, 30, 10, 5]

        tick_jumping_unit: int = jumping_unit_options[-1]  # Maximum jumping unit is hours
        for jumping_unit_to_test in jumping_unit_options:
            if seconds_in_timeline / jumping_unit_to_test > self.width * 0.10:
                # This jumping unit would result in more than a tick mark every 10 pixels
                break
            tick_jumping_unit = jumping_unit_to_test

        comfortable_number_of_labels = self.width // 100  # Don't position labels less than 100 pixels from each other

        label_jumping_unit: int = jumping_unit_options[-1]  # Maximum distance between labels is one hour
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
                initial_offset_x = self.ei.left_offset_from_datetime(start_time, moving_timestamp, self.width)
                moving_offset_x = initial_offset_x

            time_part = moving_timestamp.time()
            seconds_since_midnight = time_part.hour * 3600 + time_part.minute * 60 + time_part.second

            ticket_draw_rule: Optional[Tuple] = None
            for modder in jumping_unit_options:
                if seconds_since_midnight % modder == 0:
                    ticket_draw_rule = ZoomDateRangeDisplayBar.TICK_DRAW_RULES[modder]
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


class IntervalsTimeline:

    def __init__(self, graph_section: arcade.Section,
                 ei: EventsInspector,
                 pd_interval_rows: pd.DataFrame,
                 timeline_row_width: int,
                 timeline_row_height: int):
        """
        :param pd_interval_rows: a list of rows for a given locator, ordered by 'to:'
        """
        self.first_interval_row = pd_interval_rows.iloc[0]
        self.graph_section = graph_section
        self.window = self.graph_section.window
        self.ei = ei
        self.pd_interval_rows = pd_interval_rows

        # Image to container rendered timeline WITHOUT any mouse over
        # highlighting.
        self.shape_element_list: Optional[arcade.ShapeElementList] = None
        self.timeline_row_width = timeline_row_width
        self.timeline_row_height = timeline_row_height
        self.set_size()
        self.last_set_left = 0
        self.last_set_bottom = 0

        # If a series within this timeline is determined to be under the mouse,
        # this value will be set to the interval's row. When the mouse
        # leaves the interval's area, this value will be unset.
        self.interval_under_mouse: Optional[pd.Series] = None

    def set_size(self):
        """
        Populates an element list with the rendered timeline. on_draw uses this shape element list.
        """""
        self.shape_element_list = arcade.ShapeElementList()
        # First, we draw a single line of background color along the full width and height of the timeline area.
        # This will ensure the ShapeElementList we create has the width of a timeline bar (important for
        # centering with setting position).
        background_line_start_x = 0
        background_line_end_x = self.timeline_row_width
        background_line = arcade.create_line(
            start_x=background_line_start_x,
            start_y=self.timeline_row_height/2,
            end_x=background_line_end_x,
            end_y=self.timeline_row_height/2,
            line_width=self.timeline_row_height,
            color=Theme.COLOR_TIMELINE_BACKGROUND
        )
        self.shape_element_list.append(background_line)

        # pd_interval_rows contains a list of intervals specific to this timeline.
        # iterate through them all and draw the relevant pixels into the image buffer.
        for _, interval_row in self.pd_interval_rows.iterrows():
            interval_line_start_x, interval_line_end_x = self.get_interval_extents(interval_row)

            if interval_line_end_x > background_line_end_x:
                # If we have zoomed in on an interval and its 'to' goes beyond the end
                # of the timeline, don't draw the full line.
                interval_line_end_x = background_line_end_x
            interval_line = arcade.create_line(start_x=interval_line_start_x,
                                               start_y=self.timeline_row_height//2,
                                               end_x=interval_line_end_x,
                                               end_y=self.timeline_row_height//2,
                                               line_width=self.timeline_row_height-1,  # Subtraction will leave some spacing between rows
                                               color=interval_row['color'])
            self.shape_element_list.append(interval_line)
            # Setup information to draw directly into the PIL image buffer

    def get_interval_extents(self, interval_row: pd.Series) -> Tuple[float, float]:
        """
        Returns the starting x and ending x of an interval within the timeline. The
        x values are relative to the beginning of the timeline - not relative to the screen.
        :param interval_row: An interval on the timeline.
        :return: (start_x, end_x)
        """
        interval_left_offset = self.ei.calculate_left_offset(self.timeline_row_width, interval_row)
        interval_width = self.ei.calculate_interval_width(self.timeline_row_width, interval_row)
        interval_line_start_x = interval_left_offset
        interval_line_end_x = interval_left_offset + interval_width
        return interval_line_start_x, interval_line_end_x

    def pos(self, left, bottom):
        # While these attributes are called center_?, it appears to just be the bottom,left coordinates
        self.shape_element_list.center_x = left
        self.shape_element_list.center_y = bottom  # + (self.timeline_row_height / 2)
        self.last_set_left = left
        self.last_set_bottom = bottom

    def draw(self, check_for_mouse_over_interval=False):
        global detail_section_ref
        self.shape_element_list.draw()

        if self.interval_under_mouse is not None:
            # There is an interval under a recent mouse position --
            # enhance the size of the interval visually. Rows are painted
            # from top to bottom. If we increase size downward, the next row
            # will paint over it. So increase size upward.
            start_x, end_x = self.get_interval_extents(self.interval_under_mouse)

            # First, draw a bright white rectangle (just a wide line) that is
            # slightly offset from the selected interval. A few of these white pixels
            # will be left around the border of the interval when it renders in.
            arcade.draw_line(
                start_x=self.last_set_left + start_x - 2,
                start_y=self.last_set_bottom + self.timeline_row_height / 2 + 4,
                end_x=self.last_set_left + end_x,
                end_y=self.last_set_bottom + self.timeline_row_height / 2 + 4,
                line_width=self.timeline_row_height + 2,
                color=arcade.color.WHITE,
            )

            arcade.draw_line(
                start_x=self.last_set_left + start_x,
                start_y=self.last_set_bottom + self.timeline_row_height / 2 + 2,
                end_x=self.last_set_left + end_x,
                end_y=self.last_set_bottom + self.timeline_row_height / 2 + 2,
                line_width=self.timeline_row_height + 2,
                color=self.interval_under_mouse['color']
            )

        mouse_x, mouse_y = self.ei.last_known_mouse_location
        row_bottom = self.shape_element_list.center_y
        row_top = self.shape_element_list.center_y + self.timeline_row_height

        if detail_section_ref:

            def clear_my_interval_detail():
                if self.interval_under_mouse is not None:
                    # If we set the interval being displayed by the detail section, clear it
                    if self.interval_under_mouse.equals(detail_section_ref.mouse_over_interval):
                        detail_section_ref.set_mouse_over_interval(None)
                    self.interval_under_mouse = None

            # See if the mouse is over this particular timeline row visualization
            if mouse_y >= row_bottom and mouse_y < row_top:

                # Draw the horizontal cross hair line
                arcade.draw_line(
                    start_x=Layout.CATEGORY_BAR_WIDTH + Layout.CATEGORY_BAR_LEFT,
                    start_y=self.last_set_bottom + 1,
                    end_x=Layout.CATEGORY_BAR_WIDTH + Layout.CATEGORY_BAR_LEFT + self.timeline_row_width,
                    end_y=self.last_set_bottom + 1,
                    line_width=1,
                    color=Theme.COLOR_CROSS_HAIR_LINES
                )

                if check_for_mouse_over_interval:
                    detail_section_ref.set_mouse_over_intervals(self.pd_interval_rows)
                    # Determine whether the mouse is over a date selecting an interval in this timeline
                    if detail_section_ref.mouse_over_time_dt:
                        df = self.pd_interval_rows
                        moment = detail_section_ref.mouse_over_time_dt
                        over_intervals_df = df[(moment >= df['from']) & (moment <= df['to'])]
                        if not over_intervals_df.empty:
                            # The mouse is over one or more intervals in the timeline.
                            # Select the last one in the list since the analysis module sorts
                            # based on from, the last one in the list should be the one that started
                            # most closely to the mouse point and the one that visually occupies the
                            # screen. i.e. if there are overlapping intervals in the timeline, the
                            # newer one will paint over the order as rendering progresses left->right
                            # in the timeline draw.
                            over_interval = over_intervals_df.iloc[-1]
                            detail_section_ref.set_mouse_over_interval(over_interval)
                            self.interval_under_mouse = over_interval
                        else:
                            clear_my_interval_detail()
            else:
                # If the mouse is not over the timeline, it is definitely n52462ot over an interval
                # If we set the interval, clear it
                clear_my_interval_detail()

    def get_category_name(self) -> str:
        return self.first_interval_row['category']

    def get_locator_value(self) -> str:
        return self.first_interval_row['locator']

    def get_timeline_id(self) -> str:
        return self.first_interval_row['timeline_id']


class ZoomScrollBar(SimpleRect):

    def __init__(self, graph_section: arcade.Section):
        super().__init__(color=arcade.color.LIGHT_GRAY)
        self.graph_section = graph_section
        self.scroll_percent = 0.0
        self.window = self.graph_section.window
        self.current_scrolled_y_rows = 0
        self.current_rows_per_page = 0
        self.scrollable_row_count = 1
        self.on_resize()

    def on_scrolling_change(self, current_scrolled_y_rows: int, current_rows_per_page: int, available_row_count: int):
        self.current_scrolled_y_rows = current_scrolled_y_rows
        self.scrollable_row_count = max(1, available_row_count - current_rows_per_page + 2)  # prevent divide by zero
        self.current_rows_per_page = current_rows_per_page

    def on_resize(self):
        self.position(
            right=self.window.width - Layout.ZOOM_SCROLL_BAR_RIGHT_OFFSET,
            width=Layout.ZOOM_SCROLL_BAR_WIDTH,
            bottom=Layout.ZOOM_SCROLL_BAR_BOTTOM,
            top=self.window.height - Layout.ZOOM_SCROLL_BAR_TOP_OFFSET
        )

    def draw(self):
        super().draw()
        if self.scrollable_row_count > 0:
            handle_height_px = min(self.height, int(self.current_rows_per_page / self.scrollable_row_count * self.height))  # the handle should shrink as the rows on the page represent a smaller fraction of available rows
            available_scroll_bar_height = self.height - handle_height_px
            handle_y_percentage = self.current_scrolled_y_rows / self.scrollable_row_count  # % distance from the top
            handle_y_offset = handle_y_percentage * available_scroll_bar_height
            arcade.draw_lrtb_rectangle_filled(
                left=self.left,
                right=self.right,
                top=self.top - handle_y_offset,
                bottom=self.top - handle_y_offset - handle_height_px,
                color=arcade.color.DARK_GRAY
            )


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
                end_x=self.right + self.ei.current_timeline_width,  # draw line across timeline view / between rows
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

    # Updating the information about the selected timeline and interval
    # is a relatively expensive operation. So do it only as fast as a human
    # could conceivably react to the information.
    PERIOD_BETWEEN_INTERVAL_DETAIL_UPDATE: float = 1 / 4

    def __init__(self, ei: EventsInspector, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ei = ei
        self.color_legend_bar = ColorLegendBar(self, ei)
        self.zoom_date_range_display_bar = ZoomDateRangeDisplayBar(self, ei)
        self.category_bar = CategoryBar(self, ei)
        self.zoom_scroll_bar = ZoomScrollBar(self)

        # When rows are drawn, this reference will be set to the top most
        # interval timeline (the first row). This is useful to inform
        # keeping the user's place when they zoom in or out.
        self.first_visible_interval_timeline: Optional[IntervalsTimeline] = None

        # self.row_height_px = 10
        self.row_height_px = 3
        self.rows_to_display = 0
        self._scroll_y_rows = 0  # How many rows we have scrolled past

        self.keys_down = set()
        # When this value is <= 0, the action desired by a key being held down will be
        # performed. The time will then be increased again to ensure the key press action
        # does not happen on every frame.
        self.time_until_next_process_keys_down = 0.0

        # When this value is <= 0, then a scan will be made to determine if the mouse is
        # over a specific timeline and interval. The timer reduces the overhead of doing
        # this operation for every draw.
        self.time_until_next_interval_detail_update = 0.0

        self.mouse_over_timeline_area = False
        # When the mouse is over the rendered interval timeline area,
        # these will be set to the offset relative to that area.
        self.mouse_timeline_area_offset_x: Optional[int] = None
        self.mouse_timeline_area_offset_y: Optional[int] = None

        # If a mouse button is being held down, the position it was pressed is
        # stored in the tuple [x, y, modifies] mapped to the button int.
        self.mouse_button_down: Dict[int, Tuple[int, int, int]] = dict()
        # If the mouse is moving with the button down, the tuple indicates
        # the x,y of the mouse during the drag.
        self.mouse_button_active_drag: Dict[int, Tuple[int, int]] = dict()
        # When a mouse button is released after a registered drag, then this
        # map is populated with two tuples from button_down and active_drag.
        self.mouse_button_finished_drag: Dict[int, Tuple[Tuple[int, int, int], Tuple[int, int]]] = dict()

        self.on_resize(self.window.width, self.window.height)

    @lru_cache(1000)
    def get_interval_timeline(self, group_id, timeline_start_time, timeline_stop_time, timeline_row_width, timeline_row_height) -> IntervalsTimeline:
        """
        :param group_id: A key for ei.grouped_intervals to find a DataFrame of interval rows for a specific timeline row.
        :param timeline_row_width: The on-screen width (px) the timeline row should occupy when rendered.
        :param timeline_row_height: The on-screen height (px), the timeline row should occupy when rendered.
        :return: IntervalsTimeline capable of drawing the timeline on screen.
        """
        intervals_for_row_df: pd.DataFrame = self.ei.grouped_intervals.get_group(group_id)
        interval_timeline = IntervalsTimeline(
            graph_section=self,
            ei=self.ei,
            pd_interval_rows=intervals_for_row_df,
            timeline_row_height=timeline_row_height,
            timeline_row_width=timeline_row_width
        )
        return interval_timeline

    @property
    def scroll_y_rows(self):
        return self._scroll_y_rows

    @scroll_y_rows.setter
    def scroll_y_rows(self, val):
        if val < 0:
            val = 0

        self._scroll_y_rows = val
        self.resize_scroll_bar()

    def resize_scroll_bar(self):
        self.zoom_scroll_bar.on_scrolling_change(
            current_scrolled_y_rows=self.scroll_y_rows,
            current_rows_per_page=self.calc_rows_to_display(),
            available_row_count=len(self.ei.grouped_intervals.groups.keys())
        )

    def scroll_down_by_rows(self, count: int):
        self.scroll_y_rows += count

    def on_mouse_scroll(self, x: int, y: int, scroll_x: int, scroll_y: int):
        self.scroll_down_by_rows(-1 * int(scroll_y // 2))

    def zoom_to_dates(self, start: datetime.datetime, end: datetime.datetime):
        if self.first_visible_interval_timeline:
            # The user has zoomed with this row as the top of their view.
            previous_first_category = self.first_visible_interval_timeline.get_category_name()
            previous_first_locator = self.first_visible_interval_timeline.get_locator_value()

        self.ei.zoom_to_dates(start, end)

        # When someone zooms, some timelines might disappear from the selection (because they do not
        # have active intervals within the selected date ranges). If enough disappear, the position of the
        # old scroll location may be well away from the old visible rows -- which can be disorienting
        # because the user no longer sees the old visible patterns. To account for this, when they scroll,
        # we search for a category/locator pair that is closest to what the top row used to be before
        # the scroll.
        # If we find a good match, we scroll to that new location in the timeline rows.
        target_for_scroll: Optional[int] = None  # if we can't find anything close, scroll back to the top
        timeline_number = 0
        for row_id_tuple in self.ei.grouped_intervals.groups.keys():
            # Intervals are grouped by category, and then locator. So a group
            # key will be a tuple (category, locator).
            row_category, row_locator = row_id_tuple
            if row_category >= previous_first_category and row_locator >= previous_first_locator:
                target_for_scroll = timeline_number
                break
            timeline_number += 1

        if target_for_scroll is not None:
            self.scroll_y_rows = target_for_scroll
        else:
            self.scroll_down_by_rows(-1)

    def process_keys_down(self, delay_until_next: Optional[float] = None):
        """
        Reacts to any keys that are presently being held down
        :param delay_until_next: If the caller wants a non-default time period to pass before this method
                    is called again by on_update, specify a value.
        """
        if arcade.key.HOME in self.keys_down:
            self.scroll_y_rows = 0

        if arcade.key.END in self.keys_down:
            self.scroll_y_rows = len(self.ei.grouped_intervals.groups.keys())

        if arcade.key.R in self.keys_down:
            self.zoom_to_dates(self.ei.absolute_timeline_start, self.ei.absolute_timeline_stop)
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
            self.resize_scroll_bar()

        if arcade.key.NUM_SUBTRACT in self.keys_down or arcade.key.MINUS in self.keys_down:
            self.row_height_px = max(self.row_height_px - 2, 2)
            self.resize_scroll_bar()

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
        self.zoom_scroll_bar.on_resize()
        self.resize_scroll_bar()  # Trigger a scroll bar handle re-calculation

    def calc_rows_to_display(self) -> int:
        """
        Returns the number of FULL rows that can be displayed in the timeline graphing area.
        """
        return self.category_bar.height // self.row_height_px

    def on_draw(self):
        arcade.draw_lrtb_rectangle_filled(  # Clear the section
            left=self.left,
            right=self.right,
            top=self.top,
            bottom=self.bottom,
            color=arcade.color.BLACK
        )

        self.color_legend_bar.draw()
        self.zoom_date_range_display_bar.draw()

        # If the minimum height of each row is ?px, then figure out how
        # many we can fit in the graph area.
        rows_to_display = self.calc_rows_to_display()
        available_timelines_ids = list(self.ei.grouped_intervals.groups.keys())

        # If the user has hit END or scrolled to the end of the data, show the last full page
        if self.scroll_y_rows > len(available_timelines_ids) - self.calc_rows_to_display():
            self.scroll_y_rows = max(0, len(available_timelines_ids) - self.calc_rows_to_display())

        row_id_tuples_to_render = available_timelines_ids[self.scroll_y_rows:self.scroll_y_rows+rows_to_display]

        if len(row_id_tuples_to_render) < rows_to_display:
            # If we don't have enough rows to render, then clear the background
            # to erase any old timelines.
            arcade.draw_xywh_rectangle_filled(
                bottom_left_y=self.category_bar.bottom,
                bottom_left_x=self.category_bar.right,
                width=self.ei.get_current_timeline_width(),
                height=self.category_bar.height,
                color=arcade.color.BLACK
            )

        # In order to draw category labels in the category bar, we need to
        # know how large the area the label should cover on the screen -
        # which is how many rows are in the category * row_height. As we draw
        # the rows, keep a count of how many rows are in each category.
        # Since rows are drawn from top to bottom, the ordered dict will keep
        # the order of category labels to draw as well.
        category_label_row_count: OrderedDict[str, int] = OrderedDict()

        check_for_mouse_over_interval = False
        if self.time_until_next_interval_detail_update <= 0.0:
            check_for_mouse_over_interval = True
            self.time_until_next_interval_detail_update = GraphSection.PERIOD_BETWEEN_INTERVAL_DETAIL_UPDATE

        visual_row_number = 0
        for row_id_tuple in row_id_tuples_to_render:
            interval_timeline = self.get_interval_timeline(
                group_id=row_id_tuple,
                timeline_row_width=self.ei.get_current_timeline_width(),
                timeline_row_height=self.row_height_px,
                # start & stop are not presently used by the interval timeline when drawing itself,
                # but we use lru caching in the function, and the timeline returned needs to change
                # based on these parameters, so send them along.
                timeline_start_time=self.ei.zoom_timeline_start,
                timeline_stop_time=self.ei.zoom_timeline_stop
            )

            if visual_row_number == 0:
                # Keep track of the first row on the screen. This is useful
                # for keeping the user's place if they zoom in.
                self.first_visible_interval_timeline = interval_timeline

            # Track the number of rows in each category
            row_category = interval_timeline.get_category_name()
            if row_category not in category_label_row_count:
                category_label_row_count[row_category] = 1
            else:
                category_label_row_count[row_category] = category_label_row_count[row_category] + 1

            interval_timeline.pos(self.category_bar.right,
                                  bottom=self.category_bar.top - (visual_row_number * self.row_height_px) - self.row_height_px)
            interval_timeline.draw(check_for_mouse_over_interval=check_for_mouse_over_interval)
            visual_row_number += 1

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

        if self.mouse_over_timeline_area:
            # Draw a line which follows the mouse while it is over the timeline draw area.
            arcade.draw_line(start_x=self.category_bar.right + self.mouse_timeline_area_offset_x,
                             start_y=self.bottom,
                             end_x=self.category_bar.right + self.mouse_timeline_area_offset_x,
                             end_y=self.bottom + self.category_bar.height,
                             color=Theme.COLOR_CROSS_HAIR_LINES,
                             line_width=1)

        self.zoom_scroll_bar.draw()

        self.category_bar.draw_categories(category_label_row_count, row_height_px=self.row_height_px)

    def on_mouse_leave(self, x: int, y: int):
        self.mouse_over_timeline_area = False

    def on_mouse_release(self, x: int, y: int, button: int, modifiers: int):
        if button in self.mouse_button_down and button in self.mouse_button_active_drag:
            self.mouse_button_finished_drag[button] = (
                self.mouse_button_down[button],
                self.mouse_button_active_drag[button]
            )

        self.mouse_button_down.pop(button, None)
        self.mouse_button_active_drag.pop(button, None)

    def on_mouse_press(self, x: int, y: int, button: int, modifiers: int):
        self.mouse_button_down[button] = (x, y, modifiers)

    def on_mouse_motion(self, x: int, y: int, dx: Optional[int] = None, dy: Optional[int] = None):
        global detail_section_ref

        if x >= self.category_bar.right and x < self.zoom_scroll_bar.left and \
                y <= self.zoom_date_range_display_bar.bottom and y > self.category_bar.bottom:
            self.mouse_over_timeline_area = True
            self.mouse_timeline_area_offset_x = x - self.category_bar.right
            self.mouse_timeline_area_offset_y = y - self.category_bar.bottom
        else:
            self.mouse_over_timeline_area = False

        if arcade.MOUSE_BUTTON_LEFT in self.mouse_button_down:
            from_x, from_y, with_mod = self.mouse_button_down[arcade.MOUSE_BUTTON_LEFT]
            if abs(from_x - x) + abs(from_y - y) > 5:
                # If the mouse has moved at least 5 pixels from where the button first went down
                # create an active drag.
                self.mouse_button_active_drag[arcade.MOUSE_BUTTON_LEFT] = (x, y)
            else:
                # If there was an active drag, cancel it
                self.mouse_button_active_drag.pop(arcade.MOUSE_BUTTON_LEFT, None)

        if self.mouse_over_timeline_area:
            from_dt = None  # will be set to the datetime associated with the origin of a mouse drag, if one is active
            if arcade.MOUSE_BUTTON_LEFT in self.mouse_button_down and \
                    arcade.MOUSE_BUTTON_LEFT in self.mouse_button_active_drag:
                from_x, _, _ = self.mouse_button_down[arcade.MOUSE_BUTTON_LEFT]
                from_dt = self.ei.left_offset_to_datetime(from_x)
            detail_section_ref.set_mouse_over_time(self.ei.left_offset_to_datetime(self.mouse_timeline_area_offset_x), from_dt)

    def on_update(self, delta_time: float):
        self.time_until_next_process_keys_down -= delta_time
        if self.time_until_next_process_keys_down < 0:
            self.process_keys_down()

        self.time_until_next_interval_detail_update -= delta_time

        if arcade.MOUSE_BUTTON_LEFT in self.mouse_button_finished_drag:
            (from_x, from_y, with_mod), (to_x, to_y) = self.mouse_button_finished_drag.pop(arcade.MOUSE_BUTTON_LEFT)
            from_dt = self.ei.left_offset_to_datetime(from_x - Layout.ZOOM_DATE_RANGE_DISPLAY_BAR_LEFT)
            to_dt = self.ei.left_offset_to_datetime(to_x - Layout.ZOOM_DATE_RANGE_DISPLAY_BAR_LEFT)
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
        self.add_section(self.graph_section)
        self.add_section(self.detail_section)
        self.add_section(self.message_section)

    def adjust_section_positions(self, window_width, window_height):
        self.graph_section.width = window_width
        self.graph_section.height = window_height - Layout.GRAPH_SECTION_BOTTOM
        self.graph_section.left = 0
        self.graph_section.bottom = Layout.GRAPH_SECTION_BOTTOM

        self.detail_section.width = window_width

    def on_resize(self, window_width: int, window_height: int):
        self.ei.on_zoom_resize(window_width - Layout.ZOOM_SCROLL_BAR_WIDTH - Layout.CATEGORY_BAR_WIDTH)
        self.adjust_section_positions(window_width, window_height)
        self.message_section.on_resize(window_width, window_height)
        self.graph_section.on_resize(window_width, window_height)
        self.detail_section.on_resize(window_width, window_height)

    def on_show_view(self):
        self.on_resize(self.window.width, self.window.height)

    def on_mouse_motion(self, x: int, y: int, dx: int, dy: int):
        self.ei.last_known_mouse_location = (x, y)

    def on_key_press(self, symbol: int, modifiers: int):
        self.window.on_key_press(symbol, modifiers)  # Pass key to window in case there are view changes desired


class MainWindow(arcade.Window):

    def __init__(self, width, height, title):
        super().__init__(width, height, title, resizable=True)
        arcade.set_background_color(arcade.color.WHITE)
        self.ei = EventsInspector()
        self.import_timeline_view = ImportTimelineView(self, load_data=lambda stream: self.on_data_load(stream))
        self.graph_view: Optional[GraphInterfaceView] = None
        self.filter_view: Optional[FilteringView] = None
        self.show_view(self.import_timeline_view)

    def on_data_load(self, stream_handle):
        intervals_list = json.load(stream_handle)['items']
        self.ei.add_interval_data(intervals_list)
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
        if symbol == arcade.key.F1 or symbol == arcade.key.SLASH:
            self.show_view(self.filter_view)

        if self.current_view == self.filter_view and symbol == arcade.key.ESCAPE:
            self.show_view(self.graph_view)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    # with open('e2e-timelines_everything_20240110-190423.json', mode='r') as json_fd:
    #     events_dict = json.load(json_fd)['items']

    # with open('small.json', mode='r') as json_fd:
    #     events_dict = json.load(json_fd)['items']

    mw = MainWindow(INIT_SCREEN_WIDTH, INIT_SCREEN_HEIGHT, SCREEN_TITLE)
    mw.maximize()
    arcade.run()
