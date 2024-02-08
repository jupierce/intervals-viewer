import arcade
import arcade.gui as gui
import re
from collections import OrderedDict
from typing import Optional, Iterable, Dict, List, Set, Callable

from analyzer import EventsInspector
from ui.layout import Theme
from analyzer.intervals import IntervalAnalyzer


class FilteringField:
    SIMPLE_SEARCH_REGEX = re.compile(r'^[^=\'".]*$')  # If a user pattern does not contain =, ', ", or ., parse it as a simplified query expression

    def __init__(self, display_name: str, column_name: Optional[str] = None, filter_expression: str = ' '):
        self.display_name = display_name
        self.column_name = column_name
        self.filter_expression = filter_expression

        if column_name is None:
            autoname = display_name.lower()
            if autoname in ('category', 'classification'):
                self.column_name = f'{autoname}_str'
            elif autoname == 'message':
                self.column_name = f'{IntervalAnalyzer.STRUCTURED_MESSAGE_PREFIX}humanMessage'
            else:
                # assume autoname is a locator key
                self.column_name = f'{IntervalAnalyzer.STRUCTURED_LOCATOR_KEY_PREFIX}{autoname}'

    def get_field_text(self) -> str:
        return self.filter_expression

    def get_expression_component(self) -> Optional[str]:
        expression = self.filter_expression.lower().strip()
        if not expression:
            return None

        if FilteringField.SIMPLE_SEARCH_REGEX.match(expression):
            substrings = re.split(r'([&|()])', expression)  # split using &, |, (, ), and return a list containing delimiters
            expression = ''
            for substring in substrings:
                substring = substring.strip()
                if not substring:
                    continue
                if expression:
                    expression += ' '
                if substring in ('&', '|', 'not', ')', '('):
                    expression += substring
                else:
                    expression += f"@.contains('{substring}')"

        if 'isnull(' not in expression and 'contains(' in expression:
            # @.contains cannot run against null. Help the user out.
            expression = f'(not @.isnull()) & ({expression})'

        expression = expression.replace('contains(', 'str.lower().str.contains(')
        expression = expression.replace('@', f'`{self.column_name}`')
        return expression

    def is_field_set(self):
        return len(self.filter_expression.strip()) > 0

    def set_field_text(self, text: str):
        self.filter_expression = text


class FilteringFields:
    """
    Represents a collection of FilteringField objects tied to a set of
    field names (e.g. category, classification, ...).
    """

    def __init__(self, filtering_field_names: Iterable[str]):
        self.fields: OrderedDict[str, FilteringField] = OrderedDict()
        for field_name in filtering_field_names:
            self.fields[field_name] = FilteringField(field_name)

    def get_field_by_name(self, field_name: str) -> FilteringField:
        return self.fields[field_name]

    def get_expression_component(self) -> Optional[str]:
        if not self.any_field_set():
            return None

        expression = ''
        for filtering_field in self.fields.values():
            addition = filtering_field.get_expression_component()
            if addition:
                addition = f'({addition})'
                if expression:
                    expression += ' & '
                expression += addition

        return expression

    def reset(self):
        for field in self.fields.values():
            field.set_field_text(' ')

    def any_field_set(self) -> bool:
        for field in self.fields.values():
            if field.is_field_set():
                return True
        return False


class OrOfFilteringFields:
    """
    Represents and OR'd collection of FilteringFields.
    """

    def __init__(self, previous_button: arcade.gui.UIFlatButton, next_button: arcade.gui.UIFlatButton):
        self.previous_button = previous_button
        self.next_button = next_button
        self.filtering_field_inputs: OrderedDict[str, arcade.gui.UIInputText] = OrderedDict()
        self.field_sets: List[FilteringFields] = list()
        self.focus_offset = 0

        self.previous_button.on_click = lambda event: self.set_focus(self.focus_offset - 1)
        self.next_button.on_click = lambda event: self.set_focus(self.focus_offset + 1)

    def add_new_field_to_track(self, display_name, ui_text_input: arcade.gui.UIInputText):
        if len(self.field_sets) > 0:
            raise IOError('All fields must be added before setting focus')
        self.filtering_field_inputs[display_name] = ui_text_input

    def get_field_set_count(self):
        return len(self.field_sets)

    def set_focus(self, offset):
        if offset < 0:
            offset = 0

        while offset >= self.get_field_set_count():
            self._add_field_set()

        self.persist_focused_values()  # Store existing values before we navigate to new one
        self.focus_offset = offset
        self.refresh_focus()

    def persist_focused_values(self):
        field_set = self.field_sets[self.focus_offset]  # Store the old text values in the FilteringField objects
        for field_name, input_field in self.filtering_field_inputs.items():
            field_set.get_field_by_name(field_name).set_field_text(input_field.text)

    def refresh_focus(self):
        """
        Updates the text controls in the UI with the text associated with the
        "in focus" field set.
        """
        field_set = self.field_sets[self.focus_offset]
        for field_name, input_field in self.filtering_field_inputs.items():
            current_text = field_set.get_field_by_name(field_name).get_field_text()
            input_field.text = current_text
            input_field.trigger_full_render()

        if self.focus_offset == 0:
            self.previous_button.text = '<>'
        else:
            self.previous_button.text = f'<< Previous (#{self.focus_offset})'

        if self.focus_offset + 1 == self.get_field_set_count():
            self.next_button.text = f'Add OR (#{self.focus_offset + 2}) >>'  # Make the count 1 based for the UI
        else:
            self.next_button.text = f'Next OR (#{self.focus_offset + 2}) >>'  # Make the count 1 based for the UI

    def _add_field_set(self):
        self.field_sets.append(FilteringFields(list(self.filtering_field_inputs.keys())))

    def any_fields_set(self) -> bool:
        for field_set in self.field_sets:
            if field_set.any_field_set():
                return True
        return False

    def reset(self):
        for field_set in self.field_sets:
            field_set.reset()
        self.refresh_focus()

    def get_query_string(self) -> Optional[str]:
        if not self.any_fields_set():
            return None

        query_string = ''
        for field_set in self.field_sets:
            addition = field_set.get_expression_component()
            if addition:
                if query_string:
                    query_string += ' | '
                addition = f'({addition})'
                query_string += addition

        return query_string


class FilteringView(arcade.View):

    def __init__(self, ei: EventsInspector, window: arcade.Window, on_exit: Callable[[], None]):
        super().__init__(window)
        self.ei = ei
        self.manager = arcade.gui.UIManager()
        self.on_exit = on_exit

        self.v_box = gui.UIBoxLayout()

        # Create a text label
        self.filtering_label = arcade.gui.UILabel(
            text="Filtering Rules",
            text_color=arcade.color.DARK_RED,
            height=40,
            font_size=20,
            font_name=Theme.FONT_NAME)

        self.v_box.add(self.filtering_label.with_space_around(bottom=10))

        reset_button = gui.UIFlatButton(
            color=arcade.color.DARK_BLUE_GRAY,
            text='Reset',
        )
        reset_button.on_click = self.on_reset_click
        self.v_box.add(reset_button)

        or_buttons = gui.UIBoxLayout(vertical=False)
        self.previous_or_button = gui.UIFlatButton(
            color=arcade.color.DARK_BLUE_GRAY,
            text='',
            width=250
        )
        self.previous_or_button.active = False
        or_buttons.add(self.previous_or_button.with_space_around(right=100, top=20, bottom=20))

        self.next_or_button = gui.UIFlatButton(
            color=arcade.color.DARK_BLUE_GRAY,
            text='Next OR',
            width=250
        )
        or_buttons.add(self.next_or_button.with_space_around(left=100, top=20, bottom=20))
        self.or_fields_set = OrOfFilteringFields(previous_button=self.previous_or_button, next_button=self.next_or_button)

        self.v_box.add(or_buttons)

        # Create a texture that can be used to fill in the input fields.
        input_field_bg = arcade.make_soft_square_texture(size=1000, color=(240, 240, 240), outer_alpha=255)

        for filtering_field_name in ('Category', 'Classification', 'Namespace', 'Pod', 'UID', 'Message'):
            filter_field_hbox = gui.UIBoxLayout(vertical=False)
            field_label = arcade.gui.UILabel(
                text=filtering_field_name,
                text_color=arcade.color.BLACK,
                width=200,
                height=40,
                font_size=15,
                font_name=Theme.FONT_NAME)
            filter_field_hbox.add(field_label)

            # Create a text input field
            field_filter_ui_input = gui.UIInputText(
                text_color=arcade.color.BLACK,
                font_size=15,
                width=800,
                text=" ",
            )
            field_filter_ui_input.caret.visible = False
            filter_field_hbox.add(field_filter_ui_input.with_background(input_field_bg).with_border(color=arcade.color.DARK_GRAY).with_space_around(top=20))
            self.or_fields_set.add_new_field_to_track(filtering_field_name, field_filter_ui_input)
            self.v_box.add(filter_field_hbox)

        self.or_fields_set.set_focus(0)

        buttons_hbox = gui.UIBoxLayout(vertical=False, space_between=20)
        # Create a button
        apply_button = gui.UIFlatButton(
            color=arcade.color.DARK_BLUE_GRAY,
            text='Apply'
        )
        apply_button.on_click = self.on_apply_click
        buttons_hbox.add(apply_button)

        cancel_button = gui.UIFlatButton(
            color=arcade.color.DARK_BLUE_GRAY,
            text='Cancel',
        )
        cancel_button.on_click = self.on_cancel_click
        buttons_hbox.add(cancel_button)

        self.v_box.add(buttons_hbox.with_space_around(top=20))

        examples = arcade.gui.UITextArea(
            text='''
Expressions in the fields above are combined with AND. Add an OR using 'Add OR' button.            

Simple Expression Examples:
    (disruption | alert)            => Contains substring "disruption" or "alert"
    not(disruption | alert)         => Must not contain substring "disruption" or "alert"
    host & pod                      => Must contain substring "host" and "pod"

Complex Expression (do not mix with Simple):
    @.contains('.*pod.+tree')       => Must contain regex
    @.isnull() | @ == 'api'         => Value is null or exactly matches "api"  


'''.strip(),
            text_color=arcade.color.DARK_RED,
            width=1000,
            height=300,
            font_size=12,
            font_name=Theme.FONT_NAME,
            multiline=True
        )

        self.v_box.add(examples.with_space_around(top=30))

        self.manager.add(
            arcade.gui.UIAnchorWidget(
                anchor_x="center_x",
                anchor_y="top",
                child=self.v_box)
        )

    def on_resize(self, window_width: int, window_height: int):
        super().on_resize(window_width, window_height)

    def on_apply_click(self, event):
        self.or_fields_set.persist_focused_values()  # force existing values to be stored in data structure
        full_query_string = self.or_fields_set.get_query_string()
        print(f'Updating query: {full_query_string}')
        try:
            self.ei.set_filter_query(full_query_string)
            self.on_exit()
        except:
            # Syntax issue in query
            pass

    def on_reset_click(self, event):
        self.or_fields_set.reset()

    def on_cancel_click(self, event):
        self.on_exit()

    def on_draw(self):
        self.clear()
        self.manager.draw()

    def on_show_view(self):
        self.manager.enable()
        self.on_resize(self.window.width, self.window.height)
        arcade.set_background_color(arcade.color.DARK_BLUE_GRAY)

    def on_hide_view(self):
        self.manager.disable()

    def on_key_press(self, symbol: int, modifiers: int):
        self.window.on_key_press(symbol, modifiers)  # Pass key to window in case there are view changes desired
