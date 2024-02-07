import traceback
from typing import Callable

import pyperclip

import arcade
import arcade.gui as gui

import requests
from ui.layout import Theme


class ImportTimelineView(arcade.View):

    def __init__(self, window: arcade.Window, load_data: Callable):
        super().__init__(window)
        self.manager = arcade.gui.UIManager()
        self.load_data = load_data

        self.v_box = gui.UIBoxLayout()

        # Create a text label
        self.filtering_label = arcade.gui.UILabel(
            text="Timelines JSON",
            text_color=arcade.color.DARK_RED,
            height=40,
            font_size=20,
            font_name=Theme.FONT_NAME)

        self.v_box.add(self.filtering_label.with_space_around(bottom=10))

        # Create a texture that can be used to fill in the input fields.
        input_field_bg = arcade.make_soft_square_texture(size=1000, color=(240, 240, 240), outer_alpha=255)

        url_hbox = gui.UIBoxLayout(vertical=False)
        field_label = arcade.gui.UILabel(
            text='URL',
            text_color=arcade.color.BLACK,
            width=200,
            height=40,
            font_size=15,
            font_name=Theme.FONT_NAME)
        url_hbox.add(field_label)

        # Create a text input field
        self.url_ui_input = gui.UIInputText(
            text_color=arcade.color.BLACK,
            font_size=15,
            width=800,
            text=" ",
        )
        self.url_ui_input.caret.visible = False
        url_hbox.add(self.url_ui_input.with_background(input_field_bg).with_border(color=arcade.color.DARK_GRAY).with_space_around(top=20))
        self.v_box.add(url_hbox)

        buttons_hbox = gui.UIBoxLayout(vertical=False, space_between=20)
        # Create a button
        import_button = gui.UIFlatButton(
            color=arcade.color.DARK_BLUE_GRAY,
            text='Import'
        )
        import_button.on_click = self.on_import_click
        buttons_hbox.add(import_button)

        self.v_box.add(buttons_hbox.with_space_around(top=20))

        self.status_area = arcade.gui.UITextArea(
            text='On Linux, install xclip for Ctrl-V support. Enter URL and hit import.',
            height=500,
            width=1000,
            bold=True,
            font_size=15,
            font_name=Theme.FONT_NAME,
            align='left',
            multiline=True,
        )
        self.v_box.add(self.status_area.with_border().with_space_around(top=20))

        self.manager.add(
            arcade.gui.UIAnchorWidget(
                anchor_x="center_x",
                anchor_y="top",
                child=self.v_box)
        )

    def on_resize(self, window_width: int, window_height: int):
        super().on_resize(window_width, window_height)

    def on_import_click(self, event):
        try:
            self.status_area.text = 'Loading...'
            url = self.url_ui_input.text
            with requests.get(url, stream=True) as response:
                if response.status_code == 200:
                    with response.raw as stream:
                        self.load_data(stream)
                else:
                    print(f"Failed to download file. Status code: {response.status_code}")
                    self.status_area.text = f"Failed to download file. Status code: {response.status_code}\n{response.text}"
        except:
            err = f"Failed to download file:\n{traceback.format_exc()}"
            self.status_area.text = err
            traceback.print_exc()
            pass

    def on_draw(self):
        self.clear()
        self.manager.draw()

    def on_update(self, delta_time: float):
        self.manager.on_update(delta_time)

    def on_show_view(self):
        self.manager.enable()
        self.on_resize(self.window.width, self.window.height)
        arcade.set_background_color(arcade.color.DARK_BLUE_GRAY)

    def on_hide_view(self):
        self.manager.disable()

    def on_key_press(self, symbol: int, modifiers: int):
        # Check for Ctrl+V (Cmd+V on macOS)
        if (symbol == arcade.key.V) and (modifiers & arcade.key.MOD_CTRL):
            # Get clipboard data using pyperclip
            clipboard_data = pyperclip.paste()

            # Update the UIInputBox with clipboard data
            self.url_ui_input.text = clipboard_data
