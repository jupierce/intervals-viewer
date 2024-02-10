import traceback
from typing import Callable, Optional

import pyperclip
import threading

import io
import arcade
import arcade.gui as gui

import requests
from ui.layout import Theme

from contextlib import closing


class DownloadStatus:

    def __init__(self):
        self.message = ''
        self.buffer = io.BytesIO()
        self.exception = None
        self.complete = False


def download_file(url, status_lock, status: DownloadStatus):
    try:
        url = url.strip()
        file_path = None
        if url.lower().startswith('file://'):
            file_path = url[len('file://'):]

        if '://' not in url:
            file_path = url

        if file_path:
            with open(file_path, "rb") as fh:
                with status_lock:
                    status.message = 'Reading local file..'
                    status.buffer = io.BytesIO(fh.read())
        else:
            with closing(requests.get(url, stream=True)) as response:
                response.raise_for_status()
                downloaded_size = 0
                for chunk in response.iter_content(chunk_size=1024*1024):
                    if chunk:
                        downloaded_size += len(chunk)
                        mb = downloaded_size / (1024*1024)
                        with status_lock:
                            status.buffer.write(chunk)
                            status.message = f'Downloading.. {mb:.2f}MB'

    except Exception as e:
        with status_lock:
            status.exception = e
        traceback.print_exc()
    finally:
        with status_lock:
            status.complete = True
            status.buffer.seek(0)


class ImportTimelineView(arcade.View):

    INIT_STATUS_MESSAGE = 'On Linux, install xclip for Ctrl-V support. Enter URL and hit import.'

    def __init__(self, window: arcade.Window, load_data: Callable):
        super().__init__(window)
        self.download_checker: Optional[Callable] = None
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
        self.url_ui_input._active = True
        url_hbox.add(self.url_ui_input.with_background(input_field_bg).with_border(color=arcade.color.DARK_GRAY).with_space_around(top=20))
        self.v_box.add(url_hbox)

        buttons_hbox = gui.UIBoxLayout(vertical=False, space_between=20)
        # Create a button
        self.import_button = gui.UIFlatButton(
            color=arcade.color.DARK_BLUE_GRAY,
            text='Import'
        )
        self.import_button.on_click = self.on_import_click
        buttons_hbox.add(self.import_button)

        self.v_box.add(buttons_hbox.with_space_around(top=20))

        self.status_area = arcade.gui.UITextArea(
            text=ImportTimelineView.INIT_STATUS_MESSAGE,
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

    def on_show(self):
        self.status_area.text = ImportTimelineView.INIT_STATUS_MESSAGE

    def check_download(self, download_thread: threading.Thread, status_lock: threading.Lock, status: DownloadStatus):
        def set_status_area(description: str):
            self.status_area.text = description
            self.on_draw()

        with status_lock:
            set_status_area(status.message)
            if status.complete:
                if status.exception is None:
                    print('Loading data..')
                    self.load_data(status.buffer)
                print('Joining thread...')
                download_thread.join()
                arcade.unschedule(self.download_checker)

    def on_import_click(self, event):
        download_url = self.url_ui_input.text
        status_lock = threading.Lock()
        download_status = DownloadStatus()
        download_thread = threading.Thread(target=download_file,
                                           args=(download_url, status_lock, download_status))
        download_thread.start()
        self.download_checker = lambda delta: self.check_download(download_thread, status_lock, download_status)
        arcade.schedule(self.download_checker, 1.0)

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
            self.url_ui_input.text += clipboard_data

        if symbol == arcade.key.ENTER:
            self.on_import_click(None)
