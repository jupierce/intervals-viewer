import datetime
from typing import Optional, Dict

import arcade


class SimpleRect:

    def __init__(self, color: arcade.Color, border_color: Optional[arcade.Color] = None, border_width: int = 0):
        self._left = 0
        self._right = 0
        self._top = 0
        self._bottom = 0
        self._width = 0
        self._height = 0
        self.color = color
        self.border_color = border_color
        self.border_width = border_width

    def position(self,
                 left: Optional[int] = None,
                 right: Optional[int] = None,
                 top: Optional[int] = None,
                 bottom: Optional[int] = None,
                 width: Optional[int] = None,
                 height: Optional[int] = None):

        if left is None:
            left = right - width
        if right is None:
            right = left + width
        if width is None:
            width = right - left
        if top is None:
            top = bottom + height
        if bottom is None:
            bottom = top - height
        if height is None:
            height = top - bottom

        self._left = left
        self._right = right
        self._top = top
        self._bottom = bottom
        self._width = width
        self._height = height

        if self._left is None or self._right is None or self._top is None or self._bottom is None or self._width is None or self._height is None:
            raise IOError(f'Insufficient position information: l={self._left}, r={self._right}, t={self._top}, b={self._bottom}, w={self._width}, h={self._height}')

    @property
    def left(self) -> int:
        """ Left edge of this section """
        return self._left

    @property
    def bottom(self) -> int:
        """ The bottom edge of this section """
        return self._bottom

    @property
    def width(self) -> int:
        """ The width of this section """
        return self._width

    @property
    def height(self) -> int:
        """ The height of this section """
        return self._height

    @property
    def right(self) -> int:
        """ Right edge of this section """
        return self._right

    @property
    def top(self) -> int:
        """ Top edge of this section """
        return self._top

    def is_xy_within(self, x, y):
        """
        Returns true when an x,y coordinate falls within this rectangle
        """
        return x >= self.left and x < self.right and y >= self.bottom and y < self.top

    def offset_of_xy(self, x, y):
        return x - self.left, y - self.bottom

    def draw(self):
        if self.border_color:
            arcade.draw_lrtb_rectangle_filled(self.left, self.right, self.top, self.bottom, color=self.border_color)
        if self.right - self.left > (self.border_width*2) and self.top - self.bottom > (self.border_width*2):
            arcade.draw_lrtb_rectangle_filled(
                self.left + self.border_width,
                self.right - self.border_width,
                self.top - self.border_width,
                self.bottom + self.border_width,
                color=self.color)


def humanize_timedelta(td: datetime.timedelta) -> str:
    # Extract days, hours, minutes, and seconds
    days, seconds = td.days, td.seconds
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    # Create a human-readable string
    result = "{:02}:{:02}:{:02}".format(hours, minutes, seconds)
    if days > 0:
        result = "{} days, {}".format(days, result)

    return result


def make_color_brighter(color: arcade.Color, factor=1.2) -> arcade.Color:
    """
    Create a brighter version of the given color.

    Parameters:
    - color: arcade.Color instance
    - factor: Brightness factor (default is 1.2)

    Returns:
    - arcade.Color instance representing the brighter color
    """
    r = min(int(color[0] * factor), 255)
    g = min(int(color[1] * factor), 255)
    b = min(int(color[2] * factor), 255)
    return r, g, b, 255


def prioritized_sort(entry):
    # When these words are in a list to be sorted, ensure they are listed first, and in this order.
    required_subset_ordering: Dict[str, int] = {
        'namespace': 0,
        'pod': 1,
        'container': 2
    }

    # Return a tuple with two elements - the first is the subset order or a high value
    # if the word is not in the subset, and the second is the word itself
    return (required_subset_ordering.get(entry, float('inf')), entry)

