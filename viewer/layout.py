import arcade


class Theme:
    COLOR_HIGHLIGHTED_CATEGORY_SECTION = arcade.color.AERO_BLUE
    FONT_SIZE_DETAILS_SECTION = 10
    FONT_NAME = ('Source Code Pro', 'Monospace', 'Courier New', 'Consolas', 'Console', 'Roboto Mono', 'Menlo')
    COLOR_CROSS_HAIR_LINES = (255, 255, 0, 175)
    COLOR_TIMELINE_BACKGROUND = arcade.color.BLACK


class Layout:

    CATEGORY_BAR_WIDTH = 20
    COLOR_LEGEND_BAR_HEIGHT = 20
    ZOOM_DATE_RANGE_DISPLAY_BAR_HEIGHT = 20
    TIMELINE_SCROLL_CONTROL_HEIGHT = 50
    DETAIL_SECTION_HEIGHT = 120

    COLOR_LEGEND_BAR_RIGHT_OFFSET = 20
    COLOR_LEGEND_BAR_TOP_OFFSET = 0
    COLOR_LEGEND_BAR_LEFT = 20

    ZOOM_SCROLL_BAR_WIDTH = 20
    ZOOM_SCROLL_BAR_RIGHT_OFFSET = 0

    ZOOM_DATE_RANGE_DISPLAY_BAR_LEFT = CATEGORY_BAR_WIDTH
    ZOOM_DATE_RANGE_DISPLAY_BAR_TOP_OFFSET = COLOR_LEGEND_BAR_TOP_OFFSET + COLOR_LEGEND_BAR_HEIGHT
    ZOOM_DATE_RANGE_DISPLAY_BAR_RIGHT_OFFSET = ZOOM_SCROLL_BAR_RIGHT_OFFSET + ZOOM_SCROLL_BAR_WIDTH

    CATEGORY_BAR_LEFT = 0
    CATEGORY_BAR_TOP_OFFSET = ZOOM_DATE_RANGE_DISPLAY_BAR_TOP_OFFSET + ZOOM_DATE_RANGE_DISPLAY_BAR_HEIGHT

    DETAIL_SECTION_LEFT = 0
    DETAIL_SECTION_RIGHT_OFFSET = 0
    DETAIL_SECTION_BOTTOM = 0
    TIMELINE_SCROLL_CONTROL_BOTTOM = DETAIL_SECTION_BOTTOM + DETAIL_SECTION_HEIGHT
    CATEGORY_BAR_BOTTOM = TIMELINE_SCROLL_CONTROL_BOTTOM + TIMELINE_SCROLL_CONTROL_HEIGHT

    ZOOM_SCROLL_BAR_BOTTOM = CATEGORY_BAR_BOTTOM
    ZOOM_SCROLL_BAR_TOP_OFFSET = CATEGORY_BAR_TOP_OFFSET

    GRAPH_SECTION_BOTTOM = CATEGORY_BAR_BOTTOM

