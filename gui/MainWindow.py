import flet as ft

from gui.MapView import MapView
from gui.UploadFileButton import UploadFileButton


class MainWindow:
    def __init__(self, button : UploadFileButton, map_view : MapView):
        self.layout = ft.Column(
            controls= [
                button.button,
                map_view.map
            ],
            expand = True
        )

