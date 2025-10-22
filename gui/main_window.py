import flet as ft

from gui.map_view import MapView
from gui.upload_file_button import UploadFileButton


class MainWindow:
    def __init__(self, button: UploadFileButton, map_view: MapView):
        self.layout = ft.Column(controls=[button.button, map_view.map], expand=True)
