
import flet as ft

from ui.main_window import MainWindow
from ui.map_view import MapView
from utils.logger import AppLogger


class AppManager:
    def __init__(self, page: ft.Page) -> None:
        self.logger = AppLogger(self.__class__.__name__)

        self.page = page

        self.map_view : MapView = MapView()
        self.window = MainWindow(self.page, self.map_view)

        self.page.add(self.window.layout)




