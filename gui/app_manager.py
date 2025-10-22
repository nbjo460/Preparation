from typing import Tuple

import flet as ft
import flet_map

from gui.main_window import MainWindow
from gui.map_view import MapView
from gui.upload_file_button import UploadFileButton
from utils.coordinate_extractor import CoordinateExtractor


class AppManager:
    def __init__(self, page: ft.Page):
        self.page = page

        self.file_picker = self.append_file_picker()

        self.upload = UploadFileButton(
            on_file_selected_callback=self.add_coords_from_file, file_picker=self.file_picker
        )
        self.map_view : MapView= MapView()
        self.window = MainWindow(self.upload, self.map_view)

        self.page.add(self.window.layout)

    def append_file_picker(self) -> ft.FilePicker:
        file_picker: ft.FilePicker = ft.FilePicker()
        self.page.overlay.append(file_picker)
        return file_picker

    def add_coords_from_file(self, path: str) -> None:
        print(f"Chosen file: {path}")

        self.map_view.clear_map()

        coordinates: list[Tuple[float, float]] = CoordinateExtractor.from_bin(path)
        for coordinate in coordinates:
            self.map_view.append_coordinates(coordinate)

        self.map_view.mapp.center_on(point=flet_map.MapLatitudeLongitude(*coordinates[0]), zoom=13)


        self.page.update()
        print(f"printed {len(coordinates)}")




def main(page: ft.Page) -> None:
    AppManager(page)


ft.app(target=main)
