from typing import Tuple

import flet as ft
from pymavlink import mavutil
from pymavlink.CSVReader import CSVMessage

from gui.main_window import MainWindow
from gui.map_view import MapView
from gui.upload_file_button import UploadFileButton


class AppManager:
    def __init__(self, page: ft.Page):
        self.page = page

        self.file_picker = self.append_file_picker()

        self.upload = UploadFileButton(
            on_file_selected_callback=self.add_coords_from_file, file_picker=self.file_picker
        )
        self.map_view = MapView()
        self.window = MainWindow(self.upload, self.map_view)

        self.page.add(self.window.layout)

    def append_file_picker(self) -> ft.FilePicker:
        file_picker: ft.FilePicker = ft.FilePicker()
        self.page.overlay.append(file_picker)
        return file_picker

    def add_coords_from_file(self, path: str) -> None:
        print(f"Chosen file: {path}")

        self.map_view.clear_map()

        coordinates: list[Tuple[float, float]] = self.read_coordinates(path)
        for coordinate in coordinates:
            self.map_view.append_coordinates(coordinate)

        self.page.update()
        print("printed")

    @staticmethod
    def read_coordinates(path: str) -> list[Tuple[float, float]]:
        mav: CSVMessage = mavutil.mavlink_connection(path)

        # רשימה לאחסון נקודות
        coordinates: list[Tuple[float, float]] = []

        while True:
            msg: mav = mav.recv_match(type=["GPS"])
            if msg is None:
                break

            if msg.I == 1:
                lat: float = msg.Lat
                lon: float = msg.Lng
                coordinates.append((lat, lon))

        return coordinates


def main(page: ft.Page) -> None:
    AppManager(page)


ft.app(target=main)
