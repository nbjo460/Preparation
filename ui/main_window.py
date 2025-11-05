import flet as ft

from ui.map_view import MapView
from business_logic.messages_extractor import CoordinateExtractor
from utils.logger import AppLogger


class MainWindow:
    def __init__(self, page : ft.Page, map_view: MapView) -> None:
        self.logger = AppLogger(self.__class__.__name__)


        self.page = page
        self.map_view = map_view
        self.file_picker = self.__append_file_picker()
        self.layout = ft.Column(controls=[self.upload_file_button(), map_view.map], expand=True)

    def __append_file_picker(self) -> ft.FilePicker:
        file_picker: ft.FilePicker = ft.FilePicker()
        self.page.overlay.append(file_picker)
        return file_picker

    def upload_file_button(self) -> ft.Control:
        upload_button =  ft.Row(
            controls=[
                ft.Container(
                    ft.ElevatedButton(
                        "Choose bin file",
                        on_click=lambda _: self.file_picker.pick_files(allowed_extensions=["bin"]),
                        icon=ft.Icons.UPLOAD_FILE,
                    ),
                    width=300,
                    height=60,
                    expand=False,
                    padding=10,
                )
            ],
            alignment=ft.MainAxisAlignment.CENTER,
        )
        self.file_picker.on_result = self._on_file_picked
        return upload_button

    def _on_file_picked(self, e: ft.FilePickerResultEvent) -> None:
        if e.files:
            path : str = e.files[0].path
            self._add_coordinates_from_file(path)

    def _add_coordinates_from_file(self, path: str) -> None:
        print(f"Chosen file: {path}")
        coordinates: list[tuple[float, float]] = CoordinateExtractor().from_bin(path)
        self.map_view.append_coordinates(coordinates)
        self.page.update()

