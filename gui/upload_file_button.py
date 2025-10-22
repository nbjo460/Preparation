from typing import Callable

import flet as ft


class UploadFileButton:
    def __init__(self, on_file_selected_callback: Callable[[str], None], file_picker: ft.FilePicker):
        self.button = ft.Row(
            controls=[
                ft.Container(
                    ft.ElevatedButton(
                        "Choose bin file",
                        on_click=lambda _: file_picker.pick_files(allowed_extensions=["bin"]),
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
        self.on_file_selected_callback = on_file_selected_callback
        self.file_picker = file_picker
        self.file_picker.on_result = self.on_file_picked

    def on_file_picked(self, e: ft.FilePickerResultEvent) -> None:
        if e.files:
            path = e.files[0].path
            self.on_file_selected_callback(path)
