import flet as ft
class UploadFileButton:
    def __init__(self, upload_file):
        self.button = ft.Row(
            controls=[
                ft.Container(
                    ft.ElevatedButton("➕ בחר קובץ",
                                      on_click=upload_file, icon=ft.Icons.UPLOAD_FILE),
                    width=300, height=60,  expand=False, padding=10)
            ],
            alignment=ft.MainAxisAlignment.CENTER
        )