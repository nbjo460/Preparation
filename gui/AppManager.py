from gui.MainWindow import MainWindow
from gui.MapView import MapView
from gui.UploadFileButton import UploadFileButton

import flet as ft


class AppManager:
    def __init__(self, page: ft.Page):
        self.upload = UploadFileButton(upload_file = self.upload_file)
        self.map_view = MapView()
        self.window = MainWindow(self.upload, self.map_view)

        self.page = page

        self.page.add(self.window.layout)





    def upload_file(self, e):
        print("uploaded")

        self.map_view.append_coordinates((3.3, 4.4))
        self.page.update()


def main(page : ft.Page):
    AppManager(page)

ft.app(target=main)