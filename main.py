import flet as ft

from gui.app_manager import AppManager


def main(page: ft.Page) -> None:
    AppManager(page)


ft.app(target=main)
