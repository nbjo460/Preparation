import flet as ft

from ui.app_manager import AppManager


def main(page: ft.Page) -> None:
    AppManager(page)


ft.app(target=main)
