import random

import flet as ft
import flet_map as my_map


def main(page: ft.Page) -> None:
    # הפניה למסלול
    polyline_ref = ft.Ref[my_map.PolylineLayer]()
    # הפניה לסמנים
    marker_layer_ref = ft.Ref[my_map.MarkerLayer]()

    # נקודות התחלה
    route_points = [
        my_map.MapLatitudeLongitude(32.0809, 34.7806),  # תל אביב
        my_map.MapLatitudeLongitude(31.7683, 35.2137),  # ירושלים
    ]

    # הוספת נקודה חדשה למסלול
    def add_point(_) -> None:  # type: ignore
        def generate_random_point() -> tuple[float, float]:
            lat: float = random.Random().randint(-900000, 900000) / 10000
            lon: float = random.Random().randint(-1800000, 1800000) / 10000
            print(f"lat: {lat}, lon: {lon}")
            return lat, lon

        new_point: my_map.MapLatitudeLongitude = my_map.MapLatitudeLongitude(*generate_random_point())

        polyline_ref.current.polylines[0].coordinates.append(new_point)

        # מוסיפים גם סמן חדש
        marker_layer_ref.current.markers.append(
            my_map.Marker(
                coordinates=new_point,
                content=ft.Icon(ft.Icons.CIRCLE, size=7, color=ft.Colors.BLACK),
            )
        )

        # מעדכנים את המסך
        page.update()

    page.add(
        ft.Column(
            controls=[
                ft.Row(
                    controls=[
                        ft.Container(
                            ft.ElevatedButton("➕ הוסף נקודה למסלול", on_click=add_point),
                            width=300,
                            height=60,
                            expand=False,
                            padding=10,
                        )
                    ],
                    alignment=ft.MainAxisAlignment.CENTER,
                ),
                ft.Container(
                    my_map.Map(
                        expand=True,
                        initial_center=my_map.MapLatitudeLongitude(31.5, 34.9),
                        initial_zoom=5,
                        layers=[
                            my_map.TileLayer(url_template="https://tile.openstreetmap.org/{z}/{x}/{y}.png"),
                            # שכבת קו (Polyline)
                            my_map.PolylineLayer(
                                ref=polyline_ref,
                                polylines=[
                                    my_map.PolylineMarker(
                                        coordinates=route_points,
                                        color=ft.Colors.RED,
                                        border_color=ft.Colors.BLACK,
                                        border_stroke_width=1,
                                    )
                                ],
                            ),
                            # שכבת סמנים
                            my_map.MarkerLayer(
                                ref=marker_layer_ref,
                                markers=[
                                    my_map.Marker(
                                        coordinates=p,
                                        content=ft.Icon(ft.Icons.CIRCLE, size=6, color=ft.Colors.RED),
                                    )
                                    for p in route_points
                                ],
                            ),
                        ],
                    ),
                    expand=True,
                    padding=10,
                ),
            ],
            spacing=10,
            expand=True,
        )
    )


ft.app(target=main)
