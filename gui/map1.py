import random

import flet as ft
import flet_map as map

def main(page: ft.Page):
    # הפניה למסלול
    polyline_ref = ft.Ref[map.PolylineLayer]()
    # הפניה לסמנים
    marker_layer_ref = ft.Ref[map.MarkerLayer]()

    # נקודות התחלה
    route_points = [
        map.MapLatitudeLongitude(32.0809, 34.7806),  # תל אביב
        map.MapLatitudeLongitude(31.7683, 35.2137),  # ירושלים
    ]

    # הוספת נקודה חדשה למסלול
    def add_point(e):
        def generate_random_point() -> tuple[float, float]:
            lat = random.Random().randint(-900000, 900000) / 10000
            lon = random.Random().randint(-1800000, 1800000) / 10000
            print(f"lat: {lat}, lon: {lon}")
            return lat, lon

        new_point = map.MapLatitudeLongitude(*generate_random_point())

        polyline_ref.current.polylines[0].coordinates.append(new_point)

        # מוסיפים גם סמן חדש
        marker_layer_ref.current.markers.append(
            map.Marker(
                coordinates=new_point,
                content=ft.Icon(ft.Icons.CIRCLE, size = 7, color=ft.Colors.BLACK),
            )
        )

        # מעדכנים את המסך
        page.update()

    page.add(
        ft.Column(
            controls=[
            ft.Row(
            controls=[ft.Container(ft.ElevatedButton("➕ הוסף נקודה למסלול", on_click=add_point), width=300, height=60,  expand=False, padding=10)]
            , alignment=ft.MainAxisAlignment.CENTER),
            ft.Container(map.Map(
                expand=True,
                initial_center=map.MapLatitudeLongitude(31.5, 34.9),
                initial_zoom=5,
                layers=[
                    map.TileLayer(url_template="https://tile.openstreetmap.org/{z}/{x}/{y}.png"),

                    # שכבת קו (Polyline)
                    map.PolylineLayer(
                        ref=polyline_ref,
                        polylines=[
                            map.PolylineMarker(
                                coordinates=route_points,
                                color=ft.Colors.RED,
                                border_color=ft.Colors.BLACK,
                                border_stroke_width=1,
                            )
                        ]
                    ),

                    # שכבת סמנים
                    map.MarkerLayer(
                        ref=marker_layer_ref,
                        markers=[
                            map.Marker(
                                coordinates=p,
                                content=ft.Icon(ft.Icons.CIRCLE, size=6, color=ft.Colors.RED),
                            )
                            for p in route_points
                        ],
                    ),
                ],
            ), expand=True, padding=10
            )
            ],
            spacing=10,
            expand=True

        )
    )

ft.app(target=main)
