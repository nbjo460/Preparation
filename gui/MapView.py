from typing import Tuple

import flet as ft
import flet_map as map
class MapView:
    def __init__(self):
        self.polyline_ref = ft.Ref[map.PolylineLayer]()
        self.marker_layer_ref = ft.Ref[map.MarkerLayer]()

        route_points = [
            map.MapLatitudeLongitude(32.0809, 34.7806),  # תל אביב
            map.MapLatitudeLongitude(31.7683, 35.2137),  # ירושלים
        ]


        self.map = ft.Container(map.Map(
                expand=True,
                initial_center=map.MapLatitudeLongitude(31.5, 34.9),
                initial_zoom=5,
                layers=[
                    map.TileLayer(url_template="https://tile.openstreetmap.org/{z}/{x}/{y}.png"),

                    # שכבת קו (Polyline)
                    map.PolylineLayer(
                        ref=self.polyline_ref,
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
                        ref=self.marker_layer_ref,
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

    def append_coordinates(self, coordinates : Tuple[float,float]):
            new_point = map.MapLatitudeLongitude(*coordinates)

            self.polyline_ref.current.polylines[0].coordinates.append(new_point)

            # מוסיפים גם סמן חדש
            self.marker_layer_ref.current.markers.append(
                map.Marker(
                    coordinates= map.MapLatitudeLongitude(*coordinates),
                    content=ft.Icon(ft.Icons.CIRCLE, size=7, color=ft.Colors.BLACK),
                )
            )

            # מעדכנים את המסך
