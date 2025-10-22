from typing import Tuple

import flet as ft
import flet_map as my_map


class MapView:
    def __init__(self) -> None:
        self.polyline_ref: ft.Ref = ft.Ref[my_map.PolylineLayer]()

        route_points: list[my_map.MapLatitudeLongitude] = []

        self.map: ft.Container = ft.Container(
            my_map.Map(
                expand=True,
                # TODO: Change a location to a first DOT.
                initial_center=my_map.MapLatitudeLongitude(31.5, 34.9),
                initial_zoom=9,
                layers=[
                    my_map.TileLayer(url_template="https://tile.openstreetmap.org/{z}/{x}/{y}.png"),
                    my_map.PolylineLayer(
                        ref=self.polyline_ref,
                        polylines=[
                            my_map.PolylineMarker(
                                coordinates=route_points,
                                color=ft.Colors.RED,
                                border_color=ft.Colors.BLACK,
                                border_stroke_width=1,
                            )
                        ],
                    ),
                ],
            ),
            expand=True,
            padding=10,
        )

    def append_coordinates(self, coordinates: Tuple[float, float]) -> None:
        new_point: map.MapLatitudeLongitude = map.MapLatitudeLongitude(*coordinates)
        print(coordinates)
        self.polyline_ref.current.polylines[0].coordinates.append(new_point)

    def clear_map(self) -> None:
        self.polyline_ref.current.polylines[0].coordinates.clear()
