from typing import Tuple

import flet as ft
import flet_map as my_map


class MapView:
    def __init__(self) -> None:
        self.polyline_ref: ft.Ref = ft.Ref[
            my_map.PolylineLayer]()
        self.mapp = self.map_config()
        self.map: ft.Container = ft.Container(
            self.mapp,
            expand=True,
            padding=10,
        )

    def map_config(self) -> my_map.Map:
        return my_map.Map(
            expand=True,
            # TODO: Change a location to a first DOT.
            initial_center=my_map.MapLatitudeLongitude(31.5, 34.9),
            initial_zoom=9,
            layers=[
                my_map.TileLayer(url_template="https://tile.openstreetmap.org/{z}/{x}/{y}.png"),
                self.generate_polyline()
            ],
        )

    def append_coordinates(self, coordinates: Tuple[float, float]) -> None:
        new_point: my_map.MapLatitudeLongitude = my_map.MapLatitudeLongitude(*coordinates)
        print(coordinates)
        self.polyline_ref.current.polylines[0].coordinates.append(new_point)

    def clear_map(self) -> None:
        self.polyline_ref.current.polylines[0].coordinates.clear()

    def generate_polyline(self) -> my_map.PolylineLayer:
        route_points: list[my_map.MapLatitudeLongitude] = []
        polyline : my_map.PolylineLayer = my_map.PolylineLayer(
            ref=self.polyline_ref,
            polylines=[
                my_map.PolylineMarker(
                    coordinates=route_points,
                    color=ft.Colors.RED,
                    border_color=ft.Colors.BLACK,
                    border_stroke_width=1,
                )
            ],
        )
        return polyline
