import flet as ft
import flet_map

from utils.logger import AppLogger

# TODO create file config.json
# TODO create file config.py, this file read config.json

class MapView:
    def __init__(self) -> None:
        self.logger = AppLogger(self.__class__.__name__)

        self.polyline_route_ref: ft.Ref = ft.Ref[
            flet_map.PolylineLayer]()
        self.map_setting = self.init_map()
        self.map: ft.Container = ft.Container(
            self.map_setting,
            expand=True,
            padding=10,
        )

    def init_map(self) -> flet_map.Map:
        return flet_map.Map(
            expand=True,
            initial_center=flet_map.MapLatitudeLongitude(31.5, 34.9), #TODO add to config file
            initial_zoom=9, # TODO add to config.json
            layers=[
                flet_map.TileLayer(url_template="https://tile.openstreetmap.org/{z}/{x}/{y}.png"), # TODO add to config.json
                self._generate_polyline_layer()
            ],
        )

    def append_coordinates(self, coordinates : list[tuple[float, float]]) -> None:
        self._clear_map()
        for counter, coordinate in enumerate(coordinates):
            self._append_coordinate(coordinate)
            if counter % 10 == 0:
                self.logger.debug(f"Coordinate: {coordinate}, is num: {counter}")

        self.map_setting.center_on(point= flet_map.MapLatitudeLongitude(*coordinates[0]), zoom=13) #TODO add the number to config file

    def _append_coordinate(self, coordinate: tuple[float, float]) -> None:
        new_point: flet_map.MapLatitudeLongitude = flet_map.MapLatitudeLongitude(*coordinate)
        self.polyline_route_ref.current.polylines[0].coordinates.append(new_point)

    def _clear_map(self) -> None:
        self.polyline_route_ref.current.polylines[0].coordinates.clear()

    def _generate_polyline_layer(self, first_route_points : list[flet_map.MapLatitudeLongitude] = None) -> flet_map.PolylineLayer:
        route_points: list[flet_map.MapLatitudeLongitude] = first_route_points if first_route_points is not None else []
        polyline : flet_map.PolylineLayer = flet_map.PolylineLayer(
            ref=self.polyline_route_ref,
            polylines=[
                flet_map.PolylineMarker(
                    coordinates=route_points,
                    color=ft.Colors.RED,
                    border_color=ft.Colors.BLACK,
                    border_stroke_width=1,
                )
            ],
        )
        return polyline
