import flet as ft
import flet_map as my_map

from utils.logger import AppLogger

# TODO create file config.json
# TODO create file config.py, this file read config.json
# TODO create gitignore

class MapView:
    def __init__(self) -> None:
        self.logger = AppLogger(self.__class__.__name__)

        self.polyline_ref: ft.Ref = ft.Ref[ # TODO need clear name
            my_map.PolylineLayer]()
        self.config = self.map_config() # TODO change the name of the var
        self.map: ft.Container = ft.Container(
            self.config,
            expand=True,
            padding=10,
        )

    def map_config(self) -> my_map.Map:#TODO change the name of the function
        return my_map.Map(
            expand=True,
            initial_center=my_map.MapLatitudeLongitude(31.5, 34.9), #TODO add to config file
            initial_zoom=9, # TODO add to config.json
            layers=[
                my_map.TileLayer(url_template="https://tile.openstreetmap.org/{z}/{x}/{y}.png"), # TODO add to config.json
                self._generate_polyline()
            ],
        )

    def append_coordinates(self, coordinates : list[tuple[float, float]]) -> None:
        self._clear_map()
        for counter, coordinate in enumerate(coordinates):
            self._append_coordinate(coordinate)
            if counter % 10 == 0:
                self.logger.debug(f"Coordinate: {coordinate}, is num: {counter}")

        self.config.center_on(point= my_map.MapLatitudeLongitude(*coordinates[0]), zoom=13) #TODO add the number to config file

    def _append_coordinate(self, coordinate: tuple[float, float]) -> None:
        new_point: my_map.MapLatitudeLongitude = my_map.MapLatitudeLongitude(*coordinate)
        self.polyline_ref.current.polylines[0].coordinates.append(new_point)

    def _clear_map(self) -> None:
        self.polyline_ref.current.polylines[0].coordinates.clear()

    def _generate_polyline(self) -> my_map.PolylineLayer:
        route_points: list[my_map.MapLatitudeLongitude] = [] # TODO check if needed
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
