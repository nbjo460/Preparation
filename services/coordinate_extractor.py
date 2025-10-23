import time

from pymavlink import mavutil
from pymavlink.CSVReader import CSVMessage

from utils.logger import AppLogger


class CoordinateExtractor:
    def __init__(self) -> None:
        self.logger = AppLogger(self.__class__.__name__)

    def from_bin(self, path: str) -> list[tuple[float, float]]:
        """
        :param path: Path of a bin file.
        :return: List of all coordinates who founds.
        """
        mav: CSVMessage = mavutil.mavlink_connection(path)
        self.logger.info("Reading the file...")

        coordinates: list[tuple[float, float]] = []

        remain_info_in_file = True
        while remain_info_in_file:
            gps_massage: mav = mav.recv_match(type=["GPS"], blocking = False)
            if gps_massage is None:
                break

            if gps_massage.I == 1:
                lat: float = gps_massage.Lat
                lon: float = gps_massage.Lng
                coordinates.append((lat, lon))
        self.logger.debug(f"Found {len(coordinates)} Coordinates.")
        return coordinates
