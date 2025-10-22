from pymavlink import mavutil
from pymavlink.CSVReader import CSVMessage

class CoordinateExtractor:
    @staticmethod
    def from_bin(path: str) -> list[tuple[float, float]]:
        """

        :param path: Path of a bin file.
        :return: List of all coordinates who founds.
        """

        mav: CSVMessage = mavutil.mavlink_connection(path)

        coordinates: list[tuple[float, float]] = []

        while True:
            msg: mav = mav.recv_match(type=["GPS"])
            if msg is None:
                break

            if msg.I == 1:
                lat: float = msg.Lat
                lon: float = msg.Lng
                coordinates.append((lat, lon))

        return coordinates
