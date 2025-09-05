from cifparse.functions.field import extract_field
from cifparse.records.table_base import TableBase

from .widths import w_bas


class Base(TableBase):
    st: str
    sec_code: str
    sub_code: str
    start_lat: float
    start_lon: float
    record_number: int
    cycle_data: str

    def __init__(self, table_name: str):
        super().__init__(table_name)
        self.st = None
        self.sec_code = None
        self.sub_code = None
        self.start_lat = None
        self.start_lon = None
        self.record_number = None
        self.cycle_data = None

    def __repr__(self):
        return f"{self.__class__.__name__}"

    def from_line(self, line: str) -> "Base":
        self.st = extract_field(line, w_bas.st)
        self.sec_code = extract_field(line, w_bas.sec_code)
        self.sub_code = extract_field(line, w_bas.sub_code)
        self.start_lat = extract_field(line, w_bas.start_lat)
        self.start_lon = extract_field(line, w_bas.start_lon)
        self.record_number = extract_field(line, w_bas.record_number)
        self.cycle_data = extract_field(line, w_bas.cycle_data)
        return self

    def ordered_leading(self) -> list:
        return [
            "st",
            "sec_code",
            "sub_code",
            "start_lat",
            "start_lon"
        ]

    def ordered_trailing(self) -> list:
        return [
            "record_number",
            "cycle_data",
        ]

    def ordered_fields(self) -> dict:
        result = []
        result.extend(self.ordered_leading())
        result.extend(self.ordered_trailing())
        return result

    def get_leading_dict(self) -> dict:
        return {
            "st": self.st,
            "sec_code": self.sec_code,
            "sub_code": self.sub_code,
            "start_lat": self.start_lat,
            "start_lon": self.start_lon
        }

    def get_trailing_dict(self) -> dict:
        return {
            "record_number": self.record_number,
            "cycle_data": self.cycle_data,
        }

    def to_dict(self) -> dict:
        leading = self.get_leading_dict()
        trailing = self.get_trailing_dict()
        return {**leading, **trailing}
