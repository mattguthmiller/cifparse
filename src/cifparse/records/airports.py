from cifparse.functions.field import extract_field
from cifparse.functions.records import partition

from .appl_table import a_table
from .record_base import RecordBase
from .table_base import process_table

from .airport.primary import Primary
from .airport.continuation import Continuation
from .airport.planning import Planning
from .airport.planning_continuation import PlanningContinuation
from .airport.widths import w_pri, w_con, w_pla

from sqlite3 import Cursor


class Airport(RecordBase):
    primary: Primary
    continuation: list[Continuation]
    planning: list[Planning]
    planning_continuation: list[PlanningContinuation]

    def __init__(self, airport_partition: list[str]):
        self.primary = None
        self.continuation = []
        self.planning = []
        self.planning_continuation = []

        was_last_planning = False
        for line in airport_partition:
            cont_rec_no = extract_field(line, w_pri.cont_rec_no)
            if cont_rec_no in [0, 1]:
                primary = Primary()
                self.primary = primary.from_line(line)
                was_last_planning = False
                continue
            else:
                application = extract_field(line, w_con.application)
                if application == a_table.standard:
                    continuation = Continuation()
                    self.continuation.append(continuation.from_line(line))
                    was_last_planning = False
                    continue
                application = extract_field(line, w_pla.application)
                if application == a_table.planning:
                    planning = Planning()
                    self.planning.append(planning.from_line(line))
                    was_last_planning = True
                    continue
                if was_last_planning:
                    planning_continuation = PlanningContinuation()
                    self.planning_continuation.append(
                        planning_continuation.from_line(line)
                    )
                    was_last_planning = False
                    continue

    def __repr__(self):
        pri = ": primary: {{...}}" if self.primary else ""
        con = ", continuation: [...]" if self.continuation else ""
        pla = ", planning: [...]" if self.planning else ""
        plc = ", planning_continuation: [...]" if self.planning_continuation else ""
        return f"\n{self.__class__.__name__}{pri}{con}{pla}{plc}"

    def to_dict(self) -> dict:
        return {
            "primary": self.primary.to_dict(),
            "continuation": [item.to_dict() for item in self.continuation],
            "planning": [item.to_dict() for item in self.planning],
            "planning_continuation": [
                item.to_dict() for item in self.planning_continuation
            ],
        }


class Airports:
    records: list[Airport]

    def __init__(self, airport_lines: list[str]):
        self.records = []

        print("    Parsing Airports")
        airport_partitioned = partition(airport_lines, 0, 21)
        for airport_partition in airport_partitioned:
            airport = Airport(airport_partition)
            self.records.append(airport)

    def to_dict(self) -> list[dict]:
        result = []
        for record in self.records:
            result.append(record.to_dict())
        return result

    def to_db(self, db_cursor: Cursor, process3LDs: bool = False) -> None:
        primary = []
        continuation = []
        planning = []
        planning_continuation = []

        print("    Processing Airports")
        for airport in self.records:
            # Airports with 3LDs
            if process3LDs:
                if airport.primary.airport_id and (len(airport.primary.airport_id) == 3 or not airport.primary.airport_id.isalpha()):
                    # Don't process if in the US since we'll already have those from NASR
                    if (airport.primary.area and airport.primary.area == "USA") or (airport.primary.airport_region and (airport.primary.airport_region[0] == "K" or airport.primary.airport_region[0] == "P" or airport.primary.airport_region == "TJ")):
                        continue
                    else:
                        airport.primary.airport_id = airport.primary.airport_region + airport.primary.airport_id

            if airport.has_primary():
                primary.append(airport.primary)
            if airport.has_continuation():
                continuation.extend(airport.continuation)
            if airport.has_planning():
                planning.extend(airport.planning)
            if airport.has_planning_continuation():
                planning_continuation.extend(airport.planning_continuation)

        if primary:
            process_table(db_cursor, primary)
        if continuation:
            process_table(db_cursor, continuation)
        if planning:
            process_table(db_cursor, planning)
        if planning_continuation:
            process_table(db_cursor, planning_continuation)
        return
