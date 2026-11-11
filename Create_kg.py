"""
Milestone 2 - Formula 1 Knowledge Graph builder.

Ownership (see TODO blocks):
  Person A: load_entities()            Driver, Constructor, Circuit, Season, Status, Race   (DONE below)
  Person B: load_results()             Result nodes + 4 relationships
  Person C: load_qualifying_pitstops() Qualifying, PitStop + relationships, verify_counts()
Everyone: use ONLY the helpers in the "SHARED HELPERS" section so types stay consistent.
"""
import csv
import os
from neo4j import GraphDatabase

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BATCH_SIZE = 1000


# ----------------------------------------------------------------------
# SHARED HELPERS (do not change without telling the team)
# ----------------------------------------------------------------------
def read_config(filename="config.txt"):
    """Read KEY=VALUE lines by key (never by line number). DATABASE is optional.
    utf-8-sig so a Windows BOM at the start of the file cannot corrupt the first key."""
    cfg = {}
    with open(os.path.join(BASE_DIR, filename), encoding="utf-8-sig") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            cfg[key.strip().upper()] = value.strip()
    return cfg


def clean(v):
    """Return None for missing values (\\N or empty), else the stripped string."""
    if v is None:
        return None
    v = v.strip()
    return None if v in ("", "\\N") else v


def to_int(v):
    v = clean(v)
    return None if v is None else int(float(v))


def to_float(v):
    v = clean(v)
    return None if v is None else float(v)


def to_str(v):
    return clean(v)


def to_date(v):
    """Return an ISO string 'YYYY-MM-DD' or None; Cypher converts it with date()."""
    return clean(v)


def find_csv(filename):
    """Brief: CSVs live next to the script. Fallback for local dev only: formula1_dataset/."""
    for folder in (BASE_DIR, os.path.join(BASE_DIR, "formula1_dataset")):
        path = os.path.join(folder, filename)
        if os.path.exists(path):
            return path
    raise FileNotFoundError(f"{filename} not found next to Create_kg.py or in formula1_dataset/")


def read_csv(filename):
    """Yield raw dict rows from a CSV (see find_csv)."""
    with open(find_csv(filename), encoding="utf-8-sig", newline="") as f:
        yield from csv.DictReader(f)


def check_cols(filename, needed):
    """Fail early with a clear message if a CSV header differs from what we expect."""
    with open(find_csv(filename), encoding="utf-8-sig", newline="") as f:
        header = next(csv.reader(f))
    missing = [c for c in needed if c not in header]
    if missing:
        raise ValueError(f"{filename}: missing columns {missing}. Header is {header}")


def run_batches(session, query, rows, batch_size=BATCH_SIZE):
    """Run `query` with $rows = list of dicts, in batches. Query must use UNWIND $rows AS row."""
    batch = []
    for row in rows:
        batch.append(row)
        if len(batch) >= batch_size:
            session.run(query, rows=batch).consume()
            batch = []
    if batch:
        session.run(query, rows=batch).consume()


# Rules that make the graph schema-compliant:
#  * MERGE on the unique id only, then SET the rest  -> re-runs create no duplicates.
#  * Properties with a None value are NOT stored by `SET n += row` -> missing values omitted.
#  * Dates: use  CASE WHEN row.dob IS NULL THEN null ELSE date(row.dob) END
CONSTRAINTS = [
    ("Driver", "driverId"),
    ("Constructor", "constructorId"),
    ("Circuit", "circuitId"),
    ("Season", "year"),
    ("Status", "statusId"),
    ("Race", "raceId"),
    ("Result", "resultId"),
    ("Qualifying", "qualifyId"),
    ("PitStop", "eventId"),
]


def create_constraints(session):
    for label, prop in CONSTRAINTS:
        session.run(
            f"CREATE CONSTRAINT {label.lower()}_{prop.lower()}_unique IF NOT EXISTS "
            f"FOR (n:{label}) REQUIRE n.{prop} IS UNIQUE"
        ).consume()


# ----------------------------------------------------------------------
# PERSON A: ENTITY NODES
#   Season:      year
#   Status:      statusId, status
#   Constructor: constructorId, constructorRef, constructor_name, constructor_nationality
#   Circuit:     circuitId, circuitRef, circuit_name, circuit_loc, circuit_country, lat, lng, alt
#   Driver:      driverId, driverRef, number, code, forename, surname, dob, nationality
#   Race:        raceId, year, round, name, date, time  (+ AT_CIRCUIT, IN_SEASON)
# ----------------------------------------------------------------------
def load_seasons(session):
    check_cols("seasons.csv", ["year"])
    rows = [{"year": to_int(r["year"])} for r in read_csv("seasons.csv")]
    run_batches(session, """
        UNWIND $rows AS row
        MERGE (:Season {year: row.year})""", rows)
    print(f"Seasons loaded: {len(rows)}")


def load_statuses(session):
    check_cols("status.csv", ["statusId", "status"])
    rows = [{"statusId": to_int(r["statusId"]),
             "props": {"status": to_str(r["status"])}}
            for r in read_csv("status.csv")]
    run_batches(session, """
        UNWIND $rows AS row
        MERGE (n:Status {statusId: row.statusId})
        SET n += row.props""", rows)
    print(f"Statuses loaded: {len(rows)}")


def load_constructors(session):
    # CSV 'name' -> constructor_name, CSV 'nationality' -> constructor_nationality
    check_cols("constructors.csv", ["constructorId", "constructorRef", "name", "nationality"])
    rows = [{"constructorId": to_int(r["constructorId"]),
             "props": {"constructorRef": to_str(r["constructorRef"]),
                       "constructor_name": to_str(r["name"]),
                       "constructor_nationality": to_str(r["nationality"])}}
            for r in read_csv("constructors.csv")]
    run_batches(session, """
        UNWIND $rows AS row
        MERGE (n:Constructor {constructorId: row.constructorId})
        SET n += row.props""", rows)
    print(f"Constructors loaded: {len(rows)}")


def load_circuits(session):
    # CSV name/location/country -> circuit_name/circuit_loc/circuit_country
    check_cols("circuits.csv", ["circuitId", "circuitRef", "name", "location", "country",
                                "lat", "lng", "alt"])
    rows = [{"circuitId": to_int(r["circuitId"]),
             "props": {"circuitRef": to_str(r["circuitRef"]),
                       "circuit_name": to_str(r["name"]),
                       "circuit_loc": to_str(r["location"]),
                       "circuit_country": to_str(r["country"]),
                       "lat": to_float(r["lat"]),
                       "lng": to_float(r["lng"]),
                       "alt": to_int(r["alt"])}}
            for r in read_csv("circuits.csv")]
    run_batches(session, """
        UNWIND $rows AS row
        MERGE (n:Circuit {circuitId: row.circuitId})
        SET n += row.props""", rows)
    print(f"Circuits loaded: {len(rows)}")


def load_drivers(session):
    # dob is converted to a Neo4j date; number / code / dob may be missing (omitted)
    check_cols("drivers.csv", ["driverId", "driverRef", "number", "code", "forename",
                               "surname", "dob", "nationality"])
    rows = [{"driverId": to_int(r["driverId"]),
             "dob": to_date(r["dob"]),
             "props": {"driverRef": to_str(r["driverRef"]),
                       "number": to_int(r["number"]),
                       "code": to_str(r["code"]),
                       "forename": to_str(r["forename"]),
                       "surname": to_str(r["surname"]),
                       "nationality": to_str(r["nationality"])}}
            for r in read_csv("drivers.csv")]
    run_batches(session, """
        UNWIND $rows AS row
        MERGE (n:Driver {driverId: row.driverId})
        SET n += row.props
        SET n.dob = CASE WHEN row.dob IS NULL THEN null ELSE date(row.dob) END""", rows)
    print(f"Drivers loaded: {len(rows)}")


def load_races(session):
    # Needs Circuit and Season nodes to exist already (they are loaded before this).
    # date -> Neo4j date, time stays a string, time may be missing (omitted)
    check_cols("races.csv", ["raceId", "year", "round", "circuitId", "name", "date", "time"])
    rows = [{"raceId": to_int(r["raceId"]),
             "year": to_int(r["year"]),
             "circuitId": to_int(r["circuitId"]),
             "date": to_date(r["date"]),
             "props": {"year": to_int(r["year"]),
                       "round": to_int(r["round"]),
                       "name": to_str(r["name"]),
                       "time": to_str(r["time"])}}
            for r in read_csv("races.csv")]
    run_batches(session, """
        UNWIND $rows AS row
        MERGE (r:Race {raceId: row.raceId})
        SET r += row.props
        SET r.date = CASE WHEN row.date IS NULL THEN null ELSE date(row.date) END
        WITH r, row
        MATCH (c:Circuit {circuitId: row.circuitId})
        MATCH (s:Season {year: row.year})
        MERGE (r)-[:AT_CIRCUIT]->(c)
        MERGE (r)-[:IN_SEASON]->(s)""", rows)
    print(f"Races loaded: {len(rows)}")


def load_entities(session):
    """All parent nodes. Race goes last because it links to Circuit and Season."""
    load_seasons(session)
    load_statuses(session)
    load_constructors(session)
    load_circuits(session)
    load_drivers(session)
    load_races(session)


# ----------------------------------------------------------------------
# PERSON B: RESULT FACTS
# ----------------------------------------------------------------------
def load_results(session):
    # TODO(B): Result nodes from results.csv, MERGE on resultId only (shared drives = same
    #          driver+race twice, so never merge on driver/race).
    # TODO(B): rels FOR_RACE, BY_DRIVER, FOR_CONSTRUCTOR, HAS_STATUS (direction: Result -> X).
    # types: points/fastestLapSpeed float; grid, position, positionOrder, laps, milliseconds,
    #        fastestLap, rank, number int; positionText, time, fastestLapTime strings.
    pass


# ----------------------------------------------------------------------
# PERSON C: QUALIFYING + PITSTOP, VERIFICATION
# ----------------------------------------------------------------------
def load_qualifying_pitstops(session):
    # TODO(C): Qualifying from qualifying.csv (omit empty q2/q3), rels FOR_RACE, BY_DRIVER,
    #          FOR_CONSTRUCTOR.
    # TODO(C): PitStop from pit_stops.csv, eventId = f"{raceId}:{driverId}:{stop}" (string),
    #          rels FOR_RACE, BY_DRIVER only.
    pass


def verify_counts(session):
    nodes = session.run("MATCH (n) RETURN count(n) AS c").single()["c"]
    rels = session.run("MATCH ()-[r]->() RETURN count(r) AS c").single()["c"]
    print(f"Nodes: {nodes} (expected 51113)")
    print(f"Relationships: {rels} (expected 163510)")


# ----------------------------------------------------------------------
def main():
    cfg = read_config()
    driver = GraphDatabase.driver(cfg["URI"], auth=(cfg["USERNAME"], cfg["PASSWORD"]))
    session_kwargs = {"database": cfg["DATABASE"]} if cfg.get("DATABASE") else {}
    with driver, driver.session(**session_kwargs) as session:
        create_constraints(session)
        load_entities(session)               # parents first
        load_results(session)                # then facts
        load_qualifying_pitstops(session)
        verify_counts(session)


if __name__ == "__main__":
    main()