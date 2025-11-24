#!/usr/bin/env python3

from __future__ import annotations
import argparse
from pathlib import Path

import duckdb
import pandas as pd
import matplotlib.pyplot as plt


DEFAULT_DB_PATH = "metro.duckdb"

TABLE_POSITIONS = "positions"
TABLE_ROUTES = "routes"
TABLE_UPDATES = "updates"

COL_ROUTE_ID = "route_id"
COL_VEHICLE_ID = "vehicle_id"
COL_LAT = "latitude"
COL_LON = "longitude"
COL_TIMESTAMP = "timestamp"

COL_ROUTE_ID_ROUTES = "route_id"
COL_ROUTE_NAME = "route_long_name"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run analysis and produce plots.")
    parser.add_argument("--db", type=str, default=DEFAULT_DB_PATH)
    parser.add_argument("--out", type=str, default="plots")
    parser.add_argument("--top-n", type=int, default=10)
    return parser.parse_args()


def connect_db(db_path: str) -> duckdb.DuckDBPyConnection:
    db_path = Path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"DuckDB file not found: {db_path.resolve()}")
    return duckdb.connect(str(db_path))


def list_tables(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    df = con.execute("SHOW TABLES").fetchdf()
    print(df.to_string(index=False))
    return df


def load_combined_data(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    tables = set(con.execute("SHOW TABLES").fetchdf()["name"])

    if TABLE_ROUTES in tables:
        query = f"""
            SELECT
                p.{COL_TIMESTAMP} AS ts,
                p.{COL_ROUTE_ID} AS route_id,
                p.{COL_VEHICLE_ID} AS vehicle_id,
                p.{COL_LAT} AS lat,
                p.{COL_LON} AS lon,
                r.{COL_ROUTE_NAME} AS route_name
            FROM {TABLE_POSITIONS} p
            LEFT JOIN {TABLE_ROUTES} r
            ON p.{COL_ROUTE_ID} = r.{COL_ROUTE_ID_ROUTES}
        """
    else:
        print("No 'routes' table found; proceeding without route names.")
        query = f"""
            SELECT
                p.{COL_TIMESTAMP} AS ts,
                p.{COL_ROUTE_ID} AS route_id,
                p.{COL_VEHICLE_ID} AS vehicle_id,
                p.{COL_LAT} AS lat,
                p.{COL_LON} AS lon,
                NULL::VARCHAR AS route_name
            FROM {TABLE_POSITIONS} p
        """

    df = con.execute(query).fetchdf()
    df["ts"] = pd.to_datetime(df["ts"], errors="coerce")
    df = df.dropna(subset=["ts"])
    return df


def plot_top_routes(df: pd.DataFrame, out_dir: Path, top_n: int) -> None:
    route_counts = (
        df.groupby(["route_id", "route_name"])["vehicle_id"]
        .count()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
        .head(top_n)
    )

    if route_counts.empty:
        return

    route_counts["label"] = route_counts.apply(
        lambda row: f"{row['route_id']}\n{row['route_name'] or ''}".strip(), axis=1
    )

    plt.figure(figsize=(10, 6))
    plt.bar(route_counts["label"], route_counts["count"])
    plt.xlabel("Route")
    plt.ylabel("Observations")
    plt.title(f"Top {top_n} Routes by Observations")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()

    out_path = out_dir / "top_routes_bar.png"
    plt.savefig(out_path, dpi=150)
    plt.close()


def plot_activity_over_time(df: pd.DataFrame, out_dir: Path) -> None:
    if df.empty:
        return

    df_sorted = df.sort_values("ts")
    df_sorted = df_sorted.set_index("ts")

    df_resampled = (
        df_sorted
        .groupby("vehicle_id")
        .resample("1min")
        .size()
        .reset_index(name="dummy")
    )

    active_per_min = (
        df_resampled.groupby("ts")["vehicle_id"]
        .nunique()
        .reset_index(name="active_vehicles")
    )

    if active_per_min.empty:
        return

    # Convert to numpy arrays to avoid pandas 2.x multi-d indexing issue
    x = active_per_min["ts"].to_numpy()
    y = active_per_min["active_vehicles"].to_numpy()

    plt.figure(figsize=(12, 5))
    plt.plot(x, y)
    plt.xlabel("Time")
    plt.ylabel("Active Vehicles")
    plt.title("Active Buses Over Time")
    plt.tight_layout()

    out_path = out_dir / "activity_over_time.png"
    plt.savefig(out_path, dpi=150)
    plt.close()

def plot_heatmap(df: pd.DataFrame, out_dir: Path) -> None:
    # Ensure lat/lon columns exist
    if "lat" not in df.columns or "lon" not in df.columns:
        print("Cannot plot heatmap: expected 'lat' and 'lon' columns.")
        return

    # Drop rows with missing coordinates
    df_coords = df.dropna(subset=["lat", "lon"])

    if df_coords.empty:
        print("No lat/lon data for heatmap.")
        return

    plt.figure(figsize=(8, 8))

    # 2D density using hexbin (no extra deps)
    plt.hexbin(
        df_coords["lon"],
        df_coords["lat"],
        gridsize=60,
        bins="log"
    )
    plt.xlabel("Longitude")
    plt.ylabel("Latitude")
    plt.title("Vehicle Position Density Heatmap")
    cb = plt.colorbar()
    cb.set_label("Log(Count)")

    plt.tight_layout()
    out_path = out_dir / "heatmap.png"
    plt.savefig(out_path, dpi=200)
    plt.close()
    print(f"Saved heatmap to {out_path}")

def detect_delay_column(con: duckdb.DuckDBPyConnection) -> str | None:
    """
    Try to find a plausible delay column in the `updates` table by name.
    Returns the column name or None if not found.
    """
    try:
        df_desc = con.execute(f"DESCRIBE {TABLE_UPDATES}").fetchdf()
    except duckdb.Error as e:
        print(f"Could not DESCRIBE {TABLE_UPDATES}: {e}")
        return None

    # Map lowercase -> original name
    col_map = {name.lower(): name for name in df_desc["column_name"]}
    candidates = ["delay", "arrival_delay", "departure_delay", "schedule_delay"]

    for cand in candidates:
        if cand in col_map:
            return col_map[cand]

    print("No suitable delay column found in 'updates' table.")
    return None

def load_speed_delay(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Build a DataFrame with avg_speed and avg_delay per trip_id
    using data from positions and updates.
    """
    # 1) Average speed per trip from positions
    q_speed = f"""
        SELECT
            trip_id,
            AVG(speed) AS avg_speed
        FROM {TABLE_POSITIONS}
        WHERE speed IS NOT NULL
        GROUP BY trip_id
    """
    df_speed = con.execute(q_speed).fetchdf()

    if df_speed.empty:
        print("No speed data found in positions; cannot build speed/delay scatter.")
        return pd.DataFrame()

    # 2) Detect delay column in updates
    delay_col = detect_delay_column(con)
    if delay_col is None:
        return pd.DataFrame()

    # 3) Average delay per trip from updates
    q_delay = f"""
        SELECT
            trip_id,
            AVG({delay_col}) AS avg_delay
        FROM {TABLE_UPDATES}
        WHERE {delay_col} IS NOT NULL
        GROUP BY trip_id
    """
    df_delay = con.execute(q_delay).fetchdf()

    if df_delay.empty:
        print("No delay data found in updates; cannot build speed/delay scatter.")
        return pd.DataFrame()

    # 4) Inner join on trip_id: only trips that have both speed and delay
    df = pd.merge(df_speed, df_delay, on="trip_id", how="inner")

    # Drop any weird rows
    df = df.dropna(subset=["avg_speed", "avg_delay"])

    return df

def plot_speed_vs_delay(con: duckdb.DuckDBPyConnection, out_dir: Path) -> None:
    """
    Scatter plot of average speed vs average delay per trip.
    Uses both positions (speed) and updates (delay).
    """
    df = load_speed_delay(con)

    if df.empty:
        print("No combined speed/delay data to plot.")
        return

    plt.figure(figsize=(8, 6))
    plt.scatter(df["avg_speed"], df["avg_delay"], alpha=0.5)
    plt.xlabel("Average Speed (units of `speed` column)")
    plt.ylabel("Average Delay (units of delay column)")
    plt.title("Average Speed vs Average Delay per Trip")
    plt.tight_layout()

    out_path = out_dir / "speed_vs_delay.png"
    plt.savefig(out_path, dpi=150)
    plt.close()
    print(f"Saved speed vs delay scatter to {out_path}")


def main() -> None:
    args = parse_args()
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    con = connect_db(args.db)
    list_tables(con)
    df = load_combined_data(con)

    if df.empty:
        print("No data to plot.")
        return

    plot_top_routes(df, out_dir, args.top_n)
    plot_activity_over_time(df, out_dir)
    plot_heatmap(df, out_dir)
    plot_speed_vs_delay(con, out_dir)

    print("Analysis complete.")


if __name__ == "__main__":
    main()

