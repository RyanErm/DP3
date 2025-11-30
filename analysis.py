#!/usr/bin/env python3

from __future__ import annotations
import argparse
import logging
import re
from pathlib import Path

import duckdb
import pandas as pd
import matplotlib.pyplot as plt


logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(message)s",
)

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
COL_DELAY = "delay"  # from updates


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run analysis and produce plots.")
    parser.add_argument("--db", type=str, default=DEFAULT_DB_PATH)
    parser.add_argument("--out", type=str, default="plots")
    parser.add_argument("--top-n", type=int, default=10)

    # Optional CLI overrides for route comparison
    parser.add_argument("--route-a", type=str, help="First route ID to compare (e.g. C40)")
    parser.add_argument("--route-b", type=str, help="Second route ID to compare (e.g. A58)")

    return parser.parse_args()


def connect_db(db_path: str) -> duckdb.DuckDBPyConnection:
    db_path = Path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"DuckDB file not found: {db_path.resolve()}")
    logging.info(f"Connecting to DuckDB at {db_path.resolve()}")
    return duckdb.connect(str(db_path))


def list_tables(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    df = con.execute("SHOW TABLES").fetchdf()
    logging.info("Tables in database:\n%s", df.to_string(index=False))
    return df


def load_combined_data(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Load positions joined to routes if available.
    Always returns: ts, route_id, vehicle_id, lat, lon, route_name
    """
    tables = set(con.execute("SHOW TABLES").fetchdf()["name"])

    if TABLE_ROUTES in tables:
        logging.info("Found routes table; joining positions to routes.")
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
        logging.info("No 'routes' table found; proceeding without route names.")
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

    logging.info("Loaded %d position records.", len(df))
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
        logging.warning("No data for top routes plot.")
        return

    def make_label(row):
        name = row["route_name"]
        if pd.isna(name):
            name = ""
        return f"{row['route_id']}\n{name}".strip()

    route_counts["label"] = route_counts.apply(make_label, axis=1)

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
    logging.info("Saved top routes bar chart to %s", out_path)


def plot_heatmap(df: pd.DataFrame, out_dir: Path) -> None:
    # Use the aliased lat/lon columns from load_combined_data
    if "lat" not in df.columns or "lon" not in df.columns:
        logging.warning("Cannot plot heatmap: expected 'lat' and 'lon' columns.")
        return

    df_coords = df.dropna(subset=["lat", "lon"])
    if df_coords.empty:
        logging.warning("No lat/lon data for heatmap.")
        return

    plt.figure(figsize=(8, 8))
    hb = plt.hexbin(
        df_coords["lon"],
        df_coords["lat"],
        gridsize=60,
        bins="log"
    )
    plt.xlabel("Longitude")
    plt.ylabel("Latitude")
    plt.title("Vehicle Position Density Heatmap")
    cb = plt.colorbar(hb)
    cb.set_label("Log(Count)")

    plt.tight_layout()
    out_path = out_dir / "heatmap.png"
    plt.savefig(out_path, dpi=200)
    plt.close()
    logging.info("Saved heatmap to %s", out_path)


def load_speed_delay(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Build a DataFrame with avg_speed and avg_delay per trip_id
    using data from positions and updates.
    """
    # Average speed per trip from positions
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
        logging.warning("No speed data found in positions; cannot build speed/delay scatter.")
        return pd.DataFrame()

    # Check that updates exists
    tables = set(con.execute("SHOW TABLES").fetchdf()["name"])
    if TABLE_UPDATES not in tables:
        logging.warning("No 'updates' table found; cannot compute delay.")
        return pd.DataFrame()

    # Average delay per trip from updates
    q_delay = f"""
        SELECT
            trip_id,
            AVG({COL_DELAY}) AS avg_delay
        FROM {TABLE_UPDATES}
        WHERE {COL_DELAY} IS NOT NULL
        GROUP BY trip_id
    """
    df_delay = con.execute(q_delay).fetchdf()

    if df_delay.empty:
        logging.warning("No delay data found in updates; cannot build speed/delay scatter.")
        return pd.DataFrame()

    df = pd.merge(df_speed, df_delay, on="trip_id", how="inner")
    df = df.dropna(subset=["avg_speed", "avg_delay"])

    logging.info("Built speed/delay dataset with %d trips.", len(df))
    return df


def plot_speed_vs_delay(con: duckdb.DuckDBPyConnection, out_dir: Path) -> None:
    df = load_speed_delay(con)

    if df.empty:
        logging.warning("No combined speed/delay data to plot.")
        return

    plt.figure(figsize=(8, 6))
    plt.scatter(df["avg_speed"], df["avg_delay"], alpha=0.5)
    plt.xlabel("Average Speed")
    plt.ylabel("Average Delay")
    plt.title("Average Speed vs Average Delay per Trip")
    plt.tight_layout()

    out_path = out_dir / "speed_vs_delay.png"
    plt.savefig(out_path, dpi=150)
    plt.close()
    logging.info("Saved speed vs delay scatter to %s", out_path)

def sanitize_filename_part(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", s).strip("_") or "route"


def plot_avg_delay_for_routes(
    con: duckdb.DuckDBPyConnection,
    out_dir: Path,
    route_a: str,
    route_b: str,
) -> None:
    """
    Compare two routes (by route_id) using average delay from the updates table.
    Produces a bar chart with one bar per route.
    """
    route_a = route_a.strip()
    route_b = route_b.strip()
    if not route_a or not route_b:
        logging.warning("Empty route IDs provided; skipping route comparison.")
        return

    tables = set(con.execute("SHOW TABLES").fetchdf()["name"])
    if TABLE_UPDATES not in tables:
        logging.warning("No 'updates' table found; cannot compute route delays.")
        return

    logging.info("Computing average delay for routes %s and %s.", route_a, route_b)

    q = f"""
        SELECT
            route_id,
            AVG({COL_DELAY}) AS avg_delay
        FROM {TABLE_UPDATES}
        WHERE {COL_DELAY} IS NOT NULL
          AND route_id IN (?, ?)
        GROUP BY route_id
    """
    df = con.execute(q, [route_a, route_b]).fetchdf()

    if df.empty:
        logging.warning("No delay data found for routes %s and %s.", route_a, route_b)
        return

    plt.figure(figsize=(6, 5))
    plt.bar(df["route_id"], df["avg_delay"])
    plt.xlabel("Route ID")
    plt.ylabel("Average Delay")
    plt.title(f"Average Delay: {route_a} vs {route_b}")
    plt.tight_layout()

    part_a = sanitize_filename_part(route_a)
    part_b = sanitize_filename_part(route_b)
    out_path = out_dir / f"avg_delay_{part_a}_vs_{part_b}.png"
    plt.savefig(out_path, dpi=150)
    plt.close()
    logging.info("Saved average delay comparison for routes to %s", out_path)


def main() -> None:
    args = parse_args()
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    con = connect_db(args.db)
    list_tables(con)
    df = load_combined_data(con)

    if df.empty:
        logging.warning("No data to plot from positions.")
        return

    # 1) Top routes (by observations)
    plot_top_routes(df, out_dir, args.top_n)

    # 2) Spatial heatmap
    plot_heatmap(df, out_dir)

    # 3) Speed vs delay scatter (per trip), using both tables
    plot_speed_vs_delay(con, out_dir)

    # 4) Route vs route average delay comparison (interactive-ish)
    route_a = args.route_a
    route_b = args.route_b

    # If not provided via CLI, ask user
    if not route_a or not route_b:
        try:
            logging.info("No route IDs provided via CLI; prompting for input.")
            if not route_a:
                route_a = input("Enter first route ID (e.g., C40): ").strip()
            if not route_b:
                route_b = input("Enter second route ID (e.g., A58): ").strip()
        except EOFError:
            logging.warning("Input not available; skipping route comparison.")
            route_a = route_b = None

    if route_a and route_b:
        plot_avg_delay_for_routes(con, out_dir, route_a, route_b)
    else:
        logging.warning("Route IDs missing; skipping route comparison plot.")

    logging.info("Analysis complete.")


if __name__ == "__main__":
    main()

