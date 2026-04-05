from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import Any

from canadastats.config import load_config, write_default_config
from canadastats.logging_utils import setup_logging
from canadastats.sync_service import SyncService


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="canadastats", description="Canada high school sports aggregator")
    parser.add_argument("--config", default="config.yaml", help="Path to config file")

    sub = parser.add_subparsers(dest="command", required=True)

    sync_parser = sub.add_parser("sync", help="Run scraping sync commands")
    sync_sub = sync_parser.add_subparsers(dest="sync_command", required=True)

    sync_sub.add_parser("all", help="Sync all enabled sources")

    source_parser = sync_sub.add_parser("source", help="Sync a single source")
    source_parser.add_argument("--name", required=True, choices=["rseq", "hmhshl", "bc_football", "bc_basketball"])

    league_parser = sync_sub.add_parser("league", help="Sync one league in one source")
    league_parser.add_argument("--source", required=True, choices=["rseq", "hmhshl", "bc_football", "bc_basketball"])
    league_parser.add_argument("--league-id", required=True)

    sub.add_parser("doctor", help="Run health checks")

    export_parser = sub.add_parser("export", help="Export current snapshots")
    export_sub = export_parser.add_subparsers(dest="export_command", required=True)
    export_csv = export_sub.add_parser("csv", help="Export CSV files")
    export_csv.add_argument("--sport", required=True, choices=["basketball", "football", "hockey", "unknown"])
    export_csv.add_argument("--out", required=True, help="Output directory")

    return parser


def _print_result(result: dict[str, Any]) -> None:
    print(json.dumps(result, ensure_ascii=False, indent=2))


def main(argv: list[str] | None = None) -> int:
    setup_logging(logging.INFO)
    parser = build_parser()
    args = parser.parse_args(argv)

    write_default_config(args.config)
    config = load_config(args.config)
    service = SyncService(config)

    try:
        if args.command == "sync":
            if args.sync_command == "all":
                result = service.sync_all()
                _print_result(result)
                return 0

            if args.sync_command == "source":
                result = service.sync_source(args.name)
                _print_result(result)
                return 0

            if args.sync_command == "league":
                result = service.sync_league(args.source, args.league_id)
                _print_result(result)
                return 0

        if args.command == "doctor":
            ok, report = service.doctor()
            print(report)
            return 0 if ok else 1

        if args.command == "export" and args.export_command == "csv":
            files = service.export_csv(args.sport, args.out)
            _print_result(files)
            return 0

        parser.print_help()
        return 1
    finally:
        service.close()


if __name__ == "__main__":
    sys.exit(main())
