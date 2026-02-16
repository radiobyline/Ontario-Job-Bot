from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from .config import load_settings
from .discovery import discover_urls
from .monitor import run_monitor


def _print_json(payload: dict) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ontario municipality/First Nations job monitor")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_discover = sub.add_parser("discover", help="Canonicalize jobs URLs")
    p_discover.add_argument("--input", default=None, help="Input CSV path")
    p_discover.add_argument("--output", default=None, help="Output enriched CSV path")
    p_discover.add_argument("--limit", type=int, default=None, help="Optional row limit for smoke tests")

    p_monitor = sub.add_parser("monitor", help="Run weekly monitoring")
    p_monitor.add_argument("--input", default=None, help="Enriched CSV path")
    p_monitor.add_argument("--max-boards", type=int, default=None, help="Optional board limit for smoke tests")

    p_all = sub.add_parser("run-all", help="Run discover then monitor")
    p_all.add_argument("--input", default=None, help="Input CSV path")
    p_all.add_argument("--output", default=None, help="Output enriched CSV path")
    p_all.add_argument("--limit", type=int, default=None, help="Optional row limit for discover")
    p_all.add_argument("--max-boards", type=int, default=None, help="Optional board limit for monitor")

    return parser


def main() -> None:
    settings = load_settings()
    parser = build_parser()
    args = parser.parse_args()

    if args.cmd == "discover":
        input_csv = Path(args.input) if args.input else settings.orgs_csv
        output_csv = Path(args.output) if args.output else settings.orgs_enriched_csv
        stats = asyncio.run(discover_urls(settings, input_csv=input_csv, output_csv=output_csv, limit=args.limit))
        _print_json({"command": "discover", **stats})
        return

    if args.cmd == "monitor":
        input_csv = Path(args.input) if args.input else settings.orgs_enriched_csv
        stats = asyncio.run(run_monitor(settings, input_csv=input_csv, max_boards=args.max_boards))
        _print_json({"command": "monitor", **stats})
        return

    if args.cmd == "run-all":
        input_csv = Path(args.input) if args.input else settings.orgs_csv
        output_csv = Path(args.output) if args.output else settings.orgs_enriched_csv
        discover_stats = asyncio.run(
            discover_urls(settings, input_csv=input_csv, output_csv=output_csv, limit=args.limit)
        )
        monitor_stats = asyncio.run(
            run_monitor(settings, input_csv=output_csv, max_boards=args.max_boards)
        )
        _print_json(
            {
                "command": "run-all",
                "discover": discover_stats,
                "monitor": monitor_stats,
            }
        )
        return


if __name__ == "__main__":
    main()
