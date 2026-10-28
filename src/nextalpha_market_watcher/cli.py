from __future__ import annotations

import argparse
import asyncio
import signal

import uvicorn

from .app import App
from .config import AppConfig, load_config
from .fastapi_server import create_app
from .logging_utils import setup_logging


async def _run_with_signals(cfg: AppConfig) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _request_stop() -> None:
        stop.set()

    try:
        loop.add_signal_handler(signal.SIGINT, _request_stop)
        loop.add_signal_handler(signal.SIGTERM, _request_stop)
    except NotImplementedError:
        pass

    await App(cfg).run(stop)


def main() -> None:
    parser = argparse.ArgumentParser(description="Nextalpha market watcher")
    sub = parser.add_subparsers(dest="cmd", required=True)

    run_parser = sub.add_parser("run", help="Run watcher service")
    run_parser.add_argument("--config", required=True, help="Path to yaml config")
    run_parser.add_argument(
        "--mode",
        choices=["alerts", "klines", "all"],
        default=None,
        help="alerts=涨跌幅告警 only; klines=1h收盘推送 only; all=两者 (default from yaml or all)",
    )

    serve_parser = sub.add_parser("serve", help="Run watcher as FastAPI service")
    serve_parser.add_argument("--config", required=True, help="Path to yaml config")
    serve_parser.add_argument(
        "--mode",
        choices=["alerts", "klines", "all"],
        default=None,
        help="alerts=告警服务; klines=K线服务; all=合并服务",
    )
    serve_parser.add_argument("--host", default="0.0.0.0", help="FastAPI listen host")
    serve_parser.add_argument("--port", type=int, required=True, help="FastAPI listen port")

    args = parser.parse_args()
    if args.cmd == "run":
        cfg = load_config(args.config, service_mode=args.mode)
        setup_logging(cfg.log_level)
        try:
            asyncio.run(_run_with_signals(cfg))
        except KeyboardInterrupt:
            pass
    elif args.cmd == "serve":
        app = create_app(args.config, service_mode=args.mode)
        uvicorn.run(app, host=args.host, port=args.port, log_level="info")
