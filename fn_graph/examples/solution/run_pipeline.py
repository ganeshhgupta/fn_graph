"""
Entry point for the pluggable pipeline orchestration layer.

Usage:
    python run_pipeline.py --pipeline fn_graph.examples.machine_learning --config machine_learning_config.yaml
    python run_pipeline.py --pipeline fn_graph.examples.machine_learning --config machine_learning_config.yaml --debug
"""

import argparse
import importlib
import logging
import sys
from datetime import datetime
from pathlib import Path

# Make solution/ importable regardless of cwd
sys.path.insert(0, str(Path(__file__).parent))

# Force non-interactive backend before any pipeline code touches matplotlib.
import matplotlib
matplotlib.use("Agg")

from config import load_config, get_artifact_store, get_node_config
from composer import PipelineComposer

log = logging.getLogger("run_pipeline")


def _setup_log(pipeline: str, run_id: str) -> Path:
    """Create logs/{pipeline_name}/{run_id}/ and return a timestamped log path."""
    pipeline_name = pipeline.split(".")[-1]
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_dir = Path(__file__).parent / "logs" / pipeline_name / run_id
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir / f"{timestamp}.log"


def _configure_logging(debug: bool, log_path: Path = None) -> None:
    """
    Configure root logger.  Call once with log_path=None for console-only setup,
    then again with log_path set to attach the file handler.

    Level is DEBUG when --debug is passed, INFO otherwise.
    Third-party libraries are silenced to WARNING to keep logs clean.
    """
    level = logging.DEBUG if debug else logging.INFO
    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)-5s %(message)s",
        datefmt="%H:%M:%S",
    )

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)  # root captures all; handlers filter by level

    # Console handler — only added on first call (when no StreamHandler exists yet)
    if not any(
        isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler)
        for h in root.handlers
    ):
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(level)
        ch.setFormatter(formatter)
        root.addHandler(ch)

    # File handler — added when log_path is known
    if log_path is not None:
        fh = logging.FileHandler(log_path, encoding="utf-8")
        fh.setLevel(level)
        fh.setFormatter(formatter)
        root.addHandler(fh)

    # Silence noisy third-party loggers
    for lib in ("matplotlib", "PIL", "sklearn", "urllib3"):
        logging.getLogger(lib).setLevel(logging.WARNING)


def main():
    parser = argparse.ArgumentParser(description="Run an fn_graph pipeline.")
    parser.add_argument(
        "--pipeline",
        required=True,
        help="Dotted module path to the pipeline, e.g. fn_graph.examples.machine_learning",
    )
    parser.add_argument(
        "--config",
        default="machine_learning_config.yaml",
        help="Path to a config yaml, e.g. machine_learning_config.yaml",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug-level logging (verbose store ops, executor details, etc.)",
    )
    args = parser.parse_args()

    # Phase 1: console-only logging (log_path not known yet)
    _configure_logging(args.debug)

    log.info("[run_pipeline] starting")
    log.info(f"[run_pipeline] pipeline module: {args.pipeline}")
    log.info(f"[run_pipeline] config file:     {args.config}")

    # Import pipeline module and grab its Composer
    module = importlib.import_module(args.pipeline)
    f = module.f

    # Pandas compat shim for finance.py
    if hasattr(module, "cumulative_return"):
        import inspect as _inspect
        try:
            _src = _inspect.getsource(module.cumulative_return)
        except OSError:
            _src = ""
        if "total_position[-1]" in _src:
            def cumulative_return(total_position):
                return 100 * (total_position.iloc[-1] / total_position.iloc[0] - 1)
            f = f.update(cumulative_return=cumulative_return)
            log.debug("[run_pipeline] applied pandas compatibility patch: cumulative_return")

    log.debug("[run_pipeline] composer loaded, building pipeline")

    config = load_config(args.config)
    run_id = config["pipeline"]["run_id"]

    # Phase 2: add file handler now that log_path is known
    log_path = _setup_log(args.pipeline, run_id)
    _configure_logging(args.debug, log_path)
    log.info(f"[run_pipeline] logging to: {log_path}")

    artifact_store = get_artifact_store(config)
    log.info(f"[run_pipeline] artifact store: {type(artifact_store).__name__}")

    # Build per-node executor config
    all_nodes = list(f.dag().nodes())
    execution_config = {}
    for node_name in all_nodes:
        execution_config[node_name] = get_node_config(config, node_name)

    log.debug("[run_pipeline] executors configured:")
    for node_name, node_cfg in execution_config.items():
        log.debug(f"  {node_name}: {node_cfg.get('executor', 'memory')}")

    pipeline = PipelineComposer(
        _functions=f._functions,
        _parameters=f._parameters,
        _cache=f._cache,
        _tests=f._tests,
        _source_map=f._source_map,
        execution_config=execution_config,
        artifact_store=artifact_store,
        pipeline_config=config,
    )

    dag = pipeline.dag()
    param_names = set(pipeline.parameters().keys())
    leaf_outputs = [
        n for n in dag.nodes()
        if dag.out_degree(n) == 0 and n not in param_names
    ]
    log.info(f"[run_pipeline] leaf outputs: {leaf_outputs}")

    results = pipeline.calculate(leaf_outputs)

    log.info("")
    log.info("=" * 60)
    log.info("=== Pipeline Results ===")
    import matplotlib.figure
    for name, value in results.items():
        if isinstance(value, str):
            log.info(f"\n[{name}]\n{value}")
        elif isinstance(value, (int, float)):
            log.info(f"[{name}] {value}")
        elif isinstance(value, matplotlib.figure.Figure):
            out_path = Path(args.config).parent / f"{name}.png"
            value.savefig(out_path)
            log.info(f"[{name}] figure saved to {out_path}")
        elif hasattr(value, "get_figure"):
            out_path = Path(args.config).parent / f"{name}.png"
            value.get_figure().savefig(out_path)
            log.info(f"[{name}] figure saved to {out_path}")
        else:
            log.info(f"[{name}] {type(value).__name__}")

    log.info(f"\n[run_pipeline] log saved to: {log_path}")
    logging.shutdown()


if __name__ == "__main__":
    main()
