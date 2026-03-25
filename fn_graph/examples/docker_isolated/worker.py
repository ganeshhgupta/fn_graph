"""
Worker that runs inside each Docker container.

Receives:
  argv[1] = /ctx              (bind-mounted context directory)
  argv[2] = <node_name>       (name of the DAG node / function to run)
  argv[3] = /ctx/_fn_<node>.py  (Python source file containing the function)

Protocol (no cloudpickle for functions — avoids Python-version bytecode issues):
  - Function is exec'd from source (.py), not loaded from a pickle.
  - Inputs are read from /ctx/<name>.pkl  (cloudpickle, written by orchestrator).
  - Output is written to /ctx/<node_name>.pkl  (cloudpickle).
"""
import sys
import os
import traceback
import textwrap


def main():
    context_dir = sys.argv[1]
    node_name   = sys.argv[2]
    fn_src_path = sys.argv[3]

    print(f"[worker:{node_name}] loading source from {fn_src_path}", flush=True)

    try:
        import cloudpickle
        import inspect

        with open(fn_src_path, "r", encoding="utf-8") as fh:
            source = fh.read()

        namespace = {}
        exec(textwrap.dedent(source), namespace)   # defines the function in namespace
        fn = namespace[node_name]

        sig = inspect.signature(fn)
        param_names = list(sig.parameters.keys())

        kwargs = {}
        for name in param_names:
            arg_path = os.path.join(context_dir, f"{name}.pkl")
            if not os.path.exists(arg_path):
                print(f"[worker:{node_name}] ERROR: missing input '{name}' at {arg_path}", flush=True)
                sys.exit(1)
            with open(arg_path, "rb") as fh:
                kwargs[name] = cloudpickle.load(fh)
            print(f"[worker:{node_name}]   loaded '{name}': {type(kwargs[name]).__name__}", flush=True)

        print(f"[worker:{node_name}] running with inputs: {param_names}", flush=True)

        result = fn(**kwargs)

        print(f"[worker:{node_name}] function returned: {type(result).__name__}", flush=True)

        out_path = os.path.join(context_dir, f"{node_name}.pkl")
        with open(out_path, "wb") as fh:
            cloudpickle.dump(result, fh)

        print(f"[worker:{node_name}] done -> {out_path}", flush=True)

    except SystemExit:
        raise
    except BaseException as exc:
        print(f"[worker:{node_name}] FAILED: {type(exc).__name__}: {exc}", flush=True)
        traceback.print_exc(file=sys.stdout)
        sys.stdout.flush()
        sys.exit(1)


if __name__ == "__main__":
    main()
