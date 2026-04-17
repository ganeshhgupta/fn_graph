import base64
import importlib
import logging
import traceback as tb_module

import cloudpickle
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel

log = logging.getLogger(__name__)
app = FastAPI()


class ExecuteRequest(BaseModel):
    node_name: str
    module: str
    kwargs_b64: str


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/execute")
def execute(req: ExecuteRequest):
    try:
        log.info(f"[worker] {req.node_name} <- {req.module}")
        mod = importlib.import_module(req.module)
        fn = getattr(mod, req.node_name)
        kwargs = cloudpickle.loads(base64.b64decode(req.kwargs_b64))
        log.debug(f"[worker] inputs: {list(kwargs.keys())}")
        result = fn(**kwargs)
        log.info(f"[worker] {req.node_name} complete, output: {type(result).__name__}")
        result_b64 = base64.b64encode(cloudpickle.dumps(result, protocol=4)).decode()
        return {"result_b64": result_b64}
    except Exception as e:
        tb = tb_module.format_exc()
        log.error(f"[worker] ERROR in {req.node_name}: {e}\n{tb}")
        return JSONResponse(
            status_code=500,
            content={"error": str(e), "traceback": tb},
        )
