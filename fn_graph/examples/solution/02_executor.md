# 02 - Executor

## What the executor does

One job: given a function and its inputs, run it somewhere and return the result.

```mermaid
flowchart LR
    A[PipelineComposer] -->|node_name, fn, kwargs| B[Executor]
    B --> C[result]
```

Where "somewhere" is decides by config. The composer never needs to know.

---

## Current execution model and its problems

```mermaid
flowchart TD
    A[run_node_in_docker] --> B[write fn source to /ctx]
    B --> C[subprocess.run\ndocker run worker.py]
    C --> D[wait for container to exit]
    D --> E{pkl file appeared?}
    E -->|yes| F[done]
    E -->|no| G[crash pipeline]
```

Four problems:

- No health check before sending work. Container might not be ready.
- Failure is just an exit code. You grep logs to find out what happened.
- No in-memory fallback. Every test spins a container.
- Hardwired to Docker. Lambda means rewriting everything.

---

## Base interface

```mermaid
classDiagram
    class BaseExecutor {
        +execute(node_name, fn, kwargs) result
    }
    class InMemoryExecutor {
        +execute(node_name, fn, kwargs) result
    }
    class LocalDockerExecutor {
        +execute(node_name, fn, kwargs) result
    }
    class AWSDockerExecutor {
        +execute(node_name, fn, kwargs) result
    }
    class LambdaExecutor {
        +execute(node_name, fn, kwargs) result
    }
    BaseExecutor <|-- InMemoryExecutor
    BaseExecutor <|-- LocalDockerExecutor
    BaseExecutor <|-- AWSDockerExecutor
    BaseExecutor <|-- LambdaExecutor
```

One method. Every variant implements it. The composer only ever calls this one method.

---

## InMemoryExecutor

Calls the function directly in the same Python process. No Docker, no network, no serialization.

```mermaid
sequenceDiagram
    participant PipelineComposer
    participant InMemoryExecutor

    PipelineComposer->>InMemoryExecutor: execute("model", model_fn, {kwargs})
    InMemoryExecutor->>InMemoryExecutor: result = model_fn(**kwargs)
    InMemoryExecutor-->>PipelineComposer: result
```

Use this for local dev and unit tests. No overhead.

---

## DockerExecutor

The main redesign. Container no longer runs a one-shot script and dies. It runs a FastAPI server and waits for requests.

```mermaid
flowchart TD
    subgraph BEFORE
        A[subprocess.run docker run worker.py] --> B[container runs, exits]
        B --> C[check for pkl file]
    end

    subgraph AFTER
        D[docker run fn_graph_worker] --> E[FastAPI server starts on :8000]
        E --> F[GET /health]
        F --> G[POST /execute\nfn_source + kwargs]
        G --> H[result in HTTP response]
    end
```

Full flow:

```mermaid
sequenceDiagram
    participant PipelineComposer
    participant DockerExecutor
    participant Container
    participant FastAPI

    PipelineComposer->>DockerExecutor: execute("model", model_fn, {kwargs})
    DockerExecutor->>Container: docker run fn_graph_worker
    Container->>FastAPI: server starts on :8000
    DockerExecutor->>FastAPI: GET /health
    FastAPI-->>DockerExecutor: 200 OK
    DockerExecutor->>FastAPI: POST /execute\n{node_name, fn_source, kwargs_b64}
    FastAPI->>FastAPI: exec(fn_source)\nresult = fn(**kwargs)
    FastAPI-->>DockerExecutor: {result_b64}
    DockerExecutor-->>PipelineComposer: deserialized result
```

What the container exposes:

```mermaid
flowchart LR
    A[fn_graph_worker container] --> B["GET /health\nis it alive?"]
    A --> C["POST /execute\nrun a function, return result"]
    A --> D["GET /result/node\nfetch cached result"]
```

---

## Per-node executor config

Every node can run in a different environment. Configured in YAML.

```yaml
run_id: abc123

nodes:
  iris:
    executor: memory
  model:
    executor: lambda
    memory: 512
    timeout: 30
  predictions:
    executor: local_docker
    image: fn_graph_worker
  "*":
    executor: memory
```

```mermaid
flowchart TD
    A[pipeline_config.yaml] --> B[config.py\nExecutorFactory]
    B --> C{node name lookup}
    C -->|iris| D[InMemoryExecutor]
    C -->|model| E[LambdaExecutor\nmemory=512, timeout=30]
    C -->|predictions| F[LocalDockerExecutor\nimage=fn_graph_worker]
    C -->|anything else| G[InMemoryExecutor\ndefault fallback]
```

Functions stay clean. No infrastructure config inside business logic. Switching a node from Lambda to Docker is one line in the YAML.

---

## Switching between executors

```mermaid
flowchart LR
    A[pipeline_config.yaml] --> B[ExecutorFactory.create]
    B --> C[InMemoryExecutor]
    B --> D[LocalDockerExecutor]
    B --> E[AWSDockerExecutor]
    B --> F[LambdaExecutor]
```

```python
executor = ExecutorFactory.create(config.get_node_config(node_name))
result = executor.execute(node_name, fn, kwargs)
```

---

## Why functions are shipped as source code not bytecode

```mermaid
flowchart LR
    A[host Python 3.12] -->|cloudpickle fn bytecode| B[container Python 3.11]
    B --> C[segfault\nno exception\njust crashes]

    D[host Python 3.12] -->|fn source as string| E[container Python 3.11]
    E -->|exec, compiles with own Python| F[works fine]
```

Cloudpickle serializes a function including its bytecode. Bytecode format changes between Python versions. Shipping plain source text avoids this entirely. The container compiles it with its own Python.

---

## Key Notes

- Executor is per node, not per pipeline. Each node can run somewhere different.
- The `"*"` entry in config is the fallback. Any node not explicitly listed uses it.
- Source code shipping over HTTP preserves the same cross-version safety the current `worker.py` already had.
- LambdaExecutor posts to a Lambda function URL instead of a local Docker port. The handler logic inside Lambda and the FastAPI handler share the same function-execution core.
