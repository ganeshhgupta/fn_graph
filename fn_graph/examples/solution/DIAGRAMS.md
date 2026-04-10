# fn_graph Pipeline — Visual Reference

All diagrams cover a full execution of the ML pipeline using `machine_learning_config.yaml`.

---

## Diagram 1 — Architecture: Simple

The six moving parts and how they relate.

```mermaid
graph TD
    CLI["run_pipeline.py\nCLI entry point"]
    YAML["machine_learning_config.yaml\nstages, executors, artifact store"]
    ORC["PipelineComposer\norchestrator — walks stage DAG"]
    STAGE["StageExecutor\nruns one stage entirely in-memory"]
    EXEC["Executors\nmemory / docker / lambda"]
    STORE["ArtifactStore\nboundary nodes only"]
    PIPE["machine_learning.py\nthe pipeline — pure Python functions"]

    CLI -->|"imports"| PIPE
    CLI -->|"reads"| YAML
    CLI -->|"builds"| ORC
    ORC -->|"dispatches each stage to"| STAGE
    STAGE -->|"calls per node"| EXEC
    STAGE -->|"boundary nodes only"| STORE
    ORC -->|"memoization check"| STORE
    EXEC -->|"runs"| PIPE
```

> **One-liner:** The CLI builds an orchestrator that groups nodes into stages, dispatches independent stages in parallel, and only touches disk at stage boundaries — not between every node.

---

## Diagram 2 — Architecture: Detailed

Every file, class, and key method.

```mermaid
graph TD
    subgraph Entry["Entry Point"]
        RUN["run_pipeline.py · main()\n_setup_log · _Tee · importlib"]
    end

    subgraph Config["config.py — Factory"]
        LC["load_config(yaml_path) → dict"]
        GE["get_executor(node_config) → BaseExecutor"]
        GAS["get_artifact_store(config) → BaseArtifactStore"]
        GNC["get_node_config(config, node_name) → dict"]
        LS["load_stages(config) → dict\nreturns empty dict if no stages key"]
    end

    subgraph Pipeline["machine_learning.py — Pure DAG"]
        COMP["Composer f\n.update_parameters(model_type, do_preprocess)\n.update(iris, data, split_data, model, ...)"]
    end

    subgraph Orchestrator["composer.py — PipelineComposer(Composer)"]
        INIT["__init__(execution_config, artifact_store, pipeline_config)"]
        COPY["_copy(**kwargs)\npreserves execution_config, artifact_store,\nand pipeline_config on every fn_graph clone"]
        CALC["calculate(outputs)\n1 trim DAG  2 seed params\n3 branch: stages or node-level"]
        BOUNDARY["_analyze_stage_boundaries(stages, dag)\nbuilds inputs/outputs/internal per stage\nbuilds stage-level DAG"]
        STAGES["_calculate_with_stages(...)\nThreadPoolExecutor on stages\nFIRST_COMPLETED dispatch loop"]
        EXEC_NODE["execute_node(node_name) [inner — fallback]\nexists? skip · get inputs · dispatch · put"]
        RESOLVE["_resolve_predecessors(node_name)\nfn_graph built-in"]
    end

    subgraph Executors["executor/"]
        BASE_E["base.py · BaseExecutor\nexecute(node_name, fn, kwargs)"]
        MEM["memory.py · InMemoryExecutor\nfn(**kwargs)"]
        DOC["docker.py · DockerExecutor\n_free_port → docker run -d\n→ health poll → POST /execute → stop+rm"]
        LAM["lambda_executor.py · LambdaExecutor\nboto3 invoke"]
        STEXT["stage_executor.py · StageExecutor\nrun() — topo order in-memory\nonly writes boundary nodes to store"]
    end

    subgraph Store["artifact_store/"]
        BASE_S["base.py · BaseArtifactStore\nget / put / exists / delete / metadata"]
        FS["fs.py · LocalFSArtifactStore\nartifacts/{run_id}/{node}.pkl\natomic write via .tmp → os.replace"]
        S3["s3.py · S3ArtifactStore\ns3://bucket/{run_id}/{node}.pkl"]
    end

    subgraph Worker["worker/ — runs inside Docker"]
        SRV["server.py  FastAPI\nGET /health\nPOST /execute\nexec(fn_source, namespace) → fn(**kwargs)\n→ cloudpickle result"]
    end

    subgraph MLPipe["machine_learning.py — Functions"]
        FNS["iris · data · preprocess_data · investigate_data\nsplit_data · training_features · training_target\ntest_features · test_target · model\npredictions · classification_metrics · confusion_matrix"]
    end

    RUN -->|"importlib.import_module"| COMP
    RUN -->|"load_config"| LC
    RUN -->|"get_artifact_store"| GAS
    RUN -->|"get_node_config x N"| GNC
    RUN -->|"PipelineComposer(pipeline_config=config)"| INIT
    RUN -->|"pipeline.calculate(leaf_outputs)"| CALC

    CALC -->|"stages defined"| BOUNDARY
    BOUNDARY --> STAGES
    STAGES -->|"run_stage() per stage"| STEXT
    STEXT -->|"per node"| GE
    STEXT -->|"boundary nodes only"| BASE_S

    CALC -->|"no stages fallback"| EXEC_NODE
    EXEC_NODE --> RESOLVE
    EXEC_NODE --> GE
    EXEC_NODE --> BASE_S

    GE -->|"memory"| MEM
    GE -->|"docker"| DOC
    GE -->|"lambda"| LAM
    LS --> CALC

    GAS -->|"fs"| FS
    GAS -->|"s3"| S3

    MEM -.->|"fn(**kwargs)"| FNS
    DOC -.->|"HTTP POST"| SRV
    SRV -.->|"exec + fn(**kwargs)"| FNS

    MEM -->|"extends"| BASE_E
    DOC -->|"extends"| BASE_E
    LAM -->|"extends"| BASE_E
    STEXT -->|"uses"| BASE_E
    FS -->|"extends"| BASE_S
    S3 -->|"extends"| BASE_S
```

---

## Diagram 3 — Control & Data Flow: Simple (Stage-Based)

What happens when you run the CLI with stages defined — one pass, easy to follow.

```mermaid
sequenceDiagram
    participant U as You (terminal)
    participant R as run_pipeline.py
    participant C as config.py
    participant P as PipelineComposer
    participant S as StageExecutor
    participant E as Executor
    participant A as ArtifactStore

    U->>R: python run_pipeline.py --pipeline ... --config machine_learning_config.yaml
    R->>R: import machine_learning.py → get Composer f
    R->>C: load_config(yaml)
    C-->>R: {run_id, artifact_store, stages, nodes}
    R->>C: get_artifact_store(config)
    C-->>R: LocalFSArtifactStore
    R->>P: PipelineComposer(f internals + config + store + pipeline_config)
    R->>P: calculate(leaf_outputs)
    P->>P: ancestor_dag · seed params to store
    P->>P: _analyze_stage_boundaries() → prints analysis
    P->>P: _calculate_with_stages()

    loop for each stage in parallel where DAG allows
        P->>A: all boundary outputs exist? → skip entire stage
        P->>S: StageExecutor.run(stage_inputs)
        loop for each node in stage (in-memory, topo order)
            S->>E: execute(node_name, fn, kwargs)
            E-->>S: result  stored in-memory
            alt node is a boundary output
                S->>A: put(node, result)
            end
        end
        S-->>P: done
        P->>P: check which downstream stages just became unblocked
    end

    P-->>R: all_results dict
    R->>U: print metrics, save figures as PNG
```

---

## Diagram 4 — Control & Data Flow: Detailed

Every method call and decision, including the stage branch.

```mermaid
flowchart TD
    START(["terminal:\npython run_pipeline.py\n--pipeline fn_graph.examples.machine_learning\n--config machine_learning_config.yaml"])

    A1["run_pipeline.py · main()\nargparse: --pipeline, --config"]
    A2["importlib.import_module(pipeline)\n→ module.f  the plain Composer"]
    A3["config.py · load_config(yaml)\n→ {pipeline, artifact_store, stages, nodes}"]
    A4["_setup_log(pipeline, run_id)\nopen logs/{pipeline}/{run_id}/{ts}.log\nsys.stdout = _Tee(stdout, log_file)"]
    A5["config.py · get_artifact_store(config)\ntype==fs → LocalFSArtifactStore(base_dir, run_id)"]
    A6["f.dag().nodes() → all_nodes\nfor each node: get_node_config → execution_config dict"]
    A7["PipelineComposer.__init__(\n  _functions, _parameters, _cache, _tests, _source_map,\n  execution_config, artifact_store, pipeline_config\n)"]
    A8["pipeline.dag() → find leaf nodes\nout_degree==0 and not in params\n→ [investigate_data, classification_metrics, confusion_matrix]"]

    B1["PipelineComposer.calculate(leaf_outputs)\nfuncs = self.functions()\nparams = {name: val ...}"]
    B2["self.ancestor_dag(outputs)\nnx.topological_sort → topo_order"]
    B3["seed params into ArtifactStore\nartifact_store.put(name, val) for each param\n→ model_type.pkl, do_preprocess.pkl"]
    BRANCH{"load_stages(self._config)\nstages defined?"}

    STAGE1["_analyze_stage_boundaries(stages, ancestor_dag)\nbuild node_to_stage map\nfor each stage: find inputs, outputs, internal\nbuild stage-level NetworkX DiGraph\nprint analysis"]
    STAGE2["_calculate_with_stages(...)\nready_stages = stages with in_degree==0\nThreadPoolExecutor(max_workers=4)"]
    STAGE3["run_stage(stage_name)\nmemoized? all boundary outputs exist → skip\nload stage inputs from artifact store\nStageExecutor.run()"]
    STAGE4["StageExecutor.run()\ntopo sort stage nodes\nfor each node:\n  _resolve_predecessors → load from in-memory dict\n  get_executor(stage_def) → executor\n  executor.execute(node_name, fn, kwargs)\n  store result in-memory\n  if boundary node: artifact_store.put()"]
    STAGE5["wait(FIRST_COMPLETED)\nfor finished stage: done_stages.add()\ncheck stage_dag.successors\nif all pred stages done → submit downstream stage"]

    NODE1["Find initial ready nodes\n[n in topo_order if all preds in params]"]
    NODE2["ThreadPoolExecutor(max_workers=8)\nsubmit all ready nodes"]
    NODE3["execute_node(node_name)\nartifact_store.exists? → skip\n_resolve_predecessors → load kwargs from store\nget_executor → executor.execute(fn, kwargs)\nartifact_store.put(result)"]
    NODE4["wait(FIRST_COMPLETED)\ndone_set.add(node)\ncheck successors → submit unblocked nodes"]

    D2["DockerExecutor.execute()\n_free_port → docker run -d\npoll /health → POST /execute\ncloudpickle result ← HTTP response\ndocker stop + rm"]
    D3["worker/server.py\nexec(fn_source, namespace)\nfn(**kwargs) → result\nreturn result_b64"]

    G1["return all_results\n{name: artifact_store.get(name) for name in funcs if exists}"]
    G2["run_pipeline.py:\nstr/float → print\nFigure → savefig(name.png)"]
    END(["done — check logs/ artifacts/ *.png"])

    START --> A1 --> A2 --> A3 --> A4 --> A5 --> A6 --> A7 --> A8
    A8 --> B1 --> B2 --> B3 --> BRANCH

    BRANCH -->|"yes — stages defined"| STAGE1
    STAGE1 --> STAGE2 --> STAGE3 --> STAGE4
    STAGE4 -.->|"docker node"| D2 --> D3
    STAGE4 --> STAGE5
    STAGE5 -->|"more stages"| STAGE3
    STAGE5 -->|"all stages done"| G1

    BRANCH -->|"no — node-level fallback"| NODE1
    NODE1 --> NODE2 --> NODE3
    NODE3 -.->|"docker node"| D2
    NODE3 --> NODE4
    NODE4 -->|"more nodes"| NODE3
    NODE4 -->|"all done"| G1

    G1 --> G2 --> END
```

---

## Diagram 5 — The Machine Learning Pipeline DAG with Stage Grouping

The fn_graph DAG with stage boundaries overlaid. Node colours show which stage each belongs to.

```mermaid
graph TD
    subgraph Params["Parameters (seeded to ArtifactStore at startup)"]
        MT["model_type = ols"]
        DP["do_preprocess = True"]
    end

    subgraph S1["Stage: preprocessing  executor=memory"]
        IRIS["iris()"]
        DATA["data(iris)"]
        PRE["preprocess_data(data, do_preprocess)"]
        INV["investigate_data(data)"]
    end

    subgraph S2["Stage: splitting  executor=memory"]
        SPLIT["split_data(preprocess_data)"]
        TRF["training_features(split_data)"]
        TRT["training_target(split_data)"]
        TEF["test_features(split_data)"]
        TET["test_target(split_data)"]
    end

    subgraph S3["Stage: training  executor=docker"]
        MOD["model(training_features, training_target, model_type)"]
    end

    subgraph S4["Stage: evaluation  executor=memory"]
        PRED["predictions(model, test_features)"]
        CL["classification_metrics(predictions, test_target)"]
        CM["confusion_matrix(predictions, test_target)"]
    end

    MT --> MOD
    DP --> PRE
    IRIS --> DATA
    DATA --> PRE
    DATA --> INV
    PRE --> SPLIT
    SPLIT --> TRF & TRT & TEF & TET
    TRF & TRT --> MOD
    MOD --> PRED
    TEF --> PRED
    PRED --> CM & CL
    TET --> CM & CL

    style MT fill:#f9f,stroke:#999
    style DP fill:#f9f,stroke:#999
    style PRE fill:#ffe,stroke:#aa0,stroke-width:3px
    style TRF fill:#ffe,stroke:#aa0,stroke-width:3px
    style TRT fill:#ffe,stroke:#aa0,stroke-width:3px
    style TEF fill:#ffe,stroke:#aa0,stroke-width:3px
    style TET fill:#ffe,stroke:#aa0,stroke-width:3px
    style MOD fill:#bbf,stroke:#66f,stroke-width:3px
    style INV fill:#9f9,stroke:#090
    style CM fill:#9f9,stroke:#090
    style CL fill:#9f9,stroke:#090
```

> **Thick borders** = boundary nodes (written to ArtifactStore).  
> `preprocess_data` → boundary between preprocessing and splitting.  
> `training_features`, `training_target`, `test_features`, `test_target` → boundary between splitting and training/evaluation.  
> `model` → boundary between training and evaluation.  
> Green = leaf outputs. Blue = runs in Docker.

---

## Diagram 6 — Stage-Level Execution Timeline

What actually runs and when. Within each stage, nodes are sequential (StageExecutor runs in topo order). Parallelism is between independent stages.

```mermaid
gantt
    title Stage Execution Timeline (machine_learning_config.yaml)
    dateFormat  X
    axisFormat %s

    section Params (seeded to store)
    model_type, do_preprocess     :done, 0, 1

    section Stage: preprocessing (memory)
    iris                          :active, 1, 2
    data                          :active, 2, 3
    preprocess_data + investigate_data :active, 3, 5
    WRITE preprocess_data to store :done, 5, 5

    section Stage: splitting (memory)
    split_data                    :active, 5, 6
    training_features             :active, 6, 7
    training_target               :active, 7, 8
    test_features                 :active, 8, 9
    test_target                   :active, 9, 10
    WRITE all 4 split outputs to store :done, 10, 10

    section Stage: training (docker)
    model - docker run + health poll :crit, 10, 13
    POST /execute - fit LogisticRegression :crit, 13, 16
    docker stop + rm              :crit, 16, 17
    WRITE model to store          :done, 17, 17

    section Stage: evaluation (memory)
    predictions                   :active, 17, 18
    classification_metrics        :active, 18, 19
    confusion_matrix              :active, 18, 19
```

> In the ML pipeline the stage DAG is a straight chain so stages run sequentially. The parallel dispatch infrastructure is in place — if two stages had no dependency between them they would fire simultaneously.

---

## Diagram 7 — Memoization: Stage-Level (First Run vs Second Run)

Memoization now operates at the **stage** level. An entire stage is skipped if all its boundary output nodes already exist in the artifact store.

```mermaid
flowchart LR
    subgraph Run1["First Run — nothing cached"]
        R1A["run_stage('training')"]
        R1B["all boundary outputs exist?\nartifact_store.exists('model') → False"]
        R1C["load inputs from store\nStageExecutor.run()\nDockerExecutor → model fitted"]
        R1D["artifact_store.put('model', result)\nwrite artifacts/ml_run_001/model.pkl"]
        R1A --> R1B --> R1C --> R1D
    end

    subgraph Run2["Second Run — entire stage skipped"]
        R2A["run_stage('training')"]
        R2B["all boundary outputs exist?\nartifact_store.exists('model') → True"]
        R2C["stage fully memoized — skipping\nno StageExecutor, no Docker container"]
        R2A --> R2B --> R2C
    end

    style R1C fill:#bbf,stroke:#66f
    style R2C fill:#9f9,stroke:#090
```

> To re-run one stage: delete its boundary output artifacts.  
> `del artifacts\ml_run_001\model.pkl` → training stage re-runs, evaluation re-runs downstream.  
> preprocessing and splitting stay cached.

---

## Diagram 8 — Docker Executor Lifecycle (for the `model` node)

Exactly what happens inside `DockerExecutor.execute()` — called from StageExecutor during the training stage.

```mermaid
sequenceDiagram
    participant SE as StageExecutor\nexecutor/stage_executor.py
    participant DE as DockerExecutor\nexecutor/docker.py
    participant D as Docker daemon
    participant W as worker/server.py\n(FastAPI inside container)

    SE->>DE: execute("model", model_fn, {training_features, training_target, model_type})

    DE->>DE: _free_port() → e.g. 54321
    DE->>D: docker run -d -p 54321:8000 fn_graph_worker_v2
    D-->>DE: container_id

    loop every 0.5s up to 30s
        DE->>W: GET /health
        W-->>DE: {"status": "ok"}
    end

    DE->>DE: inspect.getsource(model_fn) → fn_source
    DE->>DE: cloudpickle.dumps(kwargs) → base64 kwargs_b64

    DE->>W: POST /execute\n{node_name, fn_source, kwargs_b64}

    W->>W: exec(fn_source, namespace)
    W->>W: fn = namespace["model"]
    W->>W: kwargs = cloudpickle.loads(kwargs_b64)
    W->>W: result = fn(**kwargs) → fitted LogisticRegression
    W->>W: result_b64 = base64(cloudpickle.dumps(result))

    W-->>DE: {"result_b64": "..."}

    DE->>DE: result = cloudpickle.loads(result_b64)
    DE->>D: docker stop container_id
    DE->>D: docker rm container_id

    DE-->>SE: fitted LogisticRegression model
    SE->>SE: store result in-memory
    SE->>SE: model is boundary node → artifact_store.put("model", result)
```

---

## Diagram 9 — Class Hierarchy & Interfaces

All abstract contracts and concrete implementations including StageExecutor.

```mermaid
classDiagram
    class Composer {
        +_functions: dict
        +_parameters: dict
        +_cache
        +_tests
        +_source_map
        +update(*fns)
        +update_parameters(**params)
        +dag() DiGraph
        +ancestor_dag(outputs)
        +functions() dict
        +parameters() dict
        +calculate(outputs)
        +_copy(**kwargs)
        +_resolve_predecessors(node_name)
    }

    class PipelineComposer {
        +execution_config: dict
        +artifact_store: BaseArtifactStore
        +_config: dict
        +__init__(execution_config, artifact_store, pipeline_config)
        +_copy(**kwargs)
        +calculate(outputs) dict
        +_analyze_stage_boundaries(stages, dag) dict
        +_calculate_with_stages(...) dict
        -execute_node(node_name)
    }

    class BaseExecutor {
        <<abstract>>
        +execute(node_name, fn, kwargs) Any
    }

    class InMemoryExecutor {
        +execute(node_name, fn, kwargs) Any
    }

    class DockerExecutor {
        +image: str
        +execute(node_name, fn, kwargs) Any
        -_free_port() int
    }

    class LambdaExecutor {
        +function_name: str
        +region: str
        +execute(node_name, fn, kwargs) Any
    }

    class StageExecutor {
        +stage_name: str
        +stage_def: dict
        +node_functions: dict
        +stage_inputs: dict
        +stage_output_nodes: set
        +artifact_store: BaseArtifactStore
        +run()
    }

    class BaseArtifactStore {
        <<abstract>>
        +get(key) Any
        +put(key, value)
        +exists(key) bool
        +delete(key)
        +metadata(key) dict
    }

    class LocalFSArtifactStore {
        +base_dir: Path
        +run_id: str
        +get(key) Any
        +put(key, value)
        +exists(key) bool
        +delete(key)
        +metadata(key) dict
        -_path(key) Path
    }

    class S3ArtifactStore {
        +bucket: str
        +run_id: str
        +region: str
        +get(key) Any
        +put(key, value)
        +exists(key) bool
        +delete(key)
        +metadata(key) dict
    }

    Composer <|-- PipelineComposer
    BaseExecutor <|-- InMemoryExecutor
    BaseExecutor <|-- DockerExecutor
    BaseExecutor <|-- LambdaExecutor
    BaseArtifactStore <|-- LocalFSArtifactStore
    BaseArtifactStore <|-- S3ArtifactStore

    PipelineComposer --> StageExecutor : creates per stage
    PipelineComposer --> BaseArtifactStore : memoization checks
    StageExecutor --> BaseExecutor : dispatches per node
    StageExecutor --> BaseArtifactStore : boundary nodes only
```

---

## Diagram 10 — Config-Driven Routing: Stages vs Node-Level

How the YAML config drives both execution paths.

```mermaid
flowchart TD
    YAML["machine_learning_config.yaml"]

    LS["config.py · load_stages(config)\nconfig.get('stages', {})"]
    GNC["config.py · get_node_config(config, node_name)\nnodes.get(node_name) or nodes.get('*')"]

    YAML --> LS
    YAML --> GNC

    LS -->|"stages key present"| STAGE_PATH["_calculate_with_stages()\nStageExecutor per stage\nexecutor from stage_def"]
    LS -->|"no stages key"| NODE_PATH["node-level execute_node loop\nexecutor from execution_config"]

    STAGE_PATH -->|"stage executor: memory"| MEM["InMemoryExecutor()"]
    STAGE_PATH -->|"stage executor: docker"| DOC["DockerExecutor(image=fn_graph_worker_v2)"]
    STAGE_PATH -->|"stage executor: lambda"| LAM["LambdaExecutor(function_name, region)"]

    NODE_PATH --> GNC
    GNC -->|"node_name == model"| SPEC["node_config: docker + image"]
    GNC -->|"any other node"| WILD["node_config: memory  (wildcard)"]
    SPEC --> DOC
    WILD --> MEM

    style DOC fill:#bbf,stroke:#66f
    style MEM fill:#9f9,stroke:#090
    style LAM fill:#ffd,stroke:#aa0
```

---

## Diagram 11 — Stage Boundary Analysis

Which nodes are boundary outputs (written to disk), boundary inputs (read from disk), and internal (memory only) for the ML pipeline.

```mermaid
flowchart LR
    subgraph STORE["ArtifactStore  artifacts/ml_run_001/"]
        P1[("preprocess_data.pkl")]
        P2[("training_features.pkl\ntraining_target.pkl\ntest_features.pkl\ntest_target.pkl")]
        P3[("model.pkl")]
    end

    subgraph S1["Stage: preprocessing"]
        direction TB
        N1["iris"]
        N2["data"]
        N3["preprocess_data ← boundary output"]
        N4["investigate_data"]
        N1 --> N2 --> N3
        N2 --> N4
    end

    subgraph S2["Stage: splitting"]
        direction TB
        N5["split_data"]
        N6["training_features ← boundary output"]
        N7["training_target ← boundary output"]
        N8["test_features ← boundary output"]
        N9["test_target ← boundary output"]
        N5 --> N6 & N7 & N8 & N9
    end

    subgraph S3["Stage: training"]
        direction TB
        N10["model ← boundary output"]
    end

    subgraph S4["Stage: evaluation"]
        direction TB
        N11["predictions"]
        N12["classification_metrics"]
        N13["confusion_matrix"]
        N11 --> N12 & N13
    end

    N3 -->|"write"| P1
    P1 -->|"read"| N5

    N6 -->|"write"| P2
    N7 -->|"write"| P2
    N8 -->|"write"| P2
    N9 -->|"write"| P2
    P2 -->|"read"| N10
    P2 -->|"read"| N11

    N10 -->|"write"| P3
    P3 -->|"read"| N11
```

> Everything inside a stage box flows in Python memory. Disk is only touched at the arrows crossing stage lines.  
> 3 store writes per node-run vs 13 in the old node-level approach (iris, data, preprocess_data, investigate_data, split_data, training_features, training_target, test_features, test_target, model, predictions, classification_metrics, confusion_matrix).

---

## Diagram 12 — Parallel Stage Dispatch Loop

The exact algorithm inside `_calculate_with_stages()`.

```mermaid
flowchart TD
    A["build stage-level DAG\nfrom _analyze_stage_boundaries()"]
    B["ready_stages = [s for s if stage_dag.in_degree == 0]\n→ [preprocessing]"]
    C["submitted_stages = set(ready_stages)\nfuture_to_stage = {pool.submit(run_stage, s): s for s in ready_stages}"]
    D{"future_to_node empty?"}
    E["wait(future_to_stage.keys(), FIRST_COMPLETED)\n→ blocks until any stage finishes"]
    F["for future in done_futures:\n  stage_name = future_to_stage.pop(future)\n  future.result()  raises on exception\n  done_stages.add(stage_name)"]
    G["for downstream in stage_dag.successors(stage_name):\n  if downstream in submitted_stages: skip\n  pred_stages = stage_dag.predecessors(downstream)\n  if pred_stages subsetof done_stages:"]
    H["submitted_stages.add(downstream)\nfuture_to_stage[pool.submit(run_stage, downstream)] = downstream"]
    I["return all_results from artifact_store"]

    A --> B --> C --> D
    D -->|"no"| E --> F --> G
    G -->|"all preds done"| H --> D
    G -->|"preds not done yet"| D
    D -->|"yes"| I
```

---

## Quick Reference — File-to-Responsibility Map

| File | Class / Key Methods | Responsibility |
|---|---|---|
| `run_pipeline.py` | `main()`, `_setup_log()`, `_Tee` | CLI entry, logging, PipelineComposer construction with `pipeline_config` |
| `composer.py` | `PipelineComposer.__init__`, `_copy`, `calculate`, `_analyze_stage_boundaries`, `_calculate_with_stages` | DAG walk, stage boundary analysis, parallel stage dispatch, node-level fallback |
| `config.py` | `load_config`, `get_executor`, `get_artifact_store`, `get_node_config`, `load_stages` | YAML → object factory, wildcard resolution, stage definition loading |
| `machine_learning.py` | `f = Composer().update_parameters().update(...)` | Pure pipeline definition — no infra knowledge |
| `executor/base.py` | `BaseExecutor.execute` | Abstract contract for all executors |
| `executor/memory.py` | `InMemoryExecutor.execute` | `fn(**kwargs)` directly in process |
| `executor/docker.py` | `DockerExecutor.execute`, `_free_port` | Spin container → health poll → HTTP POST → stop |
| `executor/lambda_executor.py` | `LambdaExecutor.execute` | boto3 invoke → deserialize |
| `executor/stage_executor.py` | `StageExecutor.run` | Run all stage nodes in topo order in-memory, write only boundary nodes to store |
| `artifact_store/base.py` | `BaseArtifactStore` | Abstract contract: get/put/exists/delete/metadata |
| `artifact_store/fs.py` | `LocalFSArtifactStore`, `_path`, `put` (atomic) | `artifacts/{run_id}/{node}.pkl` via cloudpickle |
| `artifact_store/s3.py` | `S3ArtifactStore` | Same interface, S3 backend |
| `worker/server.py` | `GET /health`, `POST /execute` | FastAPI inside Docker: exec fn_source in fresh namespace, return result |
| `machine_learning_config.yaml` | `stages`, `artifact_store`, `nodes` | Runtime wiring — defines stages, executors, and store without touching Python |
