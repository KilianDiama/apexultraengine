import asyncio
import hashlib
import inspect
import ast
import time
import json
import os
import pickle
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Type, TypeVar, AsyncGenerator, Set
from concurrent.futures import ProcessPoolExecutor

import aiosqlite
from pydantic import BaseModel, TypeAdapter
from rich.console import Console
from graphlib import TopologicalSorter

T = TypeVar("T")
console = Console()

@dataclass(slots=True, frozen=True)
class Telemetry:
    latency: float = 0.0
    status: str = "pending"
    cache_hit: bool = False
    retries: int = 0
    error: Optional[str] = None
    output: Any = None

class ApexUltraEngine:
    def __init__(self, max_parallel: int = 12, cache_path: str = "apex_vault.db", default_ttl: float = 3600):
        self.registry: Dict[str, 'ApexNode'] = {}
        self.semaphore = asyncio.Semaphore(max_parallel)
        self.cache_path = cache_path
        self.default_ttl = default_ttl
        # Gestion propre de l'executor
        self._executor = ProcessPoolExecutor(max_workers=os.cpu_count() or 4)

    async def shutdown(self):
        """Nettoyage chirurgical des ressources."""
        self._executor.shutdown(wait=True)
        console.print("[bold red]➔ Engine Shutdown Complete.[/]")

    @asynccontextmanager
    async def session(self):
        async with aiosqlite.connect(self.cache_path) as db:
            await db.execute("PRAGMA journal_mode=WAL")
            await db.execute("PRAGMA synchronous=NORMAL")
            await db.execute(
                "CREATE TABLE IF NOT EXISTS vault (key TEXT PRIMARY KEY, val BLOB, ts REAL)"
            )
            yield db

    def step(self, schema: Optional[Type[T]] = None, **cfg):
        def wrapper(func):
            node = ApexNode(func.__name__, func, schema, engine=self, **cfg)
            self.registry[node.name] = node
            return func
        return wrapper

    async def stream(self, seed: Dict[str, Any]) -> AsyncGenerator[tuple[str, Any, Telemetry], None]:
        state = {**seed}
        failed_nodes: Set[str] = set()
        
        # Filtrage dynamique des nœuds dont les dépendances sont satisfaites par le seed ou le registry
        graph = {name: (node.deps & (self.registry.keys() | seed.keys()))
                 for name, node in self.registry.items()}
        
        sorter = TopologicalSorter(graph)
        sorter.prepare()

        async with self.session() as db:
            while sorter.is_active():
                ready_nodes = sorter.get_ready()
                if not ready_nodes: break

                # Utilisation de return_exceptions=True pour une isolation totale des pannes
                tasks = []
                node_names = []
                
                for name in ready_nodes:
                    node = self.registry[name]
                    if any(dep in failed_nodes for dep in node.deps):
                        failed_nodes.add(name)
                        sorter.done(name)
                        continue
                    
                    node_inputs = {k: state[k] for k in node.deps if k in state}
                    tasks.append(node.run(node_inputs, db))
                    node_names.append(name)

                if not tasks: continue

                results = await asyncio.gather(*tasks, return_exceptions=True)

                for name, result in zip(node_names, results):
                    sorter.done(name)
                    if isinstance(result, tuple):
                        res, telemetry = result
                        if telemetry.status == "success":
                            state[name] = res
                        else:
                            failed_nodes.add(name)
                        yield name, res, telemetry
                    else:
                        # Cas d'exception non gérée par ApexNode
                        failed_nodes.add(name)
                        yield name, None, Telemetry(status="failed", error=str(result))

class ApexNode:
    def __init__(self, name: str, logic: Callable, schema: Optional[Type[T]], engine: ApexUltraEngine, **config):
        self.name = name
        self.logic = logic
        self.engine = engine
        self.adapter = TypeAdapter(schema) if schema else None
        self.config = {"retries": 3, "timeout": 30.0, "backoff": 0.5, "ttl": engine.default_ttl, **config}
        self.deps = set(inspect.signature(logic).parameters.keys())
        self.logic_hash = self._generate_logic_hash()

    def _generate_logic_hash(self) -> str:
        try:
            source = inspect.getsource(self.logic)
            # Hash étendu à 32 chars pour éviter toute collision
            return hashlib.sha256(ast.dump(ast.parse(source)).encode()).hexdigest()[:32]
        except:
            return hashlib.sha256(f"{self.logic.__name__}:{id(self.logic)}".encode()).hexdigest()[:32]

    async def run(self, inputs: Dict[str, Any], db: aiosqlite.Connection) -> tuple[Any, Telemetry]:
        # Empreinte basée sur le contenu binaire pour supporter les objets complexes
        input_blob = pickle.dumps(inputs)
        fingerprint = hashlib.blake2b(f"{self.logic_hash}".encode() + input_blob).hexdigest()

        # 1. Cache Check (Pickle pour la vitesse et la flexibilité)
        async with db.execute(
            "SELECT val FROM vault WHERE key = ? AND (?-ts) < ?",
            (fingerprint, time.time(), self.config["ttl"])
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                data = pickle.loads(row[0])
                return data, Telemetry(status="success", cache_hit=True, output=data)

        # 2. Execution Loop
        async with self.engine.semaphore:
            for attempt in range(self.config["retries"] + 1):
                start = time.perf_counter()
                try:
                    if asyncio.iscoroutinefunction(self.logic):
                        res = await asyncio.wait_for(self.logic(**inputs), timeout=self.config["timeout"])
                    else:
                        loop = asyncio.get_running_loop()
                        res = await loop.run_in_executor(self.engine._executor, lambda: self.logic(**inputs))

                    # Validation Pydantic optionnelle
                    validated = self.adapter.validate_python(res) if self.adapter else res
                    
                    # Store as Binary Pickle
                    await db.execute(
                        "INSERT OR REPLACE INTO vault VALUES (?, ?, ?)",
                        (fingerprint, pickle.dumps(validated), time.time())
                    )
                    await db.commit()

                    return validated, Telemetry(
                        latency=time.perf_counter() - start,
                        status="success",
                        retries=attempt,
                        output=validated
                    )
                except Exception as e:
                    if attempt == self.config["retries"]:
                        return None, Telemetry(status="failed", retries=attempt, error=str(e))
                    await asyncio.sleep(self.config["backoff"] * (2 ** attempt))

# --- TEST FINAL 10/10 ---
if __name__ == "__main__":
    engine = ApexUltraEngine()

    @engine.step()
    async def get_data(): return {"val": 42}

    @engine.step()
    def process(get_data: dict): return get_data["val"] * 2

    async def run():
        try:
            async for node, out, tel in engine.stream({}):
                console.print(f"[bold cyan]✔ {node}[/] -> {out} ({tel.status})")
        finally:
            await engine.shutdown()

    asyncio.run(run())
