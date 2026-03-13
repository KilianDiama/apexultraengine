# ApexUltraEngine

A lightweight, asynchronous DAG engine for Python with caching, retries, telemetry, and type validation.

---

## Features

- **Asynchronous & Parallel Execution**  
  Run tasks concurrently while respecting dependencies using asyncio and semaphores.

- **Dependency Management**  
  Automatic topological sorting of tasks with dynamic dependency resolution.

- **Caching**  
  Results are cached with TTL support to avoid recalculations.

- **Retries & Backoff**  
  Automatic retries with exponential backoff for robust execution.

- **Telemetry & Observability**  
  Track latency, status, retries, cache hits, and errors for each task.

- **Type Validation**  
  Optional Pydantic validation for task outputs.

---

## Installation


git clone https://github.com/KilianDiama/apexultraengine.git
cd apexultraengine
pip install -r requirements.txt
Usage Example
import asyncio
from apexultraengine import ApexUltraEngine

engine = ApexUltraEngine()

@engine.step()
async def get_data():
    return {"val": 42}

@engine.step()
def process(get_data: dict):
    return get_data["val"] * 2

async def run():
    async for node, out, telemetry in engine.stream({}):
        print(f"{node} -> {out} ({telemetry.status})")
    await engine.shutdown()

asyncio.run(run())
License & Commercial Use

Personal / Educational Use: Free and open-source. You can use, modify, and distribute for non-commercial purposes.

Commercial Use: Any use generating revenue requires a commercial license.

Royalties range from 8% to 30% depending on revenue and usage.

Contact the author to obtain a license: [your email/contact info].

Contributing

Contributions are welcome! Please submit issues or pull requests for bug fixes and improvements.

Disclaimer

This software is provided "as is" without any warranty. The author is not liable for damages arising from its use.
