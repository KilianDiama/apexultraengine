ApexUltraEngine

An asynchronous DAG engine for Python – orchestrate tasks reliably with caching, retries/backoff, and telemetry.

🔥 Features

✅ Define DAGs (Directed Acyclic Graphs) to orchestrate tasks.

✅ Asynchronous and parallel task execution.

✅ Configurable retries & backoff.

✅ Result caching to avoid unnecessary recomputation.

✅ Type validation and robust error handling.

✅ Built-in telemetry & logging for workflow monitoring.

✅ Easily extensible and integrable with existing Python projects.

🚀 Installation
pip install apexultraengine

The package is available via PyPI and GitHub (or you can clone the repository).

📝 Basic Usage
from apexultraengine import DAG, Task

# Define a task
class HelloTask(Task):
    async def run(self):
        print("Hello, ApexUltraEngine!")
        return "Done"

# Create a DAG
dag = DAG("ExampleDAG")
dag.add_task(HelloTask())

# Run the DAG
dag.run()
Advanced Example with Dependencies
class TaskA(Task):
    async def run(self):
        return 1

class TaskB(Task):
    async def run(self, input_a):
        return input_a + 2

dag = DAG("AdvancedDAG")
dag.add_task(TaskA(), name="A")
dag.add_task(TaskB(), name="B", dependencies=["A"])
dag.run()
⚙️ Configuration

Retries & Backoff – configure the number of attempts and backoff interval per task.

Cache – enable caching for expensive computations.

Logging – full logging for every execution step.

🧩 Contribution

Want to contribute?

Fork the repository

Create a branch (git checkout -b feature/my-feature)

Commit your changes (git commit -am 'Add new feature')

Push (git push origin feature/my-feature)

Open a Pull Request

📄 License

ApexUltraEngine is under a commercial license. Contact the owner for production use or redistribution.
Disclaimer

This software is provided "as is" without any warranty. The author is not liable for damages arising from its use.
