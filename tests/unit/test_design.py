"""Small checks for the runner boundary and the lightweight DAG design."""

import ast
import unittest
from pathlib import Path

from worker.server import commands


class RunnerTests(unittest.TestCase):
    def test_spark_runs_as_an_external_process(self):
        for stage in ["ingest", "transform", "load", "verify"]:
            with self.subTest(stage=stage):
                command = commands(stage, "2026-01-15", False)[0]
                self.assertEqual(command[0], "spark-submit")
                self.assertIn("--driver-memory", command)

    def test_failure_switch_does_not_start_spark(self):
        command = commands("ingest", "2026-01-15", True)[0]
        self.assertEqual(command[0], "python")
        self.assertNotIn("spark-submit", command)

    def test_dag_does_not_import_processing_modules(self):
        tree = ast.parse(Path("orchestration/bank_batch.py").read_text())
        modules = [node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)]
        self.assertFalse(any(name.startswith(("jobs", "pyspark", "dbt")) for name in modules))
        self.assertFalse(
            any(
                isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) for node in ast.walk(tree)
            )
        )


if __name__ == "__main__":
    unittest.main()
