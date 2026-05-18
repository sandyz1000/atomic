"""
Entry point for the py-demo examples.

Runs the local word count job followed by the range sum job.
For distributed mode see src/distributed_word_count.py.

Usage:
  python main.py
"""

import runpy
import os

_here = os.path.dirname(os.path.abspath(__file__))

print("=== Local Word Count ===")
runpy.run_path(os.path.join(_here, "src", "local_word_count.py"))

print()
print("=== Local Sum Job ===")
runpy.run_path(os.path.join(_here, "src", "local_sum_job.py"))
