import dagster as dg

# pyrefly: ignore [missing-import]
from ops import example_one_op


@dg.job
def example_one_job():
    example_one_op()
