import dagster as dg

# pyrefly: ignore [missing-import]
from ops import example_two_op


@dg.job
def example_two_job():
    example_two_op()
