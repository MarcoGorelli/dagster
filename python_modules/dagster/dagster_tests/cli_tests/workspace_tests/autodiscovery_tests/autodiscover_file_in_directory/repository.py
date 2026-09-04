import dagster as dg

# pyrefly: ignore [missing-import]
from autodiscover_src.jobs import hello_world_job


@dg.repository
def hello_world_repository():
    return [hello_world_job]
