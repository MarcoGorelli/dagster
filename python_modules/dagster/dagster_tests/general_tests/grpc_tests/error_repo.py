import dagster as dg


# pyrefly: ignore [bad-argument-type]
@dg.repository
def error_repo():
    a = None
    a()  # ty: ignore[call-non-callable]
