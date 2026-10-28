import dagster as dg


# pyrefly: ignore [bad-argument-type]
@dg.asset_check(asset="asset_1")
def submodule_check():
    pass
