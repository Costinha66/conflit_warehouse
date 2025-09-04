from src.core.metrics import DQResult


def DQBuilderBronze(metrics: dict, cutoff_year: int, start_year: int):
    metrics["records"] = metrics["records"] or 0
    metrics["bytes"] = metrics["bytes"] or 0
    metrics["hash"] = metrics["hash"] or ""
    cutoff_condition = start_year <= cutoff_year

    dq_passed = (
        cutoff_condition
        and metrics["records"] >= 0
        and metrics["bytes"] > 0
        and metrics["hash"] != ""
    )
    level = "MINOR" if dq_passed else "CRITICAL"

    return DQResult(passed=dq_passed, level=level, metrics=metrics)
