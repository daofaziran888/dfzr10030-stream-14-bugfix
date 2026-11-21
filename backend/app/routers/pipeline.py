"""跨模块管道接口：触发执行并查询结果。"""
from __future__ import annotations

from fastapi import APIRouter, Query

from app.services.event_delivery import reception_detail
from app.services.pipeline_orchestrator import (
    run_all_pipelines, list_pipeline_results,
    run_source_cdc_pipeline, run_window_metric_pipeline,
    run_rule_router_pipeline, run_metric_sink_pipeline,
    run_quality_scan, run_partition_backpressure_pipeline,
    run_checkpoint_state_pipeline,
)

router = APIRouter(prefix="/api/pipeline", tags=["管道编排"])


@router.get("/reception")
def get_reception(
    target: str | None = Query(default=None, description="按目标管道筛选接收详情"),
    received_only: bool = Query(default=False, description="只看已接收（已确认交付）的事件"),
) -> dict:
    """管道接收详情：可筛出已接收事件，并与无法路由的记录对账。"""
    return {"ok": True, "detail": reception_detail(target, received_only=received_only)}


@router.post("/run-all")
def run_all() -> dict:
    return {"ok": True, "results": run_all_pipelines()}


@router.get("/results")
def list_results() -> dict:
    return {"ok": True, "results": list_pipeline_results()}


@router.post("/source-cdc")
def trigger_source_cdc() -> dict:
    return {"ok": True, "result": run_source_cdc_pipeline()}


@router.post("/window-metric")
def trigger_window_metric() -> dict:
    return {"ok": True, "result": run_window_metric_pipeline()}


@router.post("/rule-router")
def trigger_rule_router() -> dict:
    return {"ok": True, "result": run_rule_router_pipeline()}


@router.post("/metric-sink")
def trigger_metric_sink() -> dict:
    return {"ok": True, "result": run_metric_sink_pipeline()}


@router.post("/quality-scan")
def trigger_quality_scan() -> dict:
    return {"ok": True, "result": run_quality_scan()}


@router.post("/partition-bp")
def trigger_partition_bp() -> dict:
    return {"ok": True, "result": run_partition_backpressure_pipeline()}


@router.post("/checkpoint-state")
def trigger_checkpoint_state() -> dict:
    return {"ok": True, "result": run_checkpoint_state_pipeline()}