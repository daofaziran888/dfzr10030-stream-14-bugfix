"""事件路由接口：维护路由规则，覆盖启用路由、停用路由、归档规则等动作。"""
from __future__ import annotations

import csv
import io

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from app.schemas import ActionPayload, ActionResult, PageResult
from app.services.event_delivery import (
    confirm_delivery, list_assignments, list_batches, list_deliveries,
)
from app.services.event_router import EventRouterService

router = APIRouter(prefix="/api/event_router", tags=["事件路由"])

service = EventRouterService()

LIST_FIELDS = ["规则编号", "事件类型", "匹配条件", "目标管道", "优先级", "分区键", "重试上限", "DLQ策略"]
STATUSES = ["草稿", "路由中", "已停用", "已被拒"]


class DeliveryPayload(BaseModel):
    """投递确认载荷：按触发事件身份确认，重试落盘只产生一份交付。"""

    触发事件ID: str
    投递结果: str = "成功"


def query_filters(request: Request) -> dict[str, str]:
    """只接受当前模块声明过的字段，忽略分页等保留参数。"""
    return {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in LIST_FIELDS and value.strip()
    }


@router.get("", response_model=PageResult[dict])
def list_entries(
    request: Request,
    keyword: str | None = Query(default=None, description="按规则编号检索"),
    status: str | None = Query(default=None, description="草稿、路由中、已停用、已被拒"),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    """按页面字段、规则编号关键字与状态过滤事件路由列表。"""
    items, total = service.list_entries(
        filters=query_filters(request), keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/export")
def export_entries(
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    """下载当前筛选条件下的 UTF-8 CSV；必须声明在 /{entry_id} 之前。"""
    items, _ = service.list_entries(
        filters=query_filters(request), keyword=keyword, status=status, page=1, size=1000000)
    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *LIST_FIELDS, "status"])
    for item in items:
        row = []
        for value in [item.get("id"), *(item.get(field, "") for field in LIST_FIELDS), item.get("status", "")]:
            text = "" if value is None else str(value)
            row.append("'" + text if text.startswith(("=", "+", "-", "@")) else text)
        writer.writerow(row)
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="event_router.csv"'})


@router.get("/assignments")
def get_assignments(
    target: str | None = Query(default=None, description="按归属目标管道筛选"),
    ambiguous: bool | None = Query(default=None, description="只看优先级仍无法区分的歧义候选"),
    routed: bool | None = Query(default=None, description="是否匹配到路由；false 即无法路由记录"),
) -> dict:
    """触发事件分区归属清单：路由查询入口，按目标排序。"""
    return {"ok": True, "items": list_assignments(target, ambiguous=ambiguous, routed=routed)}


@router.get("/batches")
def get_batches(
    target: str | None = Query(default=None, description="按目标管道筛选"),
    status: str | None = Query(default=None, description="待送/已确认"),
) -> dict:
    """目标批次回查：已确认批次按原去向保留可查。"""
    return {"ok": True, "items": list_batches(target, status)}


@router.get("/deliveries")
def get_deliveries(target: str | None = Query(default=None)) -> dict:
    """路由确认清单：投递结果按目标排序回查。"""
    return {"ok": True, "items": list_deliveries(target)}


@router.post("/deliveries/confirm", response_model=ActionResult)
def post_delivery_confirm(payload: DeliveryPayload) -> ActionResult:
    """确认一次投递并回写确认清单；并发/重试提交同一触发事件只确认一次。"""
    event_id = payload.触发事件ID.strip()
    if not event_id:
        return ActionResult(ok=False, message="缺少触发事件ID，无法确认交付")
    record = confirm_delivery(event_id, payload.投递结果.strip() or "成功")
    repeated = int(record.get("重复提交", 0))
    message = (
        f"触发事件 {event_id} 已确认交付（第 {repeated + 1} 次提交，仍只保留一份交付）"
        if repeated else f"触发事件 {event_id} 已确认交付"
    )
    return ActionResult(ok=True, message=message, entry=record)


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条路由规则明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"路由规则 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: dict[str, object]) -> ActionResult:
    """登记一条路由规则，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="路由规则已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    """对单条路由规则执行启用路由、停用路由、归档规则；不允许的动作会被拦下并说明原因。"""
    entry, message = service.run_action(entry_id, payload.action.strip())
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)
