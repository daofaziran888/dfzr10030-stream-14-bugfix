"""事件路由交付服务：触发事件分区归属计算、目标批次与投递确认。

调用链：rule_engine 触发事件（fixtures.RULE_TRIGGERS，携带触发事件身份与规则身份）
      → event_router 路由规则（fixtures.EVENT_ROUTES，通过「触发规则」外键明确关联）
      → 本服务计算触发事件分区归属清单
      → 按归属目标写入待送批次（pipeline_orch 按清单接收明细）
      → 投递结果回写路由确认清单（同一触发事件只确认一次交付）。

关键约束：
- 关联只认「触发事件.规则编号 == 路由规则.触发规则」这一明确业务关系，
  绝不能用字典末项覆盖规则归属；
- 同一条触发规则有多个候选目标时，目标配置的优先级参与裁决，优先级仍无法
  区分的候选在归属清单里标 ambiguous；
- 缺匹配的事件沿用既有回退约定（default 管道），并进入无法路由对账；
- 新归属生效时旧事件仍在原目标待送批次里，记为归属冲突；已确认批次按原去向
  保留可回查；并发/重试提交同一个触发事件只产生一份交付。
"""
from __future__ import annotations

import threading
from datetime import datetime
from typing import Any

from app.fixtures import EVENT_ROUTES, RULE_TRIGGERS
from app.store import store

#: 缺匹配事件的既有回退去向
DEFAULT_PIPE = "default"

#: store 内的三张内部表
ASSIGN_TABLE = "_route_assignments"   # 触发事件分区归属清单
BATCH_TABLE = "_route_batches"        # 目标管道批次（待送/已确认）
DELIVERY_TABLE = "_route_deliveries"  # 路由确认清单（投递结果）

#: 进程内串行化归属计算与交付确认，保证并发提交只确认一次
_lock = threading.RLock()


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _priority_key(value: Any) -> tuple[int, Any]:
    """优先级数值越小越优先；无法解析为整数的配置排到后面且不报错。"""
    try:
        return (0, int(value))
    except (TypeError, ValueError):
        return (1, str(value))


def candidates_for_rule(rule_id: str, routes: list[dict] | None = None) -> list[dict]:
    """按明确业务关系取候选目标，并让目标配置的优先级参与归属判定。

    排序是全序且稳定的：优先级升序，同优先级再按路由规则编号、目标管道
    兜底排序——因此归属结果与候选的配置顺序无关（调换候选顺序可复现旧缺陷，
    但本实现结果不变）。
    """
    pool = EVENT_ROUTES if routes is None else routes
    matched = [r for r in pool if str(r.get("触发规则", "")) == str(rule_id)]
    return sorted(
        matched,
        key=lambda r: (
            _priority_key(r.get("优先级")),
            str(r.get("规则编号", "")),
            str(r.get("目标管道", "")),
        ),
    )


def build_assignments(
    triggers: list[dict] | None = None,
    routes: list[dict] | None = None,
) -> list[dict[str, Any]]:
    """计算触发事件分区归属清单（纯函数，不落盘）。"""
    trigger_list = RULE_TRIGGERS if triggers is None else triggers
    route_list = EVENT_ROUTES if routes is None else routes
    assignments: list[dict[str, Any]] = []
    for trigger in trigger_list:
        rule_id = str(trigger["规则编号"])
        candidates = candidates_for_rule(rule_id, route_list)
        candidate_summary = [
            {
                "规则编号": c.get("规则编号"),
                "目标管道": c.get("目标管道"),
                "优先级": c.get("优先级"),
            }
            for c in candidates
        ]
        base = {
            "触发事件ID": trigger["触发事件ID"],
            "规则编号": rule_id,
            "触发时间": trigger.get("触发时间"),
            "候选目标": candidate_summary,
            "归属冲突": None,
        }
        if not candidates:
            # 缺匹配：兼容既有回退约定，交给无法路由记录对账
            assignments.append({
                **base,
                "归属目标": DEFAULT_PIPE,
                "归属规则编号": None,
                "routed": False,
                "ambiguous": False,
                "说明": "无匹配路由规则，按既有约定回退 default 管道",
            })
            continue
        top = candidates[0]
        tied = [
            c for c in candidates
            if _priority_key(c.get("优先级")) == _priority_key(top.get("优先级"))
        ]
        ambiguous = len(tied) > 1
        assignments.append({
            **base,
            "归属目标": top.get("目标管道"),
            "归属规则编号": top.get("规则编号"),
            "routed": True,
            "ambiguous": ambiguous,
            "说明": "优先级无法区分候选，归属清单标出 ambiguous" if ambiguous
            else "按目标配置优先级裁决归属",
        })
    return assignments


def _find_pending_conflict(event_id: str, old_target: str | None) -> dict[str, Any] | None:
    """旧事件是否仍停留在原目标的待送批次里。"""
    if not old_target:
        return None
    for batch in store.rows(BATCH_TABLE):
        if (
            batch.get("状态") == "待送"
            and batch.get("目标管道") == old_target
            and event_id in batch.get("事件", [])
        ):
            return {
                "原目标": old_target,
                "原批次编号": batch.get("批次编号"),
            }
    return None


def recompute_assignments(
    triggers: list[dict] | None = None,
    routes: list[dict] | None = None,
) -> list[dict[str, Any]]:
    """重算归属清单并落盘；与上一版归属比对，标出待送批次上的归属冲突。

    只要旧事件仍停留在原目标的待送批次里，冲突标记就会在后续重算中保留；
    旧批次被确认或移除后，标记自动解除。
    """
    with _lock:
        previous = {a["触发事件ID"]: a for a in store.rows(ASSIGN_TABLE)}
        latest = build_assignments(triggers, routes)
        for assignment in latest:
            old = previous.get(assignment["触发事件ID"])
            old_target = old.get("归属目标") if old else None
            if old_target and old_target != assignment["归属目标"]:
                conflict = _find_pending_conflict(
                    assignment["触发事件ID"], old_target)
                if conflict is not None:
                    assignment["归属冲突"] = {
                        **conflict,
                        "新目标": assignment["归属目标"],
                        "说明": "新归属已生效，但旧事件仍在原目标的待送批次里",
                    }
            elif old and old.get("归属冲突"):
                # 归属未再变化：旧冲突在待送批次仍存在时继续保留
                conflict = _find_pending_conflict(
                    assignment["触发事件ID"],
                    old["归属冲突"].get("原目标"),
                )
                if conflict is not None:
                    assignment["归属冲突"] = old["归属冲突"]
        store.rows(ASSIGN_TABLE).clear()
        store.rows(ASSIGN_TABLE).extend(latest)
        return [dict(a) for a in latest]


def enqueue_pending(
    triggers: list[dict] | None = None,
    routes: list[dict] | None = None,
    assignments: list[dict] | None = None,
) -> list[dict[str, Any]]:
    """把归属清单写入各目标管道的待送批次；已在待送批次中的事件不重复入批。

    传入 assignments 时直接复用该清单（保留归属冲突标记），否则现场重算。
    """
    with _lock:
        if assignments is None:
            assignments = recompute_assignments(triggers, routes)
        batches = store.rows(BATCH_TABLE)
        for assignment in assignments:
            event_id = assignment["触发事件ID"]
            target = assignment["归属目标"]
            # 同一目标的待送批次不重复入批；换目标（归属冲突）时仍要进入新目标批次
            if any(
                b.get("状态") == "待送"
                and b.get("目标管道") == target
                and event_id in b.get("事件", [])
                for b in batches
            ):
                continue
            open_batch = next(
                (b for b in batches if b.get("状态") == "待送" and b.get("目标管道") == target),
                None,
            )
            if open_batch is None:
                open_batch = {
                    "批次编号": f"BATCH-{len(batches) + 1:04d}",
                    "目标管道": target,
                    "状态": "待送",
                    "事件": [],
                    "已确认事件": [],
                    "创建时间": _now(),
                    "确认时间": None,
                    "投递结果": None,
                }
                batches.append(open_batch)
            open_batch["事件"].append(event_id)
        return [dict(b) for b in batches]


def confirm_delivery(trigger_event_id: str, result: str = "成功") -> dict[str, Any]:
    """确认一次投递并回写路由确认清单。

    同一触发事件并发提交或重试落盘时，直接返回首份确认记录（累计重复提交
    次数），保证只确认一次交付、不产生第二/第三份交付。
    """
    with _lock:
        deliveries = store.rows(DELIVERY_TABLE)
        existing = next(
            (d for d in deliveries if d.get("触发事件ID") == trigger_event_id), None)
        if existing is not None:
            existing["重复提交"] = int(existing.get("重复提交", 0)) + 1
            return dict(existing)

        assignment = next(
            (a for a in store.rows(ASSIGN_TABLE) if a.get("触发事件ID") == trigger_event_id),
            None,
        )
        target = assignment["归属目标"] if assignment else DEFAULT_PIPE
        batch = next(
            (b for b in store.rows(BATCH_TABLE)
             if b.get("状态") == "待送" and trigger_event_id in b.get("事件", [])),
            None,
        )
        record = {
            "触发事件ID": trigger_event_id,
            "规则编号": assignment["规则编号"] if assignment else None,
            "目标管道": target,
            "批次编号": batch["批次编号"] if batch else None,
            "投递结果": result,
            "确认时间": _now(),
            "重复提交": 0,
        }
        deliveries.append(record)
        if batch is not None:
            batch["事件"].remove(trigger_event_id)
            batch.setdefault("已确认事件", []).append(trigger_event_id)
            if not batch["事件"]:
                batch["状态"] = "已确认"
                batch["确认时间"] = record["确认时间"]
                batch["投递结果"] = result
        return dict(record)


# ---------------------------------------------------------------------------
# 查询口径
# ---------------------------------------------------------------------------

def list_assignments(
    target: str | None = None,
    *,
    ambiguous: bool | None = None,
    routed: bool | None = None,
) -> list[dict[str, Any]]:
    """路由查询入口：按目标排序，可筛选歧义候选与是否匹配到路由。"""
    rows = [dict(a) for a in store.rows(ASSIGN_TABLE)]
    if target:
        rows = [a for a in rows if a.get("归属目标") == target]
    if ambiguous is not None:
        rows = [a for a in rows if bool(a.get("ambiguous")) == ambiguous]
    if routed is not None:
        rows = [a for a in rows if bool(a.get("routed")) == routed]
    return sorted(rows, key=lambda a: (str(a.get("归属目标", "")), str(a.get("触发事件ID", ""))))


def list_batches(target: str | None = None, status: str | None = None) -> list[dict[str, Any]]:
    """批次回查：已确认批次也按原去向保留，可按目标与状态过滤。"""
    rows = [dict(b) for b in store.rows(BATCH_TABLE)]
    if target:
        rows = [b for b in rows if b.get("目标管道") == target]
    if status:
        rows = [b for b in rows if b.get("状态") == status]
    return sorted(rows, key=lambda b: (str(b.get("目标管道", "")), str(b.get("批次编号", ""))))


def list_deliveries(target: str | None = None) -> list[dict[str, Any]]:
    """路由确认清单：投递结果按目标排序回查。"""
    rows = [dict(d) for d in store.rows(DELIVERY_TABLE)]
    if target:
        rows = [d for d in rows if d.get("目标管道") == target]
    return sorted(rows, key=lambda d: (str(d.get("目标管道", "")), str(d.get("触发事件ID", ""))))


def reception_detail(
    target: str | None = None,
    *,
    received_only: bool = False,
) -> dict[str, Any]:
    """管道接收详情：已接收事件可筛选，并与无法路由的记录对账。

    对账口径：已接收事件必须来自匹配到路由的归属记录，无法路由事件单独成列；
    「已接收 + 无法路由 + 仍在待送批次」三者之和应等于归属事件总数。
    """
    all_assignments = list_assignments(target)
    received = list_deliveries(target)
    unroutable = list_assignments(target, routed=False)
    pending_events = {
        event_id
        for batch in list_batches(target, status="待送")
        for event_id in batch.get("事件", [])
    }
    received_ids = {d["触发事件ID"] for d in received}
    routed_ids = {a["触发事件ID"] for a in all_assignments if a.get("routed")}
    unroutable_ids = {a["触发事件ID"] for a in unroutable}
    accounted = received_ids | unroutable_ids | pending_events
    summary = {
        "归属事件总数": len(all_assignments),
        "已接收事件数": len(received),
        "无法路由事件数": len(unroutable),
        "待送事件数": len(pending_events),
        "歧义候选事件数": len(list_assignments(target, ambiguous=True)),
        "归属冲突事件数": sum(1 for a in all_assignments if a.get("归属冲突")),
        # 已接收事件全部来自可路由归属，且没有事件游离在三类口径之外
        "对账平衡": received_ids <= routed_ids and accounted == {a["触发事件ID"] for a in all_assignments},
    }
    return {
        "目标管道": target or "全部",
        "received": received,
        # 只看已接收时把无法路由明细收敛为空，汇总口径保持全量以便对账
        "unroutable": [] if received_only else unroutable,
        "summary": summary,
    }
