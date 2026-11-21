"""事件路由归属修复测试：覆盖从规则触发到目标批次交付的完整调用链。

场景对应需求：
1. 同一规则两个可匹配目标，候选末项不得覆盖规则归属（调换候选顺序复现旧缺陷）；
2. 触发规则与路由按明确业务关系（触发规则外键）关联；
3. 目标配置优先级参与归属判定，无法区分的候选在清单标出 ambiguous；
4. 缺匹配事件兼容既有 default 回退约定并进入无法路由对账；
5. 新归属生效而旧事件仍在原目标待送批次时标出归属冲突，已确认批次按原去向回查；
6. 并发/重试提交同一触发事件只确认一次交付，重试落盘不产生第二份交付。
"""
from __future__ import annotations

import threading

import pytest

from app.fixtures import EVENT_ROUTES, RULE_TRIGGERS
from app.services import event_delivery as ed
from app.services.pipeline_orchestrator import run_rule_router_pipeline


def _assignment(assignments, event_id):
    return next(a for a in assignments if a["触发事件ID"] == event_id)


@pytest.fixture()
def routed():
    """跑通 rule_engine -> event_router -> pipeline_orch 的归属与入批。"""
    return run_rule_router_pipeline()


class TestAttributionLogic:
    def test_trigger_and_route_joined_by_explicit_business_relation(self, routed):
        # RULE-HIGH-VALUE 必须通过 EVENT_ROUTES.触发规则 关联，而不是同名字典查找
        a = _assignment(routed["assignments"], "TRIG-0001")
        assert a["规则编号"] == "RULE-HIGH-VALUE"
        assert len(a["候选目标"]) == 2
        assert {c["规则编号"] for c in a["候选目标"]} == {
            "ROUTE-HIGH-VALUE-PRIMARY", "ROUTE-HIGH-VALUE-SECONDARY"}

    def test_priority_decides_when_two_targets_match(self, routed):
        # 配置顺序里低优先目标在前，但优先级数值小者胜出
        a = _assignment(routed["assignments"], "TRIG-0001")
        assert a["归属目标"] == "pipeline-realtime"
        assert a["归属规则编号"] == "ROUTE-HIGH-VALUE-PRIMARY"
        assert a["ambiguous"] is False
        assert a["routed"] is True

    def test_swapping_candidate_order_reproduces_bug_but_fix_is_stable(self):
        """调换两个候选的配置顺序：旧实现会取末项，正确实现结果不变。"""
        swapped = list(EVENT_ROUTES)
        idx_a = next(i for i, r in enumerate(swapped) if r["规则编号"] == "ROUTE-HIGH-VALUE-SECONDARY")
        idx_b = next(i for i, r in enumerate(swapped) if r["规则编号"] == "ROUTE-HIGH-VALUE-PRIMARY")
        swapped[idx_a], swapped[idx_b] = swapped[idx_b], swapped[idx_a]
        # 确认调换生效：末项现在是低优先目标（旧实现的末项覆盖会选错）
        assert swapped[idx_b]["规则编号"] == "ROUTE-HIGH-VALUE-SECONDARY"

        assignments = ed.build_assignments(routes=swapped)
        a = _assignment(assignments, "TRIG-0001")
        assert a["归属目标"] == "pipeline-realtime"
        assert a["归属规则编号"] == "ROUTE-HIGH-VALUE-PRIMARY"

    def test_equal_priority_candidates_flagged_ambiguous(self, routed):
        a = _assignment(routed["assignments"], "TRIG-0004")
        assert a["ambiguous"] is True
        assert len(a["候选目标"]) == 2
        assert {c["目标管道"] for c in a["候选目标"]} == {"pipeline-geo-a", "pipeline-geo-b"}
        # 仍给出确定性归属（全序排序），但清单显式标出无法仅靠优先级区分
        assert a["归属目标"] in {"pipeline-geo-a", "pipeline-geo-b"}

    def test_no_match_falls_back_to_default_and_is_reconcilable(self, routed):
        a = _assignment(routed["assignments"], "TRIG-0005")
        assert a["routed"] is False
        assert a["归属目标"] == "default"
        assert a["归属规则编号"] is None
        unroutable = ed.list_assignments(routed=False)
        assert [x["触发事件ID"] for x in unroutable] == ["TRIG-0005"]

    def test_assignment_query_sorted_by_target(self, routed):
        items = ed.list_assignments()
        targets = [i["归属目标"] for i in items]
        assert targets == sorted(targets)
        # 目标过滤口径
        only = ed.list_assignments("pipeline-realtime")
        assert all(i["归属目标"] == "pipeline-realtime" for i in only)
        assert [i["触发事件ID"] for i in only] == ["TRIG-0001"]


class TestBatchesAndDelivery:
    def test_pending_batches_follow_assignment_targets(self, routed):
        batches = ed.list_batches(status="待送")
        target_of = {}
        for b in batches:
            for event_id in b["事件"]:
                target_of[event_id] = b["目标管道"]
        for a in routed["assignments"]:
            assert target_of[a["触发事件ID"]] == a["归属目标"]

    def test_confirm_delivery_writes_back_once_and_moves_batch(self, routed):
        record = ed.confirm_delivery("TRIG-0001")
        assert record["目标管道"] == "pipeline-realtime"
        assert record["投递结果"] == "成功"
        assert record["批次编号"] is not None

        deliveries = ed.list_deliveries()
        assert [d["触发事件ID"] for d in deliveries] == ["TRIG-0001"]

        # 该事件已离开待送批次
        pending = ed.list_batches(status="待送", target="pipeline-realtime")
        assert all("TRIG-0001" not in b["事件"] for b in pending)

    def test_retry_persists_no_second_delivery(self, routed):
        first = ed.confirm_delivery("TRIG-0002")
        second = ed.confirm_delivery("TRIG-0002")  # 重试落盘
        third = ed.confirm_delivery("TRIG-0002")   # 再次重试
        assert first["确认时间"] == second["确认时间"] == third["确认时间"]
        assert third["重复提交"] == 2
        deliveries = ed.list_deliveries("pipeline-risk")
        assert len(deliveries) == 1
        assert deliveries[0]["触发事件ID"] == "TRIG-0002"

    def test_concurrent_confirm_only_one_delivery(self, routed):
        results: list[dict] = []
        barrier = threading.Barrier(8)

        def worker():
            barrier.wait()
            results.append(ed.confirm_delivery("TRIG-0003"))

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        deliveries = ed.list_deliveries("pipeline-user-profile")
        target_records = [d for d in deliveries if d["触发事件ID"] == "TRIG-0003"]
        assert len(target_records) == 1
        assert target_records[0]["重复提交"] == 7  # 8 次提交，仅首份成为交付
        assert len({r["确认时间"] for r in results}) == 1

    def test_confirmed_batch_remains_queryable_at_original_target(self, routed):
        record = ed.confirm_delivery("TRIG-0002")
        confirmed = ed.list_batches(target="pipeline-risk", status="已确认")
        assert len(confirmed) == 1
        batch = confirmed[0]
        assert batch["批次编号"] == record["批次编号"]
        assert "TRIG-0002" in batch["已确认事件"]
        assert batch["投递结果"] == "成功"

    def test_reception_detail_filters_received_and_reconciles_unroutable(self, routed):
        ed.confirm_delivery("TRIG-0001")
        ed.confirm_delivery("TRIG-0002")

        detail = ed.reception_detail()
        assert {r["触发事件ID"] for r in detail["received"]} == {"TRIG-0001", "TRIG-0002"}
        assert {u["触发事件ID"] for u in detail["unroutable"]} == {"TRIG-0005"}
        assert detail["summary"]["已接收事件数"] == 2
        assert detail["summary"]["无法路由事件数"] == 1
        assert detail["summary"]["歧义候选事件数"] == 1
        assert detail["summary"]["归属事件总数"] == len(RULE_TRIGGERS)

        target_view = ed.reception_detail("pipeline-realtime")
        assert [r["触发事件ID"] for r in target_view["received"]] == ["TRIG-0001"]


class TestAttributionConflict:
    def test_new_attribution_conflicts_with_old_pending_batch(self):
        # 首版归属：TRIG-0001 -> pipeline-realtime，进入待送批次
        first = run_rule_router_pipeline()
        old = _assignment(first["assignments"], "TRIG-0001")
        assert old["归属目标"] == "pipeline-realtime"
        pending_before = ed.list_batches(target="pipeline-realtime", status="待送")
        assert any("TRIG-0001" in b["事件"] for b in pending_before)

        # 调整路由配置：新归属切到 pipeline-archive（原低优先目标升为最高优先）
        updated = [dict(r) for r in EVENT_ROUTES]
        for r in updated:
            if r["规则编号"] == "ROUTE-HIGH-VALUE-PRIMARY":
                r["优先级"] = 5
        # 用新配置重算归属并重新入批
        recomputed = ed.recompute_assignments(routes=updated)
        ed.enqueue_pending(assignments=recomputed)

        new = _assignment(recomputed, "TRIG-0001")
        assert new["归属目标"] == "pipeline-archive"
        conflicted = _assignment(recomputed, "TRIG-0001")
        assert conflicted["归属冲突"] is not None
        assert conflicted["归属冲突"]["原目标"] == "pipeline-realtime"
        assert conflicted["归属冲突"]["新目标"] == "pipeline-archive"

        # 旧事件仍在原目标的待送批次里，按原去向可回查
        old_pending = ed.list_batches(target="pipeline-realtime", status="待送")
        assert any("TRIG-0001" in b["事件"] for b in old_pending)
        new_pending = ed.list_batches(target="pipeline-archive", status="待送")
        assert any("TRIG-0001" in b["事件"] for b in new_pending)

    def test_confirmed_batch_keeps_original_destination_after_reassignment(self):
        run_rule_router_pipeline()
        record = ed.confirm_delivery("TRIG-0001")
        original_batch = record["批次编号"]

        updated = [dict(r) for r in EVENT_ROUTES]
        for r in updated:
            if r["规则编号"] == "ROUTE-HIGH-VALUE-PRIMARY":
                r["优先级"] = 5
        recomputed = ed.recompute_assignments(routes=updated)
        ed.enqueue_pending(assignments=recomputed)

        # 已确认批次没有被新归属改写，仍按原去向回查
        confirmed = ed.list_batches(target="pipeline-realtime", status="已确认")
        assert any(b["批次编号"] == original_batch for b in confirmed)

    def test_conflict_marker_persists_while_old_batch_remains_pending(self):
        run_rule_router_pipeline()
        updated = [dict(r) for r in EVENT_ROUTES]
        for r in updated:
            if r["规则编号"] == "ROUTE-HIGH-VALUE-PRIMARY":
                r["优先级"] = 5
        first = ed.recompute_assignments(routes=updated)
        assert _assignment(first, "TRIG-0001")["归属冲突"] is not None
        # 再次重算：旧待送批次仍在，冲突标记不能丢
        again = ed.recompute_assignments(routes=updated)
        assert _assignment(again, "TRIG-0001")["归属冲突"] is not None


class TestAcceptanceScenario:
    """验收场景：交换两个候选的配置顺序，混入无匹配和已交付事件，
    核对新数据送达管道、旧批次去向，重试落盘不产生第三份交付。"""

    def test_full_flow(self):
        # 1) 交换 RULE-HIGH-VALUE 两个候选的配置顺序（低优先目标排到末项）
        swapped = list(EVENT_ROUTES)
        idx_a = next(i for i, r in enumerate(swapped) if r["规则编号"] == "ROUTE-HIGH-VALUE-SECONDARY")
        idx_b = next(i for i, r in enumerate(swapped) if r["规则编号"] == "ROUTE-HIGH-VALUE-PRIMARY")
        swapped[idx_a], swapped[idx_b] = swapped[idx_b], swapped[idx_a]
        assert swapped[idx_b]["目标管道"] == "pipeline-archive"  # 末项是低优先目标

        # 2) 归属计算与配置顺序无关，优先级裁决仍然正确
        assignments = ed.build_assignments(routes=swapped)
        assert _assignment(assignments, "TRIG-0001")["归属目标"] == "pipeline-realtime"

        # 3) 跑通管道：无匹配事件（TRIG-0005）回退 default，其余正常入批
        result = run_rule_router_pipeline()
        assert result["unroutable_count"] == 1
        batches = ed.list_batches(status="待送")
        target_of = {e: b["目标管道"] for b in batches for e in b["事件"]}
        assert target_of["TRIG-0005"] == "default"
        assert target_of["TRIG-0001"] == "pipeline-realtime"

        # 4) 已交付事件：确认后进入确认清单，原批次转为已确认并保留原去向
        first = ed.confirm_delivery("TRIG-0001")
        ed.confirm_delivery("TRIG-0001")  # 第 2 次
        third = ed.confirm_delivery("TRIG-0001")  # 第 3 次（重试落盘）
        assert third["重复提交"] == 2
        assert len(ed.list_deliveries()) == 1  # 不产生第二/第三份交付
        confirmed = ed.list_batches(target="pipeline-realtime", status="已确认")
        assert len(confirmed) == 1 and confirmed[0]["批次编号"] == first["批次编号"]

        # 5) 管道接收详情与无法路由记录对账平衡
        detail = ed.reception_detail()
        assert detail["summary"]["对账平衡"] is True
        assert detail["summary"]["已接收事件数"] == 1
        assert detail["summary"]["无法路由事件数"] == 1
        assert detail["summary"]["待送事件数"] == 4
        received_only = ed.reception_detail(received_only=True)
        assert [r["触发事件ID"] for r in received_only["received"]] == ["TRIG-0001"]
        assert received_only["unroutable"] == []
