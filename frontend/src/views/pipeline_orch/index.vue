<template>
  <section class="page" data-module="pipeline_orch">
    <header class="page-head">
      <div><h2>管道编排</h2><p class="page-desc">维护流式数据管道的DAG拓扑编排、依赖管理和全局恢复策略。</p></div>
      <div class="page-actions">
        <button class="btn primary" type="button" data-testid="open-create" @click="openCreate">登记管道DAG</button>
        <button class="btn" type="button" data-testid="export" @click="exportRows">导出关系清单</button>
      </div>
    </header>
    <div class="dag-wrap" ref="dagRef">
      <svg class="dag-svg" :viewBox="`0 0 ${svgW} ${svgH}`">
        <defs><marker id="arrowhead" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto"><path d="M0,0 L8,3 L0,6" fill="#94a3b8"/></marker></defs>
        <g v-for="edge in graph.edges" :key="edge.from+edge.to">
          <line class="dag-edge" :x1="nodePos[edge.from]?.x||0" :y1="nodePos[edge.from]?.y||0" :x2="nodePos[edge.to]?.x||0" :y2="nodePos[edge.to]?.y||0"/>
          <text :x="(nodePos[edge.from]?.x+nodePos[edge.to]?.x)/2" :y="(nodePos[edge.from]?.y+nodePos[edge.to]?.y)/2 - 6" font-size="10" fill="#64748b" text-anchor="middle">{{ edge.label }}</text>
        </g>
        <g v-for="node in graph.nodes" :key="node.id" :class="['dag-node', {selected: selectedNode?.id===node.id}]" @click="selectNode(node)" @mousedown="startDrag($event, node)">
          <rect :x="nodePos[node.id]?.x - 60" :y="nodePos[node.id]?.y - 20" width="120" height="40" rx="6" :fill="nodeColor(node.status)"/>
          <text :x="nodePos[node.id]?.x" :y="nodePos[node.id]?.y + 4" text-anchor="middle">{{ node.name || node.label }}</text>
        </g>
      </svg>
    </div>
    <div v-if="selectedNode" class="dag-detail" data-testid="dag-detail">
      <dl><dt>编号</dt><dd>{{ selectedNode.id }}</dd></dl>
      <dl><dt>名称</dt><dd>{{ selectedNode.name || selectedNode.label }}</dd></dl>
      <dl v-for="f in detailFields" :key="f"><dt>{{ f }}</dt><dd>{{ selectedNode[f] ?? '—' }}</dd></dl>
      <div style="grid-column:1/-1; margin-top:8px; display:flex; gap:6px;">
        <button v-for="action in actions" :key="action" class="btn" :data-testid="`action-${selectedNode.id}-${action}`" @click="runAction(action, selectedNode)">{{ action }}</button>
      </div>
    </div>
    <section class="reception-panel" data-testid="reception-panel">
      <header class="panel-head">
        <div>
          <h3>管道接收详情</h3>
          <p class="page-desc">按触发事件分区归属清单接收明细，已接收事件可筛选，并与无法路由记录对账。</p>
        </div>
        <div class="page-actions">
          <label class="filter-item">
            <span>目标管道</span>
            <input v-model="receptionTarget" data-testid="reception-target" placeholder="按目标筛选" />
          </label>
          <label class="filter-item">
            <input v-model="receivedOnly" type="checkbox" data-testid="reception-received-only" @change="loadReception" />
            <span>只看已接收</span>
          </label>
          <button class="btn primary" type="button" data-testid="refresh-reception" @click="loadReception">刷新接收详情</button>
        </div>
      </header>
      <div class="stat-row" v-if="reception">
        <article class="stat-card"><span class="stat-label">归属事件总数</span><strong>{{ reception.summary['归属事件总数'] }}</strong></article>
        <article class="stat-card"><span class="stat-label">已接收事件数</span><strong>{{ reception.summary['已接收事件数'] }}</strong></article>
        <article class="stat-card"><span class="stat-label">无法路由事件数</span><strong>{{ reception.summary['无法路由事件数'] }}</strong></article>
        <article class="stat-card"><span class="stat-label">歧义候选事件数</span><strong>{{ reception.summary['歧义候选事件数'] }}</strong></article>
        <article class="stat-card"><span class="stat-label">归属冲突事件数</span><strong>{{ reception.summary['归属冲突事件数'] }}</strong></article>
      </div>
      <table class="data-table" v-if="reception">
        <thead>
          <tr><th>触发事件ID</th><th>触发规则</th><th>目标管道</th><th>批次编号</th><th>投递结果</th><th>确认时间</th></tr>
        </thead>
        <tbody>
          <tr v-for="d in reception.received" :key="receivedKey(d)" :data-testid="receivedTestId(d)">
            <td>{{ d['触发事件ID'] }}</td><td>{{ d['规则编号'] ?? '—' }}</td><td>{{ d['目标管道'] }}</td>
            <td>{{ d['批次编号'] ?? '—' }}</td><td>{{ d['投递结果'] }}</td><td>{{ d['确认时间'] }}</td>
          </tr>
          <template v-if="!receivedOnly">
            <tr v-for="u in reception.unroutable" :key="unroutableKey(u)" :data-testid="unroutableTestId(u)" class="unroutable-row">
              <td>{{ u['触发事件ID'] }}</td><td>{{ u['规则编号'] }}</td><td>{{ u['归属目标'] }}（回退）</td>
              <td>—</td><td>无法路由</td><td>{{ u['触发时间'] ?? '—' }}</td>
            </tr>
          </template>
          <tr v-if="!reception.received.length && (receivedOnly || !reception.unroutable.length)">
            <td colspan="6" class="empty-state">暂无接收明细</td>
          </tr>
        </tbody>
      </table>
    </section>
    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate"><h3>登记管道DAG</h3>
        <label v-for="field in columns.slice(0,3)" :key="field" class="create-field"><span>{{ field }}</span><input v-model="createValues[field]" :placeholder="`请输入${field}`" required /></label>
        <div style="display:flex;gap:8px;justify-content:flex-end;margin-top:12px">
          <button class="btn" type="button" @click="showCreate=false">取消</button>
          <button class="btn primary" type="submit" data-testid="submit-create">提交</button>
        </div>
      </form>
    </div>
    <footer class="page-foot"><span>{{ graph.nodes.length }} 个节点</span></footer>
  </section>
</template>
<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'
import { request } from '@/api/client'
const ENDPOINT = '/api/pipeline_orch'
const columns = ["管道编号", "管道名称", "节点列表", "依赖拓扑", "并行度", "恢复策略", "超时分钟", "运行批次"]
const actions = ["部署管道", "启动运行", "停止管道"]
const statuses = ["未部署", "部署中", "运行中", "已停止"]
const STATUS_COLORS: Record<string,string> = { '草稿':'#e2e8f0','待校验':'#fef3c7','已启用':'#d1fae5','已停用':'#fee2e2' }
const nodeColor = (s: string) => STATUS_COLORS[s] || '#e2e8f0'
const graph = reactive<{nodes: any[], edges: any[]}>({ nodes: [], edges: [] })
const nodePos = ref<Record<number,{x:number;y:number}>>({})
const selectedNode = ref<any>(null)
const detailFields = computed(() => columns.filter((f: string) => !['id','name','label','status'].includes(f)))
const svgW = ref(800); const svgH = ref(400)
const errorMessage = ref('')
const creating = ref(false); const showCreate = ref(false)
const createValues = ref<Record<string,string>>({})
let dragNode: any = null; const dagRef = ref<HTMLElement|null>(null)
function layout(nodes: any[]) {
  const w = 800; const h = Math.max(400, nodes.length * 80)
  svgW.value = w; svgH.value = h
  const pos: Record<number,{x:number;y:number}> = {}
  nodes.forEach((n,i) => { pos[n.id] = { x: 120 + (i % 5) * 150, y: 50 + Math.floor(i / 5) * 80 } })
  nodePos.value = pos
}
function selectNode(node: any) { selectedNode.value = selectedNode.value?.id === node.id ? null : node }
function startDrag(e: MouseEvent, node: any) { dragNode = node; const onMove = (ev: MouseEvent) => { if(!dragNode) return; nodePos.value[node.id] = { x: ev.offsetX, y: ev.offsetY } }; const onUp = () => { dragNode = null; document.removeEventListener('mousemove', onMove); document.removeEventListener('mouseup', onUp) }; document.addEventListener('mousemove', onMove); document.addEventListener('mouseup', onUp) }
async function reload() {
  try { const res = await request(ENDPOINT); const data = await res.json(); graph.nodes = data.nodes || []; graph.edges = data.edges || []; layout(graph.nodes) } catch(e) { errorMessage.value = '加载失败' }
}
async function runAction(action: string, node: any) {
  try { await request(`${ENDPOINT}/${node.id}/actions`, { method: 'POST', body: JSON.stringify({ action }) }); await reload() } catch(e) { errorMessage.value = '操作失败' }
}
function openCreate() { createValues.value = {}; showCreate.value = true }
async function submitCreate() {
  try { const res = await request(ENDPOINT, { method: 'POST', body: JSON.stringify({ values: createValues.value }) }); if(!res.ok) throw new Error('创建失败'); showCreate.value = false; await reload() } catch(e: any) { errorMessage.value = e.message }
}
function exportRows() { window.open(`${ENDPOINT}/export`, '_blank') }

// 管道接收详情：来自跨模块管道接口的对账数据
type ReceivedRow = Record<string, string | null>
type ReceptionDetail = {
  目标管道: string
  received: ReceivedRow[]
  unroutable: Array<Record<string, unknown>>
  summary: Record<string, number | boolean>
}
const receptionTarget = ref('')
const receivedOnly = ref(false)
const reception = ref<ReceptionDetail | null>(null)
const receivedKey = (d: ReceivedRow) => `r-${String(d['触发事件ID'])}`
const receivedTestId = (d: ReceivedRow) => `received-row-${String(d['触发事件ID'])}`
const unroutableKey = (u: Record<string, unknown>) => `u-${String(u['触发事件ID'])}`
const unroutableTestId = (u: Record<string, unknown>) => `unroutable-row-${String(u['触发事件ID'])}`
async function loadReception() {
  try {
    const params = new URLSearchParams()
    if (receptionTarget.value.trim()) params.set('target', receptionTarget.value.trim())
    params.set('received_only', String(receivedOnly.value))
    const res = await request(`/api/pipeline/reception?${params.toString()}`)
    if (!res.ok) throw new Error('接收详情读取失败')
    const data = await res.json()
    reception.value = data.detail
  } catch (e) { errorMessage.value = e instanceof Error ? e.message : '接收详情读取失败' }
}
onMounted(() => { void reload(); void loadReception() })
</script>

<style scoped>
.reception-panel {
  margin-top: 20px;
  background: #f8fafc;
  border: 1px solid var(--border);
  border-radius: 10px;
  padding: 14px 16px;
}
.panel-head {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  flex-wrap: wrap;
  gap: 10px;
  margin-bottom: 10px;
}
.panel-head h3 { margin: 0; }
.unroutable-row { background: #eef2ff; }
</style>
