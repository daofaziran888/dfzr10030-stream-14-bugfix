<template>
  <section class="page" data-module="event_router">
    <header class="page-head">
      <div>
        <h2>事件路由管理</h2>
        <p class="page-desc">维护事件流的分类路由规则、优先级和死信队列处理策略。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">登记路由规则</button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">导出事件路由清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td class="row-actions">
            <button
              v-for="action in actions"
              :key="action"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 1" class="empty-state">暂无事件路由数据，可先登记路由规则</td>
        </tr>
      </tbody>
    </table>

    <section class="routing-panel" data-testid="assignment-panel">
      <header class="page-head">
        <div>
          <h3>触发事件分区归属清单</h3>
          <p class="page-desc">规则触发按「触发规则」业务关系关联路由，优先级裁决归属；结果按目标排序。</p>
        </div>
        <div class="page-actions">
          <button class="btn primary" type="button" data-testid="refresh-assignment" @click="loadRoutingData">刷新归属与确认清单</button>
        </div>
      </header>

      <form class="filter-bar" @submit.prevent="loadAssignments">
        <label class="filter-item">
          <span>目标管道</span>
          <input v-model="assignmentFilters.target" data-testid="filter-assignment-target" placeholder="按目标管道筛选" />
        </label>
        <label class="filter-item">
          <span>候选状态</span>
          <select v-model="assignmentFilters.ambiguous" data-testid="filter-assignment-ambiguous">
            <option value="">全部</option>
            <option value="true">仅歧义候选</option>
            <option value="false">仅已裁决</option>
          </select>
        </label>
        <label class="filter-item">
          <span>匹配情况</span>
          <select v-model="assignmentFilters.routed" data-testid="filter-assignment-routed">
            <option value="">全部</option>
            <option value="true">已匹配路由</option>
            <option value="false">无法路由（回退 default）</option>
          </select>
        </label>
        <button class="btn" data-testid="query-assignment" type="submit">查询</button>
      </form>

      <table class="data-table">
        <thead>
          <tr>
            <th v-for="column in assignmentColumns" :key="column">{{ column }}</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in assignments" :key="String(item['触发事件ID'])" :data-testid="assignmentTestId(item)">
            <td>{{ item['触发事件ID'] }}</td>
            <td>{{ item['规则编号'] }}</td>
            <td>{{ item['归属目标'] }}</td>
            <td>{{ item['归属规则编号'] ?? '—' }}</td>
            <td>
              <span v-if="item.ambiguous" class="flag ambiguous" data-testid="flag-ambiguous">歧义候选</span>
              <span v-else-if="!item.routed" class="flag unroutable" data-testid="flag-unroutable">无法路由</span>
              <span v-else class="flag resolved">已裁决</span>
            </td>
            <td>{{ formatCandidates(item) }}</td>
            <td>
              <span v-if="item['归属冲突']" class="flag conflict" data-testid="flag-conflict">
                冲突：原 {{ item['归属冲突']['原目标'] }} 待送
              </span>
              <span v-else>—</span>
            </td>
            <td>
              <button
                class="link"
                type="button"
                :data-testid="confirmTestId(item)"
                @click="confirmOne(String(item['触发事件ID']))"
              >
                确认交付
              </button>
            </td>
          </tr>
          <tr v-if="!assignments.length">
            <td :colspan="assignmentColumns.length + 1" class="empty-state">暂无归属数据，请先刷新清单</td>
          </tr>
        </tbody>
      </table>
    </section>

    <section class="routing-panel" data-testid="delivery-panel">
      <h3>路由确认清单（投递结果）</h3>
      <table class="data-table">
        <thead>
          <tr>
            <th v-for="column in deliveryColumns" :key="column">{{ column }}</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in deliveries" :key="String(item['触发事件ID'])" :data-testid="deliveryTestId(item)">
            <td>{{ item['触发事件ID'] }}</td>
            <td>{{ item['规则编号'] ?? '—' }}</td>
            <td>{{ item['目标管道'] }}</td>
            <td>{{ item['批次编号'] ?? '—' }}</td>
            <td>{{ item['投递结果'] }}</td>
            <td>{{ item['确认时间'] }}</td>
            <td>{{ item['重复提交'] }}</td>
          </tr>
          <tr v-if="!deliveries.length">
            <td :colspan="deliveryColumns.length" class="empty-state">尚无已确认交付</td>
          </tr>
        </tbody>
      </table>
    </section>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记路由规则</h3>
        <label v-for="field in columns" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input v-model="createValues[field]" :data-testid="`create-${field}`" required />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <footer class="page-foot">
      <span>共 {{ total }} 条事件路由记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>

type Candidate = { 规则编号: string; 目标管道: string; 优先级: number | string }
type Assignment = {
  触发事件ID: string
  规则编号: string
  归属目标: string
  归属规则编号: string | null
  routed: boolean
  ambiguous: boolean
  候选目标: Candidate[]
  归属冲突: { 原目标: string; 原批次编号: string; 新目标: string } | null
}
type Delivery = {
  触发事件ID: string
  规则编号: string | null
  目标管道: string
  批次编号: string | null
  投递结果: string
  确认时间: string
  重复提交: number
}

const ENDPOINT = '/api/event_router'
const columns = ["规则编号", "事件类型", "匹配条件", "目标管道", "优先级", "分区键", "重试上限", "DLQ策略"]
const actions = ["启用路由", "停用路由", "归档规则"]
const statuses = ["草稿", "路由中", "已停用", "已被拒"]
const metricNames = ["规则总数", "路由中规则", "已被拒规则"]

const assignmentColumns = ["触发事件ID", "触发规则", "归属目标", "归属规则", "归属判定", "候选目标（含优先级）", "归属冲突"]
const deliveryColumns = ["触发事件ID", "触发规则", "目标管道", "批次编号", "投递结果", "确认时间", "重复提交次数"]

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})

const assignments = ref<Assignment[]>([])
const deliveries = ref<Delivery[]>([])
const assignmentTestId = (item: Assignment) => `assignment-row-${item['触发事件ID']}`
const confirmTestId = (item: Assignment) => `confirm-${item['触发事件ID']}`
const deliveryTestId = (item: Delivery) => `delivery-row-${item['触发事件ID']}`
function formatCandidates(item: Assignment): string {
  const list = item['候选目标'] ?? []
  if (!list.length) return '—'
  return list.map((c) => `${c['目标管道']}(优先级${c['优先级']})`).join('；')
}
const assignmentFilters = ref<{ target: string; ambiguous: string; routed: string }>({
  target: '', ambiguous: '', routed: '',
})

async function loadAssignments() {
  const params = new URLSearchParams()
  if (assignmentFilters.value.target.trim()) params.set('target', assignmentFilters.value.target.trim())
  if (assignmentFilters.value.ambiguous) params.set('ambiguous', assignmentFilters.value.ambiguous)
  if (assignmentFilters.value.routed) params.set('routed', assignmentFilters.value.routed)
  const response = await request(`${ENDPOINT}/assignments?${params.toString()}`)
  if (!response.ok) throw new Error('归属清单读取失败')
  const payload = await response.json()
  assignments.value = payload.items ?? []
}

async function loadDeliveries() {
  const response = await request(`${ENDPOINT}/deliveries`)
  if (!response.ok) throw new Error('路由确认清单读取失败')
  const payload = await response.json()
  deliveries.value = payload.items ?? []
}

async function loadRoutingData() {
  errorMessage.value = ''
  try {
    await Promise.all([loadAssignments(), loadDeliveries()])
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '路由数据加载失败'
  }
}

async function confirmOne(eventId: string) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/deliveries/confirm`, {
      method: 'POST',
      body: JSON.stringify({ 触发事件ID: eventId, 投递结果: '成功' }),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '交付确认失败')
    await loadRoutingData()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '交付确认失败'
  }
}
const stats = computed(() => [
  { label: metricNames[0], value: total.value },
  { label: metricNames[1], value: rows.value.filter((row) => row.status === statuses[1]).length },
  { label: metricNames[2], value: rows.value.filter((row) => row.status === statuses[statuses.length - 1]).length },
])

function resetFilters() {
  filters.value = {}
  void reload()
}

async function exportRows() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value).toString()
  try {
    const response = await request(`${ENDPOINT}/export?${query}`)
    if (!response.ok) throw new Error('导出失败，请稍后重试')
    const url = URL.createObjectURL(await response.blob())
    const link = document.createElement('a')
    link.href = url
    link.download = 'event_router.csv'
    link.click()
    URL.revokeObjectURL(url)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(columns.map((field) => [field, '']))
  errorMessage.value = ''
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const response = await request(ENDPOINT, {
      method: 'POST', body: JSON.stringify(createValues.value),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '登记失败')
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    if (!response.ok) {
      throw new Error('事件路由动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '事件路由操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('路由规则列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '事件路由列表读取失败'
  }
}

onMounted(() => {
  void reload()
  void loadRoutingData()
})
</script>

<style scoped>
.routing-panel {
  margin-top: 24px;
  background: #f8fafc;
  border: 1px solid var(--border);
  border-radius: 10px;
  padding: 14px 16px;
}
.routing-panel h3 {
  margin: 0 0 10px;
}
.flag {
  display: inline-block;
  padding: 2px 8px;
  border-radius: 999px;
  font-size: 12px;
  white-space: nowrap;
}
.flag.resolved { background: #d1fae5; color: #065f46; }
.flag.ambiguous { background: #fef3c7; color: #92400e; }
.flag.unroutable { background: #e0e7ff; color: #3730a3; }
.flag.conflict { background: #fee2e2; color: #991b1b; }
</style>
