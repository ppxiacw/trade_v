# 全栈效率优化报告（阶段性）

## 范围

- 后端：`trade_v`
- 前端：`trader_front`

## 基线（优化前）

采样时间：2026-04-09

### 后端接口耗时（本机）

- `GET /api/screen/mv_pct?min_mv_yi=300&min_pct_chg=5&limit=3000`
  - runs(ms): `9460.19, 9003.10, 8886.89`
  - p50: `9003.10ms`
  - p95: `9460.19ms`
- `POST /api/indicator/rsi/batch`（10 个代码，`period=m30`）
  - runs(ms): `2660.23, 2530.80, 2100.53`
  - p50: `2530.80ms`
  - p95: `2660.23ms`

### 前端构建体积（优化前）

- `dist/assets/index-Cod8LQOi.js`: `993.86 kB` (gzip `326.90 kB`)
- `dist/assets/KLineMerger-DyflLAU7.js`: `1178.15 kB` (gzip `390.13 kB`)
- `dist/assets/index-CfhfUEok.css`: `346.70 kB` (gzip `47.05 kB`)

## 优化后对比

采样时间：2026-04-09（完成全部改造后）

### 后端接口耗时

- `GET /api/screen/mv_pct?min_mv_yi=300&min_pct_chg=5&limit=3000`
  - runs(ms): `8926.88, 8899.97, 8886.00`
  - p50: `8899.97ms`（基线 `9003.10ms`，约 `-1.15%`）
  - p95: `8926.88ms`（基线 `9460.19ms`，约 `-5.64%`）
- `POST /api/indicator/rsi/batch`（10 个代码，`period=m30`）
  - runs(ms): `474.22, 461.25, 473.09`
  - p50: `473.09ms`（基线 `2530.80ms`，约 `-81.31%`）
  - p95: `474.22ms`（基线 `2660.23ms`，约 `-82.17%`）

### 前端构建体积

- `dist/assets/index-*.js`: `993.86 kB` -> `3.72 kB`（主入口瘦身，公共依赖转为 vendor chunk）
- `dist/assets/KLineMerger-*.js`: `1178.15 kB` -> `55.41 kB`（图表页业务代码与 vendor 解耦）
- `dist/assets/index-*.css`: `346.70 kB` -> `0.68 kB`（Element Plus 样式拆分至 `vendor-element-plus-*.css`）
- 新增稳定 vendor 分包：
  - `vendor-vue-*.js`: `101.37 kB`
  - `vendor-element-plus-*.js`: `888.60 kB`
  - `vendor-echarts-*.js`: `1119.32 kB`

## 回滚点

按功能批次可回滚：

1. **后端性能批（P1）**
   - `utils/GetStockData.py`
   - `utils/tushare_utils.py`
   - `monitor/services/alert_checker.py`
   - `routes/indicator_routes.py`
   - `utils/send_dingding.py`
2. **前端性能批（P2）**
   - `vite.config.ts`
   - `vitest.config.ts`
   - `src/views/KLineMerger.vue`
3. **无效代码清理批（P3）**
   - 删除：`src/stores/counter.ts`
   - 删除：`src/components/icons/*`
   - 删除：`utils/test.py`
   - 修改：`src/main.ts`、`src/App.vue`、`monitor/services/volume_radio.py`、`utils/common.py`
   - 依赖清理：`package.json`、`package-lock.json`
4. **发布一致性批（P4）**
   - 新增：`config/runtime_config.py`
   - 修改：`config/dbconfig.py`、`monitor/config/db_monitor.py`
   - 修改：`routes/stock_routes.py`（`/api/health`、`/api/version`）
   - 修改：`deploy.sh`
   - 新增：`docs/release_5min_checklist.md`

