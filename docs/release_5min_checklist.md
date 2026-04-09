# 发布后 5 分钟自检清单

## 1) 基础可用性

- 访问 `GET /api/health`，确认：
  - HTTP `200`
  - `db_ok=true`
  - `version` 为本次发布版本
- 访问 `GET /api/version`，确认：
  - `version` 与后端部署日志中的 `git sha` 一致
  - `started_at` 为本次重启后的时间

## 2) 关键接口字段

- 访问筛选接口：
  - `GET /api/screen/mv_pct?min_mv_yi=300&min_pct_chg=5&limit=20`
  - 检查返回 `data[0]` 包含：`turnover_rate`、`board`、`concept`
  - 检查 `meta` 包含：`em_profile_non_empty`、`ths_fallback_filled`

- 访问监控页依赖接口：
  - `GET /api/groups?include_stocks=false`
  - `GET /api/monitor/stocks`
  - `GET /api/monitor/alerts/stats`

## 3) 前端一致性

- 前端页面执行强刷（`Ctrl+F5`），确认命中最新资源。
- 在浏览器 Network 中确认请求地址来自正确 `VITE_API_BASE_URL`。
- 在筛选页执行一次“开始筛选”，确认列表非空且新增字段正常显示。

## 4) 日志检查

- 查看部署日志 `trade_v_deploy.log`：
  - 包含 `部署版本: <sha>`
  - 包含健康检查成功记录
- 查看应用日志 `trade_v_app.log`：
  - 无连续 Traceback
  - 无关键接口 5xx 爆发

## 5) 回滚触发条件

满足任一条件即触发回滚：

- `GET /api/health` 连续 3 次失败
- 关键筛选接口连续返回空字段（`turnover_rate/board/concept`）
- 前端核心页面无法打开或主要接口持续 5xx
