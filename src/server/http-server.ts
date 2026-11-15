/**
 * HTTP 服务:看板 REST API + SSE 实时推送,纯 Node 内置模块实现。
 *
 * 路由:
 *  GET  /                      看板单页
 *  GET  /api/status            作业状态
 *  POST /api/jobs/live         启动实时作业
 *  POST /api/jobs/replay       启动回放( body: {speed} )
 *  POST /api/jobs/stop         停止作业
 *  GET  /api/topology          数据流拓扑
 *  GET  /api/results?operator= 窗口结果(可按算子过滤)
 *  GET  /api/late              迟到数据留痕
 *  GET  /api/checkpoints       状态快照列表 + 算子状态规模
 *  GET  /api/metrics           指标(吞吐/延迟分位/计数)
 *  GET  /api/events            最近事件样本
 *  GET  /api/stream            SSE 变更流(支持 Last-Event-ID 续传)
 */
import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import type { EngineConfig } from '../config.ts';
import type { JobService } from '../job/job-service.ts';

const HERE = dirname(fileURLToPath(import.meta.url));

function sendJson(res: ServerResponse, code: number, body: unknown): void {
  const payload = JSON.stringify(body);
  res.writeHead(code, {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store',
  });
  res.end(payload);
}

function readBody(req: IncomingMessage): Promise<string> {
  return new Promise((resolve, reject) => {
    let data = '';
    req.on('data', (chunk) => (data += chunk));
    req.on('end', () => resolve(data));
    req.on('error', reject);
  });
}

export function createHttpServer(cfg: EngineConfig, jobs: JobService) {
  const server = createServer(async (req, res) => {
    const url = new URL(req.url ?? '/', `http://${req.headers.host ?? 'localhost'}`);
    const path = url.pathname;

    try {
      // ---------- 静态看板 ----------
      if (path === '/' || path === '/index.html') {
        const html = readFileSync(join(HERE, '..', 'web', 'dashboard.html'), 'utf8');
        res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
        res.end(html);
        return;
      }

      // ---------- SSE ----------
      if (path === '/api/stream') {
        handleSse(req, res, jobs);
        return;
      }

      // ---------- API ----------
      if (req.method === 'GET' && path === '/api/status') {
        return sendJson(res, 200, jobs.getStatus());
      }
      if (req.method === 'POST' && path === '/api/jobs/live') {
        const overrides = await parseOverrides(req);
        return sendJson(res, 200, jobs.startLive(overrides));
      }
      if (req.method === 'POST' && path === '/api/jobs/replay') {
        const body = await readBody(req);
        const parsed = (body ? JSON.parse(body) : {}) as {
          speed?: number;
          overrides?: Parameters<JobService['startReplay']>[1];
        };
        const speed = parsed.speed ?? cfg.replaySpeed;
        return sendJson(res, 200, jobs.startReplay(speed, parsed.overrides));
      }
      if (req.method === 'POST' && path === '/api/jobs/stop') {
        return sendJson(res, 200, await jobs.stop());
      }
      if (req.method === 'GET' && path === '/api/topology') {
        return sendJson(res, 200, jobs.topologyView());
      }
      if (req.method === 'GET' && path === '/api/results') {
        if (!jobs.hasRun) return sendJson(res, 200, { results: [] });
        const operator = url.searchParams.get('operator');
        const store = jobs.coordinatorOrThrow.topology.resultStore;
        const results = operator
          ? store.resultsByOperator(operator)
          : store.allResults().sort(
              (a, b) =>
                a.windowStart - b.windowStart || a.regionId.localeCompare(b.regionId),
            );
        return sendJson(res, 200, {
          results: results.slice(-2_000),
          truncated: results.length > 2_000,
          total: results.length,
        });
      }
      if (req.method === 'GET' && path === '/api/late') {
        if (!jobs.hasRun) return sendJson(res, 200, { records: [] });
        const store = jobs.coordinatorOrThrow.topology.resultStore;
        return sendJson(res, 200, {
          records: store.latestLate.slice(-500).reverse(),
          total: store.latestLate.length,
        });
      }
      if (req.method === 'GET' && path === '/api/checkpoints') {
        if (!jobs.hasRun) return sendJson(res, 200, { checkpoints: [], profile: {} });
        const coordinator = jobs.coordinatorOrThrow;
        return sendJson(res, 200, {
          checkpoints: coordinator.checkpointManager.list(),
          last: coordinator.topology.lastCheckpointInfo,
          profile: coordinator.topology.stateProfile(),
        });
      }
      if (req.method === 'GET' && path === '/api/metrics') {
        if (!jobs.hasRun) return sendJson(res, 200, { metrics: {} });
        return sendJson(res, 200, {
          metrics: jobs.coordinatorOrThrow.topology.metrics.snapshot(),
        });
      }
      if (req.method === 'GET' && path === '/api/events') {
        return sendJson(res, 200, { events: jobs.recentEvents() });
      }

      sendJson(res, 404, { error: 'not found', path });
    } catch (err) {
      sendJson(res, 500, {
        error: err instanceof Error ? err.message : String(err),
      });
    }
  });

  return server;
}

async function parseOverrides(
  req: IncomingMessage,
): Promise<Parameters<JobService['startLive']>[0]> {
  const body = await readBody(req);
  if (!body) return undefined;
  const parsed = JSON.parse(body) as { overrides?: unknown };
  return parsed.overrides as Parameters<JobService['startLive']>[0];
}

/** SSE:订阅跨作业事件总线,断线通过 Last-Event-ID 续传 */
function handleSse(req: IncomingMessage, res: ServerResponse, jobs: JobService): void {
  res.writeHead(200, {
    'content-type': 'text/event-stream; charset=utf-8',
    'cache-control': 'no-store',
    connection: 'keep-alive',
  });
  res.write(': connected\n\n');

  const lastEventId = Number(req.headers['last-event-id'] ?? 0);
  const unsubscribe = jobs.subscribe((entry) => {
    res.write(
      `id: ${entry.id}\nevent: ${entry.kind}\ndata: ${JSON.stringify(entry.payload)}\n\n`,
    );
  }, lastEventId);

  const ping = setInterval(() => res.write(': ping\n\n'), 15_000);
  req.on('close', () => {
    clearInterval(ping);
    unsubscribe();
  });
}
