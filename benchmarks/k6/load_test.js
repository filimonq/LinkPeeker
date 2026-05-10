/*
 *   k6 run --env MODE=no_cache    --env BASE_URL=http://localhost:8081 load_test.js
 *   k6 run --env MODE=valkey      --env BASE_URL=http://localhost:8081 load_test.js
 *   k6 run --env MODE=valkey_csc  --env BASE_URL=http://localhost:8081 load_test.js
 * пеоед каждым прогоном надо переключить режим кэша в конфиге scrapper:
 *   no_cache   — CACHE_ENABLED=false (убрать WithCache в di.go)
 *   valkey     — DisableCache: true  (обычный Do без локального кэша)
 *   valkey_csc — DisableCache: false (текущая реализация, DoCache)
 */

import http   from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Counter } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8081';
const MODE     = __ENV.MODE     || 'no_cache';
const VUS      = parseInt(__ENV.VUS || '8');

const listLatency  = new Trend('list_latency_ms',  true);
const mutLatency   = new Trend('mut_latency_ms',   true);
const status200    = new Counter('status_200');
const status500    = new Counter('status_500');
const status502504 = new Counter('status_502_504');

export const options = {
    scenarios: {
        main: {
            executor:  'ramping-vus',
            startVUs:  0,
            stages: [
                { duration: '1m',  target: VUS }, // ramp-up
                { duration: '5m',  target: VUS }, // steady state
                { duration: '30s', target: 0   }, // ramp-down
            ],
        },
    },
    thresholds: {
        'list_latency_ms': ['p(99)<10000'],
        'http_req_failed': ['rate<0.05'],
    },
    summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

const CHAT_COUNT = 1000;
const PROBE_URL  = 'https://github.com/k6-probe/repo';

export default function () {
    const chatID  = Math.floor(Math.random() * CHAT_COUNT) + 1;
    const headers = {
        'Tg-Chat-Id':   String(chatID),
        'Content-Type': 'application/json',
    };

    if (Math.random() < 0.99) {
        const res = http.get(`${BASE_URL}/links`, { headers });

        listLatency.add(res.timings.duration);
        countStatus(res.status);

        check(res, { 'list: 200': r => r.status === 200 });

    } else {
        const body = JSON.stringify({ link: PROBE_URL, tags: ['k6'] });

        const add = http.post(`${BASE_URL}/links`, body, { headers });
        mutLatency.add(add.timings.duration);
        countStatus(add.status);

        sleep(0.05);

        const del = http.del(`${BASE_URL}/links`, body, { headers });
        mutLatency.add(del.timings.duration);
        countStatus(del.status);
    }

    sleep(0.05);
}

function countStatus(code) {
    if (code === 200 || code === 409 || code === 404) {
        status200.add(1);
    } else if (code === 500) {
        status500.add(1);
    } else if (code === 502 || code === 504) {
        status502504.add(1);
    }
}

export function handleSummary(data) {
    const m   = data.metrics;
    const get = m['list_latency_ms'];
    const mut = m['mut_latency_ms'];
    const rps = m['http_reqs'];

    const fmt = (v, k) => v && v.values[k] != null ? v.values[k].toFixed(2) : '-';

    const report = [
        ``,
        `╔══════════════════════════════════════════════════╗`,
        `  Режим: ${MODE.toUpperCase().padEnd(43)}`,
        `  VUs:   ${String(VUS).padEnd(43)}`,
        `  Данные: 1000 чатов × 100 ссылок = 100 000 записей`,
        `╠══════════════════════════════════════════════════╣`,
        ``,
        `  GET /links`,
        `  ─────────────────────────────────────────────`,
        `  RPS             ${fmt(rps,  'rate')} req/s`,
        `  avg             ${fmt(get,  'avg')} мс`,
        `  p(50)           ${fmt(get,  'med')} мс`,
        `  p(90)           ${fmt(get,  'p(90)')} мс`,
        `  p(99)           ${fmt(get,  'p(99)')} мс`,
        `  max             ${fmt(get,  'max')} мс`,
        ``,
        `  POST+DELETE /links (мутации)`,
        `  ─────────────────────────────────────────────`,
        `  avg             ${fmt(mut,  'avg')} мс`,
        `  p(99)           ${fmt(mut,  'p(99)')} мс`,
        ``,
        `  Статусы`,
        `  ─────────────────────────────────────────────`,
        `  200/ok          ${m['status_200']    ? m['status_200'].values['count']    : 0}`,
        `  500             ${m['status_500']    ? m['status_500'].values['count']    : 0}`,
        `  502/504         ${m['status_502_504']? m['status_502_504'].values['count']: 0}`,
        `  failed rate     ${m['http_req_failed'] ? (m['http_req_failed'].values['rate']*100).toFixed(2) : 0}%`,
        ``,
        `╚══════════════════════════════════════════════════╝`,
        ``,
    ].join('\n');

    // console.log(report);

    return {
        [`report_${MODE}.txt`]:     report,
        [`summary_${MODE}.json`]:   JSON.stringify(data, null, 2),
        'stdout':                   report,
    };
}