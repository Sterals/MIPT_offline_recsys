import http from "k6/http";
import { check, sleep } from "k6";
import { Trend } from "k6/metrics";
import { textSummary } from "https://jslib.k6.io/k6-summary/0.1.0/index.js";

// ── Настройки ───────────────────────────────────────────────
const BASE_URL = __ENV.BASE_URL || "http://localhost:8002";

const USER_IDS = [
  176549, 699317, 864613, 964868, 602509, 656683, 791466, 884009,
  927973, 988709, 1016458, 1032142, 203219, 616003, 646903, 648682,
];

// ── Метрики для каждого эндпоинта ───────────────────────────
const metrics = {
  recs: new Trend("endpoint_recs", true),
  recs_ann: new Trend("endpoint_recs_ann", true),
  recs_reranked: new Trend("endpoint_recs_reranked", true),
  recs_triton: new Trend("endpoint_recs_triton", true),
};

// ── Простой сценарий: фиксированная нагрузка ────────────────
export const options = {
  vus: 5,
  duration: "30s",
};

export default function () {
  const uid = USER_IDS[Math.floor(Math.random() * USER_IDS.length)];

  // Brute-force
  let res = http.get(`${BASE_URL}/recs?user_id=${uid}&top_k=10`);
  metrics.recs.add(res.timings.duration);
  check(res, { "recs 200": (r) => r.status === 200 });

  // ANN
  res = http.get(`${BASE_URL}/recs_ann?user_id=${uid}&top_k=10`);
  metrics.recs_ann.add(res.timings.duration);
  check(res, { "recs_ann 200": (r) => r.status === 200 });

  // Reranked (MLflow)
  res = http.get(`${BASE_URL}/recs_reranked?user_id=${uid}&top_k=10&n_candidates=100`);
  metrics.recs_reranked.add(res.timings.duration);
  check(res, { "recs_reranked 200": (r) => r.status === 200 });

  // Triton
  res = http.get(`${BASE_URL}/recs_triton?user_id=${uid}&top_k=10&n_candidates=100`);
  metrics.recs_triton.add(res.timings.duration);
  check(res, { "recs_triton 200": (r) => r.status === 200 });

  sleep(0.3);
}

// ── Красивый вывод результатов ───────────────────────────────
export function handleSummary(data) {
  const lines = ["\n╔══════════════════════════════════════════════════════════╗"];
  lines.push("║           Сравнение эндпоинтов (latency, ms)            ║");
  lines.push("╠══════════════════╦════════╦════════╦════════╦═══════════╣");
  lines.push("║ Endpoint         ║  avg   ║  p50   ║  p95   ║   p99    ║");
  lines.push("╠══════════════════╬════════╬════════╬════════╬═══════════╣");

  const names = ["endpoint_recs", "endpoint_recs_ann", "endpoint_recs_reranked", "endpoint_recs_triton"];
  const labels = ["/recs", "/recs_ann", "/recs_reranked", "/recs_triton"];

  for (let i = 0; i < names.length; i++) {
    const m = data.metrics[names[i]];
    if (m && m.values) {
      const v = m.values;
      const label = labels[i].padEnd(16);
      const avg = v.avg.toFixed(1).padStart(6);
      const p50 = (v["p(50)"] || v.med || 0).toFixed(1).padStart(6);
      const p95 = (v["p(95)"] || 0).toFixed(1).padStart(6);
      const p99 = (v["p(99)"] || 0).toFixed(1).padStart(9);
      lines.push(`║ ${label} ║ ${avg} ║ ${p50} ║ ${p95} ║ ${p99} ║`);
    }
  }

  lines.push("╚══════════════════╩════════╩════════╩════════╩═══════════╝\n");

  console.log(lines.join("\n"));
  return {
    stdout: textSummary(data, { indent: " ", enableColors: true }),
  };
}
