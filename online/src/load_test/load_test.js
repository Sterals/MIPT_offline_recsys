import http from "k6/http";
import { check, sleep } from "k6";
import { Trend, Rate } from "k6/metrics";

// ── Настройки ───────────────────────────────────────────────
const BASE_URL = __ENV.BASE_URL || "http://localhost:8002";

const USER_IDS = [
  176549, 699317, 864613, 964868, 602509, 656683, 791466, 884009,
  927973, 988709, 1016458, 1032142, 203219, 616003, 646903, 648682,
];

// ── Кастомные метрики ───────────────────────────────────────
const recsLatency = new Trend("recs_latency", true);
const recsAnnLatency = new Trend("recs_ann_latency", true);
const recsRerankedLatency = new Trend("recs_reranked_latency", true);
const recsTritonLatency = new Trend("recs_triton_latency", true);

const recsSuccess = new Rate("recs_success");
const recsAnnSuccess = new Rate("recs_ann_success");
const recsRerankedSuccess = new Rate("recs_reranked_success");
const recsTritonSuccess = new Rate("recs_triton_success");

// ── Сценарии нагрузки ───────────────────────────────────────
export const options = {
  scenarios: {
    // Разогрев
    warmup: {
      executor: "constant-vus",
      vus: 2,
      duration: "10s",
      exec: "warmup",
      startTime: "0s",
    },
    // Основной тест: постепенный рост нагрузки
    ramp_up: {
      executor: "ramping-vus",
      startVUs: 1,
      stages: [
        { duration: "15s", target: 5 },
        { duration: "30s", target: 10 },
        { duration: "30s", target: 20 },
        { duration: "15s", target: 0 },
      ],
      exec: "mainTest",
      startTime: "10s",
    },
  },
  thresholds: {
    recs_latency: ["p(95)<500"],
    recs_ann_latency: ["p(95)<200"],
    recs_reranked_latency: ["p(95)<1000"],
    recs_triton_latency: ["p(95)<1000"],
  },
};

// ── Хелперы ─────────────────────────────────────────────────
function randomUserId() {
  return USER_IDS[Math.floor(Math.random() * USER_IDS.length)];
}

function callEndpoint(path, latencyMetric, successMetric) {
  const url = `${BASE_URL}${path}`;
  const res = http.get(url);

  latencyMetric.add(res.timings.duration);

  const ok = check(res, {
    "status 200": (r) => r.status === 200,
    "has recommendations": (r) => {
      const body = JSON.parse(r.body);
      return (
        (body.recommendations && body.recommendations.length > 0) ||
        (body.similar && body.similar.length > 0)
      );
    },
  });
  successMetric.add(ok);

  return res;
}

// ── Сценарий разогрева ──────────────────────────────────────
export function warmup() {
  const uid = randomUserId();
  http.get(`${BASE_URL}/recs?user_id=${uid}&top_k=5`);
  http.get(`${BASE_URL}/recs_ann?user_id=${uid}&top_k=5`);
  sleep(1);
}

// ── Основной сценарий ───────────────────────────────────────
export function mainTest() {
  const uid = randomUserId();

  // 1. Brute-force
  callEndpoint(
    `/recs?user_id=${uid}&top_k=10`,
    recsLatency,
    recsSuccess,
  );

  // 2. ANN (Qdrant)
  callEndpoint(
    `/recs_ann?user_id=${uid}&top_k=10`,
    recsAnnLatency,
    recsAnnSuccess,
  );

  // 3. Reranked (MLflow CatBoost)
  callEndpoint(
    `/recs_reranked?user_id=${uid}&top_k=10&n_candidates=100`,
    recsRerankedLatency,
    recsRerankedSuccess,
  );

  // 4. Triton
  callEndpoint(
    `/recs_triton?user_id=${uid}&top_k=10&n_candidates=100`,
    recsTritonLatency,
    recsTritonSuccess,
  );

  sleep(0.5);
}
