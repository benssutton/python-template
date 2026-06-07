import http from 'k6/http';
import { check } from 'k6';
import { checkDataRows } from './lib/checks.js';
import { RELAXED_SLO } from './lib/thresholds.js';

const TARGET_RPS = parseInt(__ENV.TARGET_RPS || '50', 10);

export const options = {
  scenarios: {
    spike: {
      executor: 'ramping-arrival-rate',
      startRate: 10,
      timeUnit: '1s',
      preAllocatedVUs: 20,
      maxVUs: 100,
      stages: [
        { duration: '30s', target: 10 },
        { duration: '60s', target: TARGET_RPS },
        { duration: '30s', target: 10 },
      ],
    },
  },
  thresholds: { ...RELAXED_SLO },
};

export function setup() {
  const baseUrl = __ENV.BASE_URL || 'http://localhost:8000';
  const res = http.get(`${baseUrl}/health/status`);
  check(res, { 'app is healthy before stress': (r) => r.status === 200 });
  if (res.status !== 200) {
    throw new Error(`App health check failed: ${res.status}`);
  }
  return { baseUrl };
}

export default function (data) {
  checkDataRows(http.get(`${data.baseUrl}/data`));
}

export function teardown(data) {
  console.log(`Stress test complete. Target RPS: ${TARGET_RPS}. Base URL: ${data.baseUrl}`);
}
