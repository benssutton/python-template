import http from 'k6/http';
import { check } from 'k6';
import { checkDataCount, checkDataRows } from './lib/checks.js';
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
  return { baseUrl };
}

export default function (data) {
  if (__ITER % 2 === 0) {
    checkDataCount(http.get(`${data.baseUrl}/data/count`));
  } else {
    checkDataRows(http.get(`${data.baseUrl}/data/rows`));
  }
}

export function teardown(data) {
  console.log(`Stress test complete. Target RPS: ${TARGET_RPS}. Base URL: ${data.baseUrl}`);
}
