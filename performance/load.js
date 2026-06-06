import http from 'k6/http';
import { group, sleep } from 'k6';
import { SharedArray } from 'k6/data';
import { checkStatus200, checkDataCount, checkDataRows } from './lib/checks.js';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8000';

const rowParams = new SharedArray('rowParams', function () {
  return JSON.parse(open('./data/rows_params.json'));
});

export const options = {
  scenarios: {
    browse_data: {
      executor: 'ramping-vus',
      exec: 'browseData',
      stages: [
        { duration: '30s', target: 10 },
        { duration: '60s', target: 10 },
        { duration: '30s', target: 0 },
      ],
      tags: { scenario: 'browse_data' },
    },
    health_poll: {
      executor: 'constant-vus',
      exec: 'healthPoll',
      vus: 2,
      duration: '2m',
      tags: { scenario: 'health_poll' },
    },
  },
  thresholds: {
    'http_req_duration{scenario:browse_data}': ['p(95)<500'],
    'http_req_failed{scenario:browse_data}':   ['rate<0.01'],
    'http_req_duration{scenario:health_poll}': ['p(95)<200'],
    'http_req_failed{scenario:health_poll}':   ['rate<0.01'],
  },
};

export function browseData() {
  const p = rowParams[Math.floor(Math.random() * rowParams.length)];
  group('data', () => {
    checkDataRows(http.get(`${BASE_URL}/data/rows?limit=${p.limit}&offset=${p.offset}`));
    checkDataCount(http.get(`${BASE_URL}/data/count`));
  });
  sleep(1);
}

export function healthPoll() {
  group('health', () => {
    checkStatus200(http.get(`${BASE_URL}/health/status`));
  });
  sleep(1);
}
