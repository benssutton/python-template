import http from 'k6/http';
import { sleep } from 'k6';
import { checkStatus200, checkDataRows } from './lib/checks.js';
import { STRICT_SLO } from './lib/thresholds.js';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8000';

export const options = {
  vus: 1,
  duration: '30s',
  thresholds: { ...STRICT_SLO },
};

export default function () {
  checkStatus200(http.get(`${BASE_URL}/`,              { tags: { endpoint: 'root' } }));
  checkStatus200(http.get(`${BASE_URL}/health/status`, { tags: { endpoint: 'health' } }));
  checkDataRows( http.get(`${BASE_URL}/data`,          { tags: { endpoint: 'data' } }));
  checkStatus200(http.get(`${BASE_URL}/config/`,       { tags: { endpoint: 'config' } }));
  sleep(1);
}
