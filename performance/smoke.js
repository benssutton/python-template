import http from 'k6/http';
import { sleep } from 'k6';
import { checkStatus200, checkDataCount, checkDataRows } from './lib/checks.js';
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
  checkDataCount(http.get(`${BASE_URL}/data/count`,    { tags: { endpoint: 'data_count' } }));
  checkDataRows( http.get(`${BASE_URL}/data/rows`,     { tags: { endpoint: 'data_rows' } }));
  checkStatus200(http.get(`${BASE_URL}/config/`,       { tags: { endpoint: 'config' } }));
  sleep(1);
}
