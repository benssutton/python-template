import http from 'k6/http';
import { sleep } from 'k6';
import { checkStatus200, checkDataRows } from './lib/checks.js';
import { NORMAL_SLO } from './lib/thresholds.js';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8000';

export const options = {
  vus: 5,
  duration: '30s',
  thresholds: { ...NORMAL_SLO },
};

export default function () {
  checkDataRows(http.get(`${BASE_URL}/data/cache`, { tags: { endpoint: 'data_cache' } }));
  sleep(1);
}
