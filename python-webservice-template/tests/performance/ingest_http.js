import http from 'k6/http';
import { sleep } from 'k6';
import { checkStatus202 } from './lib/checks.js';
import { NORMAL_SLO } from './lib/thresholds.js';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8000';
const BATCH = open('./data/ingest_batch.ipc', 'b');

export const options = {
  vus: 10,
  duration: '30s',
  thresholds: { ...NORMAL_SLO },
};

export default function () {
  const res = http.post(
    `${BASE_URL}/data/ingest`,
    BATCH,
    {
      headers: { 'Content-Type': 'application/vnd.apache.arrow.stream' },
      tags: { endpoint: 'data_ingest' },
    }
  );
  checkStatus202(res);
  sleep(0.1);
}
