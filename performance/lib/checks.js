import { check } from 'k6';

export function checkStatus200(res) {
  return check(res, { 'status is 200': (r) => r.status === 200 });
}

export function checkDataCount(res) {
  return check(res, {
    'status is 200': (r) => r.status === 200,
    'has count field': (r) => JSON.parse(r.body).count !== undefined,
  });
}

export function checkDataRows(res) {
  const body = JSON.parse(res.body);
  return check(res, {
    'status is 200': (r) => r.status === 200,
    'has rows array': () => Array.isArray(body.rows),
    'has total field': () => body.total !== undefined,
  });
}
