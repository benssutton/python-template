import { check } from 'k6';

export function checkStatus200(res) {
  return check(res, { 'status is 200': (r) => r.status === 200 });
}

export function checkDataRows(res) {
  return check(res, {
    'status is 200': (r) => r.status === 200,
    'has rows array': (r) => {
      try { return Array.isArray(JSON.parse(r.body).rows); }
      catch { return false; }
    },
    'has total field': (r) => {
      try { return JSON.parse(r.body).total !== undefined; }
      catch { return false; }
    },
  });
}
