export const STRICT_SLO = {
  http_req_failed: ['rate<0.01'],
  http_req_duration: ['p(95)<200'],
};

export const NORMAL_SLO = {
  http_req_failed: ['rate<0.01'],
  http_req_duration: ['p(95)<500'],
};

export const RELAXED_SLO = {
  http_req_failed: ['rate<0.05'],
  http_req_duration: ['p(95)<2000'],
};
