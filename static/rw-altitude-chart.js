(function () {
  const PANEL_ID = 'rw-altitude-panel';
  const CANVAS_ID = 'rw-altitude-canvas';
  const SUB_ID = 'rw-altitude-sub';
  const BADGE_ID = 'rw-altitude-badge';
  let chart = null;
  let chartReady = false;
  let lastPoints = [];

  function fmtNum(v, digits=1) {
    if (v === null || v === undefined || Number.isNaN(Number(v))) return '—';
    return Number(v).toFixed(digits);
  }

  function fmtTime(ts) {
    if (!ts) return '—';
    const d = new Date(ts);
    if (Number.isNaN(d.getTime())) return String(ts);
    return d.toLocaleTimeString('pl-PL', {hour12:false});
  }

  function fmtDateTime(ts) {
    if (!ts) return '—';
    const d = new Date(ts);
    if (Number.isNaN(d.getTime())) return String(ts);
    return d.toLocaleString('pl-PL', {hour12:false});
  }

  function guessMapHost() {
    const leaf = document.querySelector('.leaflet-container');
    if (!leaf) return null;
    return leaf.parentElement || leaf;
  }

  function ensurePanel() {
    if (document.getElementById(PANEL_ID)) return true;

    const mapHost = guessMapHost();
    if (!mapHost) return false;

    const panel = document.createElement('div');
    panel.id = PANEL_ID;
    panel.className = 'rw-alt-panel';
    panel.innerHTML = [
      '<div class="rw-alt-head">',
      '  <div>',
      '    <div class="rw-alt-title">Profil wysokości lotu</div>',
      '    <div id="'+SUB_ID+'" class="rw-alt-sub">Wybierz / kliknij sondę — wykres pojawi się automatycznie nad mapą.</div>',
      '  </div>',
      '  <div id="'+BADGE_ID+'" class="rw-alt-badge">brak danych</div>',
      '</div>',
      '<div class="rw-alt-canvas-wrap">',
      '  <canvas id="'+CANVAS_ID+'" class="rw-alt-canvas"></canvas>',
      '</div>'
    ].join('');

    mapHost.parentNode.insertBefore(panel, mapHost);
    return true;
  }

  function setInfo(text, badge) {
    const sub = document.getElementById(SUB_ID);
    const bdg = document.getElementById(BADGE_ID);
    if (sub) sub.textContent = text;
    if (bdg) bdg.textContent = badge || '—';
  }

  function normalizePayload(payload) {
    let arr = [];

    if (Array.isArray(payload)) {
      arr = payload;
    } else if (payload && Array.isArray(payload.positions)) {
      arr = payload.positions;
    } else if (payload && Array.isArray(payload.data)) {
      arr = payload.data;
    } else if (payload && Array.isArray(payload.telemetry)) {
      arr = payload.telemetry;
    } else if (payload && Array.isArray(payload.points)) {
      arr = payload.points;
    } else if (payload && typeof payload === 'object') {
      const arrays = Object.values(payload).filter(v => Array.isArray(v));
      if (arrays.length) {
        arrays.sort((a,b) => b.length - a.length);
        arr = arrays[0];
      } else if (
        payload.datetime || payload.time || payload.alt || payload.altitude
      ) {
        arr = [payload];
      }
    }

    const points = arr.map((p) => {
      const ts =
        p.datetime || p.time || p.ts || p.timestamp || p.datetime_utc ||
        p.time_received || p.server_time || p.uploaded || null;

      const alt = p.alt ?? p.altitude ?? p.gps_alt ?? null;
      const lat = p.lat ?? p.latitude ?? null;
      const lon = p.lon ?? p.lng ?? p.longitude ?? null;

      return {
        t: ts,
        alt: (alt === null ? null : Number(alt)),
        lat: (lat === null ? null : Number(lat)),
        lon: (lon === null ? null : Number(lon)),
        vv: p.vel_v ?? p.climb ?? p.v_speed ?? p.vertical_speed ?? null,
        vh: p.vel_h ?? p.speed ?? p.h_speed ?? p.horizontal_speed ?? null,
        temp: p.temp ?? p.temperature ?? null,
        humidity: p.humidity ?? p.hum ?? null,
        pressure: p.pressure ?? p.press ?? null,
        sats: p.sats ?? p.gps_satellites ?? p.satellites ?? null,
        batt: p.batt ?? p.battery ?? p.battery_v ?? null,
        snr: p.snr ?? null,
        rssi: p.rssi ?? null,
        uploader: p.uploader_callsign ?? p.uploader ?? null,
        frame: p.frame ?? null,
        serial: p.serial ?? p.id ?? null
      };
    }).filter(p => p.t && p.alt !== null && !Number.isNaN(p.alt));

    points.sort((a,b) => new Date(a.t) - new Date(b.t));
    return points;
  }

  function ensureChartLib(cb) {
    if (window.Chart) {
      chartReady = true;
      cb();
      return;
    }
    const old = document.querySelector('script[data-rw-chartjs]');
    if (old) {
      old.addEventListener('load', function(){ chartReady = true; cb(); }, {once:true});
      return;
    }
    const s = document.createElement('script');
    s.src = 'https://cdn.jsdelivr.net/npm/chart.js@4.4.3/dist/chart.umd.min.js';
    s.async = true;
    s.dataset.rwChartjs = '1';
    s.onload = function(){ chartReady = true; cb(); };
    document.head.appendChild(s);
  }

  function renderChart(points) {
    if (!ensurePanel()) return;
    if (!points || !points.length) {
      setInfo('Brak historii wysokości do narysowania.', 'brak danych');
      return;
    }

    ensureChartLib(function () {
      const canvas = document.getElementById(CANVAS_ID);
      if (!canvas) return;
      const ctx = canvas.getContext('2d');

      const labels = points.map(p => fmtTime(p.t));
      const values = points.map(p => Math.round(Number(p.alt)));
      const serial = (points.find(p => p.serial)?.serial) || 'sonda';
      lastPoints = points;

      if (chart) chart.destroy();

      chart = new Chart(ctx, {
        type: 'line',
        data: {
          labels,
          datasets: [{
            label: 'Wysokość [m]',
            data: values,
            borderColor: '#55a6ff',
            backgroundColor: 'rgba(85,166,255,.14)',
            pointBackgroundColor: '#88c1ff',
            pointBorderColor: '#d6ecff',
            pointRadius: 2.5,
            pointHoverRadius: 6,
            pointHitRadius: 10,
            borderWidth: 2,
            fill: true,
            tension: 0.22
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          interaction: { mode: 'index', intersect: false },
          animation: false,
          plugins: {
            legend: { display: false },
            tooltip: {
              backgroundColor: 'rgba(12,18,28,.96)',
              borderColor: 'rgba(110,168,255,.35)',
              borderWidth: 1,
              titleColor: '#eef6ff',
              bodyColor: '#d6e7ff',
              displayColors: false,
              callbacks: {
                title: function(items) {
                  const i = items[0]?.dataIndex ?? 0;
                  const p = points[i];
                  return 'Czas: ' + fmtDateTime(p.t);
                },
                label: function(item) {
                  const i = item.dataIndex;
                  const p = points[i];
                  const out = [
                    'Wysokość: ' + fmtNum(p.alt, 0) + ' m',
                    'Prędkość pionowa: ' + fmtNum(p.vv, 2) + ' m/s',
                    'Prędkość pozioma: ' + fmtNum(p.vh, 1) + ' m/s',
                    'Temperatura: ' + fmtNum(p.temp, 1) + ' °C',
                    'Ciśnienie: ' + fmtNum(p.pressure, 1) + ' hPa',
                    'Wilgotność: ' + fmtNum(p.humidity, 1) + ' %',
                    'GPS sat: ' + (p.sats ?? '—'),
                    'Bateria: ' + fmtNum(p.batt, 2) + ' V',
                    'SNR: ' + fmtNum(p.snr, 1) + ' dB',
                    'RSSI: ' + fmtNum(p.rssi, 1) + ' dBm',
                    'Pozycja: ' + (p.lat !== null && p.lon !== null ? (fmtNum(p.lat,5)+', '+fmtNum(p.lon,5)) : '—'),
                    'Uploader: ' + (p.uploader || '—'),
                    'Ramka: ' + (p.frame ?? '—')
                  ];
                  return out;
                }
              }
            }
          },
          scales: {
            x: {
              ticks: { color: '#9db4cf', maxTicksLimit: 10 },
              grid: { color: 'rgba(130,160,210,.10)' },
              title: {
                display: true,
                text: 'Czas',
                color: '#cfe1ff',
                font: { weight: '700' }
              }
            },
            y: {
              ticks: { color: '#9db4cf' },
              grid: { color: 'rgba(130,160,210,.10)' },
              title: {
                display: true,
                text: 'Wysokość [m]',
                color: '#cfe1ff',
                font: { weight: '700' }
              }
            }
          }
        }
      });

      const first = points[0];
      const last = points[points.length - 1];
      const maxAlt = Math.max.apply(null, points.map(p => Number(p.alt) || 0));
      setInfo(
        'Sonda: ' + serial + ' • punktów: ' + points.length +
        ' • od ' + fmtTime(first.t) + ' do ' + fmtTime(last.t) +
        ' • max: ' + fmtNum(maxAlt,0) + ' m',
        'aktywne'
      );
    });
  }

  function maybeUsePayload(url, payload) {
    try {
      const low = String(url || '').toLowerCase();
      if (!low) return;
      if (
        low.includes('prediction') ||
        low.includes('reverse') ||
        low.includes('recover') ||
        low.includes('recovered') ||
        low.includes('stats') ||
        low.includes('listeners') ||
        low.includes('sites')
      ) return;

      if (!(low.includes('sonde') || low.includes('telemetry'))) return;

      const points = normalizePayload(payload);
      if (points.length >= 2) {
        renderChart(points);
      }
    } catch (e) {
      console.warn('rw-alt-chart payload parse error', e);
    }
  }

  function patchFetch() {
    if (!window.fetch || window.__rwAltFetchPatched) return;
    window.__rwAltFetchPatched = true;

    const origFetch = window.fetch;
    window.fetch = function() {
      const args = arguments;
      return origFetch.apply(this, args).then(function(res) {
        try {
          const req = args[0];
          const url = typeof req === 'string' ? req : (req && req.url ? req.url : '');
          const clone = res.clone();
          clone.json().then(function(payload) {
            maybeUsePayload(url, payload);
          }).catch(function(){});
        } catch (e) {}
        return res;
      });
    };
  }

  function patchXHR() {
    if (window.__rwAltXHRPatched) return;
    window.__rwAltXHRPatched = true;

    const origOpen = XMLHttpRequest.prototype.open;
    const origSend = XMLHttpRequest.prototype.send;

    XMLHttpRequest.prototype.open = function(method, url) {
      this.__rwUrl = url;
      return origOpen.apply(this, arguments);
    };

    XMLHttpRequest.prototype.send = function() {
      this.addEventListener('load', function() {
        try {
          const ct = this.getResponseHeader('content-type') || '';
          if (!ct.includes('application/json')) return;
          const payload = JSON.parse(this.responseText);
          maybeUsePayload(this.__rwUrl || '', payload);
        } catch (e) {}
      });
      return origSend.apply(this, arguments);
    };
  }

  function arm() {
    ensurePanel();
    patchFetch();
    patchXHR();

    let tries = 0;
    const timer = setInterval(function() {
      tries++;
      ensurePanel();
      if (document.querySelector('.leaflet-container') || tries > 30) {
        clearInterval(timer);
      }
    }, 1000);

    document.addEventListener('click', function(e) {
      const row = e.target.closest('tr');
      const btn = e.target.closest('button, a');
      if (row || btn) {
        const sub = document.getElementById(SUB_ID);
        if (sub && lastPoints.length === 0) {
          sub.textContent = 'Ładowanie profilu wysokości...';
        }
      }
    }, true);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', arm);
  } else {
    arm();
  }
})();
