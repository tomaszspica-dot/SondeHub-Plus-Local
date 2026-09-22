#!/usr/bin/env python3
import gzip
import codecs
import json
import math
import os
import threading
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from glob import glob
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import sondehub

API = os.environ.get("SONDEHUB_API_URL", 'https://api.v2.sondehub.org').rstrip("/")
WATCH = os.environ.get("RADIOSONDE_WATCH_URL", 'http://127.0.0.1:8092').rstrip("/")

# SONDEHUB_PUBLIC_WATCH_ENDPOINT_V38B
WATCH_LOCAL_ENDPOINT = os.environ.get(
    "RADIOSONDE_WATCH_LOCAL_ENDPOINT",
    "",
).strip()

CALLSIGN = os.environ.get("SONDEHUB_LISTENER_CALLSIGN", "").strip()
PORT = int(os.environ.get("SONDEHUB_PLUS_PORT", "8093"))
BIND = os.environ.get("SONDEHUB_PLUS_BIND", "0.0.0.0").strip() or "0.0.0.0"
UA = os.environ.get("SONDEHUB_PLUS_USER_AGENT", 'RadiosondeWatch-SondeHubPlus/1.1')

# SONDEHUB_PUBLIC_CONFIG_V32C
#
# Portable local configuration.
#
AUTO_RX_DIR = os.path.abspath(
    os.path.expanduser(
        os.environ.get(
            "RADIOSONDE_AUTO_RX_DIR",
            os.path.join(
                os.path.expanduser("~"),
                "radiosonde_auto_rx",
            ),
        )
    )
)

AUTO_RX_LOG_DIR = os.path.join(
    AUTO_RX_DIR,
    "log",
)

AUTO_RX_STATION_CFG = os.path.join(
    AUTO_RX_DIR,
    "station.cfg",
)



LOCK = threading.RLock()
CACHE = {}
RT = {
    "stream": None,
    "stream_started": None,
    "subscribed": set(),
    "latest": {},
    "last_message": None,
    "error": None,
}


def utcnow():
    return datetime.now(timezone.utc)


def iso_now():
    return utcnow().isoformat()


def normalize_serial(value):
    value = str(value or "").strip().upper()
    if value.startswith("DFM-"):
        value = value[4:]
    return value


def parse_dt(value):
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def haversine_km(lat1, lon1, lat2, lon2):
    r = 6371.0088
    p1 = math.radians(float(lat1))
    p2 = math.radians(float(lat2))
    dp = math.radians(float(lat2) - float(lat1))
    dl = math.radians(float(lon2) - float(lon1))
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def http_bytes(url, timeout=30, accept="application/json"):
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": UA,
            "Accept": accept,
            "Accept-Encoding": "gzip, identity",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        data = r.read()
        if r.headers.get("Content-Encoding", "").lower() == "gzip":
            data = gzip.decompress(data)
        return data


def get_json_url(url, timeout=30):
    return json.loads(http_bytes(url, timeout=timeout).decode("utf-8"))


def get_text_url(url, timeout=30):
    return http_bytes(url, timeout=timeout, accept="text/plain").decode("utf-8", errors="replace").strip()


def api_json(path, params=None, timeout=30):
    url = API + path
    if params:
        url += "?" + urllib.parse.urlencode(params)
    return get_json_url(url, timeout=timeout)


def watch_json(path, timeout=10):
    return get_json_url(WATCH + path, timeout=timeout)


def watch_local_json(timeout=5):
    endpoint = WATCH_LOCAL_ENDPOINT.strip()

    if not endpoint:
        raise RuntimeError(
            "RADIOSONDE_WATCH_LOCAL_ENDPOINT "
            "is not configured"
        )

    if not endpoint.startswith("/"):
        endpoint = "/" + endpoint

    return watch_json(
        endpoint,
        timeout=timeout,
    )



def cached(key, ttl, loader):
    now = time.time()
    with LOCK:
        item = CACHE.get(key)
        if item and now - item["ts"] < ttl:
            return item["value"]
    value = loader()
    with LOCK:
        CACHE[key] = {"ts": now, "value": value}
    return value


def home():
    d = watch_json("/api/status")
    h = d.get("home") or {}
    return {
        "name": h.get("name") or "Stacja lokalna",
        "lat": float(h["lat"]),
        "lon": float(h["lon"]),
    }


def watch_sondes():
    d = watch_json("/api/radiosondes")
    rows = d.get("radiosondes") or []
    return [x for x in rows if isinstance(x, dict)]


def active_serials():
    out = set()
    for x in watch_sondes():
        if x.get("status") in ("LIVE", "RECENT") and x.get("serial"):
            out.add(normalize_serial(x["serial"]))
    return out


def realtime_message(msg):
    if not isinstance(msg, dict):
        return
    serial = normalize_serial(msg.get("serial") or msg.get("payload_callsign"))
    if not serial:
        return
    with LOCK:
        RT["latest"][serial] = msg
        RT["last_message"] = iso_now()
        RT["error"] = None


# SONDEHUB_STREAM_FIX_V8
# SONDEHUB_STREAM_HARDEN_V13
#
# V13:
#
# - NIE używa stream.add_sonde()
# - NIE używa stream.remove_sonde()
# - NIE pozostawia pysondehub _on_connect
# - NIE pozostawia pysondehub _on_disconnect
#
# Zmiana listy sond = czyste odtworzenie Stream.
#
# Reconnect MQTT obsługuje paho loop_start,
# bez wywoływania Stream.ws_connect() z callbacków.
#

import json as _v13_json


_V13_REBUILD = threading.Event()

_V13_STATS_LOCK = threading.Lock()

_V13_STATS = {
    "created": 0,
    "stopped": 0,
    "connect": 0,
    "disconnect": 0,
    "subscribe_error": 0,
    "messages": 0,
}


def _v13_stat(name):

    with _V13_STATS_LOCK:
        _V13_STATS[name] = (
            _V13_STATS.get(name, 0)
            +
            1
        )


def _v13_log(message):

    print(
        "[STREAM-V13] "
        +
        str(message),
        flush=True
    )


def _v13_message(
    client,
    userdata,
    msg
):

    try:

        data = _v13_json.loads(
            msg.payload
        )

    except Exception as e:

        _v13_log(
            "JSON error: "
            +
            repr(e)
        )

        return


    _v13_stat(
        "messages"
    )

    realtime_message(
        data
    )


def _v13_stop_stream(stream):

    if stream is None:
        return


    mqttc = getattr(
        stream,
        "mqttc",
        None
    )


    if mqttc is not None:

        #
        # Najpierw usuwamy callbacki.
        #
        # Dzięki temu disconnect NIE może
        # wrócić do pysondehub.ws_connect().
        #
        try:
            mqttc.on_connect = None
        except Exception:
            pass

        try:
            mqttc.on_disconnect = None
        except Exception:
            pass

        try:
            mqttc.on_message = None
        except Exception:
            pass


        try:
            mqttc.disconnect()
        except Exception:
            pass


    stopper = getattr(
        stream,
        "loop_stop",
        None
    )

    if callable(stopper):

        try:
            stopper()
        except Exception:
            pass


    _v13_stat(
        "stopped"
    )


def _v13_new_stream(wanted):

    wanted_tuple = tuple(
        sorted(wanted)
    )


    #
    # Stream konfigurujemy przez pysondehub,
    # ale NIE uruchamiamy jego własnego loop.
    #
    stream = sondehub.Stream(
        on_message=realtime_message,
        sondes=list(wanted_tuple),
        auto_start_loop=False,
    )


    mqttc = getattr(
        stream,
        "mqttc",
        None
    )

    if mqttc is None:

        raise RuntimeError(
            "sondehub.Stream bez mqttc"
        )


    prefix = getattr(
        stream,
        "prefix",
        "sondes"
    )


    #
    # Własny on_connect.
    #
    # Celowo NIE wywołujemy:
    #
    #   stream._on_connect()
    #   stream.add_sonde()
    #
    # ponieważ mogą wrócić do ws_connect().
    #
    def on_connect(
        client,
        userdata,
        flags,
        rc
    ):

        _v13_stat(
            "connect"
        )


        if rc != 0:

            _v13_log(
                "connect rc="
                +
                str(rc)
            )

            _V13_REBUILD.set()

            return


        errors = []


        for serial in wanted_tuple:

            try:

                result, mid = (
                    client.subscribe(
                        f"{prefix}/{serial}",
                        0
                    )
                )

                if result != 0:

                    errors.append(
                        (
                            serial,
                            result
                        )
                    )

            except Exception as e:

                errors.append(
                    (
                        serial,
                        repr(e)
                    )
                )


        if errors:

            _v13_stat(
                "subscribe_error"
            )

            _v13_log(
                "subscribe errors="
                +
                repr(errors)
            )

            _V13_REBUILD.set()

        else:

            _v13_log(
                "connected; subscribed="
                +
                str(len(wanted_tuple))
            )

            with LOCK:
                RT["error"] = None


    def on_disconnect(
        client,
        userdata,
        rc
    ):

        _v13_stat(
            "disconnect"
        )

        _v13_log(
            "disconnect rc="
            +
            str(rc)
        )

        #
        # WAŻNE:
        #
        # nie robimy tutaj connect()
        # ani ws_connect().
        #
        # Paho network loop sam obsługuje
        # ponowną próbę połączenia.
        #
        with LOCK:

            RT["error"] = (
                "MQTT disconnected rc="
                +
                str(rc)
            )


    #
    # Nadpisujemy WSZYSTKIE callbacki,
    # które w pysondehub prowadzą do
    # automatycznego ws_connect().
    #
    mqttc.on_connect = on_connect
    mqttc.on_disconnect = on_disconnect
    mqttc.on_message = _v13_message


    #
    # Łagodny backoff reconnect.
    #
    reconnect_delay_set = getattr(
        mqttc,
        "reconnect_delay_set",
        None
    )

    if callable(reconnect_delay_set):

        reconnect_delay_set(
            min_delay=1,
            max_delay=30
        )


    starter = getattr(
        stream,
        "loop_start",
        None
    )

    if not callable(starter):

        raise RuntimeError(
            "sondehub.Stream bez loop_start"
        )


    starter()


    _v13_stat(
        "created"
    )


    _v13_log(
        "stream created; wanted="
        +
        str(len(wanted_tuple))
    )


    return stream


def stream_manager():

    stream = None

    subscribed = set()


    while True:

        try:

            wanted = active_serials()


            #
            # Błąd subskrypcji:
            # tworzymy całkowicie nową instancję.
            #
            if (
                stream is not None
                and
                _V13_REBUILD.is_set()
            ):

                _v13_log(
                    "rebuild requested"
                )

                _v13_stop_stream(
                    stream
                )

                stream = None
                subscribed = set()

                _V13_REBUILD.clear()


                with LOCK:

                    RT["stream"] = None
                    RT["subscribed"] = set()


            #
            # Zmieniła się lista aktywnych sond.
            #
            # NIE robimy add/remove.
            # Odtwarzamy Stream.
            #
            if (
                stream is not None
                and
                wanted != subscribed
            ):

                _v13_log(
                    "subscription set changed "
                    +
                    str(len(subscribed))
                    +
                    " -> "
                    +
                    str(len(wanted))
                )


                _v13_stop_stream(
                    stream
                )

                stream = None
                subscribed = set()


                with LOCK:

                    RT["stream"] = None
                    RT["subscribed"] = set()


            #
            # Brak sond.
            #
            if not wanted:

                if stream is not None:

                    _v13_stop_stream(
                        stream
                    )

                    stream = None


                subscribed = set()


                with LOCK:

                    RT["stream"] = None
                    RT["subscribed"] = set()
                    RT["latest"].clear()


            #
            # Tworzymy Stream.
            #
            elif stream is None:

                stream = _v13_new_stream(
                    wanted
                )

                subscribed = set(
                    wanted
                )


                with LOCK:

                    RT["stream"] = stream

                    RT["stream_started"] = (
                        iso_now()
                    )

                    RT["subscribed"] = set(
                        subscribed
                    )

                    RT["error"] = None


            #
            # Sprzątamy stare telemetrie.
            #
            with LOCK:

                stale = (
                    set(RT["latest"])
                    -
                    wanted
                )

                for serial in stale:

                    RT["latest"].pop(
                        serial,
                        None
                    )


        except Exception as e:

            _v13_log(
                "manager error: "
                +
                repr(e)
            )

            with LOCK:

                RT["error"] = (
                    f"{type(e).__name__}: {e}"
                )


        time.sleep(20)


def realtime_snapshot():
    with LOCK:
        return {
            "stream_started": RT["stream_started"],
            "tracked_serials": sorted(RT["subscribed"]),
            "tracked_count": len(RT["subscribed"]),
            "last_message": RT["last_message"],
            "error": RT["error"],
        }


def realtime_for(serial):
    with LOCK:
        return RT["latest"].get(normalize_serial(serial))


def all_sites():
    return cached("sites", 3600, lambda: api_json("/sites"))


def site_position(site):
    p = site.get("position") if isinstance(site, dict) else None
    if isinstance(p, list) and len(p) >= 2:
        try:
            # SondeHub /sites stores site positions in GeoJSON order [lon, lat].
            # Normalize to (lat, lon) everywhere in this local application.
            return float(p[1]), float(p[0])
        except Exception:
            return None
    return None


def site_by_id(site_id):
    if site_id is None:
        return None
    sites = all_sites()
    if not isinstance(sites, dict):
        return None
    key = str(site_id)
    obj = sites.get(key)
    if not isinstance(obj, dict):
        return None
    pos = site_position(obj)
    if not pos:
        return None
    return {
        "site_id": key,
        "station": obj.get("station"),
        "station_name": obj.get("station_name"),
        "position": [pos[0], pos[1]],
        "alt": obj.get("alt"),
        "times": obj.get("times"),
        "rs_types": obj.get("rs_types"),
        "burst_altitude": obj.get("burst_altitude"),
        "ascent_rate": obj.get("ascent_rate"),
        "descent_rate": obj.get("descent_rate"),
    }


def nearest_site(lat, lon):
    try:
        sites = all_sites()
    except Exception:
        return None
    best = None
    if not isinstance(sites, dict):
        return None
    for site_id, obj in sites.items():
        if not isinstance(obj, dict):
            continue
        pos = site_position(obj)
        if not pos:
            continue
        d = haversine_km(lat, lon, pos[0], pos[1])
        if best is None or d < best["distance_km"]:
            best = {
                "site_id": str(site_id),
                "station": obj.get("station"),
                "station_name": obj.get("station_name"),
                "position": [pos[0], pos[1]],
                "alt": obj.get("alt"),
                "times": obj.get("times"),
                "rs_types": obj.get("rs_types"),
                "burst_altitude": obj.get("burst_altitude"),
                "ascent_rate": obj.get("ascent_rate"),
                "descent_rate": obj.get("descent_rate"),
                "distance_km": round(d, 1),
            }
    return best


# SONDEHUB_LISTENER_FRESHNESS_V48
# radiosonde_auto_rx uploads station-position metadata on a multi-hour cadence.
# Query a full day, then decide freshness locally instead of treating a missing
# 3-hour result as proof that the station is offline.
SONDEHUB_LISTENER_ONLINE_SEC = 8 * 3600


def latest_listener_record(raw, callsign=CALLSIGN):
    if not isinstance(raw, dict):
        return None

    bucket = raw.get(callsign)
    if bucket is None:
        for k, v in raw.items():
            if str(k).strip().lower() == callsign.lower():
                bucket = v
                break

    if not isinstance(bucket, dict):
        return None

    if "uploader_position" in bucket:
        dt = (
            parse_dt(bucket.get("time_received"))
            or parse_dt(bucket.get("datetime"))
        )
        return bucket, dt

    candidates = []
    for ts, obj in bucket.items():
        if not isinstance(obj, dict):
            continue

        dt = (
            parse_dt(ts)
            or parse_dt(obj.get("time_received"))
            or parse_dt(obj.get("datetime"))
        )

        candidates.append(
            (
                dt or datetime.min.replace(tzinfo=timezone.utc),
                obj,
                dt,
            )
        )

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[0])
    _, obj, dt = candidates[-1]
    return obj, dt


def latest_listener(raw, callsign=CALLSIGN):
    record = latest_listener_record(raw, callsign)
    return record[0] if record else None


def own_listener():
    raw = cached(
        "listener-own-1d",
        30,
        lambda: api_json(
            "/listeners/telemetry",
            {"duration": "1d", "uploader_callsign": CALLSIGN},
        ),
    )

    record = latest_listener_record(raw)

    if record is None:
        latest = None
        latest_dt = None
    else:
        latest, latest_dt = record

    age_sec = None

    if latest_dt is not None:
        age_sec = max(
            0,
            int((utcnow() - latest_dt).total_seconds()),
        )

    online = bool(
        latest is not None
        and age_sec is not None
        and age_sec <= SONDEHUB_LISTENER_ONLINE_SEC
    )

    return {
        "raw": raw,
        "latest": latest,
        "latest_time": (
            latest_dt.isoformat()
            if latest_dt is not None
            else None
        ),
        "age_sec": age_sec,
        "online": online,
        "freshness_limit_sec": SONDEHUB_LISTENER_ONLINE_SEC,
        "query_duration": "1d",
    }


def recovery_stats(distance_km=1000):
    h = home()
    return api_json(
        "/recovered/stats",
        {
            "lat": h["lat"],
            "lon": h["lon"],
            "distance": int(float(distance_km) * 1000),
        },
    )


def local_received_today():
    day = utcnow().strftime("%Y%m%d")
    serials = set()
    for path in glob(os.path.join(AUTO_RX_LOG_DIR, f"{day}*_sonde.log")):
        try:
            with open(path, "rb") as f:
                size = os.path.getsize(path)
                f.seek(max(0, size - 32768))
                lines = f.read().decode("utf-8", errors="replace").splitlines()
            for line in reversed(lines):
                fields = line.strip().split(",", 2)
                if len(fields) >= 2:
                    s = normalize_serial(fields[1])
                    if s:
                        serials.add(s)
                        break
        except Exception:
            pass
    return sorted(serials)



# LOCAL_RX_TABLE_V18


def _rx_float(value):

    try:
        return float(value)
    except Exception:
        return None


def _rx_int(value):

    try:
        return int(float(value))
    except Exception:
        return None


def _rx_parse_line(line):

    from datetime import (
        datetime as _datetime,
        timezone as _timezone,
    )

    fields=line.rstrip(
        "\r\n"
    ).split(",")


    if len(fields) < 18:
        return None


    timestamp=fields[0].strip()


    try:

        dt=_datetime.fromisoformat(
            timestamp.replace(
                "Z",
                "+00:00"
            )
        )

        if dt.tzinfo is None:

            dt=dt.replace(
                tzinfo=_timezone.utc
            )

        epoch=dt.timestamp()

    except Exception:

        return None


    serial=normalize_serial(
        fields[1]
    )

    if not serial:
        return None


    return {

        "_epoch":
            epoch,

        "timestamp":
            timestamp,

        "serial":
            serial,

        "frame":
            fields[2].strip(),

        "lat":
            _rx_float(
                fields[3]
            ),

        "lon":
            _rx_float(
                fields[4]
            ),

        "alt_m":
            _rx_float(
                fields[5]
            ),

        "vel_v_ms":
            _rx_float(
                fields[6]
            ),

        "vel_h_ms":
            _rx_float(
                fields[7]
            ),

        "heading_deg":
            _rx_float(
                fields[8]
            ),

        "temp_c":
            _rx_float(
                fields[9]
            ),

        "humidity_pct":
            _rx_float(
                fields[10]
            ),

        "pressure_hpa":
            _rx_float(
                fields[11]
            ),

        "type":
            fields[12].strip(),

        "frequency_mhz":
            _rx_float(
                fields[13]
            ),

        "snr_db":
            _rx_float(
                fields[14]
            ),

        "frequency_error":
            _rx_float(
                fields[15]
            ),

        "sats":
            _rx_int(
                fields[16]
            ),

        "battery_v":
            _rx_float(
                fields[17]
            ),
    }



# ==============================================================
# FULL_ANALYTICS_V39
# Lokalne dane auto_rx dla analiz SondeHub+
# ==============================================================

def _v39_float(value):
    try:
        x=float(value)
        if x != x:
            return None
        return x
    except Exception:
        return None


def _v39_time(value):
    from datetime import datetime, timezone

    if value is None:
        return None

    txt=str(value).strip()

    if not txt:
        return None

    try:
        if txt.endswith("Z"):
            txt=txt[:-1] + "+00:00"

        dt=datetime.fromisoformat(txt)

        if dt.tzinfo is None:
            dt=dt.replace(tzinfo=timezone.utc)

        return dt.astimezone(timezone.utc)

    except Exception:
        return None


def _v39_station_position():
    """
    Najpierw próbujemy odczytać lokalizację ze station.cfg.
    Fallback = pozycja używana obecnie przez SondeHub+.
    """
    lat=54.2
    lon=16.184

    try:
        import configparser

        cfg=configparser.ConfigParser()
        cfg.read(AUTO_RX_STATION_CFG)

        lat_keys=(
            "station_lat",
            "station_latitude",
            "latitude",
        )

        lon_keys=(
            "station_lon",
            "station_longitude",
            "longitude",
        )

        for section in cfg.sections():

            for key in lat_keys:
                if cfg.has_option(section,key):
                    x=_v39_float(
                        cfg.get(section,key)
                    )
                    if x is not None and -90 <= x <= 90:
                        lat=x
                        break

            for key in lon_keys:
                if cfg.has_option(section,key):
                    x=_v39_float(
                        cfg.get(section,key)
                    )
                    if x is not None and -180 <= x <= 180:
                        lon=x
                        break

    except Exception:
        pass

    return lat,lon


def _v39_haversine(lat1,lon1,lat2,lon2):
    import math

    vals=[
        _v39_float(lat1),
        _v39_float(lon1),
        _v39_float(lat2),
        _v39_float(lon2),
    ]

    if any(x is None for x in vals):
        return None

    lat1,lon1,lat2,lon2=vals

    r=6371.0

    p1=math.radians(lat1)
    p2=math.radians(lat2)

    dp=math.radians(lat2-lat1)
    dl=math.radians(lon2-lon1)

    a=(
        math.sin(dp/2.0)**2
        +
        math.cos(p1)
        *
        math.cos(p2)
        *
        math.sin(dl/2.0)**2
    )

    return (
        2.0
        *
        r
        *
        math.atan2(
            math.sqrt(a),
            math.sqrt(1.0-a)
        )
    )


def _v39_parse_sonde_log(path, wanted_serial=None, cutoff=None):
    """
    V39_1_CSV_HEADER_FIX

    Czytamy auto_rx po NAZWACH KOLUMN, a nie po indeksach.

    Aktualny format:
      timestamp
      serial
      frame
      lat
      lon
      alt
      vel_v
      vel_h
      heading
      temp
      humidity
      pressure
      type
      freq_mhz
      snr
      f_error_hz
      sats
      batt_v
      burst_timer
      aux_data
    """
    import csv

    wanted=(
        str(wanted_serial)
        .strip()
        .upper()
        if wanted_serial
        else None
    )

    out=[]

    try:
        fh=open(
            path,
            "r",
            encoding="utf-8",
            errors="replace",
            newline=""
        )
    except Exception:
        return out


    def first_value(row,*names):

        for name in names:

            value=row.get(
                str(name).lower()
            )

            if (
                value is not None
                and
                str(value).strip() != ""
            ):
                return value

        return None


    with fh:

        reader=csv.DictReader(fh)

        if not reader.fieldnames:
            return out

        headers={
            str(x or "")
            .strip()
            .lower()
            for x in reader.fieldnames
        }

        if "serial" not in headers:
            return out


        for raw in reader:

            row={
                str(k or "")
                .strip()
                .lower():
                v

                for k,v in raw.items()

                if k is not None
            }


            serial=str(
                first_value(
                    row,
                    "serial"
                )
                or
                ""
            ).strip()


            if not serial:
                continue


            if (
                wanted
                and
                serial.upper() != wanted
            ):
                continue


            dt=_v39_time(
                first_value(
                    row,
                    "timestamp",
                    "datetime",
                    "time"
                )
            )


            if cutoff is not None:

                if (
                    dt is None
                    or
                    dt < cutoff
                ):
                    continue


            lat=_v39_float(
                first_value(
                    row,
                    "lat",
                    "latitude"
                )
            )

            lon=_v39_float(
                first_value(
                    row,
                    "lon",
                    "longitude"
                )
            )

            alt=_v39_float(
                first_value(
                    row,
                    "alt",
                    "altitude"
                )
            )

            vel_v=_v39_float(
                first_value(
                    row,
                    "vel_v",
                    "ascent_rate"
                )
            )

            speed=_v39_float(
                first_value(
                    row,
                    "vel_h",
                    "speed",
                    "ground_speed"
                )
            )

            heading=_v39_float(
                first_value(
                    row,
                    "heading"
                )
            )

            temp=_v39_float(
                first_value(
                    row,
                    "temp",
                    "temperature"
                )
            )

            humidity=_v39_float(
                first_value(
                    row,
                    "humidity"
                )
            )

            pressure=_v39_float(
                first_value(
                    row,
                    "pressure"
                )
            )


            sonde_type=str(
                first_value(
                    row,
                    "type",
                    "sonde_type",
                    "subtype"
                )
                or
                ""
            ).strip()


            freq=_v39_float(
                first_value(
                    row,
                    "freq_mhz",
                    "frequency_mhz",
                    "frequency",
                    "freq"
                )
            )


            snr=_v39_float(
                first_value(
                    row,
                    "snr",
                    "snr_db"
                )
            )


            # auto_rx może używać wartości około -99
            # jako brak poprawnego SNR.
            if (
                snr is not None
                and
                snr <= -90
            ):
                snr=None


            freq_error=_v39_float(
                first_value(
                    row,
                    "f_error_hz",
                    "frequency_error",
                    "frequency_error_hz"
                )
            )


            sats=_v39_float(
                first_value(
                    row,
                    "sats",
                    "satellites"
                )
            )


            battery=_v39_float(
                first_value(
                    row,
                    "batt_v",
                    "battery_v",
                    "battery"
                )
            )


            frame=_v39_float(
                first_value(
                    row,
                    "frame"
                )
            )


            burst_timer=_v39_float(
                first_value(
                    row,
                    "burst_timer"
                )
            )


            aux_data=first_value(
                row,
                "aux_data"
            )


            out.append({

                "datetime":
                    (
                        dt.isoformat()
                        if dt is not None
                        else
                        str(
                            first_value(
                                row,
                                "timestamp",
                                "datetime",
                                "time"
                            )
                            or
                            ""
                        ).strip()
                    ),

                "_epoch":
                    (
                        dt.timestamp()
                        if dt is not None
                        else None
                    ),

                "serial":
                    serial,

                "frame":
                    frame,

                "lat":
                    lat,

                "lon":
                    lon,

                "alt":
                    alt,

                "vel_v":
                    vel_v,

                "speed":
                    speed,

                "heading":
                    heading,

                "temp":
                    temp,

                "humidity":
                    humidity,

                "pressure":
                    pressure,

                "type":
                    sonde_type,

                "frequency":
                    freq,

                "snr":
                    snr,

                "frequency_error":
                    freq_error,

                "sats":
                    sats,

                "battery_v":
                    battery,

                "burst_timer":
                    burst_timer,

                "aux_data":
                    aux_data,
            })


    out.sort(
        key=lambda x:
            x.get("_epoch")
            if x.get("_epoch") is not None
            else 0
    )

    return out

def v39_local_flight(serial):
    import glob
    import os

    serial=str(serial or "").strip()

    if not serial:
        return {
            "serial":"",
            "count":0,
            "samples":[],
        }

    wanted=serial.upper()

    files=[]

    for path in glob.glob(
        os.path.join(AUTO_RX_LOG_DIR, "*_sonde.log")
    ):

        base=os.path.basename(path).upper()

        # Najpierw filtr po nazwie pliku, żeby nie czytać
        # niepotrzebnie całego archiwum.
        if f"_{wanted}_" in base:
            files.append(path)

    samples=[]

    for path in files:
        samples.extend(
            _v39_parse_sonde_log(
                path,
                wanted_serial=serial
            )
        )

    samples.sort(
        key=lambda x:
            x.get("_epoch")
            if x.get("_epoch") is not None
            else 0
    )

    # Bezpieczny limit. Normalny lot jest dużo mniejszy.
    if len(samples) > 15000:
        samples=samples[-15000:]

    return {
        "serial":serial,
        "count":len(samples),
        "samples":samples,
    }


def v39_station_stats(days=7):
    import glob
    import os
    from collections import Counter
    from datetime import datetime, timedelta, timezone

    now=datetime.now(timezone.utc)
    cutoff=now-timedelta(days=float(days))

    home_lat,home_lon=_v39_station_position()

    total_frames=0

    sondes={}
    type_counter=Counter()
    freq_counter=Counter()

    # V39_2_MAX_ALT
    best_snr=None
    min_alt=None
    min_temp_c=None
    max_alt=None
    max_distance=None

    for path in glob.glob(
        os.path.join(AUTO_RX_LOG_DIR, "*_sonde.log")
    ):

        try:
            # Stary plik nie może zawierać nowych ramek.
            if os.path.getmtime(path) < cutoff.timestamp()-86400:
                continue
        except Exception:
            pass

        rows=_v39_parse_sonde_log(
            path,
            cutoff=cutoff
        )

        for row in rows:

            serial=str(
                row.get("serial") or ""
            ).strip()

            if not serial:
                continue

            total_frames+=1

            info=sondes.setdefault(
                serial,
                {
                    "serial":serial,
                    "frames":0,
                    "type":"",
                    "frequency":None,
                    "first_epoch":None,
                    "last_epoch":None,
                    "best_snr":None,
                    "min_alt":None,
                    "max_alt":None,
                    "max_distance_km":None,
                }
            )

            info["frames"]+=1

            epoch=row.get("_epoch")

            if epoch is not None:

                if (
                    info["first_epoch"] is None
                    or
                    epoch < info["first_epoch"]
                ):
                    info["first_epoch"]=epoch

                if (
                    info["last_epoch"] is None
                    or
                    epoch > info["last_epoch"]
                ):
                    info["last_epoch"]=epoch

            typ=str(
                row.get("type") or ""
            ).strip()

            if typ:
                info["type"]=typ

            freq=row.get("frequency")

            if freq is not None:
                info["frequency"]=freq

            snr=row.get("snr")

            if snr is not None:

                if (
                    info["best_snr"] is None
                    or
                    snr > info["best_snr"]
                ):
                    info["best_snr"]=snr

                if best_snr is None or snr > best_snr:
                    best_snr=snr

            alt=row.get("alt")

            if alt is not None:

                if (
                    info["min_alt"] is None
                    or
                    alt < info["min_alt"]
                ):
                    info["min_alt"]=alt

                if min_alt is None or alt < min_alt:
                    min_alt=alt

                if (
                    info["max_alt"] is None
                    or
                    alt > info["max_alt"]
                ):
                    info["max_alt"]=alt

                if max_alt is None or alt > max_alt:
                    max_alt=alt

            temp=row.get("temp")

            if temp is not None:

                try:
                    temp=float(temp)
                except Exception:
                    temp=None

                if (
                    temp is not None
                    and
                    -120.0 <= temp <= 80.0
                ):

                    if (
                        min_temp_c is None
                        or
                        temp < min_temp_c
                    ):
                        min_temp_c=temp


            dist=_v39_haversine(
                home_lat,
                home_lon,
                row.get("lat"),
                row.get("lon")
            )

            if dist is not None:

                if (
                    info["max_distance_km"] is None
                    or
                    dist > info["max_distance_km"]
                ):
                    info["max_distance_km"]=dist

                if (
                    max_distance is None
                    or
                    dist > max_distance
                ):
                    max_distance=dist

    for info in sondes.values():

        typ=str(
            info.get("type") or ""
        ).strip()

        if typ:
            type_counter[typ]+=1

        freq=info.get("frequency")

        if freq is not None:
            freq_counter[
                f"{float(freq):.3f}"
            ]+=1

    recent=sorted(
        sondes.values(),
        key=lambda x:
            x.get("last_epoch") or 0,
        reverse=True
    )

    return {
        "days":float(days),
        "station_lat":home_lat,
        "station_lon":home_lon,
        "sondes":len(sondes),
        "total_frames":total_frames,
        "unique_types":len(type_counter),
        "best_snr":best_snr,
        "min_alt":min_alt,
        "max_alt":max_alt,
        "min_temp_c":min_temp_c,
        "max_distance_km":max_distance,
        "top_types":type_counter.most_common(8),
        "top_frequencies":freq_counter.most_common(8),
        "recent_serials":[
            x["serial"]
            for x in recent[:25]
        ],
        "sessions":recent[:100],
    }



# ============================================================
# STATION_STATS_CACHE_V40_2
#
# v39_station_stats() skanuje logi auto_rx i jest kosztowne.
#
# 60 s cache:
# - wystarczająco świeże dla statystyk 7-dniowych,
# - brak wielokrotnego parsowania tych samych logów,
# - lock zapewnia SINGLE-FLIGHT:
#   tylko jeden wątek wykonuje ciężkie przeliczenie.
# ============================================================

import threading as _v40_stats_threading
import time as _v40_stats_time

_v40_station_stats_uncached = v39_station_stats

_v40_station_stats_cache = {}

_v40_station_stats_lock = (
    _v40_stats_threading.Lock()
)

_v40_station_stats_ttl = 60.0


def v39_station_stats(days=7):

    key=float(days)

    with _v40_station_stats_lock:

        now=_v40_stats_time.monotonic()

        cached=(
            _v40_station_stats_cache
            .get(key)
        )

        if cached is not None:

            age=(
                now
                -
                cached["time"]
            )

            if age < _v40_station_stats_ttl:
                return cached["data"]


        data=(
            _v40_station_stats_uncached(
                days
            )
        )

        _v40_station_stats_cache[key]={
            "time":
                _v40_stats_time.monotonic(),

            "data":
                data,
        }

        return data


def local_rx_sessions(
    hours=24.0
):

    import os as _os
    import time as _time

    from glob import (
        glob as _glob
    )


    # LOCAL_RX_RANGE_V30
    # None = wszystkie zapisane sesje.
    if hours is None:
        cutoff=None
    else:
        hours=float(hours)

        hours=max(
            1.0,
            hours
        )

        cutoff=(
            _time.time()
            -
            hours*3600.0
        )


    sessions=[]


    for path in _glob(
        os.path.join(AUTO_RX_LOG_DIR, "*_sonde.log")
    ):

        first=None
        last=None

        frames=0

        max_alt=None
        max_snr=None

        min_snr=None


        try:

            with open(
                path,
                "r",
                encoding="utf-8",
                errors="replace"
            ) as f:

                for line in f:

                    item=_rx_parse_line(
                        line
                    )

                    if item is None:
                        continue


                    frames += 1


                    if (
                        first is None
                        or
                        item["_epoch"]
                        <
                        first["_epoch"]
                    ):

                        first=item


                    if (
                        last is None
                        or
                        item["_epoch"]
                        >
                        last["_epoch"]
                    ):

                        last=item


                    alt=item.get(
                        "alt_m"
                    )


                    if (
                        alt is not None
                        and
                        (
                            max_alt is None
                            or
                            alt > max_alt
                        )
                    ):

                        max_alt=alt


                    snr=item.get(
                        "snr_db"
                    )


                    if snr is not None:

                        if (
                            max_snr is None
                            or
                            snr > max_snr
                        ):
                            max_snr=snr


                        if (
                            min_snr is None
                            or
                            snr < min_snr
                        ):
                            min_snr=snr


        except Exception:

            continue


        if (
            first is None
            or
            last is None
        ):
            continue


        if (
            cutoff is not None
            and
            last["_epoch"] < cutoff
        ):
            continue


        duration=max(
            0,
            int(
                round(
                    last["_epoch"]
                    -
                    first["_epoch"]
                )
            )
        )


        sessions.append({

            "serial":
                last["serial"],

            "type":
                last["type"],

            "frequency_mhz":
                last[
                    "frequency_mhz"
                ],

            "first_time":
                first[
                    "timestamp"
                ],

            "last_time":
                last[
                    "timestamp"
                ],

            "duration_s":
                duration,

            "frames":
                frames,

            "max_alt_m":
                max_alt,

            "last_alt_m":
                last[
                    "alt_m"
                ],

            "last_lat":
                last[
                    "lat"
                ],

            "last_lon":
                last[
                    "lon"
                ],

            "vel_v_ms":
                last[
                    "vel_v_ms"
                ],

            "vel_h_ms":
                last[
                    "vel_h_ms"
                ],

            "heading_deg":
                last[
                    "heading_deg"
                ],

            "temp_c":
                last[
                    "temp_c"
                ],

            "humidity_pct":
                last[
                    "humidity_pct"
                ],

            "pressure_hpa":
                last[
                    "pressure_hpa"
                ],

            "last_snr_db":
                last[
                    "snr_db"
                ],

            "max_snr_db":
                max_snr,

            "min_snr_db":
                min_snr,

            "frequency_error":
                last[
                    "frequency_error"
                ],

            "sats":
                last[
                    "sats"
                ],

            "battery_v":
                last[
                    "battery_v"
                ],

            "filename":
                _os.path.basename(
                    path
                ),

            "_last_epoch":
                last["_epoch"],
        })


    sessions.sort(
        key=lambda x:
            x["_last_epoch"],
        reverse=True
    )


    for row in sessions:

        row.pop(
            "_last_epoch",
            None
        )


    return sessions


def is_local_uploader(obj):
    if not isinstance(obj, dict):
        return False
    up = str(obj.get("uploader_callsign") or "").strip().casefold()
    cs = str(CALLSIGN or "").strip().casefold()
    return bool(up) and up == cs


def local_live_now():
    with LOCK:
        snap = dict(RT["latest"])
    out = []
    for serial, obj in snap.items():
        if is_local_uploader(obj):
            s = normalize_serial(serial) or str(serial)
            out.append(s)
    return sorted(set(out))


# LOCAL_LIVE_FROM_8092_V36
#
# Źródłem prawdy dla lokalnego odbioru TERAZ są świeże
# ramki radiosonde_auto_rx wykrywane przez Radiosonde Watch 8092.
#
# Stare is_local_uploader() pozostawiamy dla kompatybilności,
# ale nie decyduje już o stanie "odbieramy lokalnie".

def local_receiver_live_now(local=None):

    if local is None:

        try:
            local = watch_local_json()

        except Exception:
            return []


    if not isinstance(
        local,
        dict
    ):
        return []


    values = local.get(
        "received_serials"
    )


    if not isinstance(
        values,
        list
    ):
        return []


    out = set()

    for value in values:

        serial = (
            normalize_serial(
                value
            )
            or
            str(
                value
                or ""
            ).strip()
        )

        if serial:
            out.add(
                serial
            )


    return sorted(
        out
    )


def current_flight(history):
    points = []
    for x in history if isinstance(history, list) else []:
        if not isinstance(x, dict):
            continue
        dt = parse_dt(x.get("datetime"))
        if dt:
            points.append((dt, x))
    points.sort(key=lambda p: p[0])
    if not points:
        return []
    selected = [points[-1]]
    for item in reversed(points[:-1]):
        gap = (selected[0][0] - item[0]).total_seconds()
        if gap >= 7200:
            break
        selected.insert(0, item)
    return [x[1] for x in selected]


def downsample(items, max_points=500):
    if len(items) <= max_points:
        return items
    step = max(1, math.ceil(len(items) / max_points))
    out = items[::step]
    if out[-1] is not items[-1]:
        out.append(items[-1])
    return out


def flight_summary(history):
    flight = current_flight(history)
    alts = []
    track = []
    for x in flight:
        try:
            alts.append(float(x["alt"]))
        except Exception:
            pass
        try:
            track.append(
                {
                    "lat": float(x["lat"]),
                    "lon": float(x["lon"]),
                    "alt": x.get("alt"),
                    "datetime": x.get("datetime"),
                }
            )
        except Exception:
            pass
    track = downsample(track)
    return {
        "samples": len(flight),
        "start": flight[0].get("datetime") if flight else None,
        "end": flight[-1].get("datetime") if flight else None,
        "max_alt_m": max(alts) if alts else None,
        "min_alt_m": min(alts) if alts else None,
        "track": track,
        "first": flight[0] if flight else None,
        "latest": flight[-1] if flight else None,
    }


def prediction_record(raw, serial):
    serial = normalize_serial(serial)
    if isinstance(raw, list):
        for x in raw:
            if isinstance(x, dict) and normalize_serial(x.get("vehicle")) == serial:
                return x
        return raw[0] if raw and isinstance(raw[0], dict) else None
    if isinstance(raw, dict):
        if normalize_serial(raw.get("vehicle")) == serial:
            return raw
        for k, v in raw.items():
            if normalize_serial(k) == serial:
                if isinstance(v, dict):
                    return v
                if isinstance(v, list):
                    return prediction_record(v, serial)
        for v in raw.values():
            if isinstance(v, dict) and normalize_serial(v.get("vehicle")) == serial:
                return v
    return None


def prediction_point(record):
    if not isinstance(record, dict):
        return None
    try:
        return {
            "lat": float(record["latitude"]),
            "lon": float(record["longitude"]),
            "alt": record.get("altitude"),
            "time": record.get("time"),
            "landed": record.get("landed"),
        }
    except Exception:
        return None


def prediction_path(record):
    if not isinstance(record, dict):
        return []
    data = record.get("data")
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except Exception:
            return []
    if not isinstance(data, list):
        return []
    out = []
    for x in data:
        if not isinstance(x, dict):
            continue
        try:
            lat = float(x.get("lat", x.get("latitude")))
            lon = float(x.get("lon", x.get("longitude")))
        except Exception:
            continue
        out.append({
            "lat": lat,
            "lon": lon,
            "alt": x.get("alt", x.get("altitude")),
            "time": x.get("time", x.get("datetime")),
        })
    return downsample(out, max_points=500)


def safe_call(fn):
    try:
        return {"ok": True, "data": fn()}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}", "data": None}



# SONDEHUB_HISTORY_SUMMARY_V27
#
# /sonde/<serial> zwraca posortowaną czasowo tablicę.
#
# Nie przechowujemy całej historii.
#
# Zachowujemy dokładnie:
#   - samples
#   - start / end
#   - first / latest
#   - min_alt_m / max_alt_m
#
# Track jest ograniczany online, aby nigdy nie
# przechowywać setek tysięcy punktów w RAM.
#

def _v27_iter_json_array_url(
    url,
    timeout=45,
):

    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": UA,
            "Accept": "application/json",
            "Accept-Encoding": "gzip, identity",
        },
    )


    decoder = json.JSONDecoder()

    utf8 = codecs.getincrementaldecoder(
        "utf-8"
    )()


    with urllib.request.urlopen(
        req,
        timeout=timeout,
    ) as response:

        encoding = (
            response.headers.get(
                "Content-Encoding",
                "",
            )
            .lower()
        )


        if encoding == "gzip":

            stream = gzip.GzipFile(
                fileobj=response,
                mode="rb",
            )

        else:

            stream = response


        buffer = ""
        pos = 0
        started = False
        eof = False


        while True:

            #
            # Zużytą część bufora wyrzucamy.
            #
            if pos > 1024 * 1024:

                buffer = buffer[pos:]
                pos = 0


            #
            # Dobieramy dane tylko małymi blokami.
            #
            if (
                not eof
                and
                (
                    pos >= len(buffer)
                    or
                    len(buffer) - pos < 262144
                )
            ):

                chunk = stream.read(
                    262144
                )


                if chunk:

                    buffer += utf8.decode(
                        chunk,
                        final=False,
                    )

                else:

                    buffer += utf8.decode(
                        b"",
                        final=True,
                    )

                    eof = True


            while (
                pos < len(buffer)
                and
                buffer[pos].isspace()
            ):

                pos += 1


            if not started:

                if pos >= len(buffer):

                    if eof:

                        raise ValueError(
                            "V27: pusty JSON"
                        )

                    continue


                if buffer[pos] != "[":

                    raise ValueError(
                        "V27: odpowiedź nie jest tablicą JSON"
                    )


                pos += 1
                started = True

                continue


            while (
                pos < len(buffer)
                and
                buffer[pos].isspace()
            ):

                pos += 1


            if pos >= len(buffer):

                if eof:

                    raise ValueError(
                        "V27: nieoczekiwany EOF"
                    )

                continue


            if buffer[pos] == "]":

                return


            if buffer[pos] == ",":

                pos += 1
                continue


            try:

                obj, end = decoder.raw_decode(
                    buffer,
                    pos,
                )

            except json.JSONDecodeError:

                if eof:
                    raise


                chunk = stream.read(
                    262144
                )


                if chunk:

                    buffer += utf8.decode(
                        chunk,
                        final=False,
                    )

                else:

                    buffer += utf8.decode(
                        b"",
                        final=True,
                    )

                    eof = True


                continue


            pos = end

            yield obj



def _v27_stream_flight_summary(
    serial,
    timeout=45,
):

    serial = normalize_serial(
        serial
    )


    url = (
        API
        +
        "/sonde/"
        +
        urllib.parse.quote(
            serial,
            safe="",
        )
    )


    previous_dt = None

    samples = 0

    first = None
    latest = None

    min_alt = None
    max_alt = None


    #
    # Track online.
    #
    # stride jest potęgą 2.
    #
    # Gdy bufor przekroczy 1000 punktów,
    # zostawiamy co drugi i podwajamy stride.
    #
    track = []
    track_seen = 0
    track_stride = 1
    track_compactions = 0

    last_track_point = None

    segment_resets = 0

    records_total = 0
    valid_total = 0

    started = time.monotonic()


    for obj in _v27_iter_json_array_url(
        url,
        timeout=timeout,
    ):

        records_total += 1


        if not isinstance(
            obj,
            dict,
        ):

            continue


        dt = parse_dt(
            obj.get(
                "datetime"
            )
        )


        if dt is None:

            continue


        valid_total += 1


        if previous_dt is not None:

            gap = (
                dt
                -
                previous_dt
            ).total_seconds()


            #
            # Oficjalny endpoint jest time-sorted.
            #
            # Jeśli kiedyś przestanie być, nie wracamy
            # do pełnego json.loads() — zgłaszamy błąd.
            #
            if gap < 0:

                raise RuntimeError(
                    "V27: historia nie jest "
                    "posortowana czasowo"
                )


            #
            # Ta sama granica lotu co w current_flight().
            #
            if gap >= 7200:

                samples = 0

                first = None
                latest = None

                min_alt = None
                max_alt = None

                track = []
                track_seen = 0
                track_stride = 1
                track_compactions = 0

                last_track_point = None

                segment_resets += 1


        previous_dt = dt


        samples += 1


        if first is None:

            first = obj


        latest = obj


        #
        # MIN / MAX altitude — ze wszystkich rekordów
        # aktualnego lotu, bez deduplikacji.
        #
        try:

            alt = float(
                obj["alt"]
            )


            if (
                min_alt is None
                or
                alt < min_alt
            ):

                min_alt = alt


            if (
                max_alt is None
                or
                alt > max_alt
            ):

                max_alt = alt

        except Exception:

            pass


        #
        # Punkt trasy.
        #
        try:

            point = {
                "lat":
                    float(
                        obj["lat"]
                    ),

                "lon":
                    float(
                        obj["lon"]
                    ),

                "alt":
                    obj.get(
                        "alt"
                    ),

                "datetime":
                    obj.get(
                        "datetime"
                    ),
            }

        except Exception:

            continue


        index = track_seen

        track_seen += 1

        last_track_point = point


        if (
            index
            %
            track_stride
            ==
            0
        ):

            track.append(
                point
            )


        #
        # Twardy, niewielki limit pamięci tracku.
        #
        if len(track) > 1000:

            track = track[::2]

            track_stride *= 2

            track_compactions += 1


    #
    # Pusta historia.
    #
    if first is None:

        result = {
            "samples": 0,
            "start": None,
            "end": None,
            "max_alt_m": None,
            "min_alt_m": None,
            "track": [],
            "first": None,
            "latest": None,
        }


    else:

        #
        # Gwarantujemy prawdziwy ostatni punkt trasy.
        #
        if (
            last_track_point is not None
            and
            (
                not track
                or
                track[-1]
                is not
                last_track_point
            )
        ):

            track.append(
                last_track_point
            )


        #
        # Ostatecznie panel nadal dostaje ~500 punktów.
        #
        track = downsample(
            track,
            max_points=500,
        )


        result = {
            "samples":
                samples,

            "start":
                first.get(
                    "datetime"
                ),

            "end":
                latest.get(
                    "datetime"
                ),

            "max_alt_m":
                max_alt,

            "min_alt_m":
                min_alt,

            "track":
                track,

            "first":
                first,

            "latest":
                latest,
        }


    elapsed = (
        time.monotonic()
        -
        started
    )


    print(
        "[HISTORY-V27]"
        f" serial={serial}"
        f" records={records_total}"
        f" valid={valid_total}"
        f" samples={samples}"
        f" track_seen={track_seen}"
        f" track_out={len(result['track'])}"
        f" stride={track_stride}"
        f" compactions={track_compactions}"
        f" resets={segment_resets}"
        f" elapsed={elapsed:.3f}s",
        flush=True,
    )


    return result



def detail(serial):
    serial = normalize_serial(serial)
    if not serial:
        raise ValueError("brak serial")

    with ThreadPoolExecutor(max_workers=5) as ex:
        f_hist = ex.submit(lambda: _v27_stream_flight_summary(serial, timeout=45))
        f_pred = ex.submit(lambda: api_json("/predictions", {"vehicles": serial}, timeout=30))
        f_rev = ex.submit(lambda: api_json("/predictions/reverse", {"vehicles": serial}, timeout=30))
        f_rec = ex.submit(lambda: api_json("/recovered", {"serial": serial, "last": 0}, timeout=30))
        f_listener = ex.submit(own_listener)

        results = {
            "history": safe_call(f_hist.result),
            "prediction": safe_call(f_pred.result),
            "reverse": safe_call(f_rev.result),
            "recovery": safe_call(f_rec.result),
            "listener": safe_call(f_listener.result),
        }

    if (
        results["history"]["ok"]
        and
        isinstance(
            results["history"]["data"],
            dict,
        )
    ):
        flight = results["history"]["data"]

    else:
        flight = flight_summary([])
    pred_raw = results["prediction"]["data"]
    rev_raw = results["reverse"]["data"]
    pred_sel = prediction_record(pred_raw, serial)
    rev_sel = prediction_record(rev_raw, serial)
    pred_point = prediction_point(pred_sel)
    rev_point = prediction_point(rev_sel)

    site_ref = None
    site_source = None

    # Prefer the launch_site ID supplied by SondeHub telemetry when present.
    # Only if it is absent do we show a clearly-labelled nearest-site approximation.
    for candidate in (realtime_for(serial), flight.get("latest"), flight.get("first")):
        if isinstance(candidate, dict) and candidate.get("launch_site") is not None:
            exact = site_by_id(candidate.get("launch_site"))
            if exact:
                site_ref = exact
                site_source = "telemetry.launch_site"
                break

    if site_ref is None and rev_point:
        site_ref = nearest_site(rev_point["lat"], rev_point["lon"])
        if site_ref:
            site_source = "nearest_to_reverse_prediction_approx"
    elif site_ref is None and flight["track"]:
        site_ref = nearest_site(flight["track"][0]["lat"], flight["track"][0]["lon"])
        if site_ref:
            site_source = "nearest_to_first_observed_position_approx"

    local_row = None
    for x in watch_sondes():
        if normalize_serial(x.get("serial")) == serial:
            local_row = x
            break

    latest = realtime_for(serial) or flight.get("latest")

    recovery_raw = results["recovery"]["data"] if results["recovery"]["ok"] else []
    recovery_latest = recovery_raw[-1] if isinstance(recovery_raw, list) and recovery_raw else None
    live_now = (
        serial
        in
        set(
            local_receiver_live_now()
        )
    )

    received_today = serial in set(local_received_today())

    return {
        "serial": serial,
        "generated_at": iso_now(),
        "local": local_row,
        "local_receive_now": live_now,
        "local_received_today": received_today,
        "latest": latest,
        "realtime": realtime_for(serial),
        "flight": flight,
        "prediction": {
            "selected": pred_sel,
            "point": pred_point,
            "path": prediction_path(pred_sel),
            "raw": pred_raw,
            "error": results["prediction"].get("error"),
        },
        "reverse_prediction": {
            "selected": rev_sel,
            "point": rev_point,
            "path": prediction_path(rev_sel),
            "raw": rev_raw,
            "error": results["reverse"].get("error"),
        },
        "launch_site": {
            "site": site_ref,
            "source": site_source,
        },
        "recovery": {
            "latest": recovery_latest,
            "raw": recovery_raw,
            "error": results["recovery"].get("error"),
        },
        "listener": results["listener"]["data"],
        "errors": {
            k: v.get("error")
            for k, v in results.items()
            if not v["ok"]
        },
    }


def filtered_sites(distance_km=1000):
    h = home()
    out = []
    sites = all_sites()
    if not isinstance(sites, dict):
        return out
    for site_id, obj in sites.items():
        if not isinstance(obj, dict):
            continue
        pos = site_position(obj)
        if not pos:
            continue
        d = haversine_km(h["lat"], h["lon"], pos[0], pos[1])
        if d <= float(distance_km):
            out.append(
                {
                    "site_id": str(site_id),
                    "station": obj.get("station"),
                    "station_name": obj.get("station_name"),
                    "position": [pos[0], pos[1]],
                    "alt": obj.get("alt"),
                    "times": obj.get("times"),
                    "rs_types": obj.get("rs_types"),
                    "burst_altitude": obj.get("burst_altitude"),
                    "ascent_rate": obj.get("ascent_rate"),
                    "descent_rate": obj.get("descent_rate"),
                    "distance_km": round(d, 1),
                }
            )
    out.sort(key=lambda x: x["distance_km"])
    return out


def flatten_listeners(raw):
    out = []
    if not isinstance(raw, dict):
        return out
    for callsign, bucket in raw.items():
        latest = latest_listener({callsign: bucket}, callsign=str(callsign))
        if latest:
            item = dict(latest)
            item.setdefault("uploader_callsign", callsign)
            out.append(item)
    return out


def nearby_listeners(distance_km=1000):
    h = home()
    raw = cached(
        "listeners-global-3h",
        120,
        lambda: api_json("/listeners/telemetry", {"duration": "3h"}, timeout=45),
    )
    out = []
    for x in flatten_listeners(raw):
        pos = x.get("uploader_position")
        if not isinstance(pos, list) or len(pos) < 2:
            continue
        try:
            d = haversine_km(h["lat"], h["lon"], float(pos[0]), float(pos[1]))
        except Exception:
            continue
        if d <= float(distance_km):
            out.append(
                {
                    "uploader_callsign": x.get("uploader_callsign"),
                    "uploader_position": pos,
                    "uploader_radio": x.get("uploader_radio"),
                    "uploader_antenna": x.get("uploader_antenna"),
                    "software_name": x.get("software_name"),
                    "software_version": x.get("software_version"),
                    "mobile": x.get("mobile"),
                    "distance_km": round(d, 1),
                }
            )
    out.sort(key=lambda x: x["distance_km"])
    return out


def station_status():
    listener = safe_call(own_listener)
    try:
        local = watch_local_json()
    except Exception as e:
        local = {"error": f"{type(e).__name__}: {e}"}
    live_now = local_receiver_live_now(
        local
    )

    today = local_received_today()
    return {
        "service": "Radiosonde SondeHub+",
        "version": "1.1",
        "port": PORT,
        "callsign": CALLSIGN,
        "home": home(),
        "realtime": realtime_snapshot(),
        "local_receiver": local,
        "sondehub_listener": listener["data"] if listener["ok"] else None,
        "sondehub_listener_error": listener.get("error"),
        "local_live_now": live_now,
        "local_live_count": len(live_now),
        "local_received_today": today,
        "local_received_today_count": len(today),
        "timestamp": iso_now(),
    }


PAGE = r'''<!doctype html>
<html lang="pl">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>SondeHub+ — Stacja lokalna</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
<style>
body{margin:0;background:#11161d;color:#eef3f8;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}
main{max-width:1500px;margin:auto;padding:20px}
a{color:#74baff}.muted{color:#9eacbb}.ok{color:#68e38d}.warn{color:#ffd166}.bad{color:#ff7b7b}
.top{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:10px;margin:14px 0}
.card{background:#1a222d;border:1px solid #33404e;border-radius:8px;padding:12px}
table{width:100%;border-collapse:collapse;background:#1a222d;min-width:1180px}
.tablewrap{overflow-x:auto;overflow-y:hidden;border:1px solid #33404e;border-radius:8px;background:#1a222d}
th,td{padding:8px;border-bottom:1px solid #33404e;text-align:left;font-size:14px;vertical-align:top;overflow-wrap:anywhere;word-break:break-word;white-space:normal}
.pill{display:inline-block;padding:2px 8px;border-radius:999px;border:1px solid #50667d;font-size:12px;white-space:nowrap}
.pillok{background:#163322;color:#7af0a0;border-color:#2b7a4b}
.pillwarn{background:#3a3117;color:#ffd166;border-color:#8d7630}
.pillbad{background:#2b2f36;color:#d7dde4;border-color:#56687a}
.small{font-size:12px;color:#9eacbb}
th{background:#222d39}.click{cursor:pointer}.click:hover{background:#24303c}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:10px;margin-top:12px}
#map{height:430px;border-radius:8px;margin-top:12px;background:#18202a}
pre{white-space:pre-wrap;word-break:break-word;max-height:420px;overflow:auto;background:#0c1117;padding:10px;border-radius:6px}
button{background:#222d39;color:#eef3f8;border:1px solid #50667d;border-radius:6px;padding:7px 10px;cursor:pointer;margin:3px}
.value{font-weight:650}

/* SONDEHUB_LAYOUT_FIX_V13 */

/*
 * Element Grid domyślnie ma min-width:auto.
 * Długi tekst w #site mógł więc poszerzyć kartę
 * poza przydzieloną kolumnę.
 */
.grid > .card,
.top > .card {
    min-width:0;
    max-width:100%;
    box-sizing:border-box;
    overflow:hidden;
}

/*
 * Treść każdej karty pozostaje wewnątrz ramki.
 */
.grid > .card div,
.top > .card div {
    min-width:0;
    max-width:100%;
}

.grid .value,
.top .value,
#site,
#prediction,
#recovery,
#telemetry {
    max-width:100%;
    white-space:normal;
    overflow-wrap:anywhere;
    word-break:break-word;
}

/*
 * Długie dane stacji startowej mają dodatkową
 * gwarancję złamania ciągu.
 */
#site .value {
    overflow-wrap:anywhere;
    word-break:break-word;
}

#site button {
    max-width:100%;
    white-space:normal;
}

/*
 * Główna tabela pozostaje we własnej ramce
 * i może przewijać się poziomo.
 */
.tablewrap {
    max-width:100%;
    width:100%;
    box-sizing:border-box;
}

.tablewrap table {
    max-width:none;
}

.tablewrap th,
.tablewrap td {
    box-sizing:border-box;
}


/* SONDEHUB_GROUPS_FINAL_V15 */

.sondegroupbar {
    display:flex;
    flex-wrap:wrap;
    gap:8px;
    margin:10px 0 12px 0;
}

.sondegroupbtn {
    background:#222d39;
    color:#eef3f8;
    border:1px solid #50667d;
    border-radius:6px;
    padding:7px 11px;
    cursor:pointer;
}

.sondegroupbtn:hover {
    background:#2c3947;
}



/* LOCAL_RX_TABLE_V18 */

.localRxSection {
    margin:18px 0;
}

.localRxSection button {
    cursor:pointer;
}

.localRxTable {
    min-width:1650px !important;
}

.localRxTable th {
    cursor:pointer;
    user-select:none;
    white-space:nowrap;
}

.localRxTable th:hover {
    background:#2b3948;
}

.localRxTable td {
    white-space:nowrap;
}

.localRxArrow {
    color:#76baff;
    padding-left:4px;
}



/* TABLE_SORT_ALL_V23 */
th.sort-v23 {
    cursor: pointer;
    user-select: none;
}
th.sort-v23:hover {
    background: #2b3948;
}

</style>

<!-- SONDE_ANALYTICS_V38 -->
<style id="sonde-analytics-v38-style">

#sondeAnalyticsV38{
    display:grid;
    grid-template-columns:
        repeat(3,minmax(0,1fr));
    gap:10px;
    margin:10px 0;
}

.sav38-card{
    min-width:0;
    padding:13px 14px;

    border:1px solid #34475b;
    border-radius:8px;

    background:
        linear-gradient(
            180deg,
            #1a2633 0%,
            #16212c 100%
        );

    box-shadow:
        0 4px 14px rgba(0,0,0,.12);
}

.sav38-title{
    color:#9fb0c3;
    font-size:12px;
    font-weight:600;
    margin-bottom:5px;
}

.sav38-main{
    color:#f2f6fa;
    font-size:17px;
    line-height:1.2;
    font-weight:800;
    margin-bottom:6px;
    overflow-wrap:anywhere;
}

.sav38-detail{
    color:#aebdcb;
    font-size:12px;
    line-height:1.45;
}

.sav38-green{
    color:#4ade80;
}

.sav38-blue{
    color:#60a5fa;
}

.sav38-amber{
    color:#fbbf24;
}

.sav38-red{
    color:#f87171;
}

.sav38-muted{
    color:#aebdcb;
}

@media(max-width:850px){

    #sondeAnalyticsV38{
        grid-template-columns:1fr;
    }
}

</style>


<!-- FULL_ANALYTICS_V39 -->
<style id="full-analytics-v39-style">

#v39TelemetryStrip{
    margin:10px 0;
    padding:9px 12px;

    border:1px solid #34475b;
    border-radius:8px;

    background:#131e29;

    color:#c8d4df;
    font-size:12px;
    line-height:1.55;
}

#v39TelemetryStrip strong{
    color:#eef5fb;
}

#v39TelemetryStrip .v39-ok{
    color:#4ade80;
    font-weight:700;
}

#v39TelemetryStrip .v39-warn{
    color:#fbbf24;
    font-weight:700;
}

#v39TelemetryStrip .v39-bad{
    color:#f87171;
    font-weight:700;
}


#v39Charts{
    margin:10px 0;
    border:1px solid #34475b;
    border-radius:8px;
    background:#131e29;
    overflow:hidden;
}

#v39Charts summary{
    cursor:pointer;
    padding:11px 13px;
    color:#eef5fb;
    font-weight:700;
    user-select:none;
}

#v39ChartGrid{
    padding:0 11px 11px;
    display:grid;
    grid-template-columns:
        repeat(2,minmax(0,1fr));
    gap:10px;
}

.v39-chart{
    min-width:0;
    padding:9px;
    border:1px solid #2d4053;
    border-radius:7px;
    background:#101923;
}

.v39-chart-title{
    margin-bottom:6px;
    color:#9fb0c3;
    font-size:12px;
    font-weight:700;
}

.v39-chart canvas{
    display:block;
    width:100%;
    height:145px;
}


#v39StationStats{
    margin:12px 0;
    padding:13px;

    border:1px solid #34475b;
    border-radius:8px;

    background:
        linear-gradient(
            180deg,
            #182531,
            #141f29
        );
}

#v39StationStats h3{
    margin:0 0 10px 0;
}

.v39-stat-grid{
    display:grid;
    grid-template-columns:
        repeat(4,minmax(0,1fr));
    gap:8px;
}

.v39-stat-box{
    border:1px solid #2e4255;
    border-radius:7px;
    padding:9px;
    background:#101923;
}

.v39-stat-label{
    color:#8fa3b7;
    font-size:11px;
    margin-bottom:3px;
}

.v39-stat-value{
    color:#f1f5f9;
    font-weight:800;
    font-size:15px;
}

.v39-stat-lists{
    margin-top:9px;
    display:grid;
    grid-template-columns:
        repeat(3,minmax(0,1fr));
    gap:8px;
}

.v39-stat-list{
    border:1px solid #2e4255;
    border-radius:7px;
    padding:9px;
    background:#101923;
    color:#c6d2de;
    font-size:12px;
    line-height:1.55;
}

.v39-extra-box{
    margin-top:9px;
    padding-top:9px;
    border-top:1px solid #33404e;
    color:#c5d2de;
    font-size:12px;
    line-height:1.55;
}

.v39-extra-box strong{
    color:#eef5fb;
}


@media(max-width:900px){

    #v39ChartGrid{
        grid-template-columns:1fr;
    }

    .v39-stat-grid{
        grid-template-columns:
            repeat(2,minmax(0,1fr));
    }

    .v39-stat-lists{
        grid-template-columns:1fr;
    }
}

</style>


<!-- STATION_PANEL_V40 -->
<style id="station-panel-v40">

#v39StationStats{
    margin-top:14px !important;
    padding:16px !important;

    border:1px solid #315b82 !important;
    border-radius:13px !important;

    background:
        linear-gradient(
            180deg,
            rgba(22,42,61,.96) 0%,
            rgba(16,29,41,.98) 100%
        ) !important;

    box-shadow:
        inset 0 1px 0 rgba(255,255,255,.025),
        0 8px 25px rgba(0,0,0,.16) !important;
}


.v40-station-head{
    display:flex;
    align-items:center;
    justify-content:space-between;
    gap:18px;

    margin-bottom:14px;
    padding-bottom:12px;

    border-bottom:
        1px solid rgba(93,139,180,.28);
}


.v40-station-head h3{
    margin:0 0 3px 0 !important;

    color:#f1f6fb !important;

    font-size:18px !important;
    font-weight:800 !important;
}


.v40-station-sub{
    color:#88a4bc;
    font-size:12px;
}


.v40-period-pill{
    flex:0 0 auto;

    padding:7px 12px;

    border:1px solid #456884;
    border-radius:999px;

    background:#1a3043;

    color:#d5e4f0;

    font-size:12px;
    font-weight:700;
}


#v39StationStats .v39-stat-grid{
    display:grid !important;

    grid-template-columns:
        repeat(5,minmax(0,1fr)) !important;

    gap:9px !important;

    margin-bottom:11px !important;
}


#v39StationStats .v39-stat-box{
    min-width:0;

    padding:11px 12px !important;

    border:1px solid #324d64 !important;
    border-radius:9px !important;

    background:
        linear-gradient(
            180deg,
            rgba(17,32,45,.90),
            rgba(13,25,36,.94)
        ) !important;

    box-shadow:
        inset 0 1px 0 rgba(255,255,255,.02) !important;
}


#v39StationStats .v39-stat-label{
    margin-bottom:4px !important;

    color:#8da6bc !important;

    font-size:10px !important;
    line-height:1.25 !important;
}


#v39StationStats .v39-stat-value{
    color:#f4f7fa !important;

    font-size:15px !important;
    line-height:1.2 !important;
    font-weight:800 !important;
}


#v39StationStats .v40-temp-box .v39-stat-value{
    color:#f1c1c1 !important;
}


#v39StationStats .v39-stat-lists{
    display:grid !important;

    grid-template-columns:
        repeat(3,minmax(0,1fr)) !important;

    gap:9px !important;

    margin-top:0 !important;
}


#v39StationStats .v39-stat-list{
    min-height:92px;

    padding:11px 12px !important;

    border:1px solid #324d64 !important;
    border-radius:9px !important;

    background:
        rgba(13,25,36,.80) !important;

    color:#c6d4df !important;

    font-size:12px !important;
    line-height:1.55 !important;
}


#v39StationStats .v39-stat-list strong{
    display:block;

    margin-bottom:4px;

    color:#eef4f8;
    font-size:12px;
}


@media(max-width:1100px){

    #v39StationStats .v39-stat-grid{
        grid-template-columns:
            repeat(3,minmax(0,1fr)) !important;
    }
}


@media(max-width:760px){

    #v39StationStats .v39-stat-grid{
        grid-template-columns:
            repeat(2,minmax(0,1fr)) !important;
    }

    #v39StationStats .v39-stat-lists{
        grid-template-columns:1fr !important;
    }

    .v40-station-head{
        align-items:flex-start;
    }
}

</style>

</head>
<body>
<main>
<!-- SONDEHUB_PLUS_UI_V1 -->
<!-- SONDEHUB_LOCAL_SIGNAL_UI_V2 -->
<h1>🛰 SondeHub+ — Stacja lokalna</h1>
<div><a href="/watch">← Radiosonde Watch</a> · <a href="https://sondehub.org/" target="_blank">SondeHub</a></div>

<div class="top">
<!-- RTL_STATUS_HUMAN_V32 -->
<div class="card">
  <div class="muted">RTL-SDR — aktualne użycie</div>
  <div id="localState" class="value">…</div>
  <div
    id="localStateReason"
    class="muted"
    style="margin-top:4px"
  >…</div>
</div>
<div class="card">
  <div class="muted">SondeHub — nasza stacja</div>
  <div id="hubState" class="value">…</div>
  <div id="hubDetail" class="muted" style="margin-top:4px">…</div>
</div>
<!-- SONDEHUB_LIVE_STATUS_V34 -->
<div class="card">
  <div class="muted">SondeHub — dane na żywo</div>
  <div id="rtState" class="value">…</div>
  <div id="rtDetail" class="muted" style="margin-top:4px">…</div>
</div>
<!-- LOCAL_RECEIVE_HUMAN_V35 -->
<div class="card">
  <div class="muted">Lokalny odbiór radiosond</div>
  <div id="localNow" class="value">…</div>
  <div
    id="localNowDetail"
    class="muted"
    style="margin-top:4px"
  >…</div>
</div>

</div>


<div
 class="localRxSection"
 data-ui="LOCAL_RX_TABLE_V18">

<!-- LOCAL_RX_RANGE_V30_UI -->
<div
 style="
 display:flex;
 align-items:center;
 gap:8px;
 flex-wrap:wrap;
 ">

<button
 id="localRxToggle"
 type="button"
 onclick="toggleLocalRx()">
▶ Pokaż moje odbiory lokalne — ostatnie 24 h
</button>

<select
 id="localRxRangeV30"
 onchange="setLocalRxRangeV30(this.value)"
 title="Zakres lokalnych odbiorów"
 style="
 background:#222d39;
 color:#eef3f8;
 border:1px solid #50667d;
 border-radius:6px;
 padding:7px 10px;
 cursor:pointer;
 ">
<option value="24">24 h</option>
<option value="168">7 dni</option>
<option value="all">Wszystkie</option>
</select>

</div>


<div
 id="localRxPanel"
 style="display:none;margin-top:10px">


<div class="tablewrap">


<table class="localRxTable">

<thead>

<tr>

<th
 data-localrx-key="serial"
 data-localrx-label="Sonda"
 onclick="localRxSort('serial','text')">
Sonda ↕
</th>

<th
 data-localrx-key="type"
 data-localrx-label="Typ"
 onclick="localRxSort('type','text')">
Typ ↕
</th>

<th
 data-localrx-key="frequency_mhz"
 data-localrx-label="MHz"
 onclick="localRxSort('frequency_mhz','number')">
MHz ↕
</th>

<th
 data-localrx-key="first_time"
 data-localrx-label="Początek"
 onclick="localRxSort('first_time','date')">
Początek ↕
</th>

<th
 data-localrx-key="last_time"
 data-localrx-label="Ostatnia ramka"
 onclick="localRxSort('last_time','date')">
Ostatnia ramka ↕
</th>

<th
 data-localrx-key="duration_s"
 data-localrx-label="Czas odbioru"
 onclick="localRxSort('duration_s','number')">
Czas odbioru ↕
</th>

<th
 data-localrx-key="frames"
 data-localrx-label="Ramki"
 onclick="localRxSort('frames','number')">
Ramki ↕
</th>

<th
 data-localrx-key="max_alt_m"
 data-localrx-label="Maks. wysokość"
 onclick="localRxSort('max_alt_m','number')">
Maks. wysokość ↕
</th>

<th
 data-localrx-key="last_alt_m"
 data-localrx-label="Ostatnia wysokość"
 onclick="localRxSort('last_alt_m','number')">
Ostatnia wysokość ↕
</th>

<th
 data-localrx-key="last_snr_db"
 data-localrx-label="SNR ostatnie"
 onclick="localRxSort('last_snr_db','number')">
SNR ostatnie ↕
</th>

<th
 data-localrx-key="max_snr_db"
 data-localrx-label="SNR maks."
 onclick="localRxSort('max_snr_db','number')">
SNR maks. ↕
</th>

<th
 data-localrx-key="frequency_error"
 data-localrx-label="Błąd częst. [Hz]"
 onclick="localRxSort('frequency_error','number')">
Błąd częst. [Hz] ↕
</th>

<th
 data-localrx-key="temp_c"
 data-localrx-label="Temperatura"
 onclick="localRxSort('temp_c','number')">
Temperatura ↕
</th>

<th
 data-localrx-key="humidity_pct"
 data-localrx-label="Wilgotność"
 onclick="localRxSort('humidity_pct','number')">
Wilgotność ↕
</th>

<th
 data-localrx-key="pressure_hpa"
 data-localrx-label="Ciśnienie"
 onclick="localRxSort('pressure_hpa','number')">
Ciśnienie ↕
</th>

<th
 data-localrx-key="sats"
 data-localrx-label="GPS"
 onclick="localRxSort('sats','number')">
GPS ↕
</th>

<th
 data-localrx-key="battery_v"
 data-localrx-label="Bateria"
 onclick="localRxSort('battery_v','number')">
Bateria ↕
</th>

</tr>

</thead>


<tbody id="localRxRows">

<tr>

<td
 colspan="17"
 class="muted">
Pobieranie…
</td>

</tr>

</tbody>

</table>

</div>


<div
 id="localRxMeta"
 class="muted"
 style="margin-top:6px">
</div>


</div>
</div>

<h2>Sondy z Radiosonde Watch</h2>
<div
    class="sondegroupbar"
    data-ui="SONDEHUB_GROUPS_FINAL_V15">

<button
    id="groupLIVE"
    class="sondegroupbtn"
    onclick='toggleSondeGroup("LIVE")'>
▶ Pokaż aktywne (0)
</button>

<button
    id="groupRECENT"
    class="sondegroupbtn"
    onclick='toggleSondeGroup("RECENT")'>
▶ Pokaż utracone (0)
</button>

<button
    id="groupARCHIVE"
    class="sondegroupbtn"
    onclick='toggleSondeGroup("ARCHIVE")'>
▶ Pokaż archiwalne (0)
</button>

</div>
<div class="tablewrap">
<table>
<thead>
<tr>
<th>Status</th>
<th>Lokalnie teraz</th>
<th>Lokalnie dziś</th>
<th>Sonda</th>
<th>Typ</th>
<th>MHz</th>
<th>Wysokość</th>
<th>SNR</th>
<th>RSSI</th>
<th>Ostatni uploader</th>
<th>Ostatnio</th>
</tr>
</thead>
<tbody id="rows"></tbody>
</table>
</div>

<h2 id="detailTitle">Szczegóły sondy</h2>
<div class="grid">
<div class="card"><h3>Telemetria</h3><div id="telemetry">Wybierz sondę.</div></div>
<div class="card"><h3>Start / stacja startowa</h3><div id="site">—</div></div>
<div class="card"><h3>Przewidywanie</h3><div id="prediction">—</div></div>
<div class="card"><h3>Odzyskanie</h3><div id="recovery">—</div></div>
</div>
<div id="map"></div>

<div class="grid">
<div class="card">
<h3>Dodatkowe dane</h3>
<button onclick="loadSites()">Stacje startowe ≤1000 km</button>
<button onclick="loadListeners()">Odbiorniki ≤1000 km</button>
<button onclick="loadAmateur()">Balony amatorskie ≤1000 km</button>
<button onclick="loadRecoveryStats()">Statystyki odzyskiwania</button>
<div id="extraSummary" class="muted"></div>
</div>
<div class="card"><h3>Surowe dane wybranej sondy</h3><details><summary>Pokaż JSON</summary><pre id="raw">—</pre></details></div>
</div>
<pre id="extra" style="display:none"></pre>
</main>

<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script>
const esc=x=>x==null?"":String(x).replaceAll("&","&amp;").replaceAll("<","&lt;").replaceAll(">","&gt;");
const fmt=(x,d=1)=>x==null?"—":Number.isFinite(Number(x))?Number(x).toFixed(d):esc(x);
let HOME=null;
let STATUS_CACHE=null;
let selectedSerial=null;
let map=null;
let layer=null;

async function jget(url){const r=await fetch(url,{cache:"no-store"});if(!r.ok)throw new Error("HTTP "+r.status+" "+url);return await r.json();}

function kv(name,val){return `<div><span class="muted">${esc(name)}:</span> <span class="value">${val==null?"—":esc(val)}</span></div>`;}

function initMap(){
 if(map||typeof L==="undefined")return;
 map=L.map("map").setView([54.2,16.184],6);
 L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",{maxZoom:18,attribution:"&copy; <a href='https://www.openstreetmap.org/copyright'>OpenStreetMap contributors</a>"}).addTo(map);
 layer=L.layerGroup().addTo(map);
}

function pnum(x){const n=Number(x);return Number.isFinite(n)?n:null;}

function addPoint(lat,lon,label){
 lat=pnum(lat);lon=pnum(lon);if(lat==null||lon==null||!layer)return;
 L.marker([lat,lon]).bindPopup(label).addTo(layer);
}


let localRxVisible=false;
let localRxRows=[];

// LOCAL_RX_RANGE_V30
let localRxRangeV30="24";

try {
    const saved=
        localStorage.getItem(
            "sondehubLocalRxRangeV30"
        );

    if (
        saved==="24"
        ||
        saved==="168"
        ||
        saved==="all"
    ) {
        localRxRangeV30=saved;
    }
} catch (_) {}


function localRxRangeLabelV30() {

    if (localRxRangeV30==="168") {
        return "ostatnie 7 dni";
    }

    if (localRxRangeV30==="all") {
        return "wszystkie";
    }

    return "ostatnie 24 h";
}


function localRxApiV30() {

    if (localRxRangeV30==="168") {
        return "/api/local-receptions-7d";
    }

    if (localRxRangeV30==="all") {
        return "/api/local-receptions-all";
    }

    return "/api/local-receptions";
}


function setLocalRxRangeV30(value) {

    if (
        value!=="24"
        &&
        value!=="168"
        &&
        value!=="all"
    ) {
        value="24";
    }

    localRxRangeV30=value;

    try {
        localStorage.setItem(
            "sondehubLocalRxRangeV30",
            value
        );
    } catch (_) {}

    loadLocalRx()
        .catch(
            e => console.error(e)
        );
}


let localRxSortKey="last_time";
let localRxSortDir=-1;
let localRxSortType="date";


function localRxFmt(
    value,
    digits=1,
    suffix=""
) {

    if (
        value===null
        ||
        value===undefined
        ||
        value===""
    ) {
        return "—";
    }

    const n=Number(value);

    if (!Number.isFinite(n)) {
        return esc(value);
    }

    return (
        n.toFixed(digits)
        +
        suffix
    );
}


function localRxTime(value) {

    if (!value) {
        return "—";
    }

    const d=new Date(value);

    if (
        Number.isNaN(
            d.getTime()
        )
    ) {
        return esc(value);
    }

    return d.toLocaleString(
        "pl-PL",
        {
            day:"2-digit",
            month:"2-digit",
            hour:"2-digit",
            minute:"2-digit",
            second:"2-digit"
        }
    );
}


function localRxDuration(sec) {

    sec=Number(sec);

    if (!Number.isFinite(sec)) {
        return "—";
    }

    sec=Math.max(
        0,
        Math.round(sec)
    );

    const h=Math.floor(sec/3600);

    const m=Math.floor(
        (sec%3600)/60
    );

    const s=sec%60;


    if (h>0) {
        return h+" h "+m+" min";
    }

    if (m>0) {
        return m+" min "+s+" s";
    }

    return s+" s";
}


function localRxComparable(
    value,
    type
) {

    if (
        value===null
        ||
        value===undefined
        ||
        value===""
    ) {
        return null;
    }


    if (type==="number") {

        const n=Number(value);

        return Number.isFinite(n)
            ? n
            : null;
    }


    if (type==="date") {

        const n=new Date(
            value
        ).getTime();

        return Number.isFinite(n)
            ? n
            : null;
    }


    return String(
        value
    ).toLocaleLowerCase(
        "pl"
    );
}


function localRxSort(
    key,
    type
) {

    if (
        localRxSortKey===key
    ) {

        localRxSortDir*=-1;

    } else {

        localRxSortKey=key;
        localRxSortType=type;

        /*
         * Numery i daty domyślnie największe/najnowsze
         * jako pierwsze.
         */
        localRxSortDir=
            (
                type==="number"
                ||
                type==="date"
            )
            ?
            -1
            :
            1;
    }


    localRxSortType=type;

    renderLocalRx();
}


function updateLocalRxHeaders() {

    document
    .querySelectorAll(
        "[data-localrx-key]"
    )
    .forEach(
        th => {

            const key=
                th.dataset.localrxKey;

            const label=
                th.dataset.localrxLabel;

            const arrow=
                (
                    key===localRxSortKey
                )
                ?
                (
                    localRxSortDir>0
                    ?
                    " ▲"
                    :
                    " ▼"
                )
                :
                " ↕";

            th.textContent=
                label+arrow;
        }
    );
}


function renderLocalRx() {

    const body=
        document.getElementById(
            "localRxRows"
        );


    const rows=[
        ...localRxRows
    ];


    rows.sort(
        (a,b) => {

            const av=
                localRxComparable(
                    a[
                        localRxSortKey
                    ],
                    localRxSortType
                );

            const bv=
                localRxComparable(
                    b[
                        localRxSortKey
                    ],
                    localRxSortType
                );


            if (
                av===null
                &&
                bv===null
            ) {
                return 0;
            }


            if (av===null) {
                return 1;
            }


            if (bv===null) {
                return -1;
            }


            if (
                typeof av
                ===
                "string"
            ) {

                return (
                    av.localeCompare(
                        bv,
                        "pl"
                    )
                    *
                    localRxSortDir
                );
            }


            if (av<bv) {
                return -1*localRxSortDir;
            }

            if (av>bv) {
                return 1*localRxSortDir;
            }

            return 0;
        }
    );


    updateLocalRxHeaders();


    if (!rows.length) {

        body.innerHTML=
            `<tr>
             <td
              colspan="17"
              class="muted">
             Brak lokalnych odbiorów
             dla wybranego zakresu.
             </td>
             </tr>`;

        return;
    }


    body.innerHTML=
        rows.map(
            x => {

                const serial=
                    String(
                        x.serial
                        ||
                        ""
                    );


                return `
<tr
 class="click"
 onclick='showDetail(
 ${JSON.stringify(serial)}
 )'>

<td>
<b>${esc(serial)}</b>
</td>

<td>
${esc(x.type||"—")}
</td>

<td>
${localRxFmt(
    x.frequency_mhz,
    3
)}
</td>

<td>
${localRxTime(
    x.first_time
)}
</td>

<td>
${localRxTime(
    x.last_time
)}
</td>

<td>
${localRxDuration(
    x.duration_s
)}
</td>

<td>
${esc(
    x.frames
    ??
    "—"
)}
</td>

<td>
${localRxFmt(
    x.max_alt_m,
    0,
    " m"
)}
</td>

<td>
${localRxFmt(
    x.last_alt_m,
    0,
    " m"
)}
</td>

<td>
${localRxFmt(
    x.last_snr_db,
    1,
    " dB"
)}
</td>

<td>
${localRxFmt(
    x.max_snr_db,
    1,
    " dB"
)}
</td>

<td>
${localRxFmt(
    x.frequency_error,
    0
)}
</td>

<td>
${localRxFmt(
    x.temp_c,
    1,
    " °C"
)}
</td>

<td>
${localRxFmt(
    x.humidity_pct,
    1,
    " %"
)}
</td>

<td>
${localRxFmt(
    x.pressure_hpa,
    1,
    " hPa"
)}
</td>

<td>
${esc(
    x.sats
    ??
    "—"
)}
</td>

<td>
${localRxFmt(
    x.battery_v,
    1,
    " V"
)}
</td>

</tr>`;

            }
        ).join("");
}


function syncLocalRxRangeV30() {

    const el=
        document.getElementById(
            "localRxRangeV30"
        );

    if (el) {
        el.value=localRxRangeV30;
    }
}


async function loadLocalRx() {

    syncLocalRxRangeV30();

    const d=
        await jget(
            localRxApiV30()        );


    localRxRows=
        d.receptions
        ||
        [];


    const button=
        document.getElementById(
            "localRxToggle"
        );


    const panel=
        document.getElementById(
            "localRxPanel"
        );


    button.textContent=
        (
            localRxVisible
            ?
            "▼ Ukryj moje odbiory lokalne"
            :
            "▶ Pokaż moje odbiory lokalne"
        )
        +
        " — "
        +
        localRxRangeLabelV30()
        +
        " ("
        +
        localRxRows.length
        +
        ")";


    panel.style.display=
        localRxVisible
        ?
        "block"
        :
        "none";


    if (localRxVisible) {
        renderLocalRx();
    }


    document.getElementById(
        "localRxMeta"
    ).textContent=
        "Źródło: lokalne pliki auto_rx. "
        +
        "Kliknij nagłówek kolumny, aby sortować.";
}


function toggleLocalRx() {

    localRxVisible=
        !localRxVisible;

    loadLocalRx()
        .catch(
            e =>
                console.error(e)
        );
}


async function loadStatus(){
 const s=await jget("/api/status");
 STATUS_CACHE=s;
 HOME=s.home;
 const lr=s.local_receiver||{};
 const localState=document.getElementById("localState");
 const localStateReason=document.getElementById("localStateReason");
 const rtlState=String(lr.state||"UNKNOWN");

 const rtlLabels={
   LISTENING:"RADIOSONDY",
   SATNOGS_RESERVED:"SATNOGS",
   MANUAL_OFF:"WYŁĄCZONY",
   FAILSAFE:"TRYB BEZPIECZNY",
   UNKNOWN:"BRAK DANYCH"
 };

 const rtlColors={
   LISTENING:"#4ade80",
   SATNOGS_RESERVED:"#60a5fa",
   MANUAL_OFF:"#cbd5e1",
   FAILSAFE:"#f87171",
   UNKNOWN:"#fbbf24"
 };

 localState.textContent=
   rtlLabels[rtlState]
   ||
   rtlState;

 localState.style.color=
   rtlColors[rtlState]
   ||
   "#fbbf24";

 let rtlReason=
   String(lr.reason||"").trim();

 if (!rtlReason) {
   if (rtlState==="LISTENING") {
     rtlReason="Nasłuch radiosond aktywny";
   } else if (rtlState==="SATNOGS_RESERVED") {
     rtlReason="Tuner zarezerwowany dla SatNOGS";
   } else if (rtlState==="MANUAL_OFF") {
     rtlReason="Nasłuch radiosond wyłączony";
   } else if (rtlState==="FAILSAFE") {
     rtlReason="Tuner pozostawiony dla SatNOGS";
   } else {
     rtlReason="Nieznany stan tunera";
   }
 }

 localStateReason.textContent=rtlReason;
 const hub=(s.sondehub_listener||{});
 const l=hub.latest;
 const hubState=document.getElementById("hubState");
 const hubDetail=document.getElementById("hubDetail");

 const hubAgeSec=
   (
     hub.age_sec===null
     ||
     hub.age_sec===undefined
   )
   ?
   null
   :
   (
     Number.isFinite(Number(hub.age_sec))
     ?
     Number(hub.age_sec)
     :
     null
   );

 if (!l) {
   hubState.textContent="BRAK DANYCH";
   hubState.style.color="#f87171";

   hubDetail.textContent=
     s.sondehub_listener_error
     ?
     String(s.sondehub_listener_error)
     :
     "Brak zgłoszenia stacji w SondeHub z ostatnich 24 h";

 } else if (hub.online) {
   hubState.textContent="ONLINE";
   hubState.style.color="#4ade80";

   hubDetail.textContent=
     hubAgeSec===null
     ?
     "Stacja widoczna w SondeHub"
     :
     "Ostatnie zgłoszenie stacji: "+rtAgoV34(hubAgeSec);

 } else {
   hubState.textContent="NIEAKTUALNA";
   hubState.style.color="#fbbf24";

   hubDetail.textContent=
     hubAgeSec===null
     ?
     "Stacja widoczna, ale brak czasu ostatniego zgłoszenia"
     :
     "Ostatnie zgłoszenie stacji: "+rtAgoV34(hubAgeSec);
 }

 const rt=s.realtime||{};
 const rtState=document.getElementById("rtState");
 const rtDetail=document.getElementById("rtDetail");

 const rtCount=Number(
     rt.tracked_count || 0
 );

 const rtLast=
     rt.last_message
     ?
     new Date(rt.last_message)
     :
     null;

 const rtLastMs=
     (
         rtLast
         &&
         !Number.isNaN(
             rtLast.getTime()
         )
     )
     ?
     rtLast.getTime()
     :
     null;

 const rtAgeSec=
     rtLastMs===null
     ?
     null
     :
     Math.max(
         0,
         Math.floor(
             (Date.now()-rtLastMs)/1000
         )
     );


 function rtAgoV34(sec) {

     if (sec===null) {
         return "brak danych";
     }

     if (sec < 5) {
         return "przed chwilą";
     }

     if (sec < 60) {
         return sec+" s temu";
     }

     const min=Math.floor(
         sec/60
     );

     if (min < 60) {
         return min+" min temu";
     }

     const godz=Math.floor(
         min/60
     );

     return godz+" godz. temu";
 }


 function rtClockV34(date) {

     if (
         !date
         ||
         Number.isNaN(
             date.getTime()
         )
     ) {
         return "—";
     }

     return new Intl.DateTimeFormat(
         "pl-PL",
         {
             hour:"2-digit",
             minute:"2-digit",
             second:"2-digit"
         }
     ).format(date);
 }


 function rtDetailsV34() {

     let text=
         "Śledzone sondy: "
         +
         String(rtCount);

     if (rtLastMs!==null) {

         text +=
             " • ostatnie dane: "
             +
             rtAgoV34(rtAgeSec)
             +
             " • "
             +
             rtClockV34(rtLast);
     }

     return text;
 }


 if (rt.error) {

     rtState.textContent="BŁĄD";
     rtState.style.color="#f87171";

     rtDetail.textContent=
         String(rt.error);

 } else if (rtCount===0) {

     rtState.textContent="BRAK SOND";
     rtState.style.color="#cbd5e1";

     rtDetail.textContent=
         "Brak aktywnych sond do śledzenia";

 } else if (rtLastMs===null) {

     rtState.textContent="OCZEKIWANIE";
     rtState.style.color="#fbbf24";

     rtDetail.textContent=
         "Śledzone sondy: "
         +
         String(rtCount)
         +
         " • oczekiwanie na pierwsze dane";

 } else if (rtAgeSec <= 60) {

     rtState.textContent="AKTYWNE";
     rtState.style.color="#4ade80";

     rtDetail.textContent=
         rtDetailsV34();

 } else if (rtAgeSec <= 180) {

     rtState.textContent="OPÓŹNIONE";
     rtState.style.color="#fbbf24";

     rtDetail.textContent=
         rtDetailsV34();

 } else {

     rtState.textContent=
         "BRAK ŚWIEŻYCH DANYCH";

     rtState.style.color="#f87171";

     rtDetail.textContent=
         rtDetailsV34();
 }
 // LOCAL_RECEIVE_HUMAN_V35
 const nowList=
   Array.isArray(lr.received_serials)
   ? lr.received_serials
   : [];

 await loadLocalRx();

 const localNow=
   document.getElementById("localNow");

 const localNowDetail=
   document.getElementById("localNowDetail");


 function localRxRowV35(serial) {

   const wanted=
     String(serial||"")
     .trim()
     .toUpperCase();

   return (
     localRxRows.find(
       row =>
         String(row.serial||"")
         .trim()
         .toUpperCase()
         === wanted
     )
     || null
   );
 }


 function localRxDescV35(serial) {

   const row=
     localRxRowV35(serial);

   if (
     row
     &&
     row.frequency_mhz != null
     &&
     Number.isFinite(
       Number(row.frequency_mhz)
     )
   ) {

     return (
       String(serial)
       +
       " • "
       +
       Number(
         row.frequency_mhz
       ).toFixed(3)
       +
       " MHz"
     );
   }

   return String(serial);
 }


 if (nowList.length > 0) {

   localNow.textContent=
     nowList.length === 1
     ? "ODBIERAMY"
     : "ODBIERAMY "+nowList.length+" SONDY";

   localNow.style.color="#4ade80";

   localNowDetail.textContent=
     nowList
       .map(localRxDescV35)
       .join(" • ");

 } else if (
   lr.state === "LISTENING"
   &&
   lr.auto_rx_running
 ) {

   localNow.textContent=
     "BRAK AKTYWNIE ODBIERANEJ SONDY";

   localNow.style.color=
     "#cbd5e1";

   localNowDetail.textContent=
     "RTL-SDR nasłuchuje";

 } else if (
   lr.state === "SATNOGS_RESERVED"
 ) {

   localNow.textContent=
     "WSTRZYMANY";

   localNow.style.color=
     "#60a5fa";

   localNowDetail.textContent=
     "RTL-SDR używa SatNOGS";

 } else if (
   lr.state === "MANUAL_OFF"
 ) {

   localNow.textContent=
     "WYŁĄCZONY";

   localNow.style.color=
     "#cbd5e1";

   localNowDetail.textContent=
     "Nasłuch radiosond wyłączony";

 } else if (
   lr.state === "FAILSAFE"
 ) {

   localNow.textContent=
     "TRYB BEZPIECZNY";

   localNow.style.color=
     "#f87171";

   localNowDetail.textContent=
     "Nasłuch wstrzymany — tuner pozostawiony dla SatNOGS";

 } else {

   localNow.textContent=
     "BRAK DANYCH";

   localNow.style.color=
     "#fbbf24";

   localNowDetail.textContent=
     String(
       lr.reason
       ||
       "Nieznany stan odbiornika"
     );
 }
initMap();
}

const sondeGroupVisible = {
    LIVE:false,
    RECENT:false,
    ARCHIVE:false
};


function sondeStatusPl(status) {

    if (status === "LIVE") {
        return "AKTYWNA";
    }

    if (status === "RECENT") {
        return "UTRACONA";
    }

    if (status === "ARCHIVE") {
        return "ARCHIWALNA";
    }

    return status || "—";
}


function sondeGroupLabel(status) {

    if (status === "LIVE") {
        return "aktywne";
    }

    if (status === "RECENT") {
        return "utracone";
    }

    if (status === "ARCHIVE") {
        return "archiwalne";
    }

    return status;
}


function updateSondeGroupButtons(rows) {

    for (
        const status
        of ["LIVE","RECENT","ARCHIVE"]
    ) {

        const count =
            rows.filter(
                x => x.status === status
            ).length;

        const button =
            document.getElementById(
                "group"+status
            );

        if (!button) {
            continue;
        }

        button.textContent =
            (
                sondeGroupVisible[status]
                ? "▼ Ukryj "
                : "▶ Pokaż "
            )
            +
            sondeGroupLabel(status)
            +
            " ("
            +
            count
            +
            ")";
    }
}


function toggleSondeGroup(status) {

    if (!(status in sondeGroupVisible)) {
        return;
    }

    sondeGroupVisible[status] =
        !sondeGroupVisible[status];

    loadSondes();
}


async function loadSondes(){
 const d=await jget("/api/sondes");
 const rows=d.radiosondes||[];

 updateSondeGroupButtons(rows);

 const visibleRows =
     rows.filter(
         x =>
         !(
             x.status
             in sondeGroupVisible
         )
         ||
         sondeGroupVisible[
             x.status
         ]
     );

 const body =
     document.getElementById(
         "rows"
     );

 if (!visibleRows.length) {

     body.innerHTML =
         `<tr>
          <td colspan="11" class="muted">
          Kategorie są zwinięte — wybierz:
          Aktywne, Utracone lub Archiwalne.
          </td>
          </tr>`;

     return;
 }

 body.innerHTML=visibleRows.map(x=>{
   const live = x.local_receive_now
      ? `<span class="pill pillok">TAK</span>`
      : `<span class="pill pillbad">NIE</span>`;
   const today = x.local_received_today
      ? `<span class="pill pillwarn">TAK</span>`
      : `<span class="pill pillbad">NIE</span>`;
   const snr = x.local_receive_now ? esc(x.rt_snr ?? "—") : "—";
   const rssi = x.local_receive_now ? esc(x.rt_rssi ?? "—") : "—";
   const upl = x.rt_uploader_callsign ? esc(x.rt_uploader_callsign) : "—";
   return `<tr class="click" onclick='showDetail(${JSON.stringify(String(x.serial||""))})'>
     <td>${esc(sondeStatusPl(x.status))}</td>
     <td>${live}</td>
     <td>${today}</td>
     <td>${esc(x.serial)}</td>
     <td>${esc(x.type||x.subtype||"")}</td>
     <td>${esc(x.frequency_mhz||x.frequency||"")}</td>
     <td>${esc(x.current_alt_m||x.alt||"")}</td>
     <td>${snr}</td>
     <td>${rssi}</td>
     <td>${upl}</td>
     <td>${esc(x.last_seen||x.rt_datetime||"")}</td>
   </tr>`;
 }).join("");
}

function telemHtml(t){
 if(!t)return "Brak telemetrii.";
 return [
 ["Typ",t.subtype||t.type],["Producent",t.manufacturer],["Częstotliwość MHz",t.frequency],
 ["Wysokość m",t.alt],["Prędkość pozioma m/s",t.vel_h],["Prędkość pionowa m/s",t.vel_v],
 ["Kierunek °",t.heading],["Temperatura °C",t.temp],["Wilgotność %",t.humidity],
 ["Ciśnienie hPa",t.pressure],["Satelity GPS",t.sats],["Bateria V",t.batt],
 ["SNR dB",t.snr],["RSSI dBm",t.rssi],["Uploader",t.uploader_callsign],["Czas",t.datetime]
 ].map(v=>kv(v[0],v[1])).join("");
}

function redraw(d){
 // MAP_FIX_V37_1
 initMap();
 if(!layer)return;

 // Widok konkretnej sondy ma pierwszeństwo nad
 // dodatkową warstwą stacji/odbiorników.
 if (
     typeof extraMapLayerV37 !== "undefined"
     &&
     extraMapLayerV37
 ) {
     extraMapLayerV37.clearLayers();
 }

 layer.clearLayers();
 const pts=[];
 if(HOME){addPoint(HOME.lat,HOME.lon,"Stacja lokalna");pts.push([HOME.lat,HOME.lon]);}
 const tr=((d.flight||{}).track)||[];
 const ll=tr.map(x=>[pnum(x.lat),pnum(x.lon)]).filter(x=>x[0]!=null&&x[1]!=null);
 if(ll.length){L.polyline(ll).addTo(layer);pts.push(...ll);}
 if(d.latest){addPoint(d.latest.lat,d.latest.lon,"Sonda "+d.serial); if(pnum(d.latest.lat)!=null)pts.push([Number(d.latest.lat),Number(d.latest.lon)]);}
 const pp=((d.prediction||{}).point)||null;
 const ppath=((d.prediction||{}).path)||[];
 const pll=ppath.map(x=>[pnum(x.lat),pnum(x.lon)]).filter(x=>x[0]!=null&&x[1]!=null);
 if(pll.length){L.polyline(pll,{dashArray:"8 8"}).addTo(layer);pts.push(...pll);}
 if(pp){addPoint(pp.lat,pp.lon,"Przewidywane lądowanie SondeHub");pts.push([pp.lat,pp.lon]);}
 const rp=((d.reverse_prediction||{}).point)||null;
 if(rp){addPoint(rp.lat,rp.lon,"Reverse prediction — przewidywany start");pts.push([rp.lat,rp.lon]);}
 const st=(((d.launch_site||{}).site)||null);
 const stsrc=((d.launch_site||{}).source)||"";
 if(st&&st.position){addPoint(st.position[0],st.position[1],(stsrc==="telemetry.launch_site"?"Stacja startowa SondeHub: ":"Najbliższa znana stacja (przybliżenie): ")+(st.station_name||st.site_id));pts.push(st.position);}
 const rec=((d.recovery||{}).latest)||null;
 if(rec){addPoint(rec.lat,rec.lon,"Recovery: "+(rec.recovered?"ODZYSKANA":rec.planned?"PLANOWANA":"zgłoszenie"));pts.push([rec.lat,rec.lon]);}
 if(pts.length){map.fitBounds(pts,{padding:[30,30],maxZoom:10});}
}

async function showDetail(serial){
 selectedSerial=serial;
 document.getElementById("detailTitle").textContent="Sonda "+serial+" — pobieranie…";
 const d=await jget("/api/sonde?serial="+encodeURIComponent(serial));
 window.__SONDE_DETAIL_V39=d;

 // SONDE_REDRAW_V37_2
 //
 // Rysujemy sondę natychmiast po pobraniu danych.
 // Dzięki temu późniejszy błąd elementu UI nie może
 // zablokować aktualizacji mapy.
 redraw(d);

 // ALTITUDE_PROFILE_V3_CALL
 rwAltV3Render(d);

 // SONDE_ANALYTICS_V38
 renderSondeAnalyticsV38(d);

 // FLIGHT_ANALYSIS_V6
 renderFlightAnalysisV6(d);

 // SONDE_STATUS_V7
 renderSondeStatusV7(d);

 // OUR_RECEPTION_V2
 renderOurReceptionV3(d);

 document.getElementById("detailTitle").textContent="Sonda "+serial;
 const rxBlock = [
   kv("Odbieramy lokalnie teraz", d.local_receive_now ? "TAK" : "NIE"),
   kv("Odebrana lokalnie dziś", d.local_received_today ? "TAK" : "NIE"),
   kv("Ostatni uploader", (d.latest||{}).uploader_callsign || "—")
 ].join("");
 document.getElementById("telemetry").innerHTML=rxBlock + "<hr style=\"border:0;border-top:1px solid #33404e;margin:10px 0\">" + telemHtml(d.latest);
 const st=((d.launch_site||{}).site);
 const stsrc=((d.launch_site||{}).source)||"";
 document.getElementById("site").innerHTML=st?
   kv(stsrc==="telemetry.launch_site"?"Stacja startowa SondeHub":"Najbliższa stacja — PRZYBLIŻENIE",st.station_name||st.site_id)+kv("Źródło",stsrc)+kv("ID",st.site_id)+(st.distance_km!=null?kv("Od punktu odniesienia",st.distance_km+" km"):"")+kv("Harmonogram UTC",(Array.isArray(st.times)?st.times.join(", "):String(st.times??"—")))+kv("Typy/f częstotliwości",(Array.isArray(st.rs_types)?st.rs_types.join(", "):String(st.rs_types??"—")))+""
   :"Brak dopasowania.";
 const pp=(d.prediction||{}).point;
 const rp=(d.reverse_prediction||{}).point;
 document.getElementById("prediction").innerHTML=
   kv("Lądowanie",pp?(fmt(pp.lat,5)+", "+fmt(pp.lon,5)):"brak")+
   kv("Czas predykcji",pp&&pp.time)+kv("Reverse start",rp?(fmt(rp.lat,5)+", "+fmt(rp.lon,5)):"brak")+
   kv("Próbek bieżącego lotu",(d.flight||{}).samples)+kv("Maks. wysokość m",(d.flight||{}).max_alt_m);
 const rec=(d.recovery||{}).latest;
 document.getElementById("recovery").innerHTML=rec?
   kv("Odzyskana",rec.recovered)+kv("Planowana",rec.planned)+kv("Kto",rec.recovered_by)+kv("Opis",rec.description)+kv("Czas",rec.datetime)
   :"Brak zgłoszenia odzyskania.";
 document.getElementById("raw").textContent=JSON.stringify(d,null,2);
 redraw(d);

 // DETAIL_POLISH_V10_CALL
 v10PolishDetail(d);

}

async function extra(url,label){
 document.getElementById("extraSummary").textContent=label+" — pobieranie…";
 const d=await jget(url);
 document.getElementById("extraSummary").textContent=label+" — gotowe";
 const e=document.getElementById("extra");e.style.display="block";e.textContent=JSON.stringify(d,null,2);
}

// ============================================================
// EXTRA_TABLES_V36
// Czytelne tabele zamiast surowego JSON.
// ============================================================

function extraStyleV36() {

    if (
        document.getElementById(
            "extraStyleV36"
        )
    ) {
        return;
    }

    const style=
        document.createElement(
            "style"
        );

    style.id=
        "extraStyleV36";

    style.textContent=`
#extraPrettyV36 {
    margin-top:14px;
    min-width:0;
}

#extraPrettyV36 .extra-v36-head {
    display:flex;
    align-items:flex-start;
    justify-content:space-between;
    gap:12px;
    flex-wrap:wrap;
    margin-bottom:9px;
}

#extraPrettyV36 .extra-v36-title {
    font-weight:700;
    font-size:15px;
    color:#eef5ff;
}

#extraPrettyV36 .extra-v36-meta {
    color:#9fb0c3;
    font-size:12px;
    margin-top:3px;
}

#extraPrettyV36 .extra-v36-scroll {
    width:100%;
    overflow:auto;
    max-height:520px;
    border:1px solid #34475b;
    border-radius:6px;
}

#extraPrettyV36 table {
    width:100%;
    border-collapse:collapse;
    font-size:12px;
    background:#111a24;
}

#extraPrettyV36 th {
    position:sticky;
    top:0;
    z-index:2;
    text-align:left;
    white-space:nowrap;
    padding:8px 9px;
    background:#1c2a38;
    color:#e8eef6;
    border-bottom:1px solid #3d5065;
    cursor:pointer;
    user-select:none;
}

#extraPrettyV36 td {
    padding:7px 9px;
    border-bottom:1px solid #2e3e4f;
    color:#e6edf5;
    vertical-align:top;
    white-space:nowrap;
}

#extraPrettyV36 tr:last-child td {
    border-bottom:0;
}

#extraPrettyV36 tbody tr:hover {
    background:#182534;
}

#extraPrettyV36 .extra-v36-foot {
    margin-top:7px;
    color:#9fb0c3;
    font-size:12px;
}

#extraPrettyV36 .extra-v36-empty {
    border:1px solid #34475b;
    border-radius:6px;
    padding:12px;
    color:#9fb0c3;
    background:#111a24;
}

#extraPrettyV36 .extra-v36-error {
    border:1px solid #7f3a3a;
    border-radius:6px;
    padding:12px;
    color:#fca5a5;
    background:#211719;
}

#extraPrettyV36 .extra-v36-kv {
    display:grid;
    grid-template-columns:
        minmax(140px,0.7fr)
        minmax(180px,1.3fr);
    border:1px solid #34475b;
    border-radius:6px;
    overflow:hidden;
    background:#111a24;
}

#extraPrettyV36 .extra-v36-k {
    padding:7px 9px;
    color:#9fb0c3;
    border-bottom:1px solid #2e3e4f;
}

#extraPrettyV36 .extra-v36-v {
    padding:7px 9px;
    color:#e6edf5;
    border-bottom:1px solid #2e3e4f;
    min-width:0;
    overflow-wrap:anywhere;
}

@media (max-width:700px) {
    #extraPrettyV36 .extra-v36-kv {
        grid-template-columns:1fr;
    }

    #extraPrettyV36 .extra-v36-k {
        padding-bottom:2px;
        border-bottom:0;
    }

    #extraPrettyV36 .extra-v36-v {
        padding-top:2px;
    }
}
`;

    document.head.appendChild(
        style
    );
}


function extraHostV36() {

    extraStyleV36();

    let host=
        document.getElementById(
            "extraPrettyV36"
        );

    if (host) {
        return host;
    }

    const headings=[
        ...document.querySelectorAll(
            "h1,h2,h3"
        )
    ];

    const heading=
        headings.find(
            x =>
                String(
                    x.textContent || ""
                ).trim()
                ===
                "Dodatkowe dane"
        );

    if (!heading) {
        return null;
    }

    const card=
        heading.closest(".card")
        ||
        heading.parentElement;

    if (!card) {
        return null;
    }

    host=
        document.createElement(
            "div"
        );

    host.id=
        "extraPrettyV36";

    card.appendChild(
        host
    );

    return host;
}


function extraLabelV36(key) {

    const labels={
        site_id:"ID",
        station_id:"ID",
        station:"Stacja",
        station_name:"Nazwa stacji",

        callsign:"Odbiornik",
        uploader_callsign:"Odbiornik",
        software_name:"Program",
        software_version:"Wersja",

        serial:"Sonda",
        payload_callsign:"Sonda",
        sonde_type:"Typ",
        type:"Typ",

        frequency_mhz:"MHz",
        frequency:"MHz",

        distance_km:"Odległość",
        alt:"Wysokość",
        altitude:"Wysokość",
        burst_altitude:"Maks. wysokość",

        ascent_rate:"Wznoszenie",
        descent_rate:"Opadanie",

        position:"Pozycja",
        lat:"Szerokość",
        latitude:"Szerokość",
        lon:"Długość",
        longitude:"Długość",

        last_seen:"Ostatnio",
        last_time:"Ostatnio",
        datetime:"Czas",
        time:"Czas",

        recovered:"Odzyskana",
        recovery:"Odzyskanie",
        count:"Liczba",
        rs_types:"Typy sond"
    };

    return (
        labels[key]
        ||
        String(key)
        .replaceAll("_"," ")
    );
}


function extraNumberV36(value) {

    const n=
        Number(value);

    return Number.isFinite(n)
        ? n
        : null;
}


function extraFormatV36(
    key,
    value
) {

    if (
        value === null
        ||
        value === undefined
        ||
        value === ""
    ) {
        return "—";
    }


    if (
        typeof value
        ===
        "boolean"
    ) {
        return value
            ? "TAK"
            : "NIE";
    }


    const number=
        extraNumberV36(value);


    if (
        key === "distance_km"
        &&
        number !== null
    ) {
        return (
            number.toFixed(1)
            +
            " km"
        );
    }


    if (
        (
            key === "frequency"
            ||
            key === "frequency_mhz"
        )
        &&
        number !== null
    ) {

        const mhz=
            number > 1000000
            ?
            number / 1000000
            :
            number;

        return (
            mhz.toFixed(3)
            +
            " MHz"
        );
    }


    if (
        (
            key === "alt"
            ||
            key === "altitude"
            ||
            key === "burst_altitude"
        )
        &&
        number !== null
    ) {
        return (
            Math.round(number)
            +
            " m"
        );
    }


    if (
        (
            key === "ascent_rate"
            ||
            key === "descent_rate"
        )
        &&
        number !== null
    ) {
        return (
            number.toFixed(1)
            +
            " m/s"
        );
    }


    if (
        (
            key === "lat"
            ||
            key === "latitude"
            ||
            key === "lon"
            ||
            key === "longitude"
        )
        &&
        number !== null
    ) {
        return number.toFixed(4);
    }


    if (
        Array.isArray(value)
    ) {

        if (
            value.length === 0
        ) {
            return "—";
        }

        const simple=
            value
            .filter(
                x =>
                    x === null
                    ||
                    [
                        "string",
                        "number",
                        "boolean"
                    ].includes(
                        typeof x
                    )
            )
            .slice(0,6)
            .map(
                x =>
                    String(x)
            );

        return simple.length
            ? simple.join(", ")
            : "—";
    }


    if (
        typeof value
        ===
        "object"
    ) {

        const parts=
            Object.entries(value)
            .filter(
                ([,v]) =>
                    v === null
                    ||
                    [
                        "string",
                        "number",
                        "boolean"
                    ].includes(
                        typeof v
                    )
            )
            .slice(0,4)
            .map(
                ([k,v]) =>
                    extraLabelV36(k)
                    +
                    ": "
                    +
                    String(v)
            );

        return parts.length
            ? parts.join(" • ")
            : "—";
    }


    if (
        typeof value
        ===
        "string"
    ) {

        const looksLikeTime=
            (
                key.includes("time")
                ||
                key.includes("seen")
                ||
                key.includes("date")
            )
            &&
            (
                value.includes("T")
                ||
                value.endsWith("Z")
            );

        if (looksLikeTime) {

            const d=
                new Date(value);

            if (
                !Number.isNaN(
                    d.getTime()
                )
            ) {
                return d.toLocaleString(
                    "pl-PL"
                );
            }
        }
    }


    return String(value);
}


function extraComparableV36(
    key,
    value
) {

    if (
        value === null
        ||
        value === undefined
    ) {
        return "";
    }

    const n=
        Number(value);

    if (
        Number.isFinite(n)
        &&
        String(value).trim() !== ""
    ) {
        return n;
    }

    return String(value)
        .toLocaleLowerCase("pl");
}


function extraRowsV36(
    data,
    preferred
) {

    if (
        Array.isArray(data)
    ) {
        return data;
    }

    if (
        !data
        ||
        typeof data !== "object"
    ) {
        return null;
    }

    if (
        preferred
        &&
        Array.isArray(
            data[preferred]
        )
    ) {
        return data[preferred];
    }

    const keys=
        Object.keys(data);

    for (
        const key
        of keys
    ) {

        if (
            Array.isArray(
                data[key]
            )
            &&
            data[key].some(
                x =>
                    x
                    &&
                    typeof x
                    ===
                    "object"
                    &&
                    !Array.isArray(x)
            )
        ) {
            return data[key];
        }
    }

    return null;
}


function extraColumnsV36(rows) {

    const present=
        new Set();

    for (
        const row
        of rows.slice(0,30)
    ) {

        if (
            !row
            ||
            typeof row !== "object"
            ||
            Array.isArray(row)
        ) {
            continue;
        }

        Object.keys(row)
        .forEach(
            key =>
                present.add(key)
        );
    }


    const priority=[
        "site_id",
        "station_id",
        "station_name",
        "station",

        "callsign",
        "uploader_callsign",

        "payload_callsign",
        "serial",

        "sonde_type",
        "type",

        "frequency_mhz",
        "frequency",

        "distance_km",

        "alt",
        "altitude",
        "burst_altitude",

        "last_seen",
        "last_time",
        "datetime",

        "position",

        "ascent_rate",
        "descent_rate",

        "software_name",
        "software_version",

        "recovered"
    ];


    const result=[];


    for (
        const key
        of priority
    ) {

        if (
            present.has(key)
            &&
            !result.includes(key)
        ) {
            result.push(key);
        }

        if (
            result.length >= 7
        ) {
            break;
        }
    }


    if (
        result.length < 7
    ) {

        for (
            const key
            of present
        ) {

            if (
                result.includes(key)
            ) {
                continue;
            }

            const value=
                rows
                .map(
                    r =>
                        r
                        ?
                        r[key]
                        :
                        null
                )
                .find(
                    v =>
                        v !== null
                        &&
                        v !== undefined
                );

            if (
                typeof value
                ===
                "object"
                &&
                !Array.isArray(value)
            ) {
                continue;
            }

            result.push(key);

            if (
                result.length >= 7
            ) {
                break;
            }
        }
    }


    return result;
}


function extraMetaV36(
    data,
    rows
) {

    const parts=[];

    let count=
        rows
        ?
        rows.length
        :
        null;

    if (
        data
        &&
        typeof data === "object"
        &&
        !Array.isArray(data)
        &&
        Number.isFinite(
            Number(data.count)
        )
    ) {
        count=
            Number(data.count);
    }

    if (
        count !== null
    ) {
        parts.push(
            "Wyników: "
            +
            String(count)
        );
    }


    if (
        data
        &&
        typeof data === "object"
        &&
        !Array.isArray(data)
        &&
        Number.isFinite(
            Number(
                data.distance_km
            )
        )
    ) {
        parts.push(
            "promień: "
            +
            Number(
                data.distance_km
            ).toFixed(0)
            +
            " km"
        );
    }


    return parts.join(" • ");
}


function extraKVV36(
    host,
    data
) {

    const entries=
        Object.entries(
            data || {}
        )
        .filter(
            ([,v]) =>
                !Array.isArray(v)
        );


    if (
        entries.length === 0
    ) {

        const empty=
            document.createElement(
                "div"
            );

        empty.className=
            "extra-v36-empty";

        empty.textContent=
            "Brak danych.";

        host.appendChild(
            empty
        );

        return;
    }


    const grid=
        document.createElement(
            "div"
        );

    grid.className=
        "extra-v36-kv";


    for (
        const [key,value]
        of entries
    ) {

        const k=
            document.createElement(
                "div"
            );

        k.className=
            "extra-v36-k";

        k.textContent=
            extraLabelV36(key);


        const v=
            document.createElement(
                "div"
            );

        v.className=
            "extra-v36-v";

        v.textContent=
            extraFormatV36(
                key,
                value
            );


        grid.appendChild(k);
        grid.appendChild(v);
    }


    host.appendChild(
        grid
    );
}


function extraTableV36(
    host,
    rows
) {

    if (
        !rows
        ||
        rows.length === 0
    ) {

        const empty=
            document.createElement(
                "div"
            );

        empty.className=
            "extra-v36-empty";

        empty.textContent=
            "Brak wyników.";

        host.appendChild(
            empty
        );

        return;
    }


    let current=
        rows.slice();

    const total=
        current.length;

    const cols=
        extraColumnsV36(
            current
        );


    if (
        cols.length === 0
    ) {
        extraKVV36(
            host,
            current[0]
        );
        return;
    }


    const scroll=
        document.createElement(
            "div"
        );

    scroll.className=
        "extra-v36-scroll";


    const table=
        document.createElement(
            "table"
        );


    const thead=
        document.createElement(
            "thead"
        );

    const headRow=
        document.createElement(
            "tr"
        );


    const tbody=
        document.createElement(
            "tbody"
        );


    let sortKey=null;
    let sortDir=1;


    function drawRows() {

        tbody.replaceChildren();

        for (
            const row
            of current.slice(0,100)
        ) {

            const tr=
                document.createElement(
                    "tr"
                );

            for (
                const key
                of cols
            ) {

                const td=
                    document.createElement(
                        "td"
                    );

                td.textContent=
                    extraFormatV36(
                        key,
                        row
                        ?
                        row[key]
                        :
                        null
                    );

                tr.appendChild(td);
            }

            tbody.appendChild(tr);
        }
    }


    function updateHeaders() {

        [
            ...headRow.children
        ].forEach(
            (th,index) => {

                const key=
                    cols[index];

                th.textContent=
                    extraLabelV36(key)
                    +
                    (
                        sortKey === key
                        ?
                        (
                            sortDir > 0
                            ?
                            " ↑"
                            :
                            " ↓"
                        )
                        :
                        " ↕"
                    );
            }
        );
    }


    for (
        const key
        of cols
    ) {

        const th=
            document.createElement(
                "th"
            );

        th.tabIndex=0;

        const sort=() => {

            if (
                sortKey === key
            ) {
                sortDir *= -1;
            } else {
                sortKey=key;
                sortDir=1;
            }

            current.sort(
                (a,b) => {

                    const av=
                        extraComparableV36(
                            key,
                            a
                            ?
                            a[key]
                            :
                            null
                        );

                    const bv=
                        extraComparableV36(
                            key,
                            b
                            ?
                            b[key]
                            :
                            null
                        );

                    if (av < bv) {
                        return -1*sortDir;
                    }

                    if (av > bv) {
                        return 1*sortDir;
                    }

                    return 0;
                }
            );

            updateHeaders();
            drawRows();
        };

        th.addEventListener(
            "click",
            sort
        );

        th.addEventListener(
            "keydown",
            e => {
                if (
                    e.key === "Enter"
                    ||
                    e.key === " "
                ) {
                    e.preventDefault();
                    sort();
                }
            }
        );

        headRow.appendChild(th);
    }


    thead.appendChild(
        headRow
    );

    table.appendChild(
        thead
    );

    table.appendChild(
        tbody
    );

    scroll.appendChild(
        table
    );

    host.appendChild(
        scroll
    );


    const foot=
        document.createElement(
            "div"
        );

    foot.className=
        "extra-v36-foot";

    foot.textContent=
        (
            "Pokazano "
            +
            String(
                Math.min(
                    total,
                    100
                )
            )
            +
            " z "
            +
            String(total)
            +
            " rekordów. "
            +
            "Kliknij nagłówek kolumny, aby sortować."
        );

    host.appendChild(
        foot
    );


    updateHeaders();
    drawRows();
}


function extraButtonV36(title) {

    const names=[
        "Stacje startowe",
        "Odbiorniki",
        "Balony amatorskie",
        "Statystyki odzyskiwania"
    ];

    const selected=
        names.find(
            name =>
                String(title)
                .startsWith(name)
        );


    for (
        const button
        of document.querySelectorAll(
            "button"
        )
    ) {

        const text=
            String(
                button.textContent || ""
            ).trim();

        const isExtra=
            names.some(
                name =>
                    text.startsWith(name)
            );

        if (!isExtra) {
            continue;
        }

        const active=
            selected
            &&
            text.startsWith(
                selected
            );

        button.style.background=
            active
            ?
            "#2563a9"
            :
            "";

        button.style.borderColor=
            active
            ?
            "#5ba3e6"
            :
            "";
    }
}


// ============================================================
// EXTRA_MAP_V37
// 1) ukrycie/usunięcie karty "Surowe dane wybranej sondy"
// 2) rysowanie Stacji startowych i Odbiorników na mapie
// ============================================================

let extraMapLayerV37=null;

function extraLeafletMapV37() {

    if (
        typeof map !== "undefined"
        &&
        map
        &&
        typeof map.addLayer === "function"
    ) {
        return map;
    }

    return null;
}

function extraEnsureLayerV37() {
    const m=extraLeafletMapV37();

    if (!m || typeof L === "undefined") {
        return null;
    }

    if (!extraMapLayerV37) {
        extraMapLayerV37=L.layerGroup().addTo(m);
    }

    return extraMapLayerV37;
}

function extraClearMapV37() {
    const layer=extraEnsureLayerV37();
    if (layer) {
        layer.clearLayers();
    }
}

function extraRemoveRawCardV37() {
    const cards=[...document.querySelectorAll(".card")];

    const rawCard=cards.find(
        x =>
            String(x.textContent || "")
            .includes("Surowe dane wybranej sondy")
    );

    if (rawCard) {
        rawCard.remove();
    }
}

function extraCoordPairV37(value) {
    if (Array.isArray(value) && value.length >= 2) {
        const lat=Number(value[0]);
        const lon=Number(value[1]);

        if (Number.isFinite(lat) && Number.isFinite(lon)) {
            return [lat, lon];
        }
    }

    if (value && typeof value === "object") {
        const lat=Number(
            value.lat ?? value.latitude
        );

        const lon=Number(
            value.lon ?? value.lng ?? value.longitude
        );

        if (Number.isFinite(lat) && Number.isFinite(lon)) {
            return [lat, lon];
        }
    }

    return null;
}

function extraLatLonV37(row) {
    if (!row || typeof row !== "object") {
        return null;
    }

    const direct=extraCoordPairV37(row.position);
    if (direct) {
        return direct;
    }

    const uploader=extraCoordPairV37(row.uploader_position);
    if (uploader) {
        return uploader;
    }

    const lat=Number(row.lat ?? row.latitude);
    const lon=Number(row.lon ?? row.lng ?? row.longitude);

    if (Number.isFinite(lat) && Number.isFinite(lon)) {
        return [lat, lon];
    }

    return null;
}

function extraPopupTextV37(row, mode) {
    const title =
        mode === "sites"
        ? (row.station_name || row.station_id || row.site_id || "Stacja startowa")
        : (row.callsign || row.uploader_callsign || row.station_name || "Odbiornik");

    const bits=[String(title)];

    if (row.distance_km != null && Number.isFinite(Number(row.distance_km))) {
        bits.push("Odległość: " + Number(row.distance_km).toFixed(1) + " km");
    }

    if (mode === "listeners") {
        if (row.software_name) {
            bits.push("Program: " + String(row.software_name));
        }
        if (row.software_version) {
            bits.push("Wersja: " + String(row.software_version));
        }
    }

    if (mode === "sites") {
        if (row.burst_altitude != null && Number.isFinite(Number(row.burst_altitude))) {
            bits.push("Maks. wysokość: " + Math.round(Number(row.burst_altitude)) + " m");
        }
        if (row.rs_types && Array.isArray(row.rs_types) && row.rs_types.length) {
            bits.push("Typy sond: " + row.rs_types.join(", "));
        }
    }

    return bits.join("<br>");
}

function extraRenderMapV37(data, mode) {
    extraRemoveRawCardV37();

    const m=extraLeafletMapV37();
    const layer=extraEnsureLayerV37();

    if (!m || !layer || typeof L === "undefined") {
        return;
    }

    layer.clearLayers();

    let rows=null;

    if (mode === "sites") {
        rows=extraRowsV36(data, "sites");
    } else if (mode === "listeners") {
        rows=extraRowsV36(data, "listeners");
    } else {
        return;
    }

    if (!rows || !Array.isArray(rows) || rows.length === 0) {
        return;
    }

    const pts=[];

    for (const row of rows) {
        const ll=extraLatLonV37(row);
        if (!ll) {
            continue;
        }

        const color =
            mode === "sites"
            ? "#f59e0b"
            : "#60a5fa";

        const marker=L.circleMarker(
            ll,
            {
                radius: 5,
                color: color,
                weight: 2,
                fillColor: color,
                fillOpacity: 0.75
            }
        );

        marker.bindPopup(
            extraPopupTextV37(row, mode)
        );

        marker.addTo(layer);
        pts.push(ll);
    }

    if (pts.length === 0) {
        return;
    }

    if (pts.length === 1) {
        m.setView(pts[0], 8);
        return;
    }

    const bounds=L.latLngBounds(pts);
    m.fitBounds(bounds, {padding:[24,24]});
}

setTimeout(extraRemoveRawCardV37, 0);
document.addEventListener("DOMContentLoaded", extraRemoveRawCardV37);

async function extraPrettyV36(
    url,
    title,
    preferred="",
    mapMode=""
) {

    extraRemoveRawCardV37();

    const host=
        extraHostV36();

    if (!host) {
        return;
    }


    extraButtonV36(
        title
    );


    host.replaceChildren();


    const head=
        document.createElement(
            "div"
        );

    head.className=
        "extra-v36-head";


    const left=
        document.createElement(
            "div"
        );


    const titleEl=
        document.createElement(
            "div"
        );

    titleEl.className=
        "extra-v36-title";

    titleEl.textContent=
        title;


    const meta=
        document.createElement(
            "div"
        );

    meta.className=
        "extra-v36-meta";

    meta.textContent=
        "Ładowanie danych…";


    left.appendChild(
        titleEl
    );

    left.appendChild(
        meta
    );

    head.appendChild(
        left
    );

    host.appendChild(
        head
    );


    try {

        const data=
            await jget(url);

        // MAP_FIX_V37_1_ASYNC_MAP
        //
        // Rysowanie mapy nie może zablokować
        // wyświetlenia tabeli.
        setTimeout(
            () => {
                try {

                    if (mapMode) {
                        extraRenderMapV37(
                            data,
                            mapMode
                        );
                    } else {
                        extraClearMapV37();
                    }

                } catch (mapError) {

                    console.error(
                        "V37.1 mapa:",
                        mapError
                    );
                }
            },
            0
        );

        const rows=
            extraRowsV36(
                data,
                preferred
            );

        meta.textContent=
            extraMetaV36(
                data,
                rows
            )
            ||
            "Dane SondeHub";


        if (rows) {

            extraTableV36(
                host,
                rows
            );

        } else if (
            data
            &&
            typeof data
            ===
            "object"
        ) {

            extraKVV36(
                host,
                data
            );

        } else {

            const empty=
                document.createElement(
                    "div"
                );

            empty.className=
                "extra-v36-empty";

            empty.textContent=
                String(
                    data
                    ??
                    "Brak danych"
                );

            host.appendChild(
                empty
            );
        }


        // Stary blok diagnostycznego JSON może pozostać
        // w DOM, ale V36 już go nie zasila.

    } catch (err) {

        extraClearMapV37();

        meta.textContent=
            "Nie udało się pobrać danych";


        const box=
            document.createElement(
                "div"
            );

        box.className=
            "extra-v36-error";

        box.textContent=
            String(
                err
                &&
                err.message
                ?
                err.message
                :
                err
            );

        host.appendChild(
            box
        );
    }
}



// ============================================================
// EXTRA_TOGGLE_V37_3
// Drugie kliknięcie tego samego przycisku = ukryj.
// ============================================================

let extraActiveModeV373="";


function extraHideV373() {

    extraClearMapV37();

    const host=
        extraHostV36();

    if (host) {
        host.replaceChildren();
    }

    extraButtonV36("");

    extraActiveModeV373="";


    // Jeżeli wcześniej użytkownik wybrał sondę,
    // po zamknięciu dodatkowej warstwy wracamy
    // do jej normalnego widoku na mapie.
    if (
        selectedSerial
        &&
        typeof showDetail === "function"
    ) {

        setTimeout(
            () => {
                showDetail(
                    selectedSerial
                ).catch(
                    err =>
                        console.error(
                            "V37.3 powrót do sondy:",
                            err
                        )
                );
            },
            0
        );
    }
}


function extraToggleV373(
    mode,
    url,
    title,
    preferred,
    mapMode
) {

    if (
        extraActiveModeV373
        ===
        mode
    ) {

        extraHideV373();

        return Promise.resolve();
    }


    extraActiveModeV373=
        mode;


    return extraPrettyV36(
        url,
        title,
        preferred,
        mapMode
    ).catch(
        err => {

            extraActiveModeV373="";

            throw err;
        }
    );
}


function loadSites(){return extraToggleV373("sites","/api/sites?distance_km=1000","Stacje startowe ≤1000 km","sites","sites");}
function loadSiteHistory(site){return extraPrettyV36("/api/site-sondes?site="+encodeURIComponent(site)+"&last=604800","Historia stacji "+site+" — 7 dni","sondes","");}
function loadListeners(){return extraToggleV373("listeners","/api/listeners?distance_km=1000","Odbiorniki SondeHub ≤1000 km","listeners","listeners");}
function loadAmateur(){return extraToggleV373("amateur","/api/amateur?distance_km=1000&last=21600","Balony amatorskie ≤1000 km","sondes","");}
function loadRecoveryStats(){return extraToggleV373("recovery","/api/recovery-stats?distance_km=1000","Statystyki odzyskiwania","recoveries","");}

async function boot(){
 try{await loadStatus();await loadSondes();}catch(e){console.error(e);}
}
async function refreshRealtime(){
 if(!selectedSerial)return;
 try{
   const d=await jget("/api/realtime?serial="+encodeURIComponent(selectedSerial));
   if(d.telemetry){
     document.getElementById("telemetry").innerHTML=telemHtml(d.telemetry);
   }
 }catch(e){console.error(e);}
}

boot();
setInterval(async()=>{try{await loadStatus();await loadSondes();}catch(e){console.error(e);}},15000);
setInterval(refreshRealtime,5000);
</script>

<!-- TABLE_SORT_ALL_V23 -->
<script>
(() => {
  "use strict";

  const done = new WeakSet();

  function value(cell) {
    const s = String(
      cell?.textContent ?? ""
    )
    .replace(/\u00a0/g, " ")
    .replace(/\s+/g, " ")
    .trim();

    if (!s || s === "—" || s === "-") {
      return { empty:true, type:"text", value:"" };
    }

    if (/^\d{4}-\d{2}-\d{2}T/u.test(s)) {
      const t = Date.parse(s);

      if (Number.isFinite(t)) {
        return { empty:false, type:"number", value:t };
      }
    }

    if (s.toLocaleLowerCase("pl").includes("temu")) {
      let sec = 0;
      let hit = false;

      for (const [re, mult] of [
        [/([0-9]+)\s*(?:d|dzień|dni)\b/iu, 86400],
        [/([0-9]+)\s*h\b/iu, 3600],
        [/([0-9]+)\s*min\b/iu, 60],
        [/([0-9]+)\s*s\b/iu, 1]
      ]) {
        const m = s.match(re);

        if (m) {
          sec += Number(m[1]) * mult;
          hit = true;
        }
      }

      if (hit) {
        return { empty:false, type:"number", value:sec };
      }
    }

    const n = s
      .replace(",", ".")
      .match(/^[-+]?\d+(?:\.\d+)?/u);

    if (n) {
      const x = Number(n[0]);

      if (Number.isFinite(x)) {
        return { empty:false, type:"number", value:x };
      }
    }

    return {
      empty:false,
      type:"text",
      value:s.toLocaleLowerCase("pl")
    };
  }


  function compare(a, b, dir) {
    if (a.empty && b.empty) return 0;
    if (a.empty) return 1;
    if (b.empty) return -1;

    const c =
      a.type === "number" && b.type === "number"
      ? a.value - b.value
      : String(a.value).localeCompare(
          String(b.value),
          "pl",
          {
            numeric:true,
            sensitivity:"base"
          }
        );

    return c * dir;
  }


  function init(table) {
    if (done.has(table)) return;

    /*
     * Lokalna tabela V18 ma już własny sorter.
     */
    if (
      table.classList.contains("localRxTable") ||
      table.querySelector("th[data-localrx-key]")
    ) {
      done.add(table);
      return;
    }

    const head = table.tHead;
    const body = table.tBodies?.[0];

    if (!head || !body || !head.rows.length) return;

    const headers = [
      ...head.rows[head.rows.length - 1].cells
    ];

    const state = {
      col:-1,
      dir:1,
      busy:false
    };

    let observer;


    function paint() {
      headers.forEach((th, i) => {
        if (!th.dataset.baseV23) {
          th.dataset.baseV23 = th.textContent.trim();
        }

        th.textContent =
          th.dataset.baseV23 +
          (
            state.col === i
            ? (state.dir > 0 ? " ▲" : " ▼")
            : " ↕"
          );
      });
    }


    function sort() {
      if (state.col < 0 || state.busy) return;

      state.busy = true;
      observer.disconnect();

      const rows = [...body.rows].filter(
        row =>
          row.cells.length > state.col &&
          !(row.cells.length === 1 && row.cells[0].colSpan > 1)
      );

      const sorted = [...rows].sort(
        (a, b) =>
          compare(
            value(a.cells[state.col]),
            value(b.cells[state.col]),
            state.dir
          )
      );

      if (
        sorted.some(
          (row, i) => row !== rows[i]
        )
      ) {
        sorted.forEach(
          row => body.appendChild(row)
        );
      }

      state.busy = false;

      observer.observe(
        body,
        { childList:true }
      );
    }


    observer = new MutationObserver(
      () => {
        if (state.col >= 0 && !state.busy) {
          setTimeout(sort, 0);
        }
      }
    );


    headers.forEach((th, i) => {
      th.classList.add("sort-v23");
      th.title = "Kliknij, aby sortować";

      th.addEventListener(
        "click",
        () => {
          if (state.col === i) {
            state.dir *= -1;
          } else {
            state.col = i;
            state.dir = 1;
          }

          paint();
          sort();
        }
      );
    });


    done.add(table);

    table.dataset.sortV23 = "1";

    paint();

    observer.observe(
      body,
      { childList:true }
    );
  }


  function scan() {
    document
      .querySelectorAll("table")
      .forEach(init);
  }


  function start() {
    scan();

    new MutationObserver(scan).observe(
      document.body,
      {
        childList:true,
        subtree:true
      }
    );
  }


  document.readyState === "loading"
    ? document.addEventListener(
        "DOMContentLoaded",
        start,
        { once:true }
      )
    : start();

})();

</script>

<!-- SONDE_ANALYTICS_V38 -->
<script id="sonde-analytics-v38-script">

function saNumV38(v) {
    const n=Number(v);
    return Number.isFinite(n)
        ? n
        : null;
}


function saTimeV38(v) {

    if (
        v === null
        ||
        v === undefined
        ||
        v === ""
    ) {
        return null;
    }

    if (
        typeof v === "number"
        &&
        Number.isFinite(v)
    ) {

        const ms=
            v > 1e12
            ?
            v
            :
            v*1000;

        const d=
            new Date(ms);

        return Number.isNaN(
            d.getTime()
        )
        ?
        null
        :
        d;
    }

    const d=
        new Date(v);

    return Number.isNaN(
        d.getTime()
    )
    ?
    null
    :
    d;
}


function saPointTimeV38(p) {

    if (!p) {
        return null;
    }

    return saTimeV38(
        p.datetime
        ??
        p.time
        ??
        p.timestamp
        ??
        p.ts
    );
}


function saDistanceV38(
    lat1,
    lon1,
    lat2,
    lon2
) {

    lat1=saNumV38(lat1);
    lon1=saNumV38(lon1);
    lat2=saNumV38(lat2);
    lon2=saNumV38(lon2);

    if (
        lat1 === null
        ||
        lon1 === null
        ||
        lat2 === null
        ||
        lon2 === null
    ) {
        return null;
    }

    const R=6371;

    const rad=
        x =>
            x*Math.PI/180;

    const dLat=
        rad(lat2-lat1);

    const dLon=
        rad(lon2-lon1);

    const a=
        Math.sin(dLat/2)**2
        +
        Math.cos(rad(lat1))
        *
        Math.cos(rad(lat2))
        *
        Math.sin(dLon/2)**2;

    return (
        2
        *
        R
        *
        Math.atan2(
            Math.sqrt(a),
            Math.sqrt(1-a)
        )
    );
}


function saBearingV38(
    lat1,
    lon1,
    lat2,
    lon2
) {

    lat1=saNumV38(lat1);
    lon1=saNumV38(lon1);
    lat2=saNumV38(lat2);
    lon2=saNumV38(lon2);

    if (
        lat1 === null
        ||
        lon1 === null
        ||
        lat2 === null
        ||
        lon2 === null
    ) {
        return null;
    }

    const r=
        x =>
            x*Math.PI/180;

    const p1=r(lat1);
    const p2=r(lat2);
    const dl=r(lon2-lon1);

    const y=
        Math.sin(dl)
        *
        Math.cos(p2);

    const x=
        Math.cos(p1)
        *
        Math.sin(p2)
        -
        Math.sin(p1)
        *
        Math.cos(p2)
        *
        Math.cos(dl);

    return (
        Math.atan2(y,x)
        *
        180
        /
        Math.PI
        +
        360
    ) % 360;
}


function saDirV38(b) {

    b=saNumV38(b);

    if (b === null) {
        return "";
    }

    const dirs=[
        "N",
        "NE",
        "E",
        "SE",
        "S",
        "SW",
        "W",
        "NW"
    ];

    return dirs[
        Math.round(b/45)%8
    ];
}


function saDurationV38(sec) {

    sec=saNumV38(sec);

    if (
        sec === null
        ||
        sec < 0
    ) {
        return "—";
    }

    sec=Math.round(sec);

    const h=
        Math.floor(sec/3600);

    const m=
        Math.floor(
            (sec%3600)/60
        );

    const s=
        sec%60;

    if (h > 0) {
        return `${h} h ${m} min`;
    }

    if (m > 0) {
        return `${m} min ${s} s`;
    }

    return `${s} s`;
}


function saKmV38(v) {

    v=saNumV38(v);

    if (v === null) {
        return "—";
    }

    return (
        v < 10
        ?
        v.toFixed(1)
        :
        v.toFixed(0)
    )
    +
    " km";
}


function saMetrV38(v) {

    v=saNumV38(v);

    return v === null
        ?
        "—"
        :
        Math.round(v)+" m";
}


function saDbV38(v) {

    v=saNumV38(v);

    return v === null
        ?
        "—"
        :
        v.toFixed(1)+" dB";
}


function saCardV38(
    id,
    title
) {

    const card=
        document.createElement(
            "div"
        );

    card.className=
        "sav38-card";

    card.innerHTML=
        `<div class="sav38-title">${title}</div>`
        +
        `<div id="${id}Main" class="sav38-main">—</div>`
        +
        `<div id="${id}Detail" class="sav38-detail">—</div>`;

    return card;
}


function saEnsureV38() {

    let row=
        document.getElementById(
            "sondeAnalyticsV38"
        );

    if (row) {
        return row;
    }

    const mapEl=
        document.getElementById(
            "map"
        );

    if (!mapEl) {
        return null;
    }

    row=
        document.createElement(
            "div"
        );

    row.id=
        "sondeAnalyticsV38";

    row.appendChild(
        saCardV38(
            "saFlightV38",
            "📈 ANALIZA LOTU"
        )
    );

    row.appendChild(
        saCardV38(
            "saNearV38",
            "📍 NAJBLIŻEJ STACJI"
        )
    );

    row.appendChild(
        saCardV38(
            "saRxV38",
            "📡 NASZ ODBIÓR"
        )
    );

    mapEl.parentNode.insertBefore(
        row,
        mapEl
    );

    return row;
}


function saSetV38(
    id,
    main,
    detail,
    color=""
) {

    const m=
        document.getElementById(
            id+"Main"
        );

    const d=
        document.getElementById(
            id+"Detail"
        );

    if (!m || !d) {
        return;
    }

    m.className=
        "sav38-main"
        +
        (
            color
            ?
            " "+color
            :
            ""
        );

    m.textContent=
        main || "—";

    d.textContent=
        detail || "—";
}


function saTrackV38(d) {

    const tr=
        (
            (
                d.flight || {}
            ).track
            ||
            []
        );

    return Array.isArray(tr)
        ?
        tr
        :
        [];
}


function saPhaseV38(latest) {

    const vv=
        saNumV38(
            latest
            ?
            latest.vel_v
            :
            null
        );

    if (vv === null) {

        return {
            text:"BRAK DANYCH",
            color:"sav38-muted"
        };
    }

    if (vv > 1.0) {

        return {
            text:"WZNOSZENIE",
            color:"sav38-green"
        };
    }

    if (vv < -1.0) {

        return {
            text:"OPADANIE",
            color:"sav38-amber"
        };
    }

    return {
        text:"STABILNIE",
        color:"sav38-blue"
    };
}


function saFlightStatsV38(d) {

    const tr=
        saTrackV38(d);

    const latest=
        d.latest || {};

    const phase=
        saPhaseV38(latest);

    const altitudes=
        tr
        .map(
            x =>
                saNumV38(x.alt)
        )
        .filter(
            x =>
                x !== null
        );

    const maxAlt=
        altitudes.length
        ?
        Math.max(
            ...altitudes
        )
        :
        saNumV38(
            latest.alt
        );


    const times=
        tr
        .map(saPointTimeV38)
        .filter(Boolean)
        .sort(
            (a,b) =>
                a-b
        );

    let duration=null;

    if (
        times.length >= 2
    ) {
        duration=
            (
                times[
                    times.length-1
                ]
                -
                times[0]
            )
            /
            1000;
    }


    let travelled=0;
    let travelledValid=false;

    for (
        let i=1;
        i<tr.length;
        i++
    ) {

        const a=tr[i-1];
        const b=tr[i];

        const dist=
            saDistanceV38(
                a.lat,
                a.lon,
                b.lat,
                b.lon
            );

        if (
            dist !== null
            &&
            dist < 500
        ) {
            travelled+=dist;
            travelledValid=true;
        }
    }


    const vv=
        saNumV38(
            latest.vel_v
        );


    let detail=
        [];

    if (vv !== null) {
        detail.push(
            "pionowo "
            +
            (
                vv >= 0
                ?
                "+"
                :
                ""
            )
            +
            vv.toFixed(1)
            +
            " m/s"
        );
    }

    if (maxAlt !== null) {
        detail.push(
            "maks. "
            +
            saMetrV38(maxAlt)
        );
    }

    if (duration !== null) {
        detail.push(
            "lot "
            +
            saDurationV38(duration)
        );
    }

    if (travelledValid) {
        detail.push(
            "ślad "
            +
            saKmV38(travelled)
        );
    }


    saSetV38(
        "saFlightV38",
        phase.text,
        detail.join(" • "),
        phase.color
    );
}


function saPredictionPointsV38(d) {

    const p=
        (
            (
                d.prediction || {}
            ).path
            ||
            []
        );

    return Array.isArray(p)
        ?
        p
        :
        [];
}


function saClosestV38(d) {

    if (
        !HOME
        ||
        saNumV38(HOME.lat) === null
        ||
        saNumV38(HOME.lon) === null
    ) {

        saSetV38(
            "saNearV38",
            "BRAK POZYCJI STACJI",
            "Nie można policzyć podejścia.",
            "sav38-muted"
        );

        return;
    }


    let points=
        saPredictionPointsV38(d);

    let source=
        "prognoza";


    if (!points.length) {

        if (d.latest) {
            points=[d.latest];
            source="aktualna pozycja";
        } else {
            points=saTrackV38(d);
            source="historia lotu";
        }
    }


    let best=null;


    for (
        const point
        of points
    ) {

        const dist=
            saDistanceV38(
                HOME.lat,
                HOME.lon,
                point.lat,
                point.lon
            );

        if (dist === null) {
            continue;
        }

        if (
            !best
            ||
            dist < best.dist
        ) {

            best={
                point,
                dist
            };
        }
    }


    if (!best) {

        saSetV38(
            "saNearV38",
            "BRAK DANYCH",
            "Brak punktów trasy z pozycją.",
            "sav38-muted"
        );

        return;
    }


    const bearing=
        saBearingV38(
            HOME.lat,
            HOME.lon,
            best.point.lat,
            best.point.lon
        );


    const alt=
        saNumV38(
            best.point.alt
        );


    const t=
        saPointTimeV38(
            best.point
        );


    let when="";

    if (t) {

        const delta=
            (
                t
                -
                new Date()
            )
            /
            1000;

        if (delta > 30) {

            when=
                "za "
                +
                saDurationV38(delta);

        } else if (delta >= -30) {

            when=
                "teraz";

        } else {

            when=
                saDurationV38(
                    Math.abs(delta)
                )
                +
                " temu";
        }
    }


    const detail=[];

    if (bearing !== null) {
        detail.push(
            `${Math.round(bearing)}° ${saDirV38(bearing)}`
        );
    }

    if (alt !== null) {
        detail.push(
            saMetrV38(alt)
        );
    }

    if (when) {
        detail.push(when);
    }

    detail.push(source);


    saSetV38(
        "saNearV38",
        saKmV38(best.dist),
        detail.join(" • "),
        best.dist <= 100
        ?
        "sav38-green"
        :
        best.dist <= 300
        ?
        "sav38-blue"
        :
        "sav38-muted"
    );
}


function saLocalFramesV38(d) {

    const tr=
        saTrackV38(d);

    return tr.filter(
        x => {

            const u=
                String(
                    x.uploader_callsign
                    ??
                    x.uploader
                    ??
                    ""
                )
                .trim()
                .toLowerCase();

            return u ===
                __SONDEHUB_LISTENER_CALLSIGN_JSON_V36C__;
        }
    );
}


function saLocalAggregateV38(serial) {

    if (
        typeof localRxRows
        ===
        "undefined"
        ||
        !Array.isArray(localRxRows)
    ) {
        return null;
    }

    const wanted=
        String(
            serial || ""
        )
        .trim()
        .toUpperCase();

    return (
        localRxRows.find(
            row => {

                const got=
                    String(
                        row.serial
                        ??
                        row.sonde
                        ??
                        row.payload_callsign
                        ??
                        ""
                    )
                    .trim()
                    .toUpperCase();

                return got === wanted;
            }
        )
        ||
        null
    );
}


function saPickV38(
    obj,
    keys
) {

    if (!obj) {
        return null;
    }

    for (
        const k
        of keys
    ) {

        if (
            obj[k] !== undefined
            &&
            obj[k] !== null
            &&
            obj[k] !== ""
        ) {
            return obj[k];
        }
    }

    return null;
}


function saRxStatsV38(d) {

    const local=
        saLocalFramesV38(d);

    const agg=
        saLocalAggregateV38(
            d.serial
        );


    if (local.length > 0) {

        const snrs=
            local
            .map(
                x =>
                    saNumV38(x.snr)
            )
            .filter(
                x =>
                    x !== null
            );


        const alts=
            local
            .map(
                x =>
                    saNumV38(x.alt)
            )
            .filter(
                x =>
                    x !== null
            );


        const times=
            local
            .map(saPointTimeV38)
            .filter(Boolean)
            .sort(
                (a,b) =>
                    a-b
            );


        const distances=
            (
                HOME
                ?
                local
                .map(
                    x =>
                        saDistanceV38(
                            HOME.lat,
                            HOME.lon,
                            x.lat,
                            x.lon
                        )
                )
                .filter(
                    x =>
                        x !== null
                )
                :
                []
            );


        const detail=[];


        if (snrs.length) {

            detail.push(
                "SNR max "
                +
                saDbV38(
                    Math.max(
                        ...snrs
                    )
                )
            );
        }


        if (distances.length) {

            detail.push(
                "max "
                +
                saKmV38(
                    Math.max(
                        ...distances
                    )
                )
            );
        }


        if (alts.length) {

            detail.push(
                "min wys. "
                +
                saMetrV38(
                    Math.min(
                        ...alts
                    )
                )
            );
        }


        if (
            times.length >= 2
        ) {

            detail.push(
                "odbiór "
                +
                saDurationV38(
                    (
                        times[
                            times.length-1
                        ]
                        -
                        times[0]
                    )
                    /
                    1000
                )
            );
        }


        saSetV38(
            "saRxV38",
            local.length
            +
            (
                local.length === 1
                ?
                " lokalna ramka"
                :
                " lokalnych ramek"
            ),
            detail.join(" • "),
            "sav38-green"
        );

        return;
    }


    if (agg) {

        const frames=
            saPickV38(
                agg,
                [
                    "frames",
                    "frame_count",
                    "packets",
                    "count"
                ]
            );

        const snrMax=
            saPickV38(
                agg,
                [
                    "snr_max",
                    "max_snr",
                    "snrMax"
                ]
            );

        const detail=[];


        if (snrMax !== null) {
            detail.push(
                "SNR max "
                +
                saDbV38(snrMax)
            );
        }


        const duration=
            saPickV38(
                agg,
                [
                    "duration_s",
                    "duration_seconds",
                    "receive_seconds"
                ]
            );


        if (
            saNumV38(duration)
            !== null
        ) {
            detail.push(
                "odbiór "
                +
                saDurationV38(duration)
            );
        }


        saSetV38(
            "saRxV38",
            frames !== null
            ?
            String(frames)
            +
            " ramek"
            :
            "ODEBRANA LOKALNIE",
            detail.join(" • ")
            ||
            "Dane z lokalnego auto_rx.",
            "sav38-green"
        );

        return;
    }


    if (d.local_receive_now) {

        saSetV38(
            "saRxV38",
            "ODBIERAMY TERAZ",
            "Stacja lokalna odbiera tę sondę.",
            "sav38-green"
        );

        return;
    }


    if (d.local_received_today) {

        saSetV38(
            "saRxV38",
            "ODEBRANA DZISIAJ",
            "Brak lokalnej ramki w bieżącej historii lotu.",
            "sav38-blue"
        );

        return;
    }


    saSetV38(
        "saRxV38",
        "BRAK LOKALNEGO ODBIORU",
        "Stacja lokalna nie odebrał tej sondy.",
        "sav38-muted"
    );
}


function renderSondeAnalyticsV38(d) {

    if (!d) {
        return;
    }

    if (!saEnsureV38()) {
        return;
    }

    try {
        saFlightStatsV38(d);
    } catch (e) {
        console.error(
            "V38 flight:",
            e
        );

        saSetV38(
            "saFlightV38",
            "BŁĄD ANALIZY",
            "Nie udało się policzyć lotu.",
            "sav38-red"
        );
    }


    try {
        saClosestV38(d);
    } catch (e) {
        console.error(
            "V38 closest:",
            e
        );

        saSetV38(
            "saNearV38",
            "BŁĄD ANALIZY",
            "Nie udało się policzyć podejścia.",
            "sav38-red"
        );
    }


    try {
        saRxStatsV38(d);
    } catch (e) {
        console.error(
            "V38 rx:",
            e
        );

        saSetV38(
            "saRxV38",
            "BŁĄD ANALIZY",
            "Nie udało się policzyć odbioru.",
            "sav38-red"
        );
    }
}

</script>


<!-- FULL_ANALYTICS_V39 -->
<script id="full-analytics-v39-script">

let v39LastDetail=null;
let v39LastLocal=null;
let v39TelemetryTimer=null;

let v39FieldRadiusKm=300;

let v39StationStatsPromise=null;


function v39FmtClock(d) {

    if (!(d instanceof Date)) {
        return "—";
    }

    return d.toLocaleTimeString(
        "pl-PL",
        {
            hour:"2-digit",
            minute:"2-digit",
            second:"2-digit"
        }
    );
}


function v39AgeSeconds(value) {

    const d=saTimeV38(value);

    if (!d) {
        return null;
    }

    return (
        Date.now()
        -
        d.getTime()
    ) / 1000;
}


function v39FlightDuration(d) {

    const tr=saTrackV38(d);

    const times=
        tr
        .map(saPointTimeV38)
        .filter(Boolean)
        .sort(
            (a,b) =>
                a-b
        );

    if (times.length < 2) {
        return null;
    }

    return (
        times[times.length-1]
        -
        times[0]
    ) / 1000;
}


function v39EnsureUI() {

    const v38=
        document.getElementById(
            "sondeAnalyticsV38"
        );

    const mapEl=
        document.getElementById(
            "map"
        );

    if (!v38 || !mapEl) {
        return false;
    }


    if (
        !document.getElementById(
            "v39TelemetryStrip"
        )
    ) {

        const strip=
            document.createElement(
                "div"
            );

        strip.id=
            "v39TelemetryStrip";

        v38.parentNode.insertBefore(
            strip,
            v38
        );
    }


    if (
        !document.getElementById(
            "v39Charts"
        )
    ) {

        const charts=
            document.createElement(
                "details"
            );

        charts.id=
            "v39Charts";

        charts.innerHTML=`
<summary>📊 Historia lotu i jakości sygnału</summary>

<div id="v39ChartGrid">

  <div class="v39-chart">
    <div class="v39-chart-title">
      SNR Stacja lokalna vs czas
    </div>
    <canvas id="v39SnrTime"></canvas>
  </div>

  <div class="v39-chart">
    <div class="v39-chart-title">
      SNR odbioru vs odległość od Stacja lokalna
    </div>
    <canvas id="v39SnrDistance"></canvas>
  </div>

  <div class="v39-chart">
    <div class="v39-chart-title">
      Wysokość vs czas
    </div>
    <canvas id="v39AltTime"></canvas>
  </div>

  <div class="v39-chart">
    <div class="v39-chart-title">
      Prędkość pionowa vs czas
    </div>
    <canvas id="v39VelTime"></canvas>
  </div>

</div>
`;

        v38.insertAdjacentElement(
            "afterend",
            charts
        );
    }

    return true;
}


function v39Telemetry(d) {

    const box=
        document.getElementById(
            "v39TelemetryStrip"
        );

    if (!box || !d) {
        return;
    }

    const t=d.latest || {};

    const sats=saNumV38(t.sats);
    const batt=saNumV38(t.batt);
    const snr=saNumV38(t.snr);
    const rssi=saNumV38(t.rssi);

    const age=
        v39AgeSeconds(
            t.datetime
        );

    const warnings=[];

    if (age !== null && age > 180) {
        warnings.push(
            `telemetria stara ${saDurationV38(age)}`
        );
    }

    if (
        batt !== null
        &&
        batt < 2.2
    ) {
        warnings.push(
            `niska bateria ${batt.toFixed(2)} V`
        );
    }

    if (
        sats !== null
        &&
        sats < 4
    ) {
        warnings.push(
            `mało satelitów GPS: ${Math.round(sats)}`
        );
    }

    if (
        saNumV38(t.lat) === null
        ||
        saNumV38(t.lon) === null
    ) {
        warnings.push(
            "brak pozycji GPS"
        );
    }


    const items=[];

    items.push(
        `<strong>GPS</strong> ${
            sats === null
            ?
            "—"
            :
            Math.round(sats)+" sat"
        }`
    );

    items.push(
        `<strong>Bateria</strong> ${
            batt === null
            ?
            "—"
            :
            batt.toFixed(2)+" V"
        }`
    );

    items.push(
        `<strong>Telemetria</strong> ${
            age === null
            ?
            "—"
            :
            saDurationV38(
                Math.max(0,age)
            )+" temu"
        }`
    );

    items.push(
        `<strong>SNR</strong> ${
            snr === null
            ?
            "—"
            :
            snr.toFixed(1)+" dB"
        }`
    );

    items.push(
        `<strong>RSSI</strong> ${
            rssi === null
            ?
            "—"
            :
            rssi.toFixed(1)+" dBm"
        }`
    );


    const state=
        warnings.length
        ?
        `<span class="v39-warn">⚠ ${warnings.join(" • ")}</span>`
        :
        `<span class="v39-ok">● telemetria prawidłowa</span>`;


    box.innerHTML=
        items.join(" &nbsp;•&nbsp; ")
        +
        " &nbsp;&nbsp; "
        +
        state;
}


function v39EnhancedFlight(d) {

    const tr=saTrackV38(d);

    const latest=d.latest || {};

    const vv=
        saNumV38(
            latest.vel_v
        );

    const alt=
        saNumV38(
            latest.alt
        );


    let phase="BRAK DANYCH";
    let color="sav38-muted";


    // WYLĄDOWAŁA = konserwatywna heurystyka.
    if (
        alt !== null
        &&
        alt < 1000
        &&
        vv !== null
        &&
        Math.abs(vv) < 0.8
    ) {

        phase="WYLĄDOWAŁA*";
        color="sav38-blue";

    } else if (
        vv !== null
        &&
        vv > 1
    ) {

        phase="WZNOSZENIE";
        color="sav38-green";

    } else if (
        vv !== null
        &&
        vv < -1
    ) {

        phase="OPADANIE";
        color="sav38-amber";

    } else if (vv !== null) {

        phase="STABILNIE";
        color="sav38-blue";
    }


    const values=
        tr
        .map(
            p => ({
                t:saPointTimeV38(p),
                vv:saNumV38(p.vel_v),
                alt:saNumV38(p.alt)
            })
        );


    const validTimes=
        values
        .map(x=>x.t)
        .filter(Boolean);


    let avg2m=null;

    if (validTimes.length) {

        const lastTime=
            new Date(
                Math.max(
                    ...validTimes.map(
                        x =>
                            x.getTime()
                    )
                )
            );

        const recent=
            values
            .filter(
                x =>
                    x.t
                    &&
                    x.vv !== null
                    &&
                    (
                        lastTime
                        -
                        x.t
                    )
                    <=
                    120000
            )
            .map(x=>x.vv);

        if (recent.length) {

            avg2m=
                recent.reduce(
                    (a,b)=>a+b,
                    0
                )
                /
                recent.length;
        }

    } else {

        const recent=
            values
            .filter(
                x =>
                    x.vv !== null
            )
            .slice(-15)
            .map(x=>x.vv);

        if (recent.length) {

            avg2m=
                recent.reduce(
                    (a,b)=>a+b,
                    0
                )
                /
                recent.length;
        }
    }


    const altitudes=
        values
        .map(x=>x.alt)
        .filter(
            x =>
                x !== null
        );


    let maxAlt=null;
    let burst=false;

    if (altitudes.length) {

        maxAlt=Math.max(
            ...altitudes
        );

        if (
            alt !== null
            &&
            maxAlt-alt > 300
            &&
            vv !== null
            &&
            vv < -1
        ) {
            burst=true;
        }
    }


    let distance=0;
    let distanceOK=false;

    for (
        let i=1;
        i<tr.length;
        i++
    ) {

        const x=
            saDistanceV38(
                tr[i-1].lat,
                tr[i-1].lon,
                tr[i].lat,
                tr[i].lon
            );

        if (
            x !== null
            &&
            x < 500
        ) {
            distance+=x;
            distanceOK=true;
        }
    }


    const duration=
        v39FlightDuration(d);


    const detail=[];

    if (vv !== null) {
        detail.push(
            `akt. ${
                vv>=0 ? "+" : ""
            }${vv.toFixed(1)} m/s`
        );
    }

    if (avg2m !== null) {
        detail.push(
            `śr. 2 min ${
                avg2m>=0 ? "+" : ""
            }${avg2m.toFixed(1)} m/s`
        );
    }

    if (maxAlt !== null) {
        detail.push(
            `maks. ${saMetrV38(maxAlt)}`
        );
    }

    if (burst) {
        detail.push(
            `BURST ${saMetrV38(maxAlt)}`
        );
    } else if (
        phase==="WZNOSZENIE"
    ) {
        detail.push(
            "burst jeszcze niewykryty"
        );
    }

    if (duration !== null) {
        detail.push(
            `lot ${saDurationV38(duration)}`
        );
    }

    if (distanceOK) {
        detail.push(
            `ślad ${saKmV38(distance)}`
        );
    }

    if (phase==="WYLĄDOWAŁA*") {
        detail.push(
            "* status wyliczony heurystycznie"
        );
    }


    saSetV38(
        "saFlightV38",
        phase,
        detail.join(" • "),
        color
    );
}


function v39Closest(d) {

    if (!HOME) {
        return;
    }

    const points=[];


    for (
        const p
        of saTrackV38(d)
    ) {
        points.push({
            ...p,
            _kind:"historia"
        });
    }


    for (
        const p
        of saPredictionPointsV38(d)
    ) {
        points.push({
            ...p,
            _kind:"prognoza"
        });
    }


    let best=null;

    for (
        const p
        of points
    ) {

        const dist=
            saDistanceV38(
                HOME.lat,
                HOME.lon,
                p.lat,
                p.lon
            );

        if (dist === null) {
            continue;
        }

        if (
            !best
            ||
            dist < best.dist
        ) {
            best={
                p,
                dist
            };
        }
    }


    if (!best) {
        return;
    }


    const bearing=
        saBearingV38(
            HOME.lat,
            HOME.lon,
            best.p.lat,
            best.p.lon
        );


    const alt=
        saNumV38(
            best.p.alt
        );


    const t=
        saPointTimeV38(
            best.p
        );


    let when="";

    if (t) {

        const sec=
            (
                t.getTime()
                -
                Date.now()
            )
            /
            1000;

        if (sec > 30) {
            when=
                "ZA "
                +
                saDurationV38(sec)
                .toUpperCase();

        } else if (sec < -30) {
            when=
                saDurationV38(
                    Math.abs(sec)
                )
                .toUpperCase()
                +
                " TEMU";

        } else {
            when="TERAZ";
        }
    }


    const inField=
        best.dist <=
        v39FieldRadiusKm;


    const main=
        saKmV38(best.dist)
        +
        (
            when
            ?
            " • "+when
            :
            ""
        );


    const detail=[];

    if (bearing !== null) {
        detail.push(
            `AZ ${Math.round(bearing)}° ${saDirV38(bearing)}`
        );
    }

    if (alt !== null) {
        detail.push(
            saMetrV38(alt)
        );
    }

    detail.push(
        best.p._kind
    );

    detail.push(
        inField
        ?
        `W POLU ≤${Math.round(v39FieldRadiusKm)} km`
        :
        `POZA POLEM ≤${Math.round(v39FieldRadiusKm)} km`
    );


    saSetV38(
        "saNearV38",
        main,
        detail.join(" • "),
        inField
        ?
        "sav38-green"
        :
        "sav38-blue"
    );
}


async function v39LocalData(serial) {

    try {

        return await jget(
            "/api/local-flight?serial="
            +
            encodeURIComponent(serial)
        );

    } catch (e) {

        console.error(
            "V39 local flight:",
            e
        );

        return {
            serial,
            count:0,
            samples:[]
        };
    }
}


function v39LocalQuality(d,localData) {

    const rows=
        (
            localData
            &&
            Array.isArray(
                localData.samples
            )
        )
        ?
        localData.samples
        :
        [];


    if (!rows.length) {

        // Zachowujemy fallback V38, jeśli brak surowego lokalnego logu.
        return;
    }


    const times=
        rows
        .map(
            x =>
                saTimeV38(
                    x.datetime
                )
        )
        .filter(Boolean)
        .sort(
            (a,b)=>a-b
        );


    const snrs=
        rows
        .map(
            x =>
                saNumV38(x.snr)
        )
        .filter(
            x =>
                x !== null
        );


    const alts=
        rows
        .map(
            x =>
                saNumV38(x.alt)
        )
        .filter(
            x =>
                x !== null
        );


    const distances=
        HOME
        ?
        rows
        .map(
            x =>
                saDistanceV38(
                    HOME.lat,
                    HOME.lon,
                    x.lat,
                    x.lon
                )
        )
        .filter(
            x =>
                x !== null
        )
        :
        [];


    let duration=null;

    if (times.length >= 2) {
        duration=
            (
                times[
                    times.length-1
                ]
                -
                times[0]
            )
            /
            1000;
    }


    let largestGap=null;

    if (times.length >= 2) {

        largestGap=0;

        for (
            let i=1;
            i<times.length;
            i++
        ) {

            const gap=
                (
                    times[i]
                    -
                    times[i-1]
                )
                /
                1000;

            if (gap > largestGap) {
                largestGap=gap;
            }
        }
    }


    const avgSnr=
        snrs.length
        ?
        snrs.reduce(
            (a,b)=>a+b,
            0
        )
        /
        snrs.length
        :
        null;


    const maxSnr=
        snrs.length
        ?
        Math.max(...snrs)
        :
        null;


    const flightDuration=
        v39FlightDuration(d);


    let coverage=null;

    if (
        duration !== null
        &&
        flightDuration !== null
        &&
        flightDuration > 0
    ) {

        coverage=
            Math.max(
                0,
                Math.min(
                    100,
                    100
                    *
                    duration
                    /
                    flightDuration
                )
            );
    }


    const detail=[];

    if (times.length) {
        detail.push(
            `pierwsza ${v39FmtClock(times[0])}`
        );

        detail.push(
            `ostatnia ${
                v39FmtClock(
                    times[times.length-1]
                )
            }`
        );
    }

    if (avgSnr !== null) {
        detail.push(
            `SNR śr. ${avgSnr.toFixed(1)} dB`
        );
    }

    if (maxSnr !== null) {
        detail.push(
            `SNR max ${maxSnr.toFixed(1)} dB`
        );
    }

    if (distances.length) {
        detail.push(
            `max ${saKmV38(
                Math.max(...distances)
            )}`
        );
    }

    if (alts.length) {
        detail.push(
            `min wys. ${saMetrV38(
                Math.min(...alts)
            )}`
        );
    }

    if (largestGap !== null) {
        detail.push(
            `najw. luka ${saDurationV38(largestGap)}`
        );
    }

    if (coverage !== null) {
        detail.push(
            `pokrycie czasu ${coverage.toFixed(0)}%`
        );
    }


    const main=
        `${rows.length} ramek`
        +
        (
            duration !== null
            ?
            ` • ${saDurationV38(duration)}`
            :
            ""
        );


    saSetV38(
        "saRxV38",
        main,
        detail.join(" • "),
        "sav38-green"
    );
}


function v39DrawChart(
    canvas,
    pairs,
    emptyText
) {

    if (!canvas) {
        return;
    }


    canvas.width=620;
    canvas.height=180;


    const ctx=
        canvas.getContext("2d");


    ctx.clearRect(
        0,
        0,
        canvas.width,
        canvas.height
    );


    const data=
        pairs
        .filter(
            p =>
                Number.isFinite(p[0])
                &&
                Number.isFinite(p[1])
        );


    ctx.font=
        "12px system-ui, sans-serif";


    if (data.length < 2) {

        ctx.fillStyle="#8fa3b7";

        ctx.fillText(
            emptyText || "Brak danych",
            16,
            28
        );

        return;
    }


    const xs=data.map(p=>p[0]);
    const ys=data.map(p=>p[1]);

    let xmin=Math.min(...xs);
    let xmax=Math.max(...xs);

    let ymin=Math.min(...ys);
    let ymax=Math.max(...ys);


    if (xmax===xmin) {
        xmax=xmin+1;
    }

    if (ymax===ymin) {
        ymax=ymin+1;
    }


    const padL=46;
    const padR=14;
    const padT=12;
    const padB=28;


    const X=
        x =>
            padL
            +
            (
                x-xmin
            )
            /
            (
                xmax-xmin
            )
            *
            (
                canvas.width
                -
                padL
                -
                padR
            );


    const Y=
        y =>
            canvas.height
            -
            padB
            -
            (
                y-ymin
            )
            /
            (
                ymax-ymin
            )
            *
            (
                canvas.height
                -
                padT
                -
                padB
            );


    ctx.strokeStyle="#31465a";
    ctx.lineWidth=1;

    ctx.beginPath();

    ctx.moveTo(
        padL,
        padT
    );

    ctx.lineTo(
        padL,
        canvas.height-padB
    );

    ctx.lineTo(
        canvas.width-padR,
        canvas.height-padB
    );

    ctx.stroke();


    ctx.fillStyle="#8fa3b7";

    ctx.fillText(
        ymax.toFixed(1),
        4,
        padT+5
    );

    ctx.fillText(
        ymin.toFixed(1),
        4,
        canvas.height-padB
    );


    ctx.strokeStyle="#60a5fa";
    ctx.lineWidth=2;

    ctx.beginPath();

    data.forEach(
        (p,i) => {

            const x=X(p[0]);
            const y=Y(p[1]);

            if (i===0) {
                ctx.moveTo(x,y);
            } else {
                ctx.lineTo(x,y);
            }
        }
    );

    ctx.stroke();
}


function v39Charts(d,localData) {

    if (!v39EnsureUI()) {
        return;
    }


    const local=
        (
            localData
            &&
            Array.isArray(
                localData.samples
            )
        )
        ?
        localData.samples
        :
        [];


    const localTime=[];

    for (const x of local) {

        const t=
            saTimeV38(
                x.datetime
            );

        const snr=
            saNumV38(
                x.snr
            );

        if (t && snr !== null) {
            localTime.push([
                t.getTime(),
                snr
            ]);
        }
    }


    const localDist=[];

    if (HOME) {

        for (const x of local) {

            const snr=
                saNumV38(
                    x.snr
                );

            const dist=
                saDistanceV38(
                    HOME.lat,
                    HOME.lon,
                    x.lat,
                    x.lon
                );

            if (
                snr !== null
                &&
                dist !== null
            ) {
                localDist.push([
                    dist,
                    snr
                ]);
            }
        }
    }


    const altTime=[];
    const velTime=[];


    for (
        const x
        of saTrackV38(d)
    ) {

        const t=
            saPointTimeV38(x);

        if (!t) {
            continue;
        }

        const alt=
            saNumV38(
                x.alt
            );

        const vv=
            saNumV38(
                x.vel_v
            );

        if (alt !== null) {
            altTime.push([
                t.getTime(),
                alt
            ]);
        }

        if (vv !== null) {
            velTime.push([
                t.getTime(),
                vv
            ]);
        }
    }


    v39DrawChart(
        document.getElementById(
            "v39SnrTime"
        ),
        localTime,
        "Brak lokalnych punktów SNR"
    );


    v39DrawChart(
        document.getElementById(
            "v39SnrDistance"
        ),
        localDist,
        "Brak lokalnych punktów SNR/odległość"
    );


    v39DrawChart(
        document.getElementById(
            "v39AltTime"
        ),
        altTime,
        "Brak historii wysokości"
    );


    v39DrawChart(
        document.getElementById(
            "v39VelTime"
        ),
        velTime,
        "Brak historii prędkości pionowej"
    );
}


function v39ScheduleValues(st) {

    if (!st) {
        return [];
    }

    const raw=
        st.times
        ??
        st.schedule
        ??
        st.launch_times
        ??
        [];

    if (Array.isArray(raw)) {
        return raw.map(String);
    }

    if (raw) {
        return [String(raw)];
    }

    return [];
}


function v39NextLaunch(st) {

    const times=
        v39ScheduleValues(st);

    if (!times.length) {
        return null;
    }


    const now=new Date();

    // JS: 0=niedziela.
    // Harmonogram SondeHub spotykany tutaj:
    // dzień_tygodnia:godzina:minuta
    const todayMon0=
        (
            now.getUTCDay()
            +
            6
        ) % 7;


    let best=null;


    for (
        const value
        of times
    ) {

        const txt=
            String(value)
            .trim();


        let m=
            txt.match(
                /^(\d+):(\d{2}):(\d{2})$/
            );


        if (m) {

            const day=Number(m[1]);
            const hour=Number(m[2]);
            const minute=Number(m[3]);

            if (
                day >= 0
                &&
                day <= 6
                &&
                hour <= 23
                &&
                minute <= 59
            ) {

                let deltaDay=
                    (
                        day
                        -
                        todayMon0
                        +
                        7
                    ) % 7;

                const candidate=
                    new Date(now);

                candidate.setUTCHours(
                    hour,
                    minute,
                    0,
                    0
                );

                candidate.setUTCDate(
                    candidate.getUTCDate()
                    +
                    deltaDay
                );

                if (
                    candidate <= now
                ) {
                    candidate.setUTCDate(
                        candidate.getUTCDate()
                        +
                        7
                    );
                }

                if (
                    !best
                    ||
                    candidate < best
                ) {
                    best=candidate;
                }

                continue;
            }
        }


        m=
            txt.match(
                /^(\d{1,2}):(\d{2})$/
            );


        if (m) {

            const hour=Number(m[1]);
            const minute=Number(m[2]);

            if (
                hour <= 23
                &&
                minute <= 59
            ) {

                const candidate=
                    new Date(now);

                candidate.setUTCHours(
                    hour,
                    minute,
                    0,
                    0
                );

                if (
                    candidate <= now
                ) {
                    candidate.setUTCDate(
                        candidate.getUTCDate()+1
                    );
                }

                if (
                    !best
                    ||
                    candidate < best
                ) {
                    best=candidate;
                }
            }
        }
    }

    return best;
}


function v39LaunchSite(d) {

    const el=
        document.getElementById(
            "site"
        );

    if (!el) {
        return;
    }

    const old=
        document.getElementById(
            "v39LaunchExtra"
        );

    if (old) {
        old.remove();
    }


    const st=
        (
            (
                d.launch_site || {}
            ).site
            ||
            null
        );


    const box=
        document.createElement(
            "div"
        );

    box.id=
        "v39LaunchExtra";

    box.className=
        "v39-extra-box";


    if (!st) {

        box.innerHTML=
            "<strong>Następny start:</strong> brak danych o stacji.";

        el.appendChild(box);

        return;
    }


    const next=
        v39NextLaunch(st);


    const types=
        st.rs_types
        ??
        st.types
        ??
        st.sonde_types
        ??
        null;


    const freq=
        st.frequencies
        ??
        st.frequency
        ??
        null;


    const bits=[];


    if (next) {

        const sec=
            (
                next.getTime()
                -
                Date.now()
            )
            /
            1000;

        bits.push(
            `<strong>Następny typowy start:</strong> ${
                next.toLocaleString(
                    "pl-PL",
                    {
                        timeZone:"UTC",
                        weekday:"short",
                        hour:"2-digit",
                        minute:"2-digit"
                    }
                )
            } UTC • za ${
                saDurationV38(sec)
            }`
        );

    } else {

        bits.push(
            "<strong>Następny typowy start:</strong> harmonogram nie pozwala wyliczyć terminu."
        );
    }


    if (types) {

        bits.push(
            `<strong>Typy:</strong> ${
                esc(
                    Array.isArray(types)
                    ?
                    types.join(", ")
                    :
                    types
                )
            }`
        );
    }


    if (freq) {

        bits.push(
            `<strong>Częstotliwości:</strong> ${
                esc(
                    Array.isArray(freq)
                    ?
                    freq.join(", ")
                    :
                    freq
                )
            }`
        );
    }


    box.innerHTML=
        bits.join("<br>");

    el.appendChild(box);
}


function v39Recovery(d) {

    const el=
        document.getElementById(
            "recovery"
        );

    if (!el) {
        return;
    }


    const old=
        document.getElementById(
            "v39RecoveryExtra"
        );

    if (old) {
        old.remove();
    }


    const rec=
        (
            (
                d.recovery || {}
            ).latest
            ||
            null
        );


    const box=
        document.createElement(
            "div"
        );

    box.id=
        "v39RecoveryExtra";

    box.className=
        "v39-extra-box";


    if (!rec) {

        box.innerHTML=
            "<strong>Status:</strong> BRAK ZGŁOSZENIA";

        el.appendChild(box);

        return;
    }


    let status="ZGŁOSZENIE";

    if (rec.recovered === true) {
        status="ODZYSKANA";
    } else if (rec.planned === true) {
        status="PLANOWANE ODZYSKANIE";
    } else if (
        rec.recovered === false
        &&
        rec.planned === false
    ) {
        status="NIEODZYSKANA";
    }


    const who=
        rec.recovered_by
        ??
        rec.reporter
        ??
        rec.callsign
        ??
        rec.uploader_callsign
        ??
        null;


    const when=
        rec.datetime
        ??
        rec.time
        ??
        rec.recovered_time
        ??
        null;


    const note=
        rec.description
        ??
        rec.notes
        ??
        rec.comment
        ??
        null;


    const bits=[
        `<strong>Status:</strong> ${esc(status)}`
    ];


    if (who) {
        bits.push(
            `<strong>Kto:</strong> ${esc(who)}`
        );
    }


    if (when) {

        const dt=
            saTimeV38(when);

        bits.push(
            `<strong>Kiedy:</strong> ${
                dt
                ?
                esc(
                    dt.toLocaleString(
                        "pl-PL"
                    )
                )
                :
                esc(when)
            }`
        );
    }


    if (note) {
        bits.push(
            `<strong>Opis:</strong> ${esc(note)}`
        );
    }


    box.innerHTML=
        bits.join("<br>");

    el.appendChild(box);
}


function v39StationStatsHost() {

    let host=
        document.getElementById(
            "v39StationStats"
        );

    if (host) {
        return host;
    }


    const mapEl=
        document.getElementById(
            "map"
        );

    if (!mapEl) {
        return null;
    }


    host=
        document.createElement(
            "section"
        );

    host.id=
        "v39StationStats";

    host.innerHTML=`
<div class="v40-station-head">
  <div>
    <h3>📡 Stacja lokalna — 7 dni</h3>
    <div class="v40-station-sub">
      Podsumowanie lokalnego odbioru radiosond
    </div>
  </div>
  <div class="v40-period-pill">7 dni</div>
</div>
<div class="muted">Ładowanie statystyk lokalnej stacji…</div>
`;


    mapEl.insertAdjacentElement(
        "afterend",
        host
    );


    return host;
}


function v39TopText(items) {

    if (
        !Array.isArray(items)
        ||
        !items.length
    ) {
        return "—";
    }

    return items
        .slice(0,6)
        .map(
            x =>
                `${x[0]} (${x[1]})`
        )
        .join("<br>");
}


async function v39TopLaunchSites(serials) {

    const list=
        Array.isArray(serials)
        ?
        serials.slice(0,12)
        :
        [];


    if (!list.length) {
        return [];
    }


    const results=
        await Promise.allSettled(
            list.map(
                serial =>
                    jget(
                        "/api/sonde?serial="
                        +
                        encodeURIComponent(serial)
                    )
            )
        );


    const counts=
        new Map();


    for (const result of results) {

        if (
            result.status
            !==
            "fulfilled"
        ) {
            continue;
        }


        const st=
            (
                (
                    result.value.launch_site
                    ||
                    {}
                ).site
                ||
                null
            );


        if (!st) {
            continue;
        }


        const name=
            String(
                st.station_name
                ||
                st.site_id
                ||
                "Nieznana"
            );


        counts.set(
            name,
            (
                counts.get(name)
                ||
                0
            )
            +
            1
        );
    }


    return [
        ...counts.entries()
    ]
    .sort(
        (a,b) =>
            b[1]-a[1]
    )
    .slice(0,6);
}


async function v39LoadStationStats() {

    if (v39StationStatsPromise) {
        return v39StationStatsPromise;
    }


    v39StationStatsPromise=
        (async()=>{

            const host=
                v39StationStatsHost();

            if (!host) {
                return null;
            }


            let data;

            try {

                data=
                    await jget(
                        "/api/station-stats-7d"
                    );

            } catch (e) {

                host.innerHTML=`
<div class="v40-station-head">
  <div>
    <h3>📡 Stacja lokalna — 7 dni</h3>
    <div class="v40-station-sub">
      Podsumowanie lokalnego odbioru radiosond
    </div>
  </div>
  <div class="v40-period-pill">7 dni</div>
</div>
<div class="sav38-red">
Nie udało się pobrać statystyk: ${esc(e)}
</div>
`;

                return null;
            }


            if (
                saNumV38(
                    data.max_distance_km
                )
                !==
                null
            ) {

                // Empiryczne pole odbioru =
                // 110% najlepszego wyniku z 7 dni,
                // minimum 100, maksimum 600 km.
                v39FieldRadiusKm=
                    Math.min(
                        600,
                        Math.max(
                            100,
                            Number(
                                data.max_distance_km
                            )
                            *
                            1.10
                        )
                    );
            }


            // STATION_STATS_FAST_V40_1
            // TOP stacje są dodatkiem i nie mogą blokować panelu.
            const topSites=
                await Promise.race([
                    v39TopLaunchSites(
                        data.recent_serials
                    ),
                    new Promise(
                        resolve =>
                            setTimeout(
                                () => resolve([]),
                                2500
                            )
                    )
                ]);


            host.innerHTML=`
<div class="v40-station-head">
  <div>
    <h3>📡 Stacja lokalna — 7 dni</h3>
    <div class="v40-station-sub">
      Podsumowanie lokalnego odbioru radiosond
    </div>
  </div>
  <div class="v40-period-pill">7 dni</div>
</div>

<div class="v39-stat-grid">

  <div class="v39-stat-box">
    <div class="v39-stat-label">📡 Odebrane sondy</div>
    <div class="v39-stat-value">${esc(data.sondes ?? "—")}</div>
  </div>

  <div class="v39-stat-box">
    <div class="v39-stat-label">📄 Łącznie ramek</div>
    <div class="v39-stat-value">${esc(data.total_frames ?? "—")}</div>
  </div>

  <div class="v39-stat-box">
    <div class="v39-stat-label">📍 Najdalszy odbiór</div>
    <div class="v39-stat-value">${
        data.max_distance_km == null
        ?
        "—"
        :
        Number(data.max_distance_km).toFixed(0)+" km"
    }</div>
  </div>

  <div class="v39-stat-box">
    <div class="v39-stat-label">⬇️ Najniższa wysokość</div>
    <div class="v39-stat-value">${
        data.min_alt == null
        ?
        "—"
        :
        Math.round(Number(data.min_alt))+" m"
    }</div>
  </div>

  <div class="v39-stat-box">
    <div class="v39-stat-label">⛰️ Najwyższa wysokość</div>
    <div class="v39-stat-value">${
        data.max_alt == null
        ?
        "—"
        :
        Math.round(Number(data.max_alt))+" m"
    }</div>
  </div>

  <div class="v39-stat-box v40-temp-box">
    <div class="v39-stat-label">🌡️ Najniższa temperatura</div>
    <div class="v39-stat-value">${
        data.min_temp_c == null
        ?
        "—"
        :
        Number(data.min_temp_c).toFixed(1)+" °C"
    }</div>
  </div>

  <div class="v39-stat-box">
    <div class="v39-stat-label">📶 Najlepszy SNR</div>
    <div class="v39-stat-value">${
        data.best_snr == null
        ?
        "—"
        :
        Number(data.best_snr).toFixed(1)+" dB"
    }</div>
  </div>

  <div class="v39-stat-box">
    <div class="v39-stat-label">🧩 Unikalne typy</div>
    <div class="v39-stat-value">${esc(data.unique_types ?? "—")}</div>
  </div>

  <div class="v39-stat-box">
    <div class="v39-stat-label">🎯 Pole odbioru — empiryczne</div>
    <div class="v39-stat-value">≤${Math.round(v39FieldRadiusKm)} km</div>
  </div>

  <div class="v39-stat-box">
    <div class="v39-stat-label">🕒 Okres</div>
    <div class="v39-stat-value">7 dni</div>
  </div>

</div>

<div class="v39-stat-lists">

  <div class="v39-stat-list">
    <strong>Typy sond</strong><br>
    ${v39TopText(data.top_types)}
  </div>

  <div class="v39-stat-list">
    <strong>Top częstotliwości</strong><br>
    ${v39TopText(data.top_frequencies)}
  </div>

  <div class="v39-stat-list">
    <strong>Top stacje startowe</strong>
    <div class="muted">
      na podstawie ostatnich ${
          Math.min(
              12,
              (data.recent_serials || []).length
          )
      } lotów
    </div>
    ${v39TopText(topSites)}
  </div>

</div>
`;

            if (v39LastDetail) {
                v39Closest(
                    v39LastDetail
                );
            }

            return data;
        })();


    return v39StationStatsPromise;
}


async function renderSondeFullV39(d) {

    if (!d) {
        return;
    }

    v39LastDetail=d;


    if (!v39EnsureUI()) {
        return;
    }


    v39Telemetry(d);
    v39EnhancedFlight(d);
    v39Closest(d);

    v39LaunchSite(d);
    v39Recovery(d);


    const local=
        await v39LocalData(
            d.serial
        );

    v39LastLocal=local;


    v39LocalQuality(
        d,
        local
    );


    v39Charts(
        d,
        local
    );


    if (!v39TelemetryTimer) {

        v39TelemetryTimer=
            setInterval(
                () => {
                    if (v39LastDetail) {
                        v39Telemetry(
                            v39LastDetail
                        );
                    }
                },
                10000
            );
    }


    v39LoadStationStats()
    .catch(
        e =>
            console.error(
                "V39 station stats:",
                e
            )
    );
}


// --------------------------------------------------------------
// Wrapper wykonuje V39 DOPIERO po zakończeniu starego showDetail.
// Dzięki temu istniejące karty Start/Recovery nie nadpisują V39.
// --------------------------------------------------------------

if (
    typeof showDetail
    ===
    "function"
    &&
    !window.__SHOW_DETAIL_WRAPPED_V39
) {

    window.__SHOW_DETAIL_WRAPPED_V39=true;

    const __showDetailBaseV39=
        showDetail;


    showDetail=
        async function(serial) {

            const result=
                await __showDetailBaseV39(
                    serial
                );


            let d=
                window.__SONDE_DETAIL_V39;


            if (
                !d
                ||
                String(d.serial || "")
                !==
                String(serial || "")
            ) {

                d=
                    await jget(
                        "/api/sonde?serial="
                        +
                        encodeURIComponent(serial)
                    );
            }


            await renderSondeFullV39(
                d
            );


            return result;
        };
}


// Statystyki stacji mogą załadować się niezależnie
// od wybrania konkretnej sondy.

document.addEventListener(
    "DOMContentLoaded",
    () => {

        setTimeout(
            () => {
                v39StationStatsHost();

                v39LoadStationStats()
                .catch(
                    e =>
                        console.error(
                            "V39 startup stats:",
                            e
                        )
                );
            },
            250
        );
    }
);

</script>

<!-- ALTITUDE_PROFILE_V3 -->

<style id="altitude-profile-v3-style">

#rwAltV3 {
    width: 100%;
    box-sizing: border-box;
    margin: 12px 0 14px;
    padding: 13px 15px 12px;

    background: #172431;
    border: 1px solid #37536b;
    border-radius: 9px;
}

#rwAltV3Head {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: 16px;
    margin-bottom: 9px;
}

#rwAltV3Title {
    color: #f0f5fa;
    font-size: 16px;
    font-weight: 800;
}

#rwAltV3Info {
    margin-top: 3px;
    color: #91a8bb;
    font-size: 11px;
}

#rwAltV3Max {
    flex: 0 0 auto;
    padding: 5px 9px;

    color: #bcdcff;
    background: #173149;
    border: 1px solid #3b6282;
    border-radius: 999px;

    font-size: 11px;
    font-weight: 700;
}

#rwAltV3Plot {
    position: relative;
    width: 100%;
    height: 285px;

    overflow: hidden;

    background: #0b151f;
    border: 1px solid #2a4053;
    border-radius: 7px;
}

#rwAltV3Canvas {
    display: block;
    width: 100%;
    height: 100%;
}

#rwAltV3Tip {
    position: absolute;
    z-index: 50;

    display: none;
    min-width: 210px;
    max-width: 290px;

    padding: 9px 11px;

    background: rgba(7, 14, 22, .97);
    border: 1px solid #527491;
    border-radius: 7px;

    color: #dce8f2;
    font-size: 11px;
    line-height: 1.45;

    box-shadow: 0 8px 24px rgba(0,0,0,.42);
    pointer-events: none;
}

.rwAltV3TipTitle {
    margin-bottom: 5px;
    color: #fff;
    font-weight: 800;
}

.rwAltV3TipName {
    color: #8fa7ba;
}

@media (max-width: 800px) {
    #rwAltV3Plot {
        height: 240px;
    }
}

</style>


<script id="altitude-profile-v3-script">

// ALTITUDE_PROFILE_V3

let rwAltV3Current = null;
let rwAltV3ResizeTimer = null;


function rwAltV3Number(v) {
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
}


function rwAltV3Date(v) {

    if (v === null || v === undefined || v === "") {
        return null;
    }

    if (typeof v === "number" && Number.isFinite(v)) {
        const d = new Date(v > 1e12 ? v : v * 1000);
        return Number.isNaN(d.getTime()) ? null : d;
    }

    const d = new Date(v);
    return Number.isNaN(d.getTime()) ? null : d;
}


function rwAltV3Escape(v) {
    return String(v ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;");
}


function rwAltV3Time(d) {

    if (!d) return "—";

    return d.toLocaleTimeString(
        "pl-PL",
        {
            hour: "2-digit",
            minute: "2-digit",
            second: "2-digit",
            hour12: false
        }
    );
}


function rwAltV3DateTime(d) {

    if (!d) return "—";

    return d.toLocaleString(
        "pl-PL",
        {
            year: "numeric",
            month: "2-digit",
            day: "2-digit",
            hour: "2-digit",
            minute: "2-digit",
            second: "2-digit",
            hour12: false
        }
    );
}


function rwAltV3Points(d) {

    const raw =
        (((d || {}).flight || {}).track) || [];

    if (!Array.isArray(raw)) {
        return [];
    }

    return raw
        .map((p, index) => {

            const alt =
                rwAltV3Number(
                    p.alt ??
                    p.altitude
                );

            const dt =
                rwAltV3Date(
                    p.datetime ??
                    p.time ??
                    p.timestamp ??
                    p.ts
                );

            if (alt === null || !dt) {
                return null;
            }

            return {
                raw: p,
                index: index,
                alt: alt,
                dt: dt,
                ms: dt.getTime()
            };
        })
        .filter(Boolean)
        .sort((a, b) => a.ms - b.ms);
}


function rwAltV3Ensure() {

    let host =
        document.getElementById("rwAltV3");

    if (host) {
        return host;
    }

    const mapEl =
        document.getElementById("map");

    if (!mapEl) {
        return null;
    }

    host =
        document.createElement("section");

    host.id = "rwAltV3";

    host.innerHTML = `
<div id="rwAltV3Head">

    <div>
        <div id="rwAltV3Title">
            📈 Profil wysokości lotu
        </div>

        <div id="rwAltV3Info">
            Oś X — czas • Oś Y — wysokość
        </div>
    </div>

    <div id="rwAltV3Max">
        —
    </div>

</div>

<div id="rwAltV3Plot">
    <canvas id="rwAltV3Canvas"></canvas>
    <div id="rwAltV3Tip"></div>
</div>
`;

    mapEl.parentNode.insertBefore(
        host,
        mapEl
    );

    return host;
}


function rwAltV3TipHtml(point) {

    const p = point.raw || {};
    const rows = [];

    function add(name, value) {

        if (
            value === null ||
            value === undefined ||
            value === "" ||
            value === "—"
        ) {
            return;
        }

        rows.push(
            `<div><span class="rwAltV3TipName">${
                rwAltV3Escape(name)
            }:</span> ${
                rwAltV3Escape(value)
            }</div>`
        );
    }


    add(
        "Wysokość",
        Math.round(point.alt) + " m"
    );


    const vv =
        rwAltV3Number(
            p.vel_v ??
            p.ascent_rate
        );

    if (vv !== null) {
        add(
            "Prędkość pionowa",
            (vv >= 0 ? "+" : "") +
            vv.toFixed(2) +
            " m/s"
        );
    }


    const vh =
        rwAltV3Number(
            p.vel_h ??
            p.speed ??
            p.ground_speed
        );

    if (vh !== null) {
        add(
            "Prędkość pozioma",
            vh.toFixed(1) + " m/s"
        );
    }


    const heading =
        rwAltV3Number(p.heading);

    if (heading !== null) {
        add(
            "Kierunek",
            heading.toFixed(1) + "°"
        );
    }


    const temp =
        rwAltV3Number(
            p.temp ??
            p.temperature
        );

    if (temp !== null) {
        add(
            "Temperatura",
            temp.toFixed(1) + " °C"
        );
    }


    const humidity =
        rwAltV3Number(p.humidity);

    if (humidity !== null) {
        add(
            "Wilgotność",
            humidity.toFixed(1) + " %"
        );
    }


    const pressure =
        rwAltV3Number(p.pressure);

    if (pressure !== null) {
        add(
            "Ciśnienie",
            pressure.toFixed(1) + " hPa"
        );
    }


    const sats =
        rwAltV3Number(
            p.sats ??
            p.satellites
        );

    if (sats !== null) {
        add(
            "Satelity GPS",
            Math.round(sats)
        );
    }


    const batt =
        rwAltV3Number(
            p.batt ??
            p.battery ??
            p.battery_v
        );

    if (batt !== null) {
        add(
            "Bateria",
            batt.toFixed(2) + " V"
        );
    }


    const snr =
        rwAltV3Number(p.snr);

    if (snr !== null) {
        add(
            "SNR",
            snr.toFixed(1) + " dB"
        );
    }


    const rssi =
        rwAltV3Number(p.rssi);

    if (rssi !== null) {
        add(
            "RSSI",
            rssi.toFixed(1) + " dBm"
        );
    }


    const freq =
        rwAltV3Number(
            p.frequency ??
            p.freq_mhz
        );

    if (freq !== null) {
        add(
            "Częstotliwość",
            freq.toFixed(4) + " MHz"
        );
    }


    const lat =
        rwAltV3Number(
            p.lat ??
            p.latitude
        );

    const lon =
        rwAltV3Number(
            p.lon ??
            p.longitude
        );

    if (lat !== null && lon !== null) {
        add(
            "Pozycja",
            lat.toFixed(5) +
            ", " +
            lon.toFixed(5)
        );
    }


    add(
        "Uploader",
        p.uploader_callsign ??
        p.uploader
    );


    if (p.frame !== undefined) {
        add(
            "Ramka",
            p.frame
        );
    }


    return `
<div class="rwAltV3TipTitle">
    ${rwAltV3Escape(
        rwAltV3DateTime(point.dt)
    )}
</div>

${rows.join("")}
`;
}


function rwAltV3Nearest(points, ms) {

    let lo = 0;
    let hi = points.length - 1;

    while (lo < hi) {

        const mid =
            Math.floor(
                (lo + hi) / 2
            );

        if (points[mid].ms < ms) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }

    if (lo === 0) {
        return 0;
    }

    const prev = lo - 1;

    return (
        Math.abs(points[lo].ms - ms) <
        Math.abs(points[prev].ms - ms)
    )
        ? lo
        : prev;
}


function rwAltV3Render(d) {

    rwAltV3Current = d;

    const host =
        rwAltV3Ensure();

    if (!host) {
        return;
    }

    const points =
        rwAltV3Points(d);

    const canvas =
        document.getElementById(
            "rwAltV3Canvas"
        );

    const plot =
        document.getElementById(
            "rwAltV3Plot"
        );

    const tip =
        document.getElementById(
            "rwAltV3Tip"
        );

    const info =
        document.getElementById(
            "rwAltV3Info"
        );

    const maxBadge =
        document.getElementById(
            "rwAltV3Max"
        );


    if (
        !canvas ||
        !plot ||
        !info ||
        !maxBadge
    ) {
        return;
    }


    if (points.length < 2) {

        info.textContent =
            "Brak wystarczającej historii do narysowania wykresu.";

        maxBadge.textContent =
            "brak danych";

        return;
    }


    const minAlt =
        Math.min(
            ...points.map(x => x.alt)
        );

    const maxAlt =
        Math.max(
            ...points.map(x => x.alt)
        );


    info.textContent =
        `${d.serial || "sonda"} • ${
            points.length
        } punktów • ${
            rwAltV3Time(points[0].dt)
        }–${
            rwAltV3Time(
                points[
                    points.length - 1
                ].dt
            )
        }`;


    maxBadge.textContent =
        `max ${Math.round(maxAlt)} m`;


    const rect =
        plot.getBoundingClientRect();


    const W =
        Math.max(
            500,
            Math.round(rect.width)
        );

    const H =
        Math.max(
            220,
            Math.round(rect.height)
        );


    const DPR =
        Math.max(
            1,
            window.devicePixelRatio || 1
        );


    canvas.width =
        Math.round(W * DPR);

    canvas.height =
        Math.round(H * DPR);


    const ctx =
        canvas.getContext("2d");


    ctx.setTransform(
        DPR, 0,
        0, DPR,
        0, 0
    );


    const left = 65;
    const right = 20;
    const top = 18;
    const bottom = 39;

    const PW = W - left - right;
    const PH = H - top - bottom;


    const tMin =
        points[0].ms;

    const tMax =
        points[
            points.length - 1
        ].ms;


    const yMin =
        Math.min(
            0,
            Math.floor(
                minAlt / 1000
            ) * 1000
        );


    let yMax =
        Math.ceil(
            maxAlt / 1000
        ) * 1000;


    if (yMax <= yMin) {
        yMax = yMin + 1000;
    }


    const xFor = ms =>
        left +
        ((ms - tMin) /
        Math.max(1, tMax - tMin)) *
        PW;


    const yFor = alt =>
        top +
        PH -
        ((alt - yMin) /
        (yMax - yMin)) *
        PH;


    /*
      Rysujemy najwyżej około 1800 punktów.
      Tooltip nadal korzysta z pełnej historii.
    */

    const step =
        Math.max(
            1,
            Math.ceil(
                points.length / 1800
            )
        );


    const drawPoints = [];

    for (
        let i = 0;
        i < points.length;
        i += step
    ) {
        drawPoints.push(points[i]);
    }


    if (
        drawPoints[
            drawPoints.length - 1
        ] !==
        points[
            points.length - 1
        ]
    ) {
        drawPoints.push(
            points[
                points.length - 1
            ]
        );
    }


    function paint(hoverIndex = null) {

        ctx.clearRect(
            0, 0,
            W, H
        );


        ctx.fillStyle = "#0b151f";

        ctx.fillRect(
            0, 0,
            W, H
        );


        ctx.font =
            "11px system-ui, -apple-system, sans-serif";


        /*
          OŚ Y
        */

        for (let i = 0; i <= 5; i++) {

            const value =
                yMin +
                (yMax - yMin) *
                i / 5;

            const y =
                yFor(value);


            ctx.strokeStyle =
                "rgba(130,160,190,.15)";

            ctx.lineWidth = 1;

            ctx.beginPath();

            ctx.moveTo(left, y);

            ctx.lineTo(
                W - right,
                y
            );

            ctx.stroke();


            ctx.fillStyle =
                "#8fa7ba";

            ctx.textAlign =
                "right";

            ctx.textBaseline =
                "middle";

            ctx.fillText(
                Math.round(value)
                    .toLocaleString("pl-PL"),
                left - 8,
                y
            );
        }


        /*
          OŚ X
        */

        for (let i = 0; i <= 5; i++) {

            const ms =
                tMin +
                (tMax - tMin) *
                i / 5;

            const x =
                xFor(ms);


            ctx.strokeStyle =
                "rgba(130,160,190,.10)";

            ctx.beginPath();

            ctx.moveTo(
                x,
                top
            );

            ctx.lineTo(
                x,
                H - bottom
            );

            ctx.stroke();


            ctx.fillStyle =
                "#8fa7ba";

            ctx.textAlign =
                "center";

            ctx.textBaseline =
                "top";

            ctx.fillText(
                rwAltV3Time(
                    new Date(ms)
                ),
                x,
                H - bottom + 9
            );
        }


        /*
          OPIS Y
        */

        ctx.save();

        ctx.translate(
            14,
            top + PH / 2
        );

        ctx.rotate(
            -Math.PI / 2
        );

        ctx.fillStyle =
            "#a9bfd1";

        ctx.textAlign =
            "center";

        ctx.fillText(
            "Wysokość [m]",
            0,
            0
        );

        ctx.restore();


        /*
          WYPEŁNIENIE
        */

        ctx.beginPath();

        drawPoints.forEach(
            (p, i) => {

                const x = xFor(p.ms);
                const y = yFor(p.alt);

                if (i === 0) {
                    ctx.moveTo(x, y);
                } else {
                    ctx.lineTo(x, y);
                }
            }
        );


        ctx.lineTo(
            xFor(
                drawPoints[
                    drawPoints.length - 1
                ].ms
            ),
            yFor(yMin)
        );

        ctx.lineTo(
            xFor(
                drawPoints[0].ms
            ),
            yFor(yMin)
        );

        ctx.closePath();


        const gradient =
            ctx.createLinearGradient(
                0,
                top,
                0,
                H - bottom
            );

        gradient.addColorStop(
            0,
            "rgba(73,162,239,.31)"
        );

        gradient.addColorStop(
            1,
            "rgba(73,162,239,.02)"
        );

        ctx.fillStyle =
            gradient;

        ctx.fill();


        /*
          LINIA
        */

        ctx.beginPath();

        drawPoints.forEach(
            (p, i) => {

                const x = xFor(p.ms);
                const y = yFor(p.alt);

                if (i === 0) {
                    ctx.moveTo(x, y);
                } else {
                    ctx.lineTo(x, y);
                }
            }
        );

        ctx.strokeStyle =
            "#54aafa";

        ctx.lineWidth =
            2.2;

        ctx.lineJoin =
            "round";

        ctx.lineCap =
            "round";

        ctx.stroke();


        /*
          PUNKT POD KURSOREM
        */

        if (
            hoverIndex !== null &&
            points[hoverIndex]
        ) {

            const p =
                points[hoverIndex];

            const x =
                xFor(p.ms);

            const y =
                yFor(p.alt);


            ctx.strokeStyle =
                "rgba(255,255,255,.30)";

            ctx.lineWidth = 1;

            ctx.beginPath();

            ctx.moveTo(x, top);

            ctx.lineTo(
                x,
                H - bottom
            );

            ctx.stroke();


            ctx.beginPath();

            ctx.arc(
                x, y,
                5,
                0,
                Math.PI * 2
            );

            ctx.fillStyle =
                "#f7fbff";

            ctx.fill();


            ctx.beginPath();

            ctx.arc(
                x, y,
                3,
                0,
                Math.PI * 2
            );

            ctx.fillStyle =
                "#46a8fa";

            ctx.fill();
        }
    }


    paint();


    canvas.onmousemove =
        function(ev) {

            const r =
                canvas.getBoundingClientRect();

            const mouseX =
                ev.clientX - r.left;


            if (
                mouseX < left ||
                mouseX > W - right
            ) {

                tip.style.display =
                    "none";

                paint();

                return;
            }


            const ratio =
                Math.max(
                    0,
                    Math.min(
                        1,
                        (mouseX - left) /
                        PW
                    )
                );


            const target =
                tMin +
                ratio *
                (tMax - tMin);


            const index =
                rwAltV3Nearest(
                    points,
                    target
                );


            const point =
                points[index];


            paint(index);


            tip.innerHTML =
                rwAltV3TipHtml(point);

            tip.style.display =
                "block";


            const desiredX =
                mouseX + 14;


            const maxX =
                W -
                tip.offsetWidth -
                8;


            tip.style.left =
                Math.max(
                    8,
                    Math.min(
                        desiredX,
                        maxX
                    )
                ) + "px";


            const pointY =
                yFor(point.alt);


            const desiredY =
                pointY -
                tip.offsetHeight / 2;


            const maxY =
                H -
                tip.offsetHeight -
                8;


            tip.style.top =
                Math.max(
                    8,
                    Math.min(
                        desiredY,
                        maxY
                    )
                ) + "px";
        };


    canvas.onmouseleave =
        function() {

            tip.style.display =
                "none";

            paint();
        };
}


/*
  Jeżeli zmienisz wielkość okna,
  przerysuj wykres.
*/

window.addEventListener(
    "resize",
    function() {

        clearTimeout(
            rwAltV3ResizeTimer
        );

        rwAltV3ResizeTimer =
            setTimeout(
                function() {

                    if (
                        rwAltV3Current
                    ) {
                        rwAltV3Render(
                            rwAltV3Current
                        );
                    }

                },
                150
            );
    }
);

</script>


<!-- SONDEHUB_TOOLTIP_STATUS_V4 -->

<script id="sondehub-tooltip-status-v4">

// ============================================================
// SONDEHUB_TOOLTIP_STATUS_V4
// ============================================================

let rwAltV4LocalSamples=[];


/* ------------------------------------------------------------
   CZAS PRÓBKI LOKALNEJ
------------------------------------------------------------ */

function rwAltV4SampleDate(s){

    if(!s){
        return null;
    }

    if(
        s.datetime
        ||
        s.time
        ||
        s.timestamp
    ){
        return rwAltV3Date(
            s.datetime
            ??
            s.time
            ??
            s.timestamp
        );
    }


    if(
        s._epoch !== undefined
        &&
        s._epoch !== null
    ){
        return rwAltV3Date(
            Number(s._epoch)
        );
    }


    return null;
}


/* ------------------------------------------------------------
   NAJBLIŻSZA LOKALNA RAMKA DO PUNKTU WYKRESU
------------------------------------------------------------ */

function rwAltV4NearestLocal(point){

    if(
        !point
        ||
        !point.dt
        ||
        !Array.isArray(rwAltV4LocalSamples)
        ||
        !rwAltV4LocalSamples.length
    ){
        return null;
    }


    const target=
        point.dt.getTime();


    let best=null;
    let bestDelta=Infinity;


    for(
        const sample
        of
        rwAltV4LocalSamples
    ){

        const dt=
            rwAltV4SampleDate(
                sample
            );


        if(!dt){
            continue;
        }


        const delta=
            Math.abs(
                dt.getTime()
                -
                target
            );


        if(delta < bestDelta){

            bestDelta=delta;
            best=sample;
        }
    }


    /*
      Nie przypisujemy przypadkowej ramki z dużej luki.
      30 sekund wystarcza dla normalnego auto_rx.
    */

    if(bestDelta > 30000){
        return null;
    }


    return best;
}


/* ------------------------------------------------------------
   FORMAT
------------------------------------------------------------ */

function rwAltV4Value(...values){

    for(const v of values){

        if(
            v !== null
            &&
            v !== undefined
            &&
            v !== ""
        ){
            const n=Number(v);

            if(Number.isFinite(n)){
                return n;
            }
        }
    }

    return null;
}


function rwAltV4MeteoLine(
    name,
    value,
    suffix
){

    const text=
        value === null
        ?
        "—"
        :
        Number(value).toFixed(1)
        +
        suffix;


    return `
<div>
  <span class="rwAltV3TipName">
    ${rwAltV3Escape(name)}:
  </span>
  ${rwAltV3Escape(text)}
</div>`;
}


/* ------------------------------------------------------------
   ROZSZERZENIE ISTNIEJĄCEGO TOOLTIPA V3
------------------------------------------------------------ */

const rwAltV4BaseTipHtml=
    rwAltV3TipHtml;


rwAltV3TipHtml=
function(point){

    let html=
        rwAltV4BaseTipHtml(
            point
        );


    const raw=
        point.raw
        ||
        {};


    const local=
        rwAltV4NearestLocal(
            point
        );


    const temp=
        rwAltV4Value(

            raw.temp,
            raw.temperature,

            local
            &&
            local.temp,

            local
            &&
            local.temperature
        );


    const humidity=
        rwAltV4Value(

            raw.humidity,

            local
            &&
            local.humidity
        );


    const pressure=
        rwAltV4Value(

            raw.pressure,

            local
            &&
            local.pressure
        );


    /*
      Jeśli V3 już dostał parametr bezpośrednio
      z SondeHub, nie dublujemy go.
      Jeśli nie — dokładamy wartość z lokalnego auto_rx.
    */


    if(
        !html.includes(
            "Temperatura"
        )
    ){
        html +=
            rwAltV4MeteoLine(
                "Temperatura",
                temp,
                " °C"
            );
    }


    if(
        !html.includes(
            "Wilgotność"
        )
    ){
        html +=
            rwAltV4MeteoLine(
                "Wilgotność",
                humidity,
                " %"
            );
    }


    if(
        !html.includes(
            "Ciśnienie"
        )
    ){
        html +=
            rwAltV4MeteoLine(
                "Ciśnienie",
                pressure,
                " hPa"
            );
    }


    return html;
};


/* ------------------------------------------------------------
   POBIERANIE LOKALNYCH RAMEK DLA WYBRANEJ SONDY

   Nie zmieniamy oryginalnego API /api/sonde.
   Pobieramy równolegle istniejące /api/local-flight.
------------------------------------------------------------ */

const rwAltV4BaseShowDetail=
    showDetail;


showDetail=
async function(serial){

    rwAltV4LocalSamples=[];


    const localPromise=
        jget(
            "/api/local-flight?serial="
            +
            encodeURIComponent(
                serial
            )
        )
        .catch(
            ()=>null
        );


    let result;


    try{

        result=
            await rwAltV4BaseShowDetail
            .apply(
                this,
                arguments
            );

    }finally{

        try{

            const localData=
                await localPromise;


            if(
                localData
                &&
                Array.isArray(
                    localData.samples
                )
                &&
                String(
                    selectedSerial
                )
                ===
                String(
                    serial
                )
            ){

                rwAltV4LocalSamples=
                    localData.samples;


                /*
                  Przerysowanie tylko tooltipów / wykresu.
                  Mapa i przyciski pozostają bez zmian.
                */

                if(
                    typeof rwAltV3Current
                    !==
                    "undefined"
                    &&
                    rwAltV3Current
                    &&
                    String(
                        rwAltV3Current.serial
                    )
                    ===
                    String(
                        serial
                    )
                ){

                    rwAltV3Render(
                        rwAltV3Current
                    );
                }
            }

        }catch(_){}
    }


    return result;
};


/* ------------------------------------------------------------
   CZYTELNY STATUS RTL-SDR

   API nadal zachowuje pełny wyjątek do diagnostyki.
   W UI nie pokazujemy ściany kodu requests/urllib3.
------------------------------------------------------------ */

function rwStatusCleanV4(){

    const cards=
        [
            ...document
            .querySelectorAll(
                ".card"
            )
        ];


    const card=
        cards.find(
            el => {

                const txt=
                    String(
                        el.textContent
                        ||
                        ""
                    );

                return (
                    txt.includes(
                        "RTL-SDR"
                    )
                    &&
                    txt.includes(
                        "aktualne użycie"
                    )
                );
            }
        );


    if(!card){
        return;
    }


    const walker=
        document.createTreeWalker(
            card,
            NodeFilter.SHOW_TEXT
        );


    const bad=
        /HTTPSConnectionPool|HTTPConnectionPool|Read timed out|ConnectTimeout|ConnectionError|MaxRetryError|network\.satnogs\.org|observation\.next:|urllib3|requests\.exceptions/i;


    let node;


    while(
        (
            node=
            walker.nextNode()
        )
    ){

        const text=
            String(
                node.nodeValue
                ||
                ""
            );


        if(
            bad.test(
                text
            )
        ){

            node.nodeValue=
                "Brak odpowiedzi API SatNOGS — harmonogram chwilowo niedostępny.";
        }
    }
}


/*
  Czyścimy od razu i po kolejnych aktualizacjach statusu.
*/

setTimeout(
    rwStatusCleanV4,
    0
);

setTimeout(
    rwStatusCleanV4,
    500
);

setInterval(
    rwStatusCleanV4,
    3000
);

</script>



<script id="REMOVE_STATION_HISTORY_DOM_V3">
/* REMOVE_STATION_HISTORY_DOM_V3 */

(function(){

    let observer=null;


    function removeHistoryV3(){

        const site=
            document.getElementById("site");

        if(!site){
            return;
        }


        /*
          Szukamy TEKSTU, a nie konkretnego typu tagu.
          Dzięki temu działa dla:
          button / a / span / div / role=button itd.
        */

        const walker=
            document.createTreeWalker(
                site,
                NodeFilter.SHOW_TEXT
            );


        const hits=[];

        let node;


        while(
            (
                node=
                walker.nextNode()
            )
        ){

            const txt=
                String(
                    node.nodeValue || ""
                )
                .replace(/\s+/g," ")
                .trim()
                .toLowerCase();


            if(
                txt.includes(
                    "historia tej stacji"
                )
            ){
                hits.push(node);
            }
        }


        for(const textNode of hits){

            const el=
                textNode.parentElement;

            if(!el){
                continue;
            }


            /*
              Usuwamy najbliższy element klikalny.
              Nie usuwamy całej karty #site.
            */

            const clickable=
                el.closest(
                    'button,' +
                    'a,' +
                    '[role="button"],' +
                    '[onclick],' +
                    '.btn'
                );


            if(
                clickable
                &&
                clickable !== site
                &&
                site.contains(clickable)
            ){

                clickable.remove();

            }else{

                /*
                  Awaryjnie usuwamy tylko najmniejszy
                  element zawierający tekst.
                */

                if(
                    el !== site
                    &&
                    site.contains(el)
                ){
                    el.remove();
                }
            }
        }
    }


    function installHistoryV3(){

        const site=
            document.getElementById("site");


        if(!site){

            setTimeout(
                installHistoryV3,
                250
            );

            return;
        }


        removeHistoryV3();


        observer=
            new MutationObserver(
                function(){
                    removeHistoryV3();
                }
            );


        observer.observe(
            site,
            {
                childList:true,
                subtree:true,
                characterData:true
            }
        );
    }


    if(
        document.readyState === "loading"
    ){

        document.addEventListener(
            "DOMContentLoaded",
            installHistoryV3
        );

    }else{

        installHistoryV3();
    }

})();
</script>


<!-- OUR_RECEPTION_V2 -->

<style id="our-reception-v2-style">

.ourrx-v2-grid{
    display:grid;
    grid-template-columns:
        repeat(2,minmax(0,1fr));
    gap:7px;
    margin-top:8px;
}

.ourrx-v2-item{
    min-width:0;
    padding:7px 8px;

    border:1px solid rgba(86,116,143,.35);
    border-radius:6px;

    background:
        rgba(12,24,35,.42);
}

.ourrx-v2-wide{
    grid-column:1 / -1;
}

.ourrx-v2-label{
    margin-bottom:2px;

    color:#91a8bb;

    font-size:10px;
    line-height:1.2;
}

.ourrx-v2-value{
    overflow:hidden;

    color:#eef5fb;

    font-size:13px;
    line-height:1.25;
    font-weight:700;

    text-overflow:ellipsis;
    white-space:nowrap;
}

.ourrx-v2-good{
    color:#7ddc9a;
}

.ourrx-v2-info{
    color:#75baff;
}

.ourrx-v2-muted{
    color:#aebdcb;
}

.ourrx-v2-note{
    grid-column:1 / -1;

    margin-top:1px;

    color:#778ea2;

    font-size:9px;
    line-height:1.35;
}

@media(max-width:850px){

    .ourrx-v2-grid{
        grid-template-columns:
            repeat(2,minmax(0,1fr));
    }
}

</style>


<script id="our-reception-v2-script">

/* ============================================================
   OUR_RECEPTION_V2
   Analiza rzeczywistego odbioru przez Stacja lokalna
   ============================================================ */


function ourRxV2Number(v){

    const n=Number(v);

    return Number.isFinite(n)
        ?
        n
        :
        null;
}


function ourRxV2Date(v){

    if(
        v===null
        ||
        v===undefined
        ||
        v===""
    ){
        return null;
    }


    if(
        typeof v==="number"
        &&
        Number.isFinite(v)
    ){

        const d=
            new Date(
                v>1e12
                ?
                v
                :
                v*1000
            );

        return Number.isNaN(
            d.getTime()
        )
        ?
        null
        :
        d;
    }


    const d=new Date(v);

    return Number.isNaN(
        d.getTime()
    )
    ?
    null
    :
    d;
}


function ourRxV2PointDate(p){

    if(!p){
        return null;
    }


    return ourRxV2Date(
        p.datetime
        ??
        p.time
        ??
        p.timestamp
        ??
        p.ts
        ??
        p._epoch
    );
}


function ourRxV2Time(d){

    if(!d){
        return "—";
    }


    return d.toLocaleString(
        "pl-PL",
        {
            day:"2-digit",
            month:"2-digit",
            hour:"2-digit",
            minute:"2-digit",
            second:"2-digit",
            hour12:false
        }
    );
}


function ourRxV2Duration(sec){

    sec=ourRxV2Number(sec);

    if(
        sec===null
        ||
        sec<0
    ){
        return "—";
    }


    sec=Math.round(sec);


    const h=
        Math.floor(
            sec/3600
        );

    const m=
        Math.floor(
            (
                sec%3600
            )
            /
            60
        );

    const s=
        sec%60;


    const out=[];


    if(h){
        out.push(
            h+" h"
        );
    }


    if(
        m
        ||
        h
    ){
        out.push(
            m+" min"
        );
    }


    out.push(
        s+" s"
    );


    return out.join(" ");
}


function ourRxV2Metric(
    label,
    value,
    css=""
){

    return `
<div class="ourrx-v2-item">

  <div class="ourrx-v2-label">
    ${esc(label)}
  </div>

  <div class="ourrx-v2-value ${css}">
    ${esc(
        value===null
        ||
        value===undefined
        ||
        value===""
        ?
        "—"
        :
        value
    )}
  </div>

</div>`;
}


function ourRxV2Wide(
    label,
    value
){

    return `
<div class="ourrx-v2-item ourrx-v2-wide">

  <div class="ourrx-v2-label">
    ${esc(label)}
  </div>

  <div class="ourrx-v2-value">
    ${esc(value || "—")}
  </div>

</div>`;
}


/* ------------------------------------------------------------
   Znajdź istniejący trzeci kafelek V38.
------------------------------------------------------------ */

function ourRxV2Card(){

    const host=
        document.getElementById(
            "sondeAnalyticsV38"
        );


    if(!host){
        return null;
    }


    const cards=[
        ...host.querySelectorAll(
            ".sav38-card"
        )
    ];


    return (
        cards.find(
            card =>
                String(
                    card.textContent
                    ||
                    ""
                )
                .toUpperCase()
                .includes(
                    "NASZ ODBIÓR"
                )
        )
        ||
        cards[2]
        ||
        null
    );
}


/* ------------------------------------------------------------
   Największa luka oraz najdłuższy ciąg odbioru.

   Za ciągły odbiór uznajemy kolejne ramki,
   między którymi nie ma przerwy >10 s.
------------------------------------------------------------ */

function ourRxV2Continuity(times){

    if(
        !Array.isArray(times)
        ||
        times.length<2
    ){
        return {
            maxGap:0,
            longest:0
        };
    }


    let maxGap=0;

    let segmentStart=
        times[0];

    let longest=0;


    for(
        let i=1;
        i<times.length;
        i++
    ){

        const gap=
            (
                times[i]
                -
                times[i-1]
            )
            /
            1000;


        if(gap>maxGap){
            maxGap=gap;
        }


        if(gap>10){

            const duration=
                (
                    times[i-1]
                    -
                    segmentStart
                )
                /
                1000;


            if(duration>longest){
                longest=duration;
            }


            segmentStart=
                times[i];
        }
    }


    const finalDuration=
        (
            times[
                times.length-1
            ]
            -
            segmentStart
        )
        /
        1000;


    if(finalDuration>longest){
        longest=finalDuration;
    }


    return {
        maxGap:maxGap,
        longest:longest
    };
}


/* ------------------------------------------------------------
   Pokrycie czasowe lotu.

   Nie porównujemy liczby ramek SondeHub z auto_rx,
   bo źródła mają różne częstotliwości próbkowania.

   Dlatego pokazujemy uczciwie:
   czas pomiędzy pierwszą i ostatnią lokalną ramką
   / czas historii lotu SondeHub.
------------------------------------------------------------ */

function ourRxV2Coverage(
    d,
    localFirst,
    localLast
){

    const track=
        (
            (
                d
                &&
                d.flight
            )
            ||
            {}
        ).track
        ||
        [];


    const flightTimes=
        track
        .map(
            p =>
                ourRxV2PointDate(p)
        )
        .filter(Boolean)
        .map(
            d =>
                d.getTime()
        )
        .sort(
            (a,b)=>
                a-b
        );


    if(
        flightTimes.length<2
        ||
        !localFirst
        ||
        !localLast
    ){
        return null;
    }


    const flightDuration=
        (
            flightTimes[
                flightTimes.length-1
            ]
            -
            flightTimes[0]
        )
        /
        1000;


    const localDuration=
        (
            localLast.getTime()
            -
            localFirst.getTime()
        )
        /
        1000;


    if(
        flightDuration<=0
        ||
        localDuration<0
    ){
        return null;
    }


    return Math.max(
        0,
        Math.min(
            100,
            localDuration
            /
            flightDuration
            *
            100
        )
    );
}


/* ------------------------------------------------------------
   Maksymalna odległość od Stacja lokalna.
------------------------------------------------------------ */

function ourRxV2MaxDistance(rows){

    if(
        typeof HOME==="undefined"
        ||
        !HOME
    ){
        return null;
    }


    let result=null;


    for(const row of rows){

        const lat=
            ourRxV2Number(
                row.lat
            );

        const lon=
            ourRxV2Number(
                row.lon
            );


        if(
            lat===null
            ||
            lon===null
        ){
            continue;
        }


        let dist=null;


        if(
            typeof saDistanceV38
            ===
            "function"
        ){

            dist=
                saDistanceV38(
                    HOME.lat,
                    HOME.lon,
                    lat,
                    lon
                );

        }else{

            /*
             * Fallback Haversine.
             */

            const aLat=
                ourRxV2Number(
                    HOME.lat
                );

            const aLon=
                ourRxV2Number(
                    HOME.lon
                );


            if(
                aLat===null
                ||
                aLon===null
            ){
                continue;
            }


            const rad=
                x =>
                    x
                    *
                    Math.PI
                    /
                    180;


            const p1=
                rad(aLat);

            const p2=
                rad(lat);

            const dp=
                rad(
                    lat-aLat
                );

            const dl=
                rad(
                    lon-aLon
                );


            const a=
                Math.sin(dp/2)
                **
                2
                +
                Math.cos(p1)
                *
                Math.cos(p2)
                *
                Math.sin(dl/2)
                **
                2;


            dist=
                6371
                *
                2
                *
                Math.atan2(
                    Math.sqrt(a),
                    Math.sqrt(1-a)
                );
        }


        dist=
            ourRxV2Number(
                dist
            );


        if(
            dist!==null
            &&
            (
                result===null
                ||
                dist>result
            )
        ){
            result=dist;
        }
    }


    return result;
}


/* ============================================================
   GŁÓWNA FUNKCJA
   ============================================================ */

async function renderOurReceptionV2(d){

    const card=
        ourRxV2Card();


    if(!card){
        return;
    }


    const serial=
        String(
            (
                d
                &&
                d.serial
            )
            ||
            ""
        );


    if(!serial){
        return;
    }


    card.innerHTML=`
<div class="sav38-title">
  📡 NASZ ODBIÓR
</div>

<div class="sav38-muted">
  Analiza lokalnych ramek Stacja lokalna…
</div>
`;


    try{

        const response=
            await fetch(
                "/api/local-flight?serial="
                +
                encodeURIComponent(
                    serial
                ),
                {
                    cache:"no-store"
                }
            );


        if(!response.ok){

            throw new Error(
                "HTTP "
                +
                response.status
            );
        }


        const data=
            await response.json();


        /*
         * Użytkownik mógł w międzyczasie kliknąć inną sondę.
         */

        if(
            String(
                selectedSerial
            )
            !==
            serial
        ){
            return;
        }


        const rows=
            Array.isArray(
                data.samples
            )
            ?
            data.samples
            :
            [];


        if(!rows.length){

            card.innerHTML=`
<div class="sav38-title">
  📡 NASZ ODBIÓR
</div>

<div class="sav38-muted">
  Brak lokalnych ramek Stacja lokalna dla tej sondy.
</div>
`;

            return;
        }


        const normalized=
            rows
            .map(
                row => {

                    const dt=
                        ourRxV2PointDate(
                            row
                        );


                    return {
                        row:row,
                        dt:dt,
                        ms:
                            dt
                            ?
                            dt.getTime()
                            :
                            null
                    };
                }
            )
            .filter(
                x =>
                    x.ms!==null
            )
            .sort(
                (a,b)=>
                    a.ms-b.ms
            );


        if(!normalized.length){

            throw new Error(
                "brak poprawnych timestampów"
            );
        }


        const first=
            normalized[0];

        const last=
            normalized[
                normalized.length-1
            ];


        const duration=
            (
                last.ms
                -
                first.ms
            )
            /
            1000;


        /*
         * SNR
         */

        const snrs=
            normalized
            .map(
                x =>
                    ourRxV2Number(
                        x.row.snr
                        ??
                        x.row.snr_db
                    )
            )
            .filter(
                x =>
                    x!==null
                    &&
                    x>-90
            );


        const avgSnr=
            snrs.length
            ?
            snrs.reduce(
                (a,b)=>
                    a+b,
                0
            )
            /
            snrs.length
            :
            null;


        const maxSnr=
            snrs.length
            ?
            Math.max(
                ...snrs
            )
            :
            null;


        /*
         * Minimalna odebrana wysokość.
         */

        const alts=
            normalized
            .map(
                x =>
                    ourRxV2Number(
                        x.row.alt
                    )
            )
            .filter(
                x =>
                    x!==null
            );


        const minAlt=
            alts.length
            ?
            Math.min(
                ...alts
            )
            :
            null;


        /*
         * Ciągłość.
         */

        const continuity=
            ourRxV2Continuity(
                normalized.map(
                    x =>
                        x.ms
                )
            );


        /*
         * Dystans.
         */

        const maxDistance=
            ourRxV2MaxDistance(
                normalized.map(
                    x =>
                        x.row
                )
            );


        /*
         * Pokrycie czasowe.
         */

        const coverage=
            ourRxV2Coverage(
                d,
                first.dt,
                last.dt
            );


        /*
         * RENDER.
         */

        card.innerHTML=`
<div class="sav38-title">
  📡 NASZ ODBIÓR
</div>

<div class="ourrx-v2-grid">

  ${
    ourRxV2Metric(
        "Lokalne ramki",
        normalized.length
            .toLocaleString(
                "pl-PL"
            ),
        "ourrx-v2-good"
    )
  }

  ${
    ourRxV2Metric(
        "Czas odbioru",
        ourRxV2Duration(
            duration
        )
    )
  }

  ${
    ourRxV2Wide(
        "Pierwsza lokalna ramka",
        ourRxV2Time(
            first.dt
        )
    )
  }

  ${
    ourRxV2Wide(
        "Ostatnia lokalna ramka",
        ourRxV2Time(
            last.dt
        )
    )
  }

  ${
    ourRxV2Metric(
        "Średni SNR",
        avgSnr===null
            ?
            "—"
            :
            avgSnr.toFixed(1)
            +
            " dB",
        "ourrx-v2-info"
    )
  }

  ${
    ourRxV2Metric(
        "Maksymalny SNR",
        maxSnr===null
            ?
            "—"
            :
            maxSnr.toFixed(1)
            +
            " dB",
        "ourrx-v2-good"
    )
  }

  ${
    ourRxV2Metric(
        "Maks. dystans",
        maxDistance===null
            ?
            "—"
            :
            maxDistance.toFixed(1)
            +
            " km"
    )
  }

  ${
    ourRxV2Metric(
        "Min. wysokość",
        minAlt===null
            ?
            "—"
            :
            Math.round(
                minAlt
            )
            .toLocaleString(
                "pl-PL"
            )
            +
            " m"
    )
  }

  ${
    ourRxV2Metric(
        "Najdłuższy ciąg odbioru",
        ourRxV2Duration(
            continuity.longest
        )
    )
  }

  ${
    ourRxV2Metric(
        "Największa przerwa",
        ourRxV2Duration(
            continuity.maxGap
        )
    )
  }

  ${
    ourRxV2Metric(
        "Pokrycie czasu lotu",
        coverage===null
            ?
            "—"
            :
            coverage.toFixed(1)
            +
            " %",
        coverage!==null
        &&
        coverage>=50
            ?
            "ourrx-v2-good"
            :
            ""
    )
  }

  ${
    ourRxV2Metric(
        "Zakres",
        first.row.frequency
        ??
        last.row.frequency
        ?
        Number(
            first.row.frequency
            ??
            last.row.frequency
        ).toFixed(3)
        +
        " MHz"
        :
        "—"
    )
  }

  <div class="ourrx-v2-note">
    Pokrycie = czas od pierwszej do ostatniej lokalnej
    ramki względem dostępnej historii lotu SondeHub.
    Źródła mają różne częstotliwości próbkowania,
    dlatego nie porównujemy surowej liczby ramek 1:1.
  </div>

</div>
`;

    }catch(err){

        console.error(
            "OUR_RECEPTION_V2:",
            err
        );


        if(
            String(
                selectedSerial
            )
            !==
            serial
        ){
            return;
        }


        card.innerHTML=`
<div class="sav38-title">
  📡 NASZ ODBIÓR
</div>

<div class="sav38-muted">
  Nie udało się obliczyć statystyk lokalnego odbioru.
</div>
`;
    }
}

</script>


<!-- OUR_RECEPTION_PRETTY_V3 -->

<style id="our-reception-pretty-v3-style">

/*
 * Trzy kafelki analityki mają pozostać równe,
 * ale bez ogromnej pustej przestrzeni.
 */

#sondeAnalyticsV38{
    align-items:stretch;
}

#sondeAnalyticsV38 .sav38-card{
    min-height:142px;
}


/* ===== NASZ ODBIÓR V3 ===== */

.ourrx-v3-top{
    display:flex;
    align-items:center;
    flex-wrap:wrap;

    gap:5px 9px;

    margin:
        7px 0
        9px 0;

    color:#aebdcb;

    font-size:11px;
    line-height:1.3;
}

.ourrx-v3-top strong{
    color:#eef5fb;
    font-weight:700;
}

.ourrx-v3-dot{
    color:#52697d;
}


.ourrx-v3-metrics{
    display:grid;

    grid-template-columns:
        repeat(2,minmax(0,1fr));

    gap:
        7px
        16px;

    margin-bottom:9px;
}


.ourrx-v3-metric{
    min-width:0;
}

.ourrx-v3-label{
    color:#8298aa;

    font-size:9px;
    line-height:1.2;

    margin-bottom:2px;
}

.ourrx-v3-value{
    overflow:hidden;

    color:#eef5fb;

    font-size:13px;
    line-height:1.2;
    font-weight:700;

    white-space:nowrap;
    text-overflow:ellipsis;
}

.ourrx-v3-blue{
    color:#63b3ff;
}

.ourrx-v3-green{
    color:#72dfa0;
}


.ourrx-v3-strip{
    padding-top:7px;

    border-top:
        1px solid
        rgba(81,106,129,.42);

    color:#aebdcb;

    font-size:10px;
    line-height:1.4;
}


.ourrx-v3-strip strong{
    color:#e6edf3;
}


.ourrx-v3-time{
    margin-top:5px;

    color:#72899c;

    font-size:9px;
    line-height:1.3;
}


@media(max-width:850px){

    #sondeAnalyticsV38 .sav38-card{
        min-height:0;
    }

}

</style>


<script id="our-reception-pretty-v3-script">

/* OUR_RECEPTION_PRETTY_V3 */


function ourRxV3Clock(d){

    if(!d){
        return "—";
    }

    return d.toLocaleTimeString(
        "pl-PL",
        {
            hour:"2-digit",
            minute:"2-digit",
            second:"2-digit",
            hour12:false
        }
    );
}


function ourRxV3Date(d){

    if(!d){
        return "—";
    }

    return d.toLocaleDateString(
        "pl-PL",
        {
            day:"2-digit",
            month:"2-digit"
        }
    );
}


function ourRxV3Metric(
    label,
    value,
    css=""
){

    return `
<div class="ourrx-v3-metric">

    <div class="ourrx-v3-label">
        ${esc(label)}
    </div>

    <div class="ourrx-v3-value ${css}">
        ${esc(
            value==null
            ?
            "—"
            :
            value
        )}
    </div>

</div>`;
}


async function renderOurReceptionV3(d){

    const card=
        ourRxV2Card();

    if(!card){
        return;
    }


    const serial=
        String(
            d?.serial
            ||
            ""
        );


    if(!serial){
        return;
    }


    card.innerHTML=`
<div class="sav38-title">
    📡 NASZ ODBIÓR
</div>

<div class="sav38-muted">
    Pobieranie lokalnych danych…
</div>
`;


    try{

        const response=
            await fetch(
                "/api/local-flight?serial="
                +
                encodeURIComponent(serial),
                {
                    cache:"no-store"
                }
            );


        if(!response.ok){
            throw new Error(
                "HTTP "
                +
                response.status
            );
        }


        const data=
            await response.json();


        if(
            String(selectedSerial)
            !==
            serial
        ){
            return;
        }


        const rows=
            Array.isArray(data.samples)
            ?
            data.samples
            :
            [];


        if(!rows.length){

            card.innerHTML=`
<div class="sav38-title">
    📡 NASZ ODBIÓR
</div>

<div class="sav38-muted">
    Brak lokalnego odbioru tej sondy.
</div>
`;

            return;
        }


        const normalized=
            rows
            .map(
                row => {

                    const dt=
                        ourRxV2PointDate(row);

                    return {
                        row,
                        dt,
                        ms:
                            dt
                            ?
                            dt.getTime()
                            :
                            null
                    };
                }
            )
            .filter(
                x =>
                    x.ms!==null
            )
            .sort(
                (a,b)=>
                    a.ms-b.ms
            );


        if(!normalized.length){
            throw new Error(
                "brak poprawnych timestampów"
            );
        }


        const first=
            normalized[0];

        const last=
            normalized[
                normalized.length-1
            ];


        const duration=
            (
                last.ms
                -
                first.ms
            )
            /
            1000;


        const snrs=
            normalized
            .map(
                x =>
                    ourRxV2Number(
                        x.row.snr
                        ??
                        x.row.snr_db
                    )
            )
            .filter(
                x =>
                    x!==null
                    &&
                    x>-90
            );


        const avgSnr=
            snrs.length
            ?
            snrs.reduce(
                (a,b)=>
                    a+b,
                0
            )
            /
            snrs.length
            :
            null;


        const maxSnr=
            snrs.length
            ?
            Math.max(...snrs)
            :
            null;


        const alts=
            normalized
            .map(
                x =>
                    ourRxV2Number(
                        x.row.alt
                    )
            )
            .filter(
                x =>
                    x!==null
            );


        const minAlt=
            alts.length
            ?
            Math.min(...alts)
            :
            null;


        const continuity=
            ourRxV2Continuity(
                normalized.map(
                    x =>
                        x.ms
                )
            );


        const maxDistance=
            ourRxV2MaxDistance(
                normalized.map(
                    x =>
                        x.row
                )
            );


        const coverage=
            ourRxV2Coverage(
                d,
                first.dt,
                last.dt
            );


        const freq=
            ourRxV2Number(
                first.row.frequency
                ??
                last.row.frequency
            );


        const dateSame=
            first.dt
            &&
            last.dt
            &&
            ourRxV3Date(first.dt)
            ===
            ourRxV3Date(last.dt);


        const datePrefix=
            dateSame
            ?
            ourRxV3Date(first.dt)
            +
            " • "
            :
            "";


        card.innerHTML=`

<div class="sav38-title">
    📡 NASZ ODBIÓR
</div>


<div class="ourrx-v3-top">

    <strong>
        ${normalized.length.toLocaleString("pl-PL")} ramek
    </strong>

    <span class="ourrx-v3-dot">•</span>

    <span>
        ${esc(ourRxV2Duration(duration))}
    </span>

    ${
        freq!==null
        ?
        `
        <span class="ourrx-v3-dot">•</span>

        <span>
            ${freq.toFixed(3)} MHz
        </span>
        `
        :
        ""
    }

</div>


<div class="ourrx-v3-metrics">

    ${
        ourRxV3Metric(
            "Średni SNR",
            avgSnr===null
            ?
            "—"
            :
            avgSnr.toFixed(1)
            +
            " dB",
            "ourrx-v3-blue"
        )
    }


    ${
        ourRxV3Metric(
            "Maks. SNR",
            maxSnr===null
            ?
            "—"
            :
            maxSnr.toFixed(1)
            +
            " dB",
            "ourrx-v3-green"
        )
    }


    ${
        ourRxV3Metric(
            "Maks. dystans",
            maxDistance===null
            ?
            "—"
            :
            maxDistance.toFixed(1)
            +
            " km"
        )
    }


    ${
        ourRxV3Metric(
            "Min. wysokość",
            minAlt===null
            ?
            "—"
            :
            Math.round(minAlt)
                .toLocaleString("pl-PL")
            +
            " m"
        )
    }

</div>


<div class="ourrx-v3-strip">

    ciąg
    <strong>
        ${esc(
            ourRxV2Duration(
                continuity.longest
            )
        )}
    </strong>

    &nbsp;•&nbsp;

    przerwa
    <strong>
        ${esc(
            ourRxV2Duration(
                continuity.maxGap
            )
        )}
    </strong>

    &nbsp;•&nbsp;

    pokrycie
    <strong>
        ${
            coverage===null
            ?
            "—"
            :
            coverage.toFixed(1)
            +
            "%"
        }
    </strong>

</div>


<div class="ourrx-v3-time">

    ${esc(datePrefix)}

    ${esc(
        ourRxV3Clock(
            first.dt
        )
    )}

    &nbsp;→&nbsp;

    ${esc(
        ourRxV3Clock(
            last.dt
        )
    )}

</div>

`;

    }catch(err){

        console.error(
            "OUR_RECEPTION_PRETTY_V3:",
            err
        );


        if(
            String(selectedSerial)
            !==
            serial
        ){
            return;
        }


        card.innerHTML=`
<div class="sav38-title">
    📡 NASZ ODBIÓR
</div>

<div class="sav38-muted">
    Brak danych lokalnego odbioru.
</div>
`;

    }

}

</script>


<!-- ANALYTICS_LAYOUT_CSS_V5 -->
<style id="analytics-layout-css-v5">

/*
 * Bez JS.
 *
 * 1 = ANALIZA LOTU
 * 2 = NAJBLIŻEJ STACJI
 * 3 = NASZ ODBIÓR
 */

#sondeAnalyticsV38{
    display:grid !important;

    grid-template-columns:
        minmax(0,1fr)
        minmax(0,1fr) !important;

    grid-template-rows:
        auto
        auto !important;

    gap:10px !important;

    align-items:stretch !important;
}


/* usuwa stare wymuszone wysokości */

#sondeAnalyticsV38 > .sav38-card{
    width:auto !important;
    height:auto !important;
    min-height:0 !important;
}


/* ANALIZA LOTU — lewa góra */

#sondeAnalyticsV38 > .sav38-card:nth-child(1){
    grid-column:1 !important;
    grid-row:1 !important;
}


/* NAJBLIŻEJ — lewy dół */

#sondeAnalyticsV38 > .sav38-card:nth-child(2){
    grid-column:1 !important;
    grid-row:2 !important;
}


/* NASZ ODBIÓR — prawa strona, wysokość obu */

#sondeAnalyticsV38 > .sav38-card:nth-child(3){
    grid-column:2 !important;
    grid-row:1 / span 2 !important;

    align-self:stretch !important;
}


/*
 * Na małym ekranie wracamy do jednej kolumny.
 */

@media(max-width:850px){

    #sondeAnalyticsV38{
        grid-template-columns:
            1fr !important;

        grid-template-rows:
            auto !important;
    }

    #sondeAnalyticsV38 > .sav38-card:nth-child(1),
    #sondeAnalyticsV38 > .sav38-card:nth-child(2),
    #sondeAnalyticsV38 > .sav38-card:nth-child(3){

        grid-column:1 !important;
        grid-row:auto !important;
    }
}

</style>


<!-- OUR_RECEPTION_SPACING_V5_1 -->
<style id="our-reception-spacing-v5-1">

/* OUR_RECEPTION_SPACING_V5_1 */

/*
 * Dotyczy tylko prawego kafla "Nasz odbiór".
 * Rozkładamy istniejącą treść równiej na pełną wysokość.
 */

#sondeAnalyticsV38 > .sav38-card:nth-child(3){
    display:flex !important;
    flex-direction:column !important;
}


/*
 * Nagłówek zostaje na górze.
 */

#sondeAnalyticsV38 > .sav38-card:nth-child(3) .sav38-title{
    margin-bottom:8px !important;
}


/*
 * Główna zawartość wykorzystuje pełną wysokość.
 */

#sondeAnalyticsV38 > .sav38-card:nth-child(3) .ourrx-v3-top{
    margin:
        4px 0
        10px 0 !important;

    gap:
        5px
        10px !important;

    line-height:1.4 !important;
}


/*
 * Cztery główne parametry:
 * więcej oddechu pionowo.
 */

#sondeAnalyticsV38 > .sav38-card:nth-child(3) .ourrx-v3-metrics{
    flex:1 1 auto !important;

    display:grid !important;

    grid-template-columns:
        repeat(2,minmax(0,1fr)) !important;

    align-content:space-evenly !important;

    gap:
        12px
        22px !important;

    margin:
        4px 0
        10px 0 !important;
}


/*
 * Same pola metryk.
 */

#sondeAnalyticsV38 > .sav38-card:nth-child(3) .ourrx-v3-metric{
    padding:
        3px
        0 !important;
}


/*
 * Etykiety i wartości odrobinę większe.
 */

#sondeAnalyticsV38 > .sav38-card:nth-child(3) .ourrx-v3-label{
    margin-bottom:3px !important;

    font-size:10px !important;
    line-height:1.25 !important;
}

#sondeAnalyticsV38 > .sav38-card:nth-child(3) .ourrx-v3-value{
    font-size:14px !important;
    line-height:1.25 !important;
}


/*
 * Dolny pasek ciąg / przerwa / pokrycie.
 */

#sondeAnalyticsV38 > .sav38-card:nth-child(3) .ourrx-v3-strip{
    margin-top:auto !important;

    padding-top:10px !important;

    line-height:1.5 !important;

    border-top:
        1px solid
        rgba(81,106,129,.48) !important;
}


/*
 * Czas pierwszej/ostatniej ramki
 * przy samym dole, ale z czytelnym odstępem.
 */

#sondeAnalyticsV38 > .sav38-card:nth-child(3) .ourrx-v3-time{
    margin-top:8px !important;

    padding-bottom:1px !important;

    font-size:10px !important;
    line-height:1.4 !important;
}


/*
 * Na małym ekranie bez sztucznego rozciągania.
 */

@media(max-width:850px){

    #sondeAnalyticsV38 > .sav38-card:nth-child(3){
        display:block !important;
    }

    #sondeAnalyticsV38 > .sav38-card:nth-child(3) .ourrx-v3-metrics{
        display:grid !important;

        gap:
            8px
            16px !important;

        margin:
            6px 0
            8px 0 !important;
    }
}

</style>


<!-- FLIGHT_ANALYSIS_V6 -->

<style id="flight-analysis-v6-style">

/* FLIGHT_ANALYSIS_V6 */

.fa6-status{
    margin:
        4px 0
        5px 0;

    font-size:13px;
    line-height:1.15;
    font-weight:800;
}

.fa6-up{
    color:#62d890;
}

.fa6-down{
    color:#fbbf24;
}

.fa6-landed{
    color:#62d890;
}

.fa6-neutral{
    color:#75baff;
}

.fa6-line{
    color:#b7c4cf;

    font-size:10px;
    line-height:1.45;

    white-space:normal;
}

.fa6-line + .fa6-line{
    margin-top:2px;
}

.fa6-line strong{
    color:#eef5fb;
    font-weight:700;
}

.fa6-sub{
    color:#758a9b;

    font-size:9px;
    line-height:1.3;

    margin-top:3px;
}

</style>


<script id="flight-analysis-v6-script">

/* ============================================================
   FLIGHT_ANALYSIS_V6
   ============================================================ */


function fa6Num(v){

    const n=Number(v);

    return Number.isFinite(n)
        ?
        n
        :
        null;
}


function fa6Date(v){

    if(
        v===null
        ||
        v===undefined
        ||
        v===""
    ){
        return null;
    }


    if(
        typeof v==="number"
        &&
        Number.isFinite(v)
    ){

        const d=
            new Date(
                v>1e12
                ?
                v
                :
                v*1000
            );

        return Number.isNaN(
            d.getTime()
        )
        ?
        null
        :
        d;
    }


    const d=new Date(v);

    return Number.isNaN(
        d.getTime()
    )
    ?
    null
    :
    d;
}


function fa6PointTime(p){

    return fa6Date(
        p?.datetime
        ??
        p?.time
        ??
        p?.timestamp
        ??
        p?.ts
        ??
        p?._epoch
    );
}


function fa6Duration(sec){

    sec=fa6Num(sec);

    if(
        sec===null
        ||
        sec<0
    ){
        return "—";
    }


    sec=Math.round(sec);


    const h=
        Math.floor(
            sec/3600
        );

    const m=
        Math.floor(
            (
                sec%3600
            )
            /
            60
        );


    if(h){
        return (
            h
            +
            " h "
            +
            m
            +
            " min"
        );
    }


    return (
        m
        +
        " min"
    );
}


function fa6Clock(d){

    if(!d){
        return "—";
    }


    return d.toLocaleTimeString(
        "pl-PL",
        {
            hour:"2-digit",
            minute:"2-digit",
            second:"2-digit",
            hour12:false
        }
    );
}


function fa6Signed(
    value,
    digits=1
){

    value=fa6Num(value);

    if(value===null){
        return "—";
    }


    return (
        value>0
        ?
        "+"
        :
        ""
    )
    +
    value.toFixed(digits);
}


function fa6Haversine(
    lat1,
    lon1,
    lat2,
    lon2
){

    lat1=fa6Num(lat1);
    lon1=fa6Num(lon1);
    lat2=fa6Num(lat2);
    lon2=fa6Num(lon2);


    if(
        lat1===null
        ||
        lon1===null
        ||
        lat2===null
        ||
        lon2===null
    ){
        return 0;
    }


    const r=
        x =>
            x
            *
            Math.PI
            /
            180;


    const p1=r(lat1);
    const p2=r(lat2);

    const dp=
        r(
            lat2-lat1
        );

    const dl=
        r(
            lon2-lon1
        );


    const a=
        Math.sin(dp/2)
        **
        2
        +
        Math.cos(p1)
        *
        Math.cos(p2)
        *
        Math.sin(dl/2)
        **
        2;


    return (
        6371
        *
        2
        *
        Math.atan2(
            Math.sqrt(a),
            Math.sqrt(1-a)
        )
    );
}


/* ------------------------------------------------------------
   NORMALIZACJA TRACK
------------------------------------------------------------ */

function fa6Track(d){

    const raw=
        d?.flight?.track
        ??
        [];


    return raw
        .map(
            p => {

                const dt=
                    fa6PointTime(p);

                return {
                    raw:p,
                    dt:dt,
                    ms:
                        dt
                        ?
                        dt.getTime()
                        :
                        null,
                    alt:
                        fa6Num(
                            p.alt
                        ),
                    vv:
                        fa6Num(
                            p.vel_v
                            ??
                            p.vertical_rate
                        ),
                    vh:
                        fa6Num(
                            p.vel_h
                            ??
                            p.speed
                        ),
                    lat:
                        fa6Num(
                            p.lat
                        ),
                    lon:
                        fa6Num(
                            p.lon
                        )
                };
            }
        )
        .filter(
            p =>
                p.ms!==null
        )
        .sort(
            (a,b)=>
                a.ms-b.ms
        );
}


/* ------------------------------------------------------------
   ŚREDNIA PRĘDKOŚĆ PIONOWA — OSTATNIE 2 MIN
------------------------------------------------------------ */

function fa6Average2Min(track){

    if(!track.length){
        return null;
    }


    const end=
        track[
            track.length-1
        ].ms;


    const start=
        end
        -
        120000;


    const win=
        track.filter(
            p =>
                p.ms>=start
        );


    const direct=
        win
        .map(
            p =>
                p.vv
        )
        .filter(
            v =>
                v!==null
        );


    if(direct.length>=2){

        return (
            direct.reduce(
                (a,b)=>
                    a+b,
                0
            )
            /
            direct.length
        );
    }


    /*
     * Fallback: wyliczamy ze zmiany wysokości.
     */

    if(win.length>=2){

        const first=
            win[0];

        const last=
            win[
                win.length-1
            ];


        if(
            first.alt!==null
            &&
            last.alt!==null
        ){

            const sec=
                (
                    last.ms
                    -
                    first.ms
                )
                /
                1000;


            if(sec>0){

                return (
                    last.alt
                    -
                    first.alt
                )
                /
                sec;
            }
        }
    }


    return null;
}


/* ------------------------------------------------------------
   BURST

   Uznajemy maksimum za burst dopiero,
   gdy po maksimum mamy wyraźnie potwierdzone opadanie.
------------------------------------------------------------ */

function fa6Burst(track){

    const valid=
        track.filter(
            p =>
                p.alt!==null
        );


    if(valid.length<5){

        return {
            detected:false,
            point:null
        };
    }


    let best=
        valid[0];

    let bestIndex=0;


    for(
        let i=1;
        i<valid.length;
        i++
    ){

        if(
            valid[i].alt
            >
            best.alt
        ){
            best=
                valid[i];

            bestIndex=i;
        }
    }


    const after=
        valid.slice(
            bestIndex+1
        );


    if(after.length<2){

        return {
            detected:false,
            point:best
        };
    }


    const latest=
        valid[
            valid.length-1
        ];


    const altitudeDrop=
        latest.alt!==null
        ?
        best.alt
        -
        latest.alt
        :
        0;


    const descentSamples=
        after.filter(
            p =>
                p.vv!==null
                &&
                p.vv<-1
        ).length;


    const detected=
        altitudeDrop>=300
        ||
        descentSamples>=2;


    return {
        detected:detected,
        point:best
    };
}


/* ------------------------------------------------------------
   PEWNE LĄDOWANIE

   1. recovery.recovered = pewne
   albo
   2. bardzo restrykcyjna stabilna telemetria:
      - >=5 minut danych
      - wysokość zmienia się <=30 m
      - |średnia pionowa| <=0.35 m/s
      - prędkość pozioma <=3 m/s
      - wysokość <=1500 m
------------------------------------------------------------ */

function fa6Landed(
    d,
    track
){

    const recovery=
        d?.recovery?.latest;


    if(
        recovery
        &&
        recovery.recovered
    ){

        return {
            landed:true,
            reason:"recovery"
        };
    }


    if(track.length<5){

        return {
            landed:false,
            reason:""
        };
    }


    const end=
        track[
            track.length-1
        ].ms;


    const win=
        track.filter(
            p =>
                p.ms
                >=
                end
                -
                600000
        );


    if(win.length<4){

        return {
            landed:false,
            reason:""
        };
    }


    const duration=
        (
            win[
                win.length-1
            ].ms
            -
            win[0].ms
        )
        /
        1000;


    if(duration<300){

        return {
            landed:false,
            reason:""
        };
    }


    const alts=
        win
        .map(
            p =>
                p.alt
        )
        .filter(
            v =>
                v!==null
        );


    if(alts.length<3){

        return {
            landed:false,
            reason:""
        };
    }


    const altRange=
        Math.max(...alts)
        -
        Math.min(...alts);


    const vv=
        win
        .map(
            p =>
                p.vv
        )
        .filter(
            v =>
                v!==null
        );


    const avgAbsV=
        vv.length
        ?
        vv.reduce(
            (a,b)=>
                a
                +
                Math.abs(b),
            0
        )
        /
        vv.length
        :
        null;


    const last=
        win[
            win.length-1
        ];


    const horizontalOk=
        last.vh!==null
        &&
        last.vh<=3;


    const altitudeOk=
        last.alt!==null
        &&
        last.alt<=1500;


    if(
        altRange<=30
        &&
        avgAbsV!==null
        &&
        avgAbsV<=0.35
        &&
        horizontalOk
        &&
        altitudeOk
    ){

        return {
            landed:true,
            reason:"stabilna telemetria"
        };
    }


    return {
        landed:false,
        reason:""
    };
}


/* ------------------------------------------------------------
   DŁUGOŚĆ ŚLADU
------------------------------------------------------------ */

function fa6TrackLength(track){

    let km=0;


    for(
        let i=1;
        i<track.length;
        i++
    ){

        km +=
            fa6Haversine(
                track[i-1].lat,
                track[i-1].lon,
                track[i].lat,
                track[i].lon
            );
    }


    return km;
}


/* ============================================================
   RENDER
============================================================ */

function renderFlightAnalysisV6(d){

    const host=
        document.getElementById(
            "sondeAnalyticsV38"
        );


    if(!host){
        return;
    }


    const card=
        host.querySelector(
            ":scope > .sav38-card:nth-child(1)"
        );


    if(!card){
        return;
    }


    const track=
        fa6Track(d);


    if(!track.length){

        return;
    }


    const latest=
        d?.latest
        ??
        {};


    const currentV=
        fa6Num(
            latest.vel_v
        )
        ??
        track[
            track.length-1
        ].vv;


    const avg2=
        fa6Average2Min(
            track
        );


    const burst=
        fa6Burst(
            track
        );


    const landed=
        fa6Landed(
            d,
            track
        );


    let phase=
        "STABILNIE";

    let phaseClass=
        "fa6-neutral";


    if(landed.landed){

        phase=
            "WYLĄDOWAŁA";

        phaseClass=
            "fa6-landed";

    }else{

        const phaseV=
            avg2
            ??
            currentV;


        if(
            phaseV!==null
            &&
            phaseV>0.8
        ){

            phase=
                "WZNOSZENIE";

            phaseClass=
                "fa6-up";

        }else if(
            phaseV!==null
            &&
            phaseV<-0.8
        ){

            phase=
                "OPADANIE";

            phaseClass=
                "fa6-down";
        }
    }


    const maxAlt=
        track
        .map(
            p =>
                p.alt
        )
        .filter(
            v =>
                v!==null
        );


    const maximum=
        maxAlt.length
        ?
        Math.max(
            ...maxAlt
        )
        :
        null;


    const first=
        track[0];

    const last=
        track[
            track.length-1
        ];


    const flightSec=
        (
            last.ms
            -
            first.ms
        )
        /
        1000;


    const trackKm=
        fa6TrackLength(
            track
        );


    const burstText=
        burst.detected
        &&
        burst.point
        ?
        (
            Math.round(
                burst.point.alt
            )
            .toLocaleString(
                "pl-PL"
            )
            +
            " m • "
            +
            fa6Clock(
                burst.point.dt
            )
        )
        :
        "jeszcze niewykryty";


    card.innerHTML=`

<div class="sav38-title">
    📈 ANALIZA LOTU
</div>

<div class="fa6-status ${phaseClass}">
    ${esc(phase)}
</div>

<div class="fa6-line">

    pionowo
    <strong>
        ${esc(
            fa6Signed(
                currentV
            )
        )}
        m/s
    </strong>

    &nbsp;•&nbsp;

    średnia 2 min
    <strong>
        ${esc(
            fa6Signed(
                avg2
            )
        )}
        m/s
    </strong>

    &nbsp;•&nbsp;

    maks.
    <strong>
        ${
            maximum===null
            ?
            "—"
            :
            Math.round(
                maximum
            )
            .toLocaleString(
                "pl-PL"
            )
            +
            " m"
        }
    </strong>

</div>


<div class="fa6-line">

    burst
    <strong>
        ${esc(
            burstText
        )}
    </strong>

    &nbsp;•&nbsp;

    lot
    <strong>
        ${esc(
            fa6Duration(
                flightSec
            )
        )}
    </strong>

    &nbsp;•&nbsp;

    ślad
    <strong>
        ${
            Number.isFinite(
                trackKm
            )
            ?
            Math.round(
                trackKm
            )
            +
            " km"
            :
            "—"
        }
    </strong>

</div>


${
    landed.landed
    ?
    `
<div class="fa6-sub">
    status lądowania:
    ${
        landed.reason==="recovery"
        ?
        "potwierdzone recovery"
        :
        "potwierdzony stabilną telemetrią"
    }
</div>
`
    :
    ""
}

`;

}

</script>


<!-- SONDE_STATUS_V7 -->

<style id="sonde-status-v7-style">

/* ============================================================
   SONDE_STATUS_V7
   ============================================================ */

#sondeStatusV7{
    display:flex;
    align-items:center;
    justify-content:space-between;

    flex-wrap:wrap;

    gap:7px 14px;

    margin:
        10px 0;

    padding:
        9px
        12px;

    border:
        1px solid
        #34475b;

    border-radius:
        8px;

    background:
        #172431;

    color:
        #b9c7d3;

    font-size:
        11px;

    line-height:
        1.3;
}


.ss7-main{
    display:flex;
    align-items:center;
    flex-wrap:wrap;

    gap:
        6px;
}


.ss7-title{
    margin-right:
        3px;

    color:
        #eef5fb;

    font-weight:
        800;

    white-space:
        nowrap;
}


.ss7-pill{
    display:inline-flex;
    align-items:center;

    gap:
        4px;

    padding:
        4px
        7px;

    border:
        1px solid
        #33485a;

    border-radius:
        6px;

    background:
        #101c27;

    color:
        #aebdca;

    white-space:
        nowrap;
}


.ss7-pill strong{
    color:
        #edf4fa;

    font-weight:
        800;
}


.ss7-ok{
    border-color:
        rgba(67,190,116,.48) !important;
}


.ss7-ok strong{
    color:
        #75dfa1 !important;
}


.ss7-info{
    border-color:
        rgba(75,158,228,.50) !important;
}


.ss7-info strong{
    color:
        #66b8ff !important;
}


.ss7-warn{
    border-color:
        rgba(245,184,61,.68) !important;

    background:
        rgba(85,60,15,.18) !important;
}


.ss7-warn strong{
    color:
        #f7c84c !important;
}


.ss7-danger{
    border-color:
        rgba(238,84,84,.75) !important;

    background:
        rgba(85,22,22,.22) !important;
}


.ss7-danger strong{
    color:
        #ff7373 !important;
}


.ss7-alert{
    margin-left:auto;

    font-weight:
        700;

    white-space:
        normal;

    text-align:
        right;
}


.ss7-alert-ok{
    color:
        #70dc9b;
}


.ss7-alert-warn{
    color:
        #f7c84c;
}


.ss7-alert-danger{
    color:
        #ff7373;
}


@media(max-width:850px){

    #sondeStatusV7{
        align-items:flex-start;
    }

    .ss7-alert{
        width:100%;

        margin-left:0;

        text-align:left;
    }
}

</style>


<script id="sonde-status-v7-script">

/* ============================================================
   SONDE_STATUS_V7

   Bez:
   - MutationObserver
   - setInterval
   - setTimeout

   Uruchamiany tylko po wybraniu sondy.
   ============================================================ */


function ss7Num(v){

    const n=
        Number(v);

    return Number.isFinite(n)
        ?
        n
        :
        null;
}


function ss7Date(v){

    if(
        v===null
        ||
        v===undefined
        ||
        v===""
    ){
        return null;
    }


    const d=
        new Date(v);


    return Number.isNaN(
        d.getTime()
    )
    ?
    null
    :
    d;
}


function ss7AgeInfo(v){

    const d=
        ss7Date(v);


    if(!d){

        return {
            text:"—",
            sec:null,
            cls:""
        };
    }


    let sec=
        (
            Date.now()
            -
            d.getTime()
        )
        /
        1000;


    if(sec<0){
        sec=0;
    }


    let text="";


    if(sec<60){

        text=
            Math.round(sec)
            +
            " s temu";

    }else if(sec<3600){

        text=
            Math.floor(
                sec/60
            )
            +
            " min temu";

    }else if(sec<86400){

        text=
            Math.floor(
                sec/3600
            )
            +
            " h temu";

    }else{

        text=
            Math.floor(
                sec/86400
            )
            +
            " dni temu";
    }


    let cls=
        "ss7-ok";


    if(sec>600){

        cls=
            "ss7-danger";

    }else if(sec>120){

        cls=
            "ss7-warn";
    }


    return {
        text:text,
        sec:sec,
        cls:cls
    };
}


/* ------------------------------------------------------------
   BATERIA

   Progi są zależne od typu sondy.
   Dla nieznanych typów tylko pokazujemy napięcie,
   bez zgadywania czy jest niskie.
------------------------------------------------------------ */

function ss7Battery(
    type,
    value
){

    const v=
        ss7Num(value);


    if(v===null){

        return {
            text:"—",
            cls:"",
            warning:null
        };
    }


    const t=
        String(
            type
            ||
            ""
        )
        .toUpperCase();


    let cls=
        "ss7-ok";

    let warning=null;


    if(
        t.includes("RS41")
    ){

        if(v<2.3){

            cls=
                "ss7-danger";

            warning=
                "bardzo niska bateria";

        }else if(v<2.5){

            cls=
                "ss7-warn";

            warning=
                "niska bateria";
        }

    }else if(
        t.includes("DFM")
    ){

        if(v<3.4){

            cls=
                "ss7-danger";

            warning=
                "bardzo niska bateria";

        }else if(v<3.8){

            cls=
                "ss7-warn";

            warning=
                "niska bateria";
        }

    }else{

        cls=
            "ss7-info";
    }


    return {
        text:
            v.toFixed(2)
            +
            " V",

        cls:cls,
        warning:warning
    };
}


/* ------------------------------------------------------------
   GPS
------------------------------------------------------------ */

function ss7Gps(v){

    const sats=
        ss7Num(v);


    if(sats===null){

        return {
            text:"—",
            cls:"",
            warning:null
        };
    }


    if(sats<4){

        return {
            text:
                Math.round(sats),

            cls:
                "ss7-danger",

            warning:
                "za mało satelitów GPS"
        };
    }


    if(sats<6){

        return {
            text:
                Math.round(sats),

            cls:
                "ss7-warn",

            warning:
                "słaby GPS"
        };
    }


    return {
        text:
            Math.round(sats),

        cls:
            "ss7-ok",

        warning:
            null
    };
}


/* ------------------------------------------------------------
   SNR

   To tylko wizualna ocena wartości.
   Nie wpływa na logikę odbioru.
------------------------------------------------------------ */

function ss7Snr(v){

    const snr=
        ss7Num(v);


    if(snr===null){

        return {
            text:"—",
            cls:""
        };
    }


    let cls=
        "ss7-ok";


    if(snr<6){

        cls=
            "ss7-warn";

    }else if(snr<10){

        cls=
            "ss7-info";
    }


    return {
        text:
            snr.toFixed(1)
            +
            " dB",

        cls:cls
    };
}


/* ------------------------------------------------------------
   RSSI — bez klasyfikowania jakości.
------------------------------------------------------------ */

function ss7Rssi(v){

    const rssi=
        ss7Num(v);


    if(rssi===null){

        return "—";
    }


    return (
        Math.round(rssi)
        +
        " dBm"
    );
}


/* ------------------------------------------------------------
   Znajdujemy wspólnego rodzica kart:
   Telemetria + Start/stacja startowa.

   Dzięki temu pasek trafia bezpośrednio POD cztery
   górne karty, bez dotykania ich układu.
------------------------------------------------------------ */

function ss7CommonAncestor(
    a,
    b
){

    if(
        !a
        ||
        !b
    ){
        return null;
    }


    let n=a;


    while(n){

        if(
            n.contains(b)
        ){
            return n;
        }

        n=
            n.parentElement;
    }


    return null;
}


function ss7Mount(){

    let strip=
        document.getElementById(
            "sondeStatusV7"
        );


    if(strip){

        return strip;
    }


    strip=
        document.createElement(
            "div"
        );


    strip.id=
        "sondeStatusV7";


    const telemetry=
        document.getElementById(
            "telemetry"
        );


    const site=
        document.getElementById(
            "site"
        );


    const common=
        ss7CommonAncestor(
            telemetry,
            site
        );


    /*
     * Preferowany wariant:
     * wspólny grid czterech kart.
     */

    if(
        common
        &&
        common!==document.body
        &&
        common.tagName!=="MAIN"
        &&
        common.parentNode
    ){

        common.parentNode.insertBefore(
            strip,
            common.nextSibling
        );


        return strip;
    }


    /*
     * Bezpieczny fallback:
     * bezpośrednio przed sekcją analityki.
     */

    const analytics=
        document.getElementById(
            "sondeAnalyticsV38"
        );


    if(
        analytics
        &&
        analytics.parentNode
    ){

        analytics.parentNode.insertBefore(
            strip,
            analytics
        );


        return strip;
    }


    return null;
}


/* ============================================================
   RENDER
============================================================ */

function renderSondeStatusV7(d){

    const strip=
        ss7Mount();


    if(!strip){

        return;
    }


    const latest=
        d?.latest
        ||
        {};


    const type=
        latest.subtype
        ??
        latest.type
        ??
        "";


    const gps=
        ss7Gps(
            latest.sats
        );


    const battery=
        ss7Battery(
            type,
            latest.batt
            ??
            latest.battery_v
            ??
            latest.battery
        );


    const age=
        ss7AgeInfo(
            latest.datetime
            ??
            latest.time
            ??
            latest.timestamp
        );


    const snr=
        ss7Snr(
            latest.snr
        );


    const rssi=
        ss7Rssi(
            latest.rssi
        );


    const warnings=[];


    if(gps.warning){

        warnings.push(
            gps.warning
        );
    }


    if(battery.warning){

        warnings.push(
            battery.warning
        );
    }


    if(
        age.sec!==null
        &&
        age.sec>600
    ){

        warnings.push(
            "stara telemetria"
        );

    }else if(
        age.sec!==null
        &&
        age.sec>120
    ){

        warnings.push(
            "telemetria opóźniona"
        );
    }


    let alertClass=
        "ss7-alert-ok";


    let alertText=
        "✓ brak ostrzeżeń";


    if(warnings.length){

        const severe=
            gps.cls==="ss7-danger"
            ||
            battery.cls==="ss7-danger"
            ||
            (
                age.sec!==null
                &&
                age.sec>600
            );


        alertClass=
            severe
            ?
            "ss7-alert-danger"
            :
            "ss7-alert-warn";


        alertText=
            (
                severe
                ?
                "⚠ "
                :
                "● "
            )
            +
            warnings.join(
                " • "
            );
    }


    strip.innerHTML=`

<div class="ss7-main">

    <span class="ss7-title">
        🩺 STAN SONDY
    </span>


    <span class="ss7-pill ${gps.cls}">
        GPS
        <strong>
            ${esc(gps.text)}
        </strong>
    </span>


    <span class="ss7-pill ${battery.cls}">
        🔋
        <strong>
            ${esc(battery.text)}
        </strong>
    </span>


    <span class="ss7-pill ${age.cls}">
        🕒
        <strong>
            ${esc(age.text)}
        </strong>
    </span>


    <span class="ss7-pill ${snr.cls}">
        SNR
        <strong>
            ${esc(snr.text)}
        </strong>
    </span>


    <span class="ss7-pill">
        RSSI
        <strong>
            ${esc(rssi)}
        </strong>
    </span>

</div>


<div class="ss7-alert ${alertClass}">
    ${esc(alertText)}
</div>

`;

}

</script>


<!-- FLIGHT_PROFILE_SWITCHER_V8 -->

<style id="flight-profile-switcher-v8-style">

/* ============================================================
   FLIGHT_PROFILE_SWITCHER_V8
   ============================================================ */

#rwAltV3Head{
    flex-wrap:wrap;
    gap:8px 12px;
}


#rwV8Controls{
    display:flex;
    align-items:center;
    justify-content:flex-end;
    flex-wrap:wrap;

    gap:5px;

    margin-left:auto;
}


.rwV8Btn{
    appearance:none;

    border:
        1px solid
        #3b5368;

    border-radius:
        6px;

    background:
        #101c27;

    color:
        #9fb2c2;

    padding:
        4px
        8px;

    font:
        inherit;

    font-size:
        10px;

    line-height:
        1.2;

    font-weight:
        700;

    cursor:
        pointer;

    transition:
        border-color .12s ease,
        background .12s ease,
        color .12s ease;
}


.rwV8Btn:hover{
    border-color:
        #5c7b96;

    color:
        #eef5fb;
}


.rwV8Btn.active{
    border-color:
        #4ca7f5;

    background:
        #17334a;

    color:
        #72bfff;
}


#rwV8Source{
    color:#71889b;

    font-size:9px;

    margin-left:5px;
}


@media(max-width:850px){

    #rwV8Controls{
        width:100%;
        justify-content:flex-start;

        margin-left:0;
    }

    .rwV8Btn{
        padding:
            5px
            7px;
    }
}

</style>


<script id="flight-profile-switcher-v8-script">

/* ============================================================
   FLIGHT_PROFILE_SWITCHER_V8

   Wysokość:
       istniejący, nietknięty renderer V3 + SondeHub track

   Temperatura / prędkość pionowa / SNR:
       lokalne ramki auto_rx — Stacja lokalna

   Bez:
       MutationObserver
       setInterval
       dodatkowego canvasa
   ============================================================ */


const rwV8OriginalRender =
    rwAltV3Render;


let rwV8Mode =
    "alt";


let rwV8LastSerial =
    null;


const rwV8Cache =
    new Map();


/* ------------------------------------------------------------
   SPECYFIKACJE
------------------------------------------------------------ */

function rwV8Spec(mode){

    if(mode==="temp"){

        return {
            title:"🌡️ Temperatura lotu",
            button:"Temperatura",
            axis:"Temperatura [°C]",
            unit:"°C"
        };
    }


    if(mode==="vel"){

        return {
            title:"↕️ Prędkość pionowa",
            button:"Prędkość pionowa",
            axis:"Prędkość pionowa [m/s]",
            unit:"m/s"
        };
    }


    if(mode==="snr"){

        return {
            title:"📡 SNR lokalnego odbioru",
            button:"SNR",
            axis:"SNR [dB]",
            unit:"dB"
        };
    }


    return {
        title:"📈 Profil wysokości lotu",
        button:"Wysokość",
        axis:"Wysokość [m]",
        unit:"m"
    };
}


/* ------------------------------------------------------------
   DATY / LICZBY
------------------------------------------------------------ */

function rwV8Number(v){

    const n=
        Number(v);

    return Number.isFinite(n)
        ?
        n
        :
        null;
}


function rwV8Date(v){

    if(
        v===null
        ||
        v===undefined
        ||
        v===""
    ){
        return null;
    }


    if(
        typeof v==="number"
        &&
        Number.isFinite(v)
    ){

        const d=
            new Date(
                v>1e12
                ?
                v
                :
                v*1000
            );

        return Number.isNaN(
            d.getTime()
        )
        ?
        null
        :
        d;
    }


    const d=
        new Date(v);


    return Number.isNaN(
        d.getTime()
    )
    ?
    null
    :
    d;
}


/* ------------------------------------------------------------
   PRZEŁĄCZNIKI
------------------------------------------------------------ */

function rwV8EnsureControls(){

    const head=
        document.getElementById(
            "rwAltV3Head"
        );


    if(!head){
        return null;
    }


    let controls=
        document.getElementById(
            "rwV8Controls"
        );


    if(controls){

        rwV8UpdateControls();

        return controls;
    }


    controls=
        document.createElement(
            "div"
        );


    controls.id=
        "rwV8Controls";


    controls.innerHTML=`

<button
  type="button"
  class="rwV8Btn"
  data-rwv8-mode="alt">
  Wysokość
</button>

<button
  type="button"
  class="rwV8Btn"
  data-rwv8-mode="temp">
  Temperatura
</button>

<button
  type="button"
  class="rwV8Btn"
  data-rwv8-mode="vel">
  Prędkość pionowa
</button>

<button
  type="button"
  class="rwV8Btn"
  data-rwv8-mode="snr">
  SNR
</button>

`;


    const maxBadge=
        document.getElementById(
            "rwAltV3Max"
        );


    if(
        maxBadge
        &&
        maxBadge.parentNode===head
    ){

        head.insertBefore(
            controls,
            maxBadge
        );

    }else{

        head.appendChild(
            controls
        );
    }


    controls.addEventListener(
        "click",
        function(ev){

            const button=
                ev.target.closest(
                    "[data-rwv8-mode]"
                );


            if(
                !button
                ||
                !controls.contains(button)
            ){
                return;
            }


            const mode=
                button.getAttribute(
                    "data-rwv8-mode"
                );


            if(
                ![
                    "alt",
                    "temp",
                    "vel",
                    "snr"
                ].includes(mode)
            ){
                return;
            }


            rwV8Mode=
                mode;


            rwV8UpdateControls();


            if(rwAltV3Current){

                rwAltV3Render(
                    rwAltV3Current
                );
            }
        }
    );


    rwV8UpdateControls();


    return controls;
}


function rwV8UpdateControls(){

    const controls=
        document.getElementById(
            "rwV8Controls"
        );


    if(!controls){
        return;
    }


    controls
        .querySelectorAll(
            "[data-rwv8-mode]"
        )
        .forEach(
            button => {

                button.classList.toggle(
                    "active",
                    button.getAttribute(
                        "data-rwv8-mode"
                    )
                    ===
                    rwV8Mode
                );
            }
        );
}


/* ------------------------------------------------------------
   LOCAL auto_rx
------------------------------------------------------------ */

async function rwV8LoadLocal(serial){

    const cached=
        rwV8Cache.get(
            serial
        );


    /*
     * Cache 15 s.
     * Bez żadnego timera — sprawdzamy tylko wiek przy użyciu.
     */

    if(
        cached
        &&
        Date.now()
        -
        cached.at
        <
        15000
    ){

        return cached.data;
    }


    const response=
        await fetch(
            "/api/local-flight?serial="
            +
            encodeURIComponent(
                serial
            ),
            {
                cache:"no-store"
            }
        );


    if(!response.ok){

        throw new Error(
            "local-flight HTTP "
            +
            response.status
        );
    }


    const data=
        await response.json();


    rwV8Cache.set(
        serial,
        {
            at:Date.now(),
            data:data
        }
    );


    return data;
}


/* ------------------------------------------------------------
   PUNKTY
------------------------------------------------------------ */

function rwV8LocalPoints(
    rows,
    mode
){

    const out=[];


    for(const row of rows || []){

        const dt=
            rwV8Date(
                row.datetime
                ??
                row.time
                ??
                row._epoch
            );


        if(!dt){
            continue;
        }


        let value=null;


        if(mode==="temp"){

            value=
                rwV8Number(
                    row.temp
                    ??
                    row.temperature
                );


            /*
             * auto_rx przy inicjalizacji potrafi mieć
             * wartość -273 jako brak prawidłowego pomiaru.
             */

            if(
                value===null
                ||
                value < -120
                ||
                value > 80
            ){
                continue;
            }

        }else if(mode==="vel"){

            value=
                rwV8Number(
                    row.vel_v
                    ??
                    row.vertical_rate
                );


            if(
                value===null
                ||
                Math.abs(value)>100
            ){
                continue;
            }

        }else if(mode==="snr"){

            value=
                rwV8Number(
                    row.snr
                );


            if(
                value===null
                ||
                value < -30
                ||
                value > 100
            ){
                continue;
            }

        }else{

            continue;
        }


        out.push({

            ms:
                dt.getTime(),

            dt:
                dt,

            value:
                value,

            alt:
                rwV8Number(
                    row.alt
                ),

            lat:
                rwV8Number(
                    row.lat
                ),

            lon:
                rwV8Number(
                    row.lon
                ),

            temp:
                rwV8Number(
                    row.temp
                ),

            snr:
                rwV8Number(
                    row.snr
                ),

            vel_v:
                rwV8Number(
                    row.vel_v
                ),

            raw:
                row
        });
    }


    out.sort(
        (a,b)=>
            a.ms-b.ms
    );


    return out;
}


/* ------------------------------------------------------------
   ZAKRES Y
------------------------------------------------------------ */

function rwV8Range(
    points,
    mode
){

    const values=
        points.map(
            p =>
                p.value
        );


    let min=
        Math.min(
            ...values
        );


    let max=
        Math.max(
            ...values
        );


    if(mode==="vel"){

        let abs=
            Math.max(
                1,
                Math.abs(min),
                Math.abs(max)
            );


        abs=
            Math.ceil(
                abs
                /
                2
            )
            *
            2;


        return {
            min:-abs,
            max:abs
        };
    }


    if(mode==="temp"){

        const range=
            Math.max(
                1,
                max-min
            );


        const pad=
            Math.max(
                2,
                range*0.08
            );


        min=
            Math.floor(
                (min-pad)
                /
                5
            )
            *
            5;


        max=
            Math.ceil(
                (max+pad)
                /
                5
            )
            *
            5;


        if(max<=min){
            max=min+5;
        }


        return {
            min:min,
            max:max
        };
    }


    /*
     * SNR
     */

    const range=
        Math.max(
            1,
            max-min
        );


    const pad=
        Math.max(
            1,
            range*0.08
        );


    min=
        Math.floor(
            min-pad
        );


    max=
        Math.ceil(
            max+pad
        );


    if(
        max-min<5
    ){

        const mid=
            (
                max+min
            )
            /
            2;


        min=
            Math.floor(
                mid-2.5
            );


        max=
            Math.ceil(
                mid+2.5
            );
    }


    return {
        min:min,
        max:max
    };
}


/* ------------------------------------------------------------
   FORMAT
------------------------------------------------------------ */

function rwV8AxisValue(
    value,
    mode
){

    if(mode==="temp"){

        return Math.round(
            value
        );
    }


    if(mode==="vel"){

        return (
            Math.abs(value)<0.05
            ?
            "0"
            :
            value.toFixed(1)
        );
    }


    if(mode==="snr"){

        return value.toFixed(1);
    }


    return value;
}


function rwV8ValueText(
    value,
    mode
){

    if(
        value===null
        ||
        value===undefined
    ){
        return "—";
    }


    if(mode==="temp"){

        return (
            value.toFixed(1)
            +
            " °C"
        );
    }


    if(mode==="vel"){

        return (
            (
                value>0
                ?
                "+"
                :
                ""
            )
            +
            value.toFixed(2)
            +
            " m/s"
        );
    }


    return (
        value.toFixed(1)
        +
        " dB"
    );
}


/* ------------------------------------------------------------
   TOOLTIP
------------------------------------------------------------ */

function rwV8TipRow(
    name,
    value
){

    if(
        value===null
        ||
        value===undefined
        ||
        value===""
    ){
        return "";
    }


    return `
<div>
  <span class="rwAltV3TipName">
    ${rwAltV3Escape(name)}:
  </span>
  ${rwAltV3Escape(value)}
</div>`;
}


function rwV8TipHtml(
    point,
    mode
){

    let html=`

<div class="rwAltV3TipTitle">
    ${rwAltV3Escape(
        rwAltV3DateTime(
            point.dt
        )
    )}
</div>

`;


    const spec=
        rwV8Spec(
            mode
        );


    html +=
        rwV8TipRow(
            spec.button,
            rwV8ValueText(
                point.value,
                mode
            )
        );


    if(point.alt!==null){

        html +=
            rwV8TipRow(
                "Wysokość",
                Math.round(
                    point.alt
                )
                +
                " m"
            );
    }


    if(
        point.lat!==null
        &&
        point.lon!==null
    ){

        html +=
            rwV8TipRow(
                "Pozycja",
                point.lat.toFixed(5)
                +
                ", "
                +
                point.lon.toFixed(5)
            );
    }


    if(
        mode!=="temp"
        &&
        point.temp!==null
        &&
        point.temp>-120
    ){

        html +=
            rwV8TipRow(
                "Temperatura",
                point.temp.toFixed(1)
                +
                " °C"
            );
    }


    if(
        mode!=="vel"
        &&
        point.vel_v!==null
    ){

        html +=
            rwV8TipRow(
                "Prędkość pionowa",
                (
                    point.vel_v>0
                    ?
                    "+"
                    :
                    ""
                )
                +
                point.vel_v.toFixed(2)
                +
                " m/s"
            );
    }


    if(
        mode!=="snr"
        &&
        point.snr!==null
    ){

        html +=
            rwV8TipRow(
                "SNR",
                point.snr.toFixed(1)
                +
                " dB"
            );
    }


    return html;
}


/* ------------------------------------------------------------
   RENDERER 3 TRYBÓW LOKALNYCH
------------------------------------------------------------ */

function rwV8DrawLocal(
    d,
    mode,
    points
){

    const canvas=
        document.getElementById(
            "rwAltV3Canvas"
        );


    const plot=
        document.getElementById(
            "rwAltV3Plot"
        );


    const tip=
        document.getElementById(
            "rwAltV3Tip"
        );


    const info=
        document.getElementById(
            "rwAltV3Info"
        );


    const maxBadge=
        document.getElementById(
            "rwAltV3Max"
        );


    const title=
        document.getElementById(
            "rwAltV3Title"
        );


    if(
        !canvas
        ||
        !plot
        ||
        !tip
        ||
        !info
        ||
        !maxBadge
        ||
        !title
    ){
        return;
    }


    const spec=
        rwV8Spec(
            mode
        );


    title.textContent=
        spec.title;


    if(points.length<2){

        info.textContent=
            "Brak wystarczających lokalnych danych Stacja lokalna dla tego wykresu.";

        maxBadge.textContent=
            "brak danych";


        const rect=
            plot.getBoundingClientRect();


        const W=
            Math.max(
                500,
                Math.round(
                    rect.width
                )
            );


        const H=
            Math.max(
                220,
                Math.round(
                    rect.height
                )
            );


        const DPR=
            Math.max(
                1,
                window.devicePixelRatio
                ||
                1
            );


        canvas.width=
            Math.round(
                W*DPR
            );


        canvas.height=
            Math.round(
                H*DPR
            );


        canvas.style.width=
            W+"px";


        canvas.style.height=
            H+"px";


        const ctx=
            canvas.getContext(
                "2d"
            );


        ctx.setTransform(
            DPR,0,
            0,DPR,
            0,0
        );


        ctx.fillStyle=
            "#0b151f";


        ctx.fillRect(
            0,0,
            W,H
        );


        ctx.fillStyle=
            "#8195a6";


        ctx.font=
            "12px system-ui, -apple-system, sans-serif";


        ctx.textAlign=
            "center";


        ctx.fillText(
            "Brak lokalnych danych dla wybranego parametru",
            W/2,
            H/2
        );


        tip.style.display=
            "none";


        return;
    }


    const serial=
        String(
            d?.serial
            ||
            "sonda"
        );


    info.textContent=
        `${serial} • ${
            points.length
                .toLocaleString(
                    "pl-PL"
                )
        } lokalnych punktów • ${
            rwAltV3Time(
                points[0].dt
            )
        }–${
            rwAltV3Time(
                points[
                    points.length-1
                ].dt
            )
        } • Stacja lokalna`;


    const values=
        points.map(
            p =>
                p.value
        );


    const minValue=
        Math.min(
            ...values
        );


    const maxValue=
        Math.max(
            ...values
        );


    if(mode==="temp"){

        maxBadge.textContent=
            `min ${
                minValue.toFixed(1)
            } °C`;

    }else if(mode==="vel"){

        maxBadge.textContent=
            `${
                minValue.toFixed(1)
            }…+${
                Math.max(
                    0,
                    maxValue
                ).toFixed(1)
            } m/s`;

    }else{

        maxBadge.textContent=
            `max ${
                maxValue.toFixed(1)
            } dB`;
    }


    const rect=
        plot.getBoundingClientRect();


    const W=
        Math.max(
            500,
            Math.round(
                rect.width
            )
        );


    const H=
        Math.max(
            220,
            Math.round(
                rect.height
            )
        );


    const DPR=
        Math.max(
            1,
            window.devicePixelRatio
            ||
            1
        );


    canvas.width=
        Math.round(
            W*DPR
        );


    canvas.height=
        Math.round(
            H*DPR
        );


    canvas.style.width=
        W+"px";


    canvas.style.height=
        H+"px";


    const ctx=
        canvas.getContext(
            "2d"
        );


    ctx.setTransform(
        DPR,0,
        0,DPR,
        0,0
    );


    const left=65;
    const right=20;
    const top=18;
    const bottom=39;

    const PW=
        W-left-right;

    const PH=
        H-top-bottom;


    const tMin=
        points[0].ms;


    const tMax=
        points[
            points.length-1
        ].ms;


    const range=
        rwV8Range(
            points,
            mode
        );


    const yMin=
        range.min;


    const yMax=
        range.max;


    const xFor=
        ms =>
            left
            +
            (
                (
                    ms-tMin
                )
                /
                Math.max(
                    1,
                    tMax-tMin
                )
            )
            *
            PW;


    const yFor=
        value =>
            top
            +
            PH
            -
            (
                (
                    value-yMin
                )
                /
                Math.max(
                    0.000001,
                    yMax-yMin
                )
            )
            *
            PH;


    /*
     * Rysowanie maks. ~1800 punktów.
     * Tooltip korzysta z pełnych danych.
     */

    const step=
        Math.max(
            1,
            Math.ceil(
                points.length
                /
                1800
            )
        );


    const drawPoints=[];


    for(
        let i=0;
        i<points.length;
        i+=step
    ){

        drawPoints.push(
            points[i]
        );
    }


    if(
        drawPoints[
            drawPoints.length-1
        ]
        !==
        points[
            points.length-1
        ]
    ){

        drawPoints.push(
            points[
                points.length-1
            ]
        );
    }


    function paint(
        hoverIndex=null
    ){

        ctx.clearRect(
            0,0,
            W,H
        );


        ctx.fillStyle=
            "#0b151f";


        ctx.fillRect(
            0,0,
            W,H
        );


        ctx.font=
            "11px system-ui, -apple-system, sans-serif";


        /*
         * poziome linie + Y
         */

        for(
            let i=0;
            i<=5;
            i++
        ){

            const value=
                yMin
                +
                (
                    yMax-yMin
                )
                *
                i
                /
                5;


            const y=
                yFor(
                    value
                );


            ctx.beginPath();

            ctx.moveTo(
                left,
                y
            );

            ctx.lineTo(
                W-right,
                y
            );

            ctx.strokeStyle=
                "rgba(93,119,141,.23)";

            ctx.lineWidth=
                1;

            ctx.stroke();


            ctx.fillStyle=
                "#8499aa";

            ctx.textAlign=
                "right";

            ctx.textBaseline=
                "middle";

            ctx.fillText(
                rwV8AxisValue(
                    value,
                    mode
                ),
                left-9,
                y
            );
        }


        /*
         * pionowe linie + czas
         */

        for(
            let i=0;
            i<=5;
            i++
        ){

            const ms=
                tMin
                +
                (
                    tMax-tMin
                )
                *
                i
                /
                5;


            const x=
                xFor(
                    ms
                );


            ctx.beginPath();

            ctx.moveTo(
                x,
                top
            );

            ctx.lineTo(
                x,
                H-bottom
            );

            ctx.strokeStyle=
                "rgba(93,119,141,.16)";

            ctx.lineWidth=
                1;

            ctx.stroke();


            ctx.fillStyle=
                "#8499aa";

            ctx.textAlign=
                i===0
                ?
                "left"
                :
                (
                    i===5
                    ?
                    "right"
                    :
                    "center"
                );

            ctx.textBaseline=
                "top";

            ctx.fillText(
                rwAltV3Time(
                    new Date(ms)
                ),
                x,
                H-bottom+9
            );
        }


        /*
         * Oś zerowa dla prędkości pionowej.
         */

        if(
            mode==="vel"
            &&
            yMin<0
            &&
            yMax>0
        ){

            const zeroY=
                yFor(0);


            ctx.beginPath();

            ctx.moveTo(
                left,
                zeroY
            );

            ctx.lineTo(
                W-right,
                zeroY
            );

            ctx.strokeStyle=
                "rgba(220,228,235,.38)";

            ctx.lineWidth=
                1;

            ctx.stroke();
        }


        /*
         * Opis osi Y.
         */

        ctx.save();

        ctx.translate(
            16,
            top+PH/2
        );

        ctx.rotate(
            -Math.PI/2
        );

        ctx.fillStyle=
            "#91a4b5";

        ctx.textAlign=
            "center";

        ctx.textBaseline=
            "middle";

        ctx.fillText(
            spec.axis,
            0,
            0
        );

        ctx.restore();


        /*
         * Linia.
         */

        ctx.beginPath();


        drawPoints.forEach(
            (p,index) => {

                const x=
                    xFor(
                        p.ms
                    );

                const y=
                    yFor(
                        p.value
                    );


                if(index===0){

                    ctx.moveTo(
                        x,
                        y
                    );

                }else{

                    ctx.lineTo(
                        x,
                        y
                    );
                }
            }
        );


        ctx.strokeStyle=
            "#4da6ff";

        ctx.lineWidth=
            2;

        ctx.lineJoin=
            "round";

        ctx.lineCap=
            "round";

        ctx.stroke();


        /*
         * Hover.
         */

        if(
            hoverIndex!==null
            &&
            points[hoverIndex]
        ){

            const point=
                points[
                    hoverIndex
                ];


            const x=
                xFor(
                    point.ms
                );


            const y=
                yFor(
                    point.value
                );


            ctx.beginPath();

            ctx.moveTo(
                x,
                top
            );

            ctx.lineTo(
                x,
                H-bottom
            );

            ctx.strokeStyle=
                "rgba(180,206,226,.45)";

            ctx.lineWidth=
                1;

            ctx.stroke();


            ctx.beginPath();

            ctx.arc(
                x,
                y,
                4,
                0,
                Math.PI*2
            );

            ctx.fillStyle=
                "#6bb9ff";

            ctx.fill();

            ctx.strokeStyle=
                "#dceeff";

            ctx.lineWidth=
                1;

            ctx.stroke();
        }
    }


    paint();


    canvas.onmousemove=
        function(ev){

            const rect=
                canvas.getBoundingClientRect();


            const cssX=
                (
                    ev.clientX
                    -
                    rect.left
                )
                *
                (
                    W
                    /
                    Math.max(
                        1,
                        rect.width
                    )
                );


            const ratio=
                Math.max(
                    0,
                    Math.min(
                        1,
                        (
                            cssX-left
                        )
                        /
                        Math.max(
                            1,
                            PW
                        )
                    )
                );


            const targetMs=
                tMin
                +
                ratio
                *
                (
                    tMax-tMin
                );


            const index=
                rwAltV3Nearest(
                    points,
                    targetMs
                );


            const point=
                points[
                    index
                ];


            paint(
                index
            );


            tip.innerHTML=
                rwV8TipHtml(
                    point,
                    mode
                );


            tip.style.display=
                "block";


            const px=
                xFor(
                    point.ms
                );


            const py=
                yFor(
                    point.value
                );


            const tipWidth=
                250;


            let leftPos=
                px+12;


            if(
                leftPos+tipWidth
                >
                W-5
            ){

                leftPos=
                    px-tipWidth-12;
            }


            leftPos=
                Math.max(
                    5,
                    leftPos
                );


            let topPos=
                py-30;


            topPos=
                Math.max(
                    5,
                    Math.min(
                        H-125,
                        topPos
                    )
                );


            tip.style.left=
                leftPos
                +
                "px";


            tip.style.top=
                topPos
                +
                "px";
        };


    canvas.onmouseleave=
        function(){

            tip.style.display=
                "none";

            paint();
        };
}


/* ------------------------------------------------------------
   ASYNC RENDER
------------------------------------------------------------ */

async function rwV8RenderLocal(
    d,
    mode
){

    const host=
        rwAltV3Ensure();


    if(!host){
        return;
    }


    rwV8EnsureControls();

    rwV8UpdateControls();


    const serial=
        String(
            d?.serial
            ||
            ""
        );


    if(!serial){
        return;
    }


    const info=
        document.getElementById(
            "rwAltV3Info"
        );


    const title=
        document.getElementById(
            "rwAltV3Title"
        );


    const maxBadge=
        document.getElementById(
            "rwAltV3Max"
        );


    if(title){

        title.textContent=
            rwV8Spec(
                mode
            ).title;
    }


    if(info){

        info.textContent=
            "Pobieranie lokalnych ramek Stacja lokalna…";
    }


    if(maxBadge){

        maxBadge.textContent=
            "…";
    }


    try{

        const data=
            await rwV8LoadLocal(
                serial
            );


        /*
         * W międzyczasie użytkownik mógł:
         * - wybrać inną sondę
         * - kliknąć inny parametr.
         */

        if(
            String(
                rwAltV3Current?.serial
                ||
                ""
            )
            !==
            serial
            ||
            rwV8Mode!==mode
        ){

            return;
        }


        const points=
            rwV8LocalPoints(
                data.samples
                ||
                [],
                mode
            );


        rwV8DrawLocal(
            d,
            mode,
            points
        );


    }catch(err){

        console.error(
            "FLIGHT_PROFILE_SWITCHER_V8:",
            err
        );


        if(
            String(
                rwAltV3Current?.serial
                ||
                ""
            )
            !==
            serial
            ||
            rwV8Mode!==mode
        ){

            return;
        }


        if(info){

            info.textContent=
                "Nie udało się pobrać lokalnych danych Stacja lokalna.";
        }


        if(maxBadge){

            maxBadge.textContent=
                "brak danych";
        }
    }
}


/* ------------------------------------------------------------
   WRAPPER ISTNIEJĄCEGO V3

   Nie zmieniamy oryginalnej funkcji.
   W trybie wysokości wywołujemy ją 1:1.
------------------------------------------------------------ */

rwAltV3Render=
    function(d){

        const serial=
            String(
                d?.serial
                ||
                ""
            );


        /*
         * Nowa sonda zawsze zaczyna od bezpiecznego,
         * oryginalnego widoku wysokości.
         */

        if(
            rwV8LastSerial!==null
            &&
            serial
            !==
            rwV8LastSerial
        ){

            rwV8Mode=
                "alt";
        }


        rwV8LastSerial=
            serial;


        rwAltV3Current=
            d;


        if(
            rwV8Mode==="alt"
        ){

            rwV8OriginalRender(
                d
            );


            rwV8EnsureControls();

            rwV8UpdateControls();


            return;
        }


        rwV8RenderLocal(
            d,
            rwV8Mode
        );
    };


/*
 * Jeżeli V3 zdążył się już narysować przed załadowaniem
 * tego dodatku, dokładamy tylko przyciski.
 */

rwV8EnsureControls();

rwV8UpdateControls();

</script>


<!-- FINAL_UI_CLEANUP_V9 -->

<style id="final-ui-cleanup-v9-style">

/*
 * V8 jest głównym przełączanym wykresem.
 *
 * Ukrywamy stare odpowiedniki:
 *   SNR / czas
 *   wysokość / czas
 *   prędkość pionowa / czas
 *
 * Zostaje unikalny:
 *   SNR / odległość
 */

.v39-chart:has(#v39SnrTime),
.v39-chart:has(#v39AltTime),
.v39-chart:has(#v39VelTime){
    display:none !important;
}


/*
 * Pozostały wykres wykorzystuje pełną szerokość.
 */

.v39-chart:has(#v39SnrDistance){
    grid-column:1 / -1 !important;
    width:100% !important;
    max-width:none !important;
}

</style>


<!-- DETAIL_POLISH_V10 -->

<script id="detail-polish-v10-script">

/* ============================================================
   DETAIL_POLISH_V10

   Wyłącznie prezentacja danych:
   - Telemetria
   - Start / stacja startowa
   - Przewidywanie
   - Odzyskanie

   Backend / API / mapa / wykresy pozostają bez zmian.
   ============================================================ */


function v10Num(v){

    const n =
        Number(v);

    return Number.isFinite(n)
        ?
        n
        :
        null;
}


function v10Int(v){

    const n =
        v10Num(v);

    if(n===null){
        return "—";
    }

    return Math.round(n)
        .toLocaleString(
            "pl-PL"
        );
}


function v10Dec(
    v,
    digits=1
){

    const n =
        v10Num(v);

    if(n===null){
        return "—";
    }

    return n.toLocaleString(
        "pl-PL",
        {
            minimumFractionDigits:digits,
            maximumFractionDigits:digits
        }
    );
}


function v10Freq(v){

    const n =
        v10Num(v);

    if(n===null){
        return "—";
    }

    return n.toFixed(3)
        +
        " MHz";
}


function v10Coord(v){

    const n =
        v10Num(v);

    if(n===null){
        return "—";
    }

    return n.toFixed(5);
}


function v10Date(v){

    if(
        v===null
        ||
        v===undefined
        ||
        v===""
    ){
        return null;
    }

    const d =
        new Date(v);

    if(
        Number.isNaN(
            d.getTime()
        )
    ){
        return null;
    }

    return d;
}


function v10DateLocal(v){

    const d =
        v10Date(v);

    if(!d){
        return "—";
    }

    return new Intl.DateTimeFormat(
        "pl-PL",
        {
            timeZone:
                "Europe/Warsaw",

            year:
                "numeric",

            month:
                "2-digit",

            day:
                "2-digit",

            hour:
                "2-digit",

            minute:
                "2-digit",

            second:
                "2-digit"
        }
    ).format(d);
}


function v10DateUTC(v){

    const d =
        v10Date(v);

    if(!d){
        return "—";
    }

    return new Intl.DateTimeFormat(
        "pl-PL",
        {
            timeZone:
                "UTC",

            year:
                "numeric",

            month:
                "2-digit",

            day:
                "2-digit",

            hour:
                "2-digit",

            minute:
                "2-digit",

            second:
                "2-digit",

            hour12:
                false
        }
    ).format(d)
    +
    " UTC";
}


function v10SourceLabel(src){

    const s =
        String(
            src || ""
        );

    const labels = {

        "telemetry.launch_site":
            "stacja podana bezpośrednio przez telemetrię SondeHub",

        "nearest_to_reverse_prediction_approx":
            "stacja przybliżona na podstawie reverse prediction",

        "nearest_to_first_observed_position_approx":
            "stacja przybliżona na podstawie pierwszej zaobserwowanej pozycji sondy"
    };


    return labels[s]
        ||
        (
            s
            ?
            s
            :
            "brak informacji"
        );
}


function v10Schedule(times){

    if(
        !Array.isArray(times)
        ||
        !times.length
    ){
        return "—";
    }


    return times.map(
        value => {

            const s =
                String(value);


            const m =
                s.match(
                    /^(\d+):(\d{2}):(\d{2})$/
                );


            if(!m){
                return s;
            }


            const hours =
                String(
                    Number(m[1])
                ).padStart(
                    2,
                    "0"
                );


            const minutes =
                String(
                    Number(m[2])
                ).padStart(
                    2,
                    "0"
                );


            return (
                hours
                +
                ":"
                +
                minutes
                +
                " UTC"
            );
        }
    ).join(
        ", "
    );
}


function v10Bool(v){

    if(v===true){
        return "TAK";
    }

    if(v===false){
        return "NIE";
    }

    return "—";
}


function v10ValidTemperature(v){

    const n =
        v10Num(v);

    return (
        n!==null
        &&
        n > -150
        &&
        n < 100
    )
    ?
    n
    :
    null;
}


function v10ValidHumidity(v){

    const n =
        v10Num(v);

    return (
        n!==null
        &&
        n>=0
        &&
        n<=100
    )
    ?
    n
    :
    null;
}


function v10ValidPressure(v){

    const n =
        v10Num(v);

    return (
        n!==null
        &&
        n>0
        &&
        n<1200
    )
    ?
    n
    :
    null;
}


/* ------------------------------------------------------------
   TELEMETRIA
------------------------------------------------------------ */

function v10Telemetry(d){

    const el =
        document.getElementById(
            "telemetry"
        );


    if(!el){
        return;
    }


    const t =
        d.latest
        ||
        {};


    const temp =
        v10ValidTemperature(
            t.temp
        );


    const humidity =
        v10ValidHumidity(
            t.humidity
        );


    const pressure =
        v10ValidPressure(
            t.pressure
        );


    const batt =
        t.batt
        ??
        t.battery
        ??
        t.battery_v;


    const type =
        t.subtype
        ||
        t.type
        ||
        "—";


    const rows = [

        kv(
            "Odbieramy lokalnie teraz",
            d.local_receive_now
                ?
                "TAK"
                :
                "NIE"
        ),

        kv(
            "Odebrana lokalnie dziś",
            d.local_received_today
                ?
                "TAK"
                :
                "NIE"
        ),

        kv(
            "Ostatni uploader",
            t.uploader_callsign
            ||
            "—"
        ),

        '<hr style="border:0;border-top:1px solid #33404e;margin:10px 0">',

        kv(
            "Typ",
            type
        ),

        kv(
            "Producent",
            t.manufacturer
            ||
            "—"
        ),

        kv(
            "Częstotliwość",
            v10Freq(
                t.frequency
            )
        ),

        kv(
            "Wysokość",
            v10Num(t.alt)===null
                ?
                "—"
                :
                v10Int(t.alt)
                +
                " m"
        ),

        kv(
            "Prędkość pozioma",
            v10Num(t.vel_h)===null
                ?
                "—"
                :
                v10Dec(t.vel_h,1)
                +
                " m/s"
        ),

        kv(
            "Prędkość pionowa",
            v10Num(t.vel_v)===null
                ?
                "—"
                :
                v10Dec(t.vel_v,1)
                +
                " m/s"
        ),

        kv(
            "Kierunek",
            v10Num(t.heading)===null
                ?
                "—"
                :
                v10Int(t.heading)
                +
                "°"
        ),

        kv(
            "Temperatura",
            temp===null
                ?
                "—"
                :
                v10Dec(temp,1)
                +
                " °C"
        ),

        kv(
            "Wilgotność",
            humidity===null
                ?
                "—"
                :
                v10Dec(humidity,1)
                +
                " %"
        ),

        kv(
            "Ciśnienie",
            pressure===null
                ?
                "—"
                :
                v10Dec(pressure,1)
                +
                " hPa"
        ),

        kv(
            "Satelity GPS",
            v10Num(t.sats)===null
                ?
                "—"
                :
                v10Int(t.sats)
        ),

        kv(
            "Bateria",
            v10Num(batt)===null
                ?
                "—"
                :
                v10Dec(batt,2)
                +
                " V"
        ),

        kv(
            "SNR",
            v10Num(t.snr)===null
                ?
                "—"
                :
                v10Dec(t.snr,1)
                +
                " dB"
        ),

        kv(
            "RSSI",
            v10Num(t.rssi)===null
                ?
                "—"
                :
                v10Dec(t.rssi,1)
                +
                " dBm"
        ),

        kv(
            "Czas lokalny",
            v10DateLocal(
                t.datetime
            )
        ),

        kv(
            "Czas UTC",
            v10DateUTC(
                t.datetime
            )
        )
    ];


    el.innerHTML =
        rows.join("");
}


/* ------------------------------------------------------------
   STACJA STARTOWA
------------------------------------------------------------ */

function v10Site(d){

    const el =
        document.getElementById(
            "site"
        );


    if(!el){
        return;
    }


    const launch =
        d.launch_site
        ||
        {};


    const st =
        launch.site
        ||
        null;


    if(!st){

        el.innerHTML =
            "Brak wiarygodnego dopasowania stacji startowej.";

        return;
    }


    const src =
        String(
            launch.source
            ||
            ""
        );


    const title =
        src==="telemetry.launch_site"
        ?
        "Stacja startowa SondeHub"
        :
        "Stacja przybliżona";


    const rows = [

        kv(
            title,
            st.station_name
            ||
            st.station
            ||
            st.site_id
            ||
            "—"
        ),

        kv(
            "Sposób ustalenia",
            v10SourceLabel(
                src
            )
        ),

        kv(
            "ID stacji",
            st.site_id
            ||
            "—"
        ),

        kv(
            "Od punktu odniesienia",
            v10Num(
                st.distance_km
            )===null
            ?
            "—"
            :
            v10Dec(
                st.distance_km,
                1
            )
            +
            " km"
        ),

        kv(
            "Wysokość stacji",
            v10Num(st.alt)===null
            ?
            "—"
            :
            v10Int(st.alt)
            +
            " m"
        ),

        kv(
            "Harmonogram",
            v10Schedule(
                st.times
            )
        ),

        kv(
            "Typy wg SondeHub",
            Array.isArray(
                st.rs_types
            )
            ?
            st.rs_types.join(
                ", "
            )
            :
            (
                st.rs_types
                ??
                "—"
            )
        )
    ];


    el.innerHTML =
        rows.join("");
}


/* ------------------------------------------------------------
   PRZEWIDYWANIE
------------------------------------------------------------ */

function v10Prediction(d){

    const el =
        document.getElementById(
            "prediction"
        );


    if(!el){
        return;
    }


    const prediction =
        d.prediction
        ||
        {};


    const reverse =
        d.reverse_prediction
        ||
        {};


    const pp =
        prediction.point
        ||
        null;


    const rp =
        reverse.point
        ||
        null;


    const flight =
        d.flight
        ||
        {};


    const rows = [];


    if(pp){

        rows.push(
            kv(
                "Przewidywane lądowanie",
                v10Coord(pp.lat)
                +
                ", "
                +
                v10Coord(pp.lon)
            )
        );


        if(
            v10Num(pp.alt)!==null
        ){

            rows.push(
                kv(
                    "Wysokość punktu predykcji",
                    v10Int(pp.alt)
                    +
                    " m"
                )
            );
        }


        rows.push(
            kv(
                "Czas lokalny",
                v10DateLocal(
                    pp.time
                )
            )
        );


        rows.push(
            kv(
                "Czas UTC",
                v10DateUTC(
                    pp.time
                )
            )
        );

    }else{

        rows.push(
            kv(
                "Przewidywane lądowanie",
                "brak aktualnej predykcji"
            )
        );
    }


    rows.push(
        kv(
            "Przewidywany start",
            rp
            ?
            (
                v10Coord(rp.lat)
                +
                ", "
                +
                v10Coord(rp.lon)
            )
            :
            "brak"
        )
    );


    rows.push(
        kv(
            "Punkty bieżącego lotu",
            Number(
                flight.samples
                ||
                0
            ).toLocaleString(
                "pl-PL"
            )
        )
    );


    rows.push(
        kv(
            "Maks. wysokość",
            v10Num(
                flight.max_alt_m
            )===null
            ?
            "—"
            :
            v10Int(
                flight.max_alt_m
            )
            +
            " m"
        )
    );


    el.innerHTML =
        rows.join("");
}


/* ------------------------------------------------------------
   ODZYSKANIE
------------------------------------------------------------ */

function v10Recovery(d){

    const el =
        document.getElementById(
            "recovery"
        );


    if(!el){
        return;
    }


    const rec =
        (
            d.recovery
            ||
            {}
        ).latest
        ||
        null;


    if(!rec){

        el.innerHTML =
            '<span class="muted">Brak zgłoszenia odzyskania w SondeHub.</span>';

        return;
    }


    let status =
        "ZGŁOSZENIE";


    if(
        rec.recovered===true
    ){

        status =
            "ODZYSKANA";

    }else if(
        rec.planned===true
    ){

        status =
            "PLANOWANE ODZYSKANIE";

    }else if(
        rec.recovered===false
        &&
        rec.planned===false
    ){

        status =
            "NIEODZYSKANA";
    }


    const rows = [

        kv(
            "Status",
            status
        ),

        kv(
            "Odzyskana",
            v10Bool(
                rec.recovered
            )
        ),

        kv(
            "Planowane odzyskanie",
            v10Bool(
                rec.planned
            )
        ),

        kv(
            "Zgłaszający",
            rec.recovered_by
            ||
            rec.uploader_callsign
            ||
            "—"
        ),

        kv(
            "Pozycja",
            (
                v10Num(rec.lat)!==null
                &&
                v10Num(rec.lon)!==null
            )
            ?
            (
                v10Coord(rec.lat)
                +
                ", "
                +
                v10Coord(rec.lon)
            )
            :
            "—"
        ),

        kv(
            "Czas lokalny",
            v10DateLocal(
                rec.datetime
            )
        ),

        kv(
            "Czas UTC",
            v10DateUTC(
                rec.datetime
            )
        ),

        kv(
            "Opis",
            rec.description
            ||
            "—"
        )
    ];


    el.innerHTML =
        rows.join("");
}


/* ------------------------------------------------------------
   RENDER KOŃCOWY
------------------------------------------------------------ */

function v10PolishDetail(d){

    if(!d){
        return;
    }

    v10Telemetry(d);
    v10Site(d);
    v10Prediction(d);
    v10Recovery(d);
}

</script>

</body>
</html>
'''



# ============================================================
# WATCH_TO_SONDEHUB_V11_1
# Modyfikujemy konkretnie HTML zmiennej PAGE.
# ============================================================

WATCH_TO_SONDEHUB_V11_1_HTML = r"""
<!-- WATCH_TO_SONDEHUB_V11_1 -->

<script id="watch-to-sondehub-v11-1-plus-script">

(function(){

"use strict";


function v11SerialFromUrl(){

    const params =
        new URLSearchParams(
            window.location.search
        );


    const serial =
        String(
            params.get("serial")
            ||
            ""
        ).trim();


    if(
        !/^[A-Za-z0-9._-]{3,64}$/.test(
            serial
        )
    ){
        return null;
    }


    return serial;
}


async function v11OpenSerialFromUrl(){

    const serial =
        v11SerialFromUrl();


    if(!serial){
        return;
    }


    if(
        typeof showDetail
        !==
        "function"
    ){

        console.error(
            "V11.1: showDetail() niedostępne"
        );

        return;
    }


    try{

        await showDetail(
            serial
        );


        const title =
            document.getElementById(
                "detailTitle"
            );


        if(title){

            title.scrollIntoView({
                behavior:
                    "smooth",

                block:
                    "start"
            });
        }


        document.title =
            "Sonda "
            +
            serial
            +
            " — SondeHub+";


    }catch(err){

        console.error(
            "WATCH_TO_SONDEHUB_V11_1:",
            err
        );
    }
}


if(
    document.readyState
    ===
    "loading"
){

    document.addEventListener(
        "DOMContentLoaded",
        v11OpenSerialFromUrl,
        {
            once:true
        }
    );

}else{

    v11OpenSerialFromUrl();
}


})();

</script>
"""


if "</body>" not in PAGE:

    raise RuntimeError(
        "SONDEHUB+ V11.1: PAGE nie zawiera </body>"
    )


PAGE = PAGE.replace(
    "</body>",
    WATCH_TO_SONDEHUB_V11_1_HTML
    +
    "\n</body>",
    1
)



# ============================================================
# MAP_ENHANCEMENT_V13_2
# ============================================================

MAP_ENHANCEMENT_V13_2_HTML = r"""
<!-- MAP_ENHANCEMENT_V13_2 -->

<style id="map-enhancement-v13-2-style">

.v13-map-legend{
    background:rgba(11,24,36,.94);
    color:#eaf2fb;
    border:1px solid #49647d;
    border-radius:7px;
    padding:8px 10px;
    font-size:12px;
    line-height:1.55;
    box-shadow:0 2px 8px rgba(0,0,0,.30);
}

.v13-map-legend b{
    display:block;
    margin-bottom:4px;
}

.v13-dot{
    display:inline-block;
    width:10px;
    height:10px;
    border-radius:50%;
    margin-right:6px;
}

</style>

<script id="map-enhancement-v13-2-script">

(function(){

"use strict";

const redrawBeforeV13_2 = redraw;

let v13FieldKm = 408.5;
let v13Legend = null;


function v13Num(v){

    const n = Number(v);

    return Number.isFinite(n)
        ? n
        : null;
}


function v13Distance(lat1,lon1,lat2,lon2){

    if(typeof saDistanceV38 === "function"){

        const x = saDistanceV38(
            lat1,
            lon1,
            lat2,
            lon2
        );

        if(Number.isFinite(Number(x))){
            return Number(x);
        }
    }


    lat1=v13Num(lat1);
    lon1=v13Num(lon1);
    lat2=v13Num(lat2);
    lon2=v13Num(lon2);

    if(
        lat1===null ||
        lon1===null ||
        lat2===null ||
        lon2===null
    ){
        return null;
    }


    const rad=x=>x*Math.PI/180;
    const R=6371;

    const dlat=rad(lat2-lat1);
    const dlon=rad(lon2-lon1);

    const a=
        Math.sin(dlat/2)**2
        +
        Math.cos(rad(lat1))
        *
        Math.cos(rad(lat2))
        *
        Math.sin(dlon/2)**2;

    return 2*R*Math.atan2(
        Math.sqrt(a),
        Math.sqrt(1-a)
    );
}


/* BURST_FIX_V13_3 */

function v13BurstPoint(d){

    const track =
        Array.isArray((d.flight||{}).track)
        ? d.flight.track
        : [];


    let best=null;


    for(const p of track){

        const alt=v13Num(p.alt);
        const lat=v13Num(p.lat);
        const lon=v13Num(p.lon);

        if(
            alt===null ||
            lat===null ||
            lon===null
        ){
            continue;
        }


        if(
            !best ||
            alt > best.alt
        ){
            best={
                point:p,
                alt:alt
            };
        }
    }


    if(!best){
        return null;
    }


    const latest=
        d.latest || {};


    const vv=
        v13Num(
            latest.vel_v
        );


    const latestAlt=
        v13Num(
            latest.alt
        );


    /*
     * BURST uznajemy za potwierdzony dopiero gdy:
     *
     * 1. aktualna prędkość pionowa jest ujemna
     *    i sonda rzeczywiście opada,
     *
     * ALBO
     *
     * 2. aktualna wysokość jest wyraźnie niższa
     *    od maksimum śladu.
     *
     * Dzięki temu podczas wznoszenia najwyższy
     * aktualny punkt nie jest błędnie nazywany burstem.
     */

    let confirmed=false;


    if(
        vv !== null
        &&
        vv < -0.5
    ){
        confirmed=true;
    }


    if(
        latestAlt !== null
        &&
        best.alt - latestAlt >= 300
    ){
        confirmed=true;
    }


    return {
        point:best.point,
        alt:best.alt,
        confirmed:confirmed,
        vel_v:vv,
        latest_alt:latestAlt
    };
}


function v13ClosestPoint(d){

    if(
        !HOME ||
        v13Num(HOME.lat)===null ||
        v13Num(HOME.lon)===null
    ){
        return null;
    }


    let points =
        Array.isArray((d.prediction||{}).path)
        ? d.prediction.path
        : [];


    let source="prognoza";


    if(!points.length){

        if(d.latest){

            points=[d.latest];
            source="aktualna pozycja";

        }else{

            points=
                Array.isArray((d.flight||{}).track)
                ? d.flight.track
                : [];

            source="historia lotu";
        }
    }


    let best=null;


    for(const p of points){

        const lat=v13Num(p.lat);
        const lon=v13Num(p.lon);

        if(
            lat===null ||
            lon===null
        ){
            continue;
        }


        const dist=
            v13Distance(
                HOME.lat,
                HOME.lon,
                lat,
                lon
            );


        if(dist===null){
            continue;
        }


        if(
            !best ||
            dist < best.distance
        ){
            best={
                point:p,
                distance:dist,
                source:source
            };
        }
    }


    return best;
}


function v13Marker(
    lat,
    lon,
    label,
    color
){

    lat=v13Num(lat);
    lon=v13Num(lon);

    if(
        lat===null ||
        lon===null ||
        !layer
    ){
        return;
    }


    L.circleMarker(
        [lat,lon],
        {
            radius:8,
            color:color,
            fillColor:color,
            fillOpacity:.92,
            weight:2
        }
    )
    .bindPopup(label)
    .addTo(layer);
}


function v13EnsureLegend(){

    if(
        !map ||
        v13Legend
    ){
        return;
    }


    v13Legend=L.control({
        position:"bottomleft"
    });


    v13Legend.onAdd=function(){

        const div=
            L.DomUtil.create(
                "div",
                "v13-map-legend"
            );


        div.innerHTML=`
            <b>Legenda mapy</b>
            <div>🔵 ślad sondy</div>
            <div>┄┄ prognoza SondeHub</div>
            <div>
              <span class="v13-dot"
                    style="background:#ffb020"></span>
              najwyższy punkt / BURST po wykryciu
            </div>
            <div>
              <span class="v13-dot"
                    style="background:#a66cff"></span>
              najbliżej Stacja lokalna
            </div>
            <!-- REMOVE_MAP_COVERAGE_V13_4_FIXED -->
        `;


        L.DomEvent.disableClickPropagation(div);

        return div;
    };


    v13Legend.addTo(map);
}


function v13EnhanceMap(d){

    if(
        !map ||
        !layer
    ){
        return;
    }


    /*
     * REMOVE_MAP_COVERAGE_V13_4_FIXED
     *
     * Zielone pole odbioru wyłączone na mapie.
     * Statystyki zasięgu pozostają bez zmian.
     */

    if(
        false &&
        HOME &&
        v13Num(HOME.lat)!==null &&
        v13Num(HOME.lon)!==null
    ){

        L.circle(
            [HOME.lat,HOME.lon],
            {
                radius:v13FieldKm*1000,
                color:"#39d98a",
                weight:1,
                opacity:.65,
                fillOpacity:.025,
                dashArray:"7 7"
            }
        )
        .bindPopup(
            "Empiryczne pole odbioru Stacja lokalna: ≤"
            +
            Math.round(v13FieldKm)
            +
            " km"
        )
        .addTo(layer);
    }


    /*
     * Burst = najwyższy punkt śladu.
     */

    const burst=
        v13BurstPoint(d);


    if(burst){

        const burstLabel=
            burst.confirmed
            ?
            (
                "BURST / maks. wysokość: "
                +
                Math.round(burst.alt)
                +
                " m"
            )
            :
            (
                "Najwyższy punkt dotychczas: "
                +
                Math.round(burst.alt)
                +
                " m"
                +
                (
                    burst.vel_v !== null
                    ?
                    " • v pionowa "
                    +
                    burst.vel_v.toFixed(1)
                    +
                    " m/s"
                    :
                    ""
                )
            );


        const burstColor=
            burst.confirmed
            ?
            "#ff9500"
            :
            "#ffd166";


        v13Marker(
            burst.point.lat,
            burst.point.lon,
            burstLabel,
            burstColor
        );
    }


    /*
     * Punkt najbliższego podejścia.
     */

    const closest=
        v13ClosestPoint(d);


    if(closest){

        let label=
            "Najbliżej Stacja lokalna: "
            +
            closest.distance.toFixed(1)
            +
            " km";


        const alt=
            v13Num(
                closest.point.alt
            );


        if(alt!==null){

            label+=
                " • "
                +
                Math.round(alt)
                +
                " m";
        }


        label+=
            " • "
            +
            closest.source;


        v13Marker(
            closest.point.lat,
            closest.point.lon,
            label,
            "#a66cff"
        );
    }


    v13EnsureLegend();
}


redraw=function(d){

    redrawBeforeV13_2(d);

    try{

        v13EnhanceMap(d);

    }catch(err){

        console.error(
            "MAP_ENHANCEMENT_V13_2:",
            err
        );
    }
};


/*
 * Raz po załadowaniu strony pobieramy aktualne
 * statystyki 7 dni i aktualizujemy promień pola.
 *
 * Bez timerów i bez obserwatorów DOM.
 */

fetch("/api/station-stats-7d")

    .then(r=>{

        if(!r.ok){
            throw new Error(
                "HTTP "+r.status
            );
        }

        return r.json();
    })

    .then(data=>{

        const max=
            v13Num(
                data.max_distance_km
            );


        if(max!==null){

            v13FieldKm=
                Math.min(
                    600,
                    Math.max(
                        100,
                        max*1.10
                    )
                );
        }


        if(window.__SONDE_DETAIL_V39){

            redraw(
                window.__SONDE_DETAIL_V39
            );
        }
    })

    .catch(err=>{

        console.warn(
            "V13.2 station stats:",
            err
        );
    });


})();

</script>
"""


if "</body>" not in PAGE:
    raise RuntimeError(
        "V13.2: PAGE nie zawiera </body>"
    )


PAGE = PAGE.replace(
    "</body>",
    MAP_ENHANCEMENT_V13_2_HTML
    +
    "\n</body>",
    1
)



# SONDEHUB_PERF_CACHE_V3_BEGIN
#
# Cache + single-flight dla ciężkich operacji.
#
# Nie zmienia formatu API.
# Nie zmienia frontendu.
#

import threading as _pc_threading
import time as _pc_time


_PC_CACHE = {}
_PC_LOCKS = {}

_PC_MASTER = _pc_threading.Lock()

_PC_CACHE_MAX = 64


def _pc_cached(
    key,
    ttl,
    loader
):
    now = _pc_time.monotonic()


    with _PC_MASTER:

        item = _PC_CACHE.get(
            key
        )

        if (
            item is not None
            and
            now - item["ts"] < ttl
        ):
            return item["value"]


        lock = _PC_LOCKS.get(
            key
        )

        if lock is None:

            lock = _pc_threading.Lock()

            _PC_LOCKS[key] = lock


    #
    # Jeden ciężki worker na konkretny klucz.
    #
    with lock:

        now = _pc_time.monotonic()


        with _PC_MASTER:

            item = _PC_CACHE.get(
                key
            )

            if (
                item is not None
                and
                now - item["ts"] < ttl
            ):
                return item["value"]


            have_stale = (
                item is not None
            )

            stale = (
                item["value"]
                if have_stale
                else None
            )


        try:

            value = loader()

        except Exception:

            #
            # Gdy zewnętrzne API chwilowo padnie,
            # lepiej podać poprzedni poprawny wynik.
            #
            if have_stale:
                return stale

            raise


        with _PC_MASTER:

            _PC_CACHE[key] = {
                "ts":
                    _pc_time.monotonic(),

                "value":
                    value,
            }


            #
            # Nie pozwalamy cache rosnąć bez końca.
            #
            if len(_PC_CACHE) > _PC_CACHE_MAX:

                ordered = sorted(
                    _PC_CACHE.items(),
                    key=lambda kv:
                        kv[1]["ts"]
                )

                excess = (
                    len(_PC_CACHE)
                    -
                    _PC_CACHE_MAX
                )

                for old_key, _ in ordered[:excess]:

                    _PC_CACHE.pop(
                        old_key,
                        None
                    )


        return value


#
# Zachowujemy oryginalne implementacje.
#

_pc_orig_station_status = station_status

_pc_orig_local_rx_sessions = local_rx_sessions

_pc_orig_station_stats = v39_station_stats

_pc_orig_detail = detail

_pc_orig_local_received_today = local_received_today


def local_received_today():

    return _pc_cached(
        (
            "received_today",
        ),
        30.0,
        _pc_orig_local_received_today
    )


def local_rx_sessions(
    hours=24.0
):

    if hours is None:

        key = (
            "local_rx",
            "all",
        )

        ttl = 300.0

    else:

        h = float(
            hours
        )

        key = (
            "local_rx",
            h,
        )

        ttl = 60.0


    return _pc_cached(
        key,
        ttl,
        lambda:
            _pc_orig_local_rx_sessions(
                hours
            )
    )


def v39_station_stats(
    days=7
):

    d = float(
        days
    )

    return _pc_cached(
        (
            "station_stats",
            d,
        ),
        300.0,
        lambda:
            _pc_orig_station_stats(
                days
            )
    )


def station_status():

    return _pc_cached(
        (
            "station_status",
        ),
        5.0,
        _pc_orig_station_status
    )


# SONDEHUB_DETAIL_SERIAL_V15
# SONDEHUB_DETAIL_CACHE_V16
#
# Jeden ciężki detail naraz +
# osobny ograniczony cache szczegółów.
#
# Maksymalnie 6 sond.
# TTL = 60 sekund.
#
# Cache V3 dla innych danych pozostaje bez zmian.
#

_PC_DETAIL_GATE = _pc_threading.Lock()

_PC_DETAIL_CACHE_V16 = {}

_PC_DETAIL_CACHE_LOCK_V16 = (
    _pc_threading.Lock()
)

_PC_DETAIL_TTL_V16 = 60.0

_PC_DETAIL_MAX_V16 = 6


def _pc_detail_cache_get_v16(
    serial_key
):

    now = _pc_time.monotonic()


    with _PC_DETAIL_CACHE_LOCK_V16:

        #
        # Aktywne usuwanie wygasłych wpisów.
        #
        expired = [
            key
            for key, item
            in _PC_DETAIL_CACHE_V16.items()
            if (
                now
                -
                item["ts"]
                >=
                _PC_DETAIL_TTL_V16
            )
        ]


        for key in expired:

            _PC_DETAIL_CACHE_V16.pop(
                key,
                None
            )


        item = _PC_DETAIL_CACHE_V16.get(
            serial_key
        )


        if item is None:

            return (
                False,
                None,
            )


        #
        # Aktualizujemy LRU.
        #
        item["last"] = now


        return (
            True,
            item["value"],
        )


def _pc_detail_cache_put_v16(
    serial_key,
    value
):

    now = _pc_time.monotonic()


    with _PC_DETAIL_CACHE_LOCK_V16:

        #
        # Najpierw wyrzucamy wszystko,
        # co już przekroczyło TTL.
        #
        expired = [
            key
            for key, item
            in _PC_DETAIL_CACHE_V16.items()
            if (
                now
                -
                item["ts"]
                >=
                _PC_DETAIL_TTL_V16
            )
        ]


        for key in expired:

            _PC_DETAIL_CACHE_V16.pop(
                key,
                None
            )


        _PC_DETAIL_CACHE_V16[
            serial_key
        ] = {
            "ts": now,
            "last": now,
            "value": value,
        }


        #
        # Twardy limit liczby szczegółów.
        #
        while (
            len(_PC_DETAIL_CACHE_V16)
            >
            _PC_DETAIL_MAX_V16
        ):

            oldest_key = min(
                _PC_DETAIL_CACHE_V16,
                key=lambda key:
                    _PC_DETAIL_CACHE_V16[
                        key
                    ]["last"]
            )


            _PC_DETAIL_CACHE_V16.pop(
                oldest_key,
                None
            )


def _pc_detail_cached_v16(
    serial,
    serial_key
):

    hit, value = (
        _pc_detail_cache_get_v16(
            serial_key
        )
    )


    if hit:

        print(
            "[DETAIL-V16]"
            f" HIT serial={serial_key}",
            flush=True,
        )

        return value


    wait_start = (
        _pc_time.monotonic()
    )


    #
    # Globalny gate V15.
    #
    with _PC_DETAIL_GATE:

        waited = (
            _pc_time.monotonic()
            -
            wait_start
        )


        #
        # Drugi check po oczekiwaniu.
        #
        # Jeżeli inny request właśnie policzył
        # tę samą sondę, nie liczymy jej ponownie.
        #
        hit, value = (
            _pc_detail_cache_get_v16(
                serial_key
            )
        )


        if hit:

            print(
                "[DETAIL-V16]"
                f" HIT-AFTER-WAIT"
                f" serial={serial_key}"
                f" waited={waited:.3f}s",
                flush=True,
            )

            return value


        run_start = (
            _pc_time.monotonic()
        )


        print(
            "[DETAIL-V16]"
            f" MISS serial={serial_key}"
            f" waited={waited:.3f}s"
            f" threads={_pc_threading.active_count()}",
            flush=True,
        )


        try:

            value = _pc_orig_detail(
                serial
            )


            _pc_detail_cache_put_v16(
                serial_key,
                value
            )


            return value


        finally:

            elapsed = (
                _pc_time.monotonic()
                -
                run_start
            )


            with _PC_DETAIL_CACHE_LOCK_V16:

                count = len(
                    _PC_DETAIL_CACHE_V16
                )


            print(
                "[DETAIL-V16]"
                f" DONE serial={serial_key}"
                f" elapsed={elapsed:.3f}s"
                f" cache={count}"
                f" threads={_pc_threading.active_count()}",
                flush=True,
            )


def detail(
    serial
):

    try:

        serial_key = normalize_serial(
            serial
        )

    except Exception:

        serial_key = str(
            serial
        )


    return _pc_detail_cached_v16(
        serial,
        serial_key
    )


# koniec performance cache V3



# SONDEHUB_SONDES_CACHE_V4
#
# Pełna lista /api/sondes jest stosunkowo duża
# i frontend potrafi żądać jej wielokrotnie.
#
# Budujemy ją maksymalnie raz na 30 sekund.
#

def _pc_build_sondes_payload_v4():

    d = watch_json(
        "/api/radiosondes"
    )

    rows = (
        d.get("radiosondes")
        or
        []
    )


    today = set(
        local_received_today()
    )


    with LOCK:

        latest_map = dict(
            RT["latest"]
        )

        available = set(
            latest_map
        )


    for x in rows:

        if not isinstance(
            x,
            dict
        ):
            continue


        serial = normalize_serial(
            x.get("serial")
        )


        rt = latest_map.get(
            serial
        )


        x["realtime_available"] = (
            serial in available
        )

        x["local_receive_now"] = (
            is_local_uploader(rt)
        )

        x["local_received_today"] = (
            serial in today
        )


        if isinstance(
            rt,
            dict
        ):

            x["rt_uploader_callsign"] = (
                rt.get(
                    "uploader_callsign"
                )
            )

            x["rt_snr"] = (
                rt.get("snr")
            )

            x["rt_rssi"] = (
                rt.get("rssi")
            )

            x["rt_datetime"] = (
                rt.get("datetime")
            )

        else:

            x["rt_uploader_callsign"] = None
            x["rt_snr"] = None
            x["rt_rssi"] = None
            x["rt_datetime"] = None


    return d



# SONDEHUB_PUBLIC_HOST_V34
def watch_browser_url(request_host=""):
    try:
        parsed = urllib.parse.urlparse(WATCH)

        scheme = (
            parsed.scheme
            if parsed.scheme in ("http", "https")
            else "http"
        )

        host = parsed.hostname
        port = parsed.port

    except Exception:
        return WATCH

    if host in {
        "127.0.0.1",
        "localhost",
        "::1",
    }:
        try:
            request_parsed = urllib.parse.urlparse(
                "//" + str(request_host or "")
            )

            browser_host = request_parsed.hostname

        except Exception:
            browser_host = None

        if browser_host:
            host = browser_host

    if not host:
        return WATCH

    if ":" in host:
        display_host = f"[{host}]"
    else:
        display_host = host

    netloc = display_host

    if port is not None:
        netloc += f":{port}"

    return urllib.parse.urlunsplit(
        (
            scheme,
            netloc,
            parsed.path or "/",
            parsed.query,
            "",
        )
    )



# SONDEHUB_PUBLIC_CALLSIGN_V36C
# SONDEHUB_PUBLIC_INLINE_JSON_V45
def render_page():
    configured_callsign = (
        CALLSIGN.strip().lower()
        or
        "__sondehub_plus_callsign_not_configured__"
    )

    encoded_callsign = json.dumps(
        configured_callsign,
        ensure_ascii=True,
    )

    encoded_callsign = (
        encoded_callsign
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
        .replace("&", "\\u0026")
    )

    return PAGE.replace(
        "__SONDEHUB_LISTENER_CALLSIGN_JSON_V36C__",
        encoded_callsign,
    )


class Handler(BaseHTTPRequestHandler):
    server_version = "RadiosondeSondeHubPlus/1.1"

    def log_message(self, fmt, *args):
        print("[HTTP]", fmt % args, flush=True)

    def send_json(self, obj, code=200):
        raw = json.dumps(obj, ensure_ascii=False, indent=2, default=str).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(raw)

    def send_html(self, text):
        raw = text.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(raw)

    def send_redirect(self, location, code=302):
        self.send_response(code)
        self.send_header(
            "Location",
            str(location),
        )
        self.send_header(
            "Content-Length",
            "0",
        )
        self.send_header(
            "Cache-Control",
            "no-store",
        )
        self.end_headers()

    def fail(self, e, code=502):
        self.send_json({"error": f"{type(e).__name__}: {e}"}, code)

    def do_GET(self):
        u = urllib.parse.urlparse(self.path)
        path = u.path
        q = urllib.parse.parse_qs(u.query)

        try:
            if path == "/":
                return self.send_html(render_page())

            if path == "/watch":
                return self.send_redirect(
                    watch_browser_url(
                        self.headers.get(
                            "Host",
                            "",
                        )
                    )
                )

            if path == "/healthz":
                return self.send_json({"ok": True, "time": iso_now()})

            if path == "/api/status":
                return self.send_json(station_status())

            # LOCAL_RX_RANGE_V30_ENDPOINTS

            if path == "/api/local-receptions-7d":

                rows=local_rx_sessions(
                    168.0
                )

                return self.send_json({
                    "source":
                        "local auto_rx *_sonde.log",
                    "range":
                        "7d",
                    "hours":
                        168,
                    "count":
                        len(rows),
                    "receptions":
                        rows,
                })


            if path == "/api/local-receptions-all":

                rows=local_rx_sessions(
                    None
                )

                return self.send_json({
                    "source":
                        "local auto_rx *_sonde.log",
                    "range":
                        "all",
                    "hours":
                        None,
                    "count":
                        len(rows),
                    "receptions":
                        rows,
                })



            # FULL_ANALYTICS_V39 — lokalne próbki i statystyki
            if self.path.startswith("/api/local-flight"):
                from urllib.parse import urlparse, parse_qs
                _u=urlparse(self.path)
                _q=parse_qs(_u.query)
                _serial=(_q.get("serial") or [""])[0]
                return self.send_json(
                    v39_local_flight(_serial)
                )

            if self.path.startswith("/api/station-stats-7d"):
                return self.send_json(
                    v39_station_stats(7)
                )

            if path == "/api/local-receptions":

                rows=local_rx_sessions(
                    24.0
                )

                return self.send_json({

                    "source":
                        "local auto_rx *_sonde.log",

                    "hours":
                        24,

                    "count":
                        len(rows),

                    "receptions":
                        rows,

                    "timestamp":
                        iso_now(),
                })


            if path == "/api/sondes":

                d = _pc_cached(
                    ("sondes_payload_v4",),
                    30.0,
                    _pc_build_sondes_payload_v4
                )

                return self.send_json(d)

            if path == "/api/realtime":
                serial = (q.get("serial") or [""])[0]
                if not serial:
                    return self.send_json({"error": "serial required"}, 400)
                return self.send_json({
                    "serial": normalize_serial(serial),
                    "telemetry": realtime_for(serial),
                    "stream": realtime_snapshot(),
                })

            if path == "/api/sonde":
                serial = (q.get("serial") or [""])[0]
                if not serial:
                    return self.send_json({"error": "serial required"}, 400)
                return self.send_json(detail(serial))

            if path == "/api/sites":
                distance = float((q.get("distance_km") or ["1000"])[0])
                distance = max(1.0, min(distance, 3000.0))
                rows = filtered_sites(distance)
                return self.send_json({"distance_km": distance, "count": len(rows), "sites": rows})

            if path == "/api/site-sondes":
                site = (q.get("site") or [""])[0]
                last = int(float((q.get("last") or ["86400"])[0]))
                last = max(0, min(last, 604800))
                if not site:
                    return self.send_json({"error": "site required"}, 400)
                d = api_json("/sondes/site/" + urllib.parse.quote(site, safe=""), {"last": last})
                return self.send_json({"site": site, "last": last, "data": d})

            if path == "/api/listener":
                return self.send_json(own_listener())

            if path == "/api/listeners":
                distance = float((q.get("distance_km") or ["1000"])[0])
                distance = max(1.0, min(distance, 3000.0))
                rows = nearby_listeners(distance)
                return self.send_json({
                    "distance_km": distance,
                    "count": len(rows),
                    "note": "SondeHub /listeners/telemetry has no distance parameter; filtering is local after one on-demand 3h query.",
                    "listeners": rows,
                })

            if path == "/api/recovery-stats":
                distance = float((q.get("distance_km") or ["1000"])[0])
                distance = max(1.0, min(distance, 3000.0))
                return self.send_json(recovery_stats(distance))

            if path == "/api/amateur":
                distance = float((q.get("distance_km") or ["1000"])[0])
                last = int(float((q.get("last") or ["21600"])[0]))
                h = home()
                d = api_json(
                    "/amateur",
                    {
                        "lat": h["lat"],
                        "lon": h["lon"],
                        "distance": int(distance * 1000),
                        "last": last,
                    },
                )
                return self.send_json({"distance_km": distance, "last": last, "data": d})

            if path == "/api/listener-stats":
                return self.send_json(api_json("/listeners/stats"))

            if path == "/api/websocket-info":
                url = cached("ws-info", 600, lambda: get_text_url(API + "/sondes/websocket"))
                p = urllib.parse.urlparse(url)
                return self.send_json({
                    "scheme": p.scheme,
                    "host": p.hostname,
                    "path": p.path,
                    "has_query": bool(p.query),
                    "note": "The presigned URL itself is intentionally not exposed by this local API.",
                })

            return self.send_json({"error": "not found"}, 404)

        except Exception as e:
            return self.fail(e)


def main():
    threading.Thread(target=stream_manager, daemon=True, name="sondehub-stream-manager").start()
    server = ThreadingHTTPServer((BIND, PORT), Handler)
    print(f"SondeHub+ listening on {BIND}:{PORT}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
