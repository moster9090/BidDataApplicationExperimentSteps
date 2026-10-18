#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
铜陵矿区大气污染 —— 传感器原始数据生成器
==========================================================
输出：JSONL（每行一条记录），按 Hive 风格分区落盘
      {OUTPUT_DIR}/etl_date=YYYYMMDD/sensor_HH.json

相对旧版的改动：
  1. 全区域共享一个风场，随时间随机游走 —— 同一时刻所有矿的风向风速一致
  2. 每个矿独立参数（基准浓度、源强、波动、事件概率、事件类型）
  3. 补充模型必需字段：emissionRateGps（源强 Q）、stackHeightM（烟囱高）
  4. 补充稳定度推算所需气象字段：温度 / 湿度 / 气压 / 云量 / 太阳辐射
  5. 污染指标以 SO2 / PM10 / PM25 为主，CO2 作为背景项
  6. 异常事件成段出现（持续数十秒）并带标注：
     isAbnormal / abnormalEventId / abnormalType
  7. 单位写进字段名；浓度钳制非负；保证 PM10 >= PM25
  8. 支持优雅退出（systemd 托管），支持 env 覆盖配置便于本地测试

环境变量：
  SENSOR_OUTPUT_DIR       输出根目录，默认 /opt/data/sensor_raw
  SENSOR_INTERVAL         采样间隔秒，默认 1
  SENSOR_EMIT_ALL_MINES   1=每个 tick 所有矿各出一条；0=只随机挑一个矿
  SENSOR_MAX_RECORDS      生成多少条后退出，0=无限（测试用）
  SENSOR_FORMAT           输出格式：json（JSONL，默认）或 csv
  SENSOR_CSV_HEADER       csv 时是否每个文件写一次表头，默认 1
  SENSOR_LAYOUT           目录布局：flat（默认，扁平目录 + 文件名带日期）
                          或 dated（etl_date=YYYYMMDD 子目录）
                          注意：Flume Taildir 只能读单层目录，给它采必须用 flat
  SENSOR_WEATHER          气象来源：sim（默认，模拟）或 openmeteo（真实气象）
                          openmeteo 拉取失败时自动回退到模拟，不会影响生成
  SENSOR_LAT/SENSOR_LNG   气象查询点（默认取第一个矿的坐标）
  SENSOR_WEATHER_REFRESH  真实气象刷新间隔秒，默认 600
  SENSOR_WEATHER_FILE     离线气象包路径，默认 /opt/data/weather/tongling_weather.csv
"""

import csv
import json
import math
import os
import random
import signal
import sys
import time
import urllib.request
import uuid
from datetime import datetime

# ============================ 配置 ============================

OUTPUT_DIR   = os.environ.get("SENSOR_OUTPUT_DIR", "/opt/data/sensor_raw")
INTERVAL_SEC = float(os.environ.get("SENSOR_INTERVAL", "1"))
EMIT_ALL     = os.environ.get("SENSOR_EMIT_ALL_MINES", "1") == "1"
MAX_RECORDS  = int(os.environ.get("SENSOR_MAX_RECORDS", "0"))   # 0 = 无限
OUTPUT_FORMAT = os.environ.get("SENSOR_FORMAT", "json").strip().lower()   # json | csv
CSV_HEADER    = os.environ.get("SENSOR_CSV_HEADER", "1") == "1"
LAYOUT        = os.environ.get("SENSOR_LAYOUT", "flat").strip().lower()   # flat | dated
WEATHER_MODE  = os.environ.get("SENSOR_WEATHER", "sim").strip().lower()    # sim | openmeteo
WEATHER_LAT   = float(os.environ.get("SENSOR_LAT", "30.93"))
WEATHER_LNG   = float(os.environ.get("SENSOR_LNG", "117.82"))
WEATHER_REFRESH_SEC = int(os.environ.get("SENSOR_WEATHER_REFRESH", "600"))
WEATHER_OFFLINE_PATH = os.environ.get(
    "SENSOR_WEATHER_FILE", "/opt/data/weather/tongling_weather.csv")

# CSV 列顺序（必须与 build_record 返回的字段严格一致）
FIELDS = ["recordId", "timestamp", "eventTime", "mineId", "mineName", "lng", "lat",
          "stackHeightM", "stackDiameterM", "exitVelocityMs", "stackGasTempC", "terrainType",
          "emissionRateGps",
          "so2_ugm3", "pm10_ugm3", "pm25_ugm3", "co2_ppm",
          "windDirectionDeg", "windDirection", "windSpeedMs",
          "temperatureC", "humidityPct", "pressureHpa", "cloudCover",
          "solarRadiationWm2", "stabilityClass",
          "isAbnormal", "abnormalEventId", "abnormalType"]

# 风场参数
WIND_DIR_DRIFT   = 0.9     # 风向每秒随机游走标准差（度）
WIND_SPEED_DRIFT = 0.12    # 风速每秒随机游走标准差（m/s）
WIND_SPEED_MIN   = 1.0     # 低于 1 m/s 高斯烟羽进入静风失效区，这里留个下限
WIND_SPEED_MAX   = 12.0
WIND_SHIFT_PROB  = 0.002   # 每秒发生一次明显风场变化的概率（模拟冷空气过境）

# 气象参数
TEMP_MEAN, TEMP_AMP = 17.0, 6.0
TEMP_PEAK_HOUR      = 15.0
PRESSURE_MEAN       = 1013.0

# ---------- 监测点定义 ----------
# lng/lat 请按实际点位校准。
# 源项参数（抬升公式要用的）：
#   stackHeightM    烟囱/井口几何高（米）
#   stackDiameterM  内径（米）
#   exitVelocityMs  出口流速（m/s）
#   stackGasTempC   烟气/排出气体温度（℃）—— 决定热浮力，常温排风取 30~45
#   terrainType     地表类型，给 GB/T 3840 查 σ 用：rural / urban / industrial / hilly
MINES = [
    dict(mineId="TL001", mineName="冬瓜山矿", lng=117.82, lat=30.93,
         stackHeightM=60.0, stackDiameterM=4.0, exitVelocityMs=10.0, stackGasTempC=45.0,
         terrainType="hilly",
         baseEmission=18.0, peakEmission=65.0,
         baseSo2=12.0, basePm25=32.0, basePm10=48.0, baseCo2=415.0,
         noiseSo2=3.0, noisePm25=6.0, noisePm10=9.0, noiseCo2=6.0,
         eventProb=0.0015, eventTypes=["SO2_SURGE", "BOTH"]),

    dict(mineId="TL002", mineName="铜官山矿", lng=117.80, lat=30.88,
         stackHeightM=40.0, stackDiameterM=3.0, exitVelocityMs=9.0, stackGasTempC=40.0,
         terrainType="hilly",
         baseEmission=9.0, peakEmission=40.0,
         baseSo2=8.0, basePm25=45.0, basePm10=72.0, baseCo2=408.0,
         noiseSo2=2.5, noisePm25=8.0, noisePm10=12.0, noiseCo2=5.0,
         eventProb=0.0018, eventTypes=["DUST", "BOTH"]),

    dict(mineId="TL003", mineName="天马山矿", lng=117.85, lat=30.95,
         stackHeightM=30.0, stackDiameterM=2.5, exitVelocityMs=8.0, stackGasTempC=35.0,
         terrainType="hilly",
         baseEmission=4.0, peakEmission=20.0,
         baseSo2=6.0, basePm25=25.0, basePm10=38.0, baseCo2=402.0,
         noiseSo2=2.0, noisePm25=5.0, noisePm10=7.0, noiseCo2=4.0,
         eventProb=0.0006, eventTypes=["SO2_SURGE"]),

    dict(mineId="TL004", mineName="凤凰山矿", lng=117.90, lat=30.90,
         stackHeightM=80.0, stackDiameterM=4.0, exitVelocityMs=12.0, stackGasTempC=90.0,
         terrainType="industrial",
         baseEmission=26.0, peakEmission=95.0,
         baseSo2=22.0, basePm25=38.0, basePm10=55.0, baseCo2=425.0,
         noiseSo2=4.5, noisePm25=7.0, noisePm10=10.0, noiseCo2=7.0,
         eventProb=0.0022, eventTypes=["SO2_SURGE", "BOTH"]),

    dict(mineId="TL005", mineName="狮子山矿", lng=117.78, lat=30.85,
         stackHeightM=35.0, stackDiameterM=3.0, exitVelocityMs=9.0, stackGasTempC=38.0,
         terrainType="hilly",
         baseEmission=12.0, peakEmission=55.0,
         baseSo2=9.0, basePm25=55.0, basePm10=88.0, baseCo2=412.0,
         noiseSo2=2.8, noisePm25=9.0, noisePm10=14.0, noiseCo2=6.0,
         eventProb=0.0030, eventTypes=["DUST", "BOTH"]),
]

DIRS_8 = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]


# ============================ 风场 ============================

class WindField:
    """全区域共享的风场。同一时刻所有监测点使用同一风向/风速。
    有真实气象时用 feed() 注入，否则做随机游走。"""

    def __init__(self):
        self.dir_deg = random.uniform(0, 360)
        self.speed_ms = random.uniform(1.5, 5.0)
        self._fed_at = 0.0
        self._fed_ttl = 30.0     # 真实值的有效期（秒）

    def feed(self, speed_ms, dir_deg):
        """注入真实观测值（来自气象 API）"""
        self.speed_ms = min(WIND_SPEED_MAX, max(WIND_SPEED_MIN, float(speed_ms)))
        self.dir_deg = float(dir_deg) % 360.0
        self._fed_at = time.time()

    def is_fed(self):
        return (time.time() - self._fed_at) < self._fed_ttl

    def step(self, dt=1.0):
        if self.is_fed():
            return               # 用真实值，不做随机游走

        self.dir_deg = (self.dir_deg + random.gauss(0, WIND_DIR_DRIFT) * dt) % 360
        self.speed_ms += random.gauss(0, WIND_SPEED_DRIFT) * dt

        # 偶发的一次明显风场变化（冷空气过境 / 风向突变）
        if random.random() < WIND_SHIFT_PROB * dt:
            self.dir_deg = (self.dir_deg + random.uniform(-40, 40)) % 360
            self.speed_ms += random.uniform(0, 3.0)

        self.speed_ms = min(WIND_SPEED_MAX, max(WIND_SPEED_MIN, self.speed_ms))

    def text(self):
        return DIRS_8[int(((self.dir_deg + 22.5) % 360) // 45)]


# ============================ 气象 ============================

class Weather:
    """气温 / 气压 / 云量随时间缓变，用于推算大气稳定度。"""

    def __init__(self):
        self.cloud = random.uniform(0.1, 0.6)
        self.pressure = PRESSURE_MEAN + random.uniform(-5, 5)

    def step(self, dt=1.0):
        self.cloud = min(1.0, max(0.0, self.cloud + random.gauss(0, 0.01) * dt))
        self.pressure = min(1035.0, max(990.0,
                            self.pressure + random.gauss(0, 0.05) * dt))

    def temperature(self, hour_float):
        t = TEMP_MEAN + TEMP_AMP * math.cos(2 * math.pi * (hour_float - TEMP_PEAK_HOUR) / 24)
        t -= 2.0 * self.cloud                       # 阴天降温
        return round(t + random.gauss(0, 0.4), 1)

    def humidity(self, temp):
        h = 70.0 - 0.8 * (temp - TEMP_MEAN) + 12.0 * self.cloud
        return int(min(100, max(20, h + random.gauss(0, 2))))

    def solar(self, hour_float):
        """太阳辐射：夜间为 0，正午最强，受云量衰减。"""
        if not (6.0 <= hour_float < 18.0):
            return 0.0
        x = (hour_float - 12.0) / 6.0
        clear_sky = 800.0 * max(0.0, 1.0 - x * x)
        return round(clear_sky * (1.0 - 0.7 * self.cloud), 1)


# ==================== 真实气象（Open-Meteo） ====================

def _http_get_json(url, timeout=10):
    """标准库 GET，不引第三方依赖（便于测试时 monkeypatch）"""
    req = urllib.request.Request(url, headers={"User-Agent": "tongling-sensor-gen/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


class OpenMeteoWeather:
    """从 Open-Meteo 拉真实气象，接口与 Weather 保持一致。
    拉取失败时自动回退，保证数据生成不中断。"""

    def __init__(self, lat=None, lng=None):
        self.lat = WEATHER_LAT if lat is None else lat
        self.lng = WEATHER_LNG if lng is None else lng
        self._sim = Weather()            # 回退用的模拟值
        self.cloud = self._sim.cloud
        self.pressure = self._sim.pressure
        self.temp = 20.0
        self.humidity_pct = 60
        self.solar_wm2 = 0.0
        self.wind_speed = 2.0
        self.wind_dir = 0.0
        self.source = "fallback"         # openmeteo | fallback
        self._last_fetch = 0.0
        self.fetch()

    def url(self):
        return (
            "https://api.open-meteo.com/v1/forecast"
            "?latitude={}&longitude={}"
            "&current=temperature_2m,relative_humidity_2m,pressure_msl,"
            "cloud_cover,wind_speed_10m,wind_direction_10m,is_day"
            "&wind_speed_unit=ms"          # ★ 不加就是 km/h，差 3.6 倍
            "&timezone=Asia%2FShanghai"
        ).format(self.lat, self.lng)

    def fetch(self):
        try:
            cur = _http_get_json(self.url())["current"]
            self.temp         = float(cur["temperature_2m"])
            self.humidity_pct = int(cur["relative_humidity_2m"])
            self.pressure     = float(cur["pressure_msl"])
            self.cloud        = float(cur["cloud_cover"]) / 100.0   # ★ 0~100 → 0~1
            self.wind_speed   = float(cur["wind_speed_10m"])
            self.wind_dir     = float(cur["wind_direction_10m"])
            is_day = int(cur.get("is_day", 1)) == 1
            self.solar_wm2 = (800.0 * (1.0 - 0.7 * self.cloud)) if is_day else 0.0
            self.source = "openmeteo"
            self._last_fetch = time.time()
            return True
        except Exception as e:
            print("⚠ 气象拉取失败，沿用上一次/模拟值: {}".format(e))
            return False

    def step(self, dt=1.0):
        if time.time() - self._last_fetch >= WEATHER_REFRESH_SEC:
            self.fetch()

    # --- 与 Weather 同一接口 ---
    def temperature(self, hour_float):
        return round(self.temp, 1)

    def humidity(self, temp):
        return self.humidity_pct

    def solar(self, hour_float):
        return round(self.solar_wm2, 1)


class OfflineWeather:
    """离线真实气象。

    读预先下载好的铜陵逐小时实测数据（由 fetch_weather_offline.py 生成），
    按「年内第几天 + 当前小时」去取对应位置的值（忽略年份），
    这样季节规律、昼夜变化、风向分布都是真实的。
    """

    NAME = "offline"

    def __init__(self, path=None):
        path = path or WEATHER_OFFLINE_PATH
        self.rows = []
        with open(path, encoding="utf-8") as f:
            for r in csv.DictReader(f):
                self.rows.append(r)
        if not self.rows:
            raise ValueError("离线气象包为空: {}".format(path))
        self.path = path
        self.source = "offline"
        self.temp = 20.0
        self.humidity_pct = 60
        self.cloud = 0.3
        self.pressure = 1013.0
        self.solar_wm2 = 0.0
        self.wind_speed = 2.0
        self.wind_dir = 0.0
        self.step()

    def step(self, dt=1.0):
        now = datetime.now()
        # 一年 8760 小时，正好能用「年内小时序」直接索引
        idx = ((now.timetuple().tm_yday - 1) * 24 + now.hour) % len(self.rows)
        r = self.rows[idx]

        base_speed = float(r["windSpeedMs"])
        base_dir   = float(r["windDirectionDeg"])
        base_temp  = float(r["temperatureC"])

        # 整点观测值 + 微小自然扰动。
        # 真实风速/风向本来就在秒级波动；不加的话一整小时风纹丝不动，很假。
        self.wind_speed   = max(0.5, base_speed + random.gauss(0, 0.12))
        self.wind_dir     = (base_dir + random.gauss(0, 1.2)) % 360.0
        self.temp         = base_temp + random.gauss(0, 0.15)

        self.humidity_pct = int(float(r["humidityPct"]))
        self.pressure     = float(r["pressureHpa"])
        self.cloud        = float(r["cloudCover"])
        is_day = int(float(r["isDay"])) == 1
        self.solar_wm2 = (800.0 * (1.0 - 0.7 * self.cloud)) if is_day else 0.0

    # --- 与 Weather 同一接口 ---
    def temperature(self, hour_float):
        return round(self.temp, 1)

    def humidity(self, temp):
        return self.humidity_pct

    def solar(self, hour_float):
        return round(self.solar_wm2, 1)


def pasquill_class(wind_ms, solar_wm2, hour):
    """简化版 Pasquill 大气稳定度分级（A 极不稳定 ~ F 极稳定）。
    白天按太阳辐射强度 + 风速查表；夜间按风速查表。
    注明：这是工程简化版，严格版还需要云量/云高参与判断。"""
    if 6 <= hour < 18:
        if solar_wm2 >= 600:
            table = [(2, "A"), (3, "A-B"), (5, "B"), (6, "C"), (1e9, "C")]
        elif solar_wm2 >= 300:
            table = [(2, "A-B"), (3, "B"), (5, "B-C"), (6, "C-D"), (1e9, "D")]
        else:
            table = [(2, "B"), (3, "C"), (5, "C"), (6, "D"), (1e9, "D")]
    else:
        table = [(2, "F"), (3, "E"), (5, "D"), (1e9, "D")]

    for limit, cls in table:
        if wind_ms < limit:
            return cls
    return "D"


# ======================= 异常事件状态机 =======================

class MineState:
    """给每个矿维护一个"排放异常事件"状态：
    事件成段出现（持续 20~180 秒），期间所有记录都打上同一个 abnormalEventId，
    这样下游 AI 能拿到连续的标注片段，而不是孤立的毛刺。"""

    def __init__(self, cfg):
        self.cfg = cfg
        self.remaining = 0
        self.event_id = None
        self.event_type = "NONE"
        self.strength = 1.0

    def tick(self):
        if self.remaining > 0:
            self.remaining -= 1
            if self.remaining == 0:
                self.event_id, self.event_type, self.strength = None, "NONE", 1.0
            else:
                return True, self.event_id, self.event_type, self.strength
            return False, None, "NONE", 1.0

        if random.random() < self.cfg["eventProb"]:
            self.remaining = random.randint(20, 180)
            self.event_id = "EVT{}-{}".format(
                datetime.now().strftime("%Y%m%d"), uuid.uuid4().hex[:6].upper())
            self.event_type = random.choice(self.cfg["eventTypes"])
            self.strength = random.uniform(2.5, 6.0)
            return True, self.event_id, self.event_type, self.strength

        return False, None, "NONE", 1.0


# ======================= 单条记录生成 =======================

def build_record(cfg, st, wind, weather, seq):
    now = datetime.now()
    hour_float = now.hour + now.minute / 60.0 + now.second / 3600.0

    is_ab, evt_id, evt_type, strength = st.tick()

    so2_k, pm_k = 1.0, 1.0
    if is_ab:
        if evt_type in ("SO2_SURGE", "BOTH"):
            so2_k = strength
        if evt_type in ("DUST", "BOTH"):
            pm_k = strength

    # 浓度：高斯噪声 + 非负钳制
    so2 = max(0.0, random.gauss(cfg["baseSo2"], cfg["noiseSo2"])) * so2_k
    pm25 = max(0.0, random.gauss(cfg["basePm25"], cfg["noisePm25"])) * pm_k
    pm10 = max(0.0, random.gauss(cfg["basePm10"], cfg["noisePm10"])) * pm_k
    pm10 = max(pm10, pm25)                     # 物理约束：PM10 必然包含 PM2.5

    # CO2 背景项：夜间略高（逆温+呼吸作用），凌晨 4 点达峰
    co2_base = cfg["baseCo2"] + 15.0 * math.cos(2 * math.pi * (hour_float - 4.0) / 24)
    co2 = max(380.0, random.gauss(co2_base, cfg["noiseCo2"]))

    # 源强 Q：异常期间按 strength 放大
    emission = cfg["baseEmission"] * (strength if is_ab else 1.0)
    emission *= random.uniform(0.9, 1.1)

    temp = weather.temperature(hour_float)
    solar = weather.solar(hour_float)
    ts = int(time.time())

    return {
        # ---- 标识 ----
        "recordId": "{}-{}-{}".format(cfg["mineId"], ts * 1000 + seq, seq),
        "timestamp": ts,
        "eventTime": now.strftime("%Y-%m-%d %H:%M:%S"),

        # ---- 监测点 ----
        "mineId": cfg["mineId"],
        "mineName": cfg["mineName"],
        "lng": cfg["lng"],
        "lat": cfg["lat"],

        # ---- 模型输入：源项 ----
        "stackHeightM": cfg["stackHeightM"],
        "stackDiameterM": cfg["stackDiameterM"],
        "exitVelocityMs": cfg["exitVelocityMs"],
        "stackGasTempC": cfg["stackGasTempC"],
        "terrainType": cfg["terrainType"],
        "emissionRateGps": round(emission, 2),

        # ---- 污染物浓度 ----
        "so2_ugm3": round(so2, 2),
        "pm10_ugm3": round(pm10, 2),
        "pm25_ugm3": round(pm25, 2),
        "co2_ppm": round(co2, 1),

        # ---- 气象 ----
        "windDirectionDeg": round(wind.dir_deg, 1),
        "windDirection": wind.text(),
        "windSpeedMs": round(wind.speed_ms, 2),
        "temperatureC": temp,
        "humidityPct": weather.humidity(temp),
        "pressureHpa": round(weather.pressure, 1),
        "cloudCover": round(weather.cloud, 2),
        "solarRadiationWm2": solar,
        "stabilityClass": pasquill_class(wind.speed_ms, solar, now.hour),

        # ---- 异常标注 ----
        "isAbnormal": is_ab,
        "abnormalEventId": evt_id,
        "abnormalType": evt_type,
    }


# ============================ 落盘 ============================

def _cell(v):
    if v is None:
        return ""
    if isinstance(v, bool):
        return "true" if v else "false"
    return v


def write_record(record):
    now = datetime.now()
    if LAYOUT == "dated":
        # 目录带 etl_date= 分区，Spark 能直接做分区发现
        day_dir = os.path.join(OUTPUT_DIR, "etl_date=" + now.strftime("%Y%m%d"))
        stem = "sensor_" + now.strftime("%H")
    else:
        # 扁平目录 + 文件名带日期。
        # Flume Taildir 只能读单层目录，要让 Flume 采集就必须用这个布局。
        day_dir = OUTPUT_DIR
        stem = "sensor_" + now.strftime("%Y%m%d_%H")
    os.makedirs(day_dir, exist_ok=True)

    # 每条都 open/close，保证 Flume/文件采集器永远只读到完整行
    if OUTPUT_FORMAT == "csv":
        path = os.path.join(day_dir, stem + ".csv")
        # 表头只在文件首次创建时写一次
        need_header = CSV_HEADER and not os.path.exists(path)
        with open(path, "a", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            if need_header:
                w.writerow(FIELDS)
            w.writerow([_cell(record[k]) for k in FIELDS])
    else:
        path = os.path.join(day_dir, stem + ".json")
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


# ============================ 主循环 ============================

_running = True


def _stop(signum, _frame):
    global _running
    _running = False
    print("\n收到信号 {}，准备退出...".format(signum))


def main():
    try:
        signal.signal(signal.SIGINT, _stop)
        signal.signal(signal.SIGTERM, _stop)
    except (ValueError, AttributeError, OSError):
        pass          # Windows 等平台可能不支持全部信号

    wind = WindField()
    if WEATHER_MODE == "openmeteo":
        weather = OpenMeteoWeather()
        print("气象来源: Open-Meteo (当前状态: {})".format(weather.source))
    elif WEATHER_MODE == "offline":
        try:
            weather = OfflineWeather()
            print("气象来源: 离线真实气象包 -> {} ({} 条)".format(
                weather.path, len(weather.rows)))
        except Exception as e:
            print("⚠ 离线气象包不可用（{}），回退到模拟".format(e))
            weather = Weather()
            print("气象来源: 模拟")
    else:
        weather = Weather()
        print("气象来源: 模拟")
    states = [(cfg, MineState(cfg)) for cfg in MINES]

    print("数据生成器启动，输出目录: {}".format(OUTPUT_DIR))
    print("格式: {} | 布局: {} | 采样间隔: {}s | 每个 tick 所有矿: {} | 上限: {}".format(
        OUTPUT_FORMAT, LAYOUT, INTERVAL_SEC, EMIT_ALL, MAX_RECORDS or "无限"))

    count = 0
    abnormal = 0
    failures = 0        # 连续失败计数

    while _running:
        try:
            weather.step()
            if getattr(weather, "source", "sim") in ("openmeteo", "offline"):
                wind.feed(weather.wind_speed, weather.wind_dir)
            wind.step()

            targets = states if EMIT_ALL else [random.choice(states)]
            for cfg, st in targets:
                rec = build_record(cfg, st, wind, weather, count)
                write_record(rec)
                count += 1
                if rec["isAbnormal"]:
                    abnormal += 1

            failures = 0      # 本 tick 成功 → 清零

            if count and count % 300 < len(targets):
                print("[{}] 已生成 {} 条（异常 {} 条）| 风: {}° {} m/s | {}℃".format(
                    datetime.now().strftime("%H:%M:%S"), count, abnormal,
                    round(wind.dir_deg), round(wind.speed_ms, 1),
                    weather.temperature(datetime.now().hour)))

            if MAX_RECORDS and count >= MAX_RECORDS:
                print("达到上限 {} 条，退出。".format(MAX_RECORDS))
                break

            time.sleep(INTERVAL_SEC)

        except Exception as e:
            failures += 1
            print("错误（连续第 {} 次）: {}".format(failures, e))
            # 磁盘满、权限错这类持久故障：不要永远重试，退出让 systemd 重启并报警
            if failures >= 20:
                print("连续失败 {} 次，退出（请检查磁盘空间 / 目录权限）。".format(failures))
                sys.exit(1)
            time.sleep(5)

    print("已停止，共生成 {} 条，其中异常 {} 条。".format(count, abnormal))


if __name__ == "__main__":
    main()
