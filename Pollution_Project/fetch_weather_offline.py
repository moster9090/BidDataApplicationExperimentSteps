#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
铜陵离线气象包下载器
====================================================
★ 在你【有网的电脑】上运行（Windows 也行），生成一个 CSV，
  然后用 scp 拷到服务器。服务器不需要联网。

数据来源：Open-Meteo Archive API（免费、无需注册）
下载内容：铜陵上一个完整年份的逐小时实测气象（8760 条左右）

用法：
    python fetch_weather_offline.py
    scp tongling_weather_offline.csv master@192.168.1.100:/opt/data/weather/tongling_weather.csv
"""

import csv
import json
import os
import sys
import urllib.parse
import urllib.request
from datetime import datetime

LAT, LNG = 30.93, 117.82          # 铜陵
OUT_DEFAULT = "tongling_weather_offline.csv"

# 取"上一个完整年份"，保证 1 月 1 日 0 点开始，正好 8760（或 8784）行
YEAR = datetime.now().year - 1


def build_url():
    params = {
        "latitude": LAT,
        "longitude": LNG,
        "start_date": "{}-01-01".format(YEAR),
        "end_date": "{}-12-31".format(YEAR),
        "hourly": ("temperature_2m,relative_humidity_2m,pressure_msl,"
                   "cloud_cover,wind_speed_10m,wind_direction_10m,is_day"),
        "wind_speed_unit": "ms",       # ★ 默认是 km/h，必须指定
        "timezone": "Asia/Shanghai",
    }
    return "https://archive-api.open-meteo.com/v1/archive?" + urllib.parse.urlencode(params)


def fetch(url, timeout=120):
    req = urllib.request.Request(url, headers={"User-Agent": "tongling-weather-fetch/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def main():
    out = sys.argv[1] if len(sys.argv) > 1 else OUT_DEFAULT
    url = build_url()
    print("请求年份:", YEAR)
    print("请求地址:", url)
    print("下载中……")

    data = fetch(url)
    h = data.get("hourly") or {}
    times = h.get("time") or []
    if not times:
        print("❌ 没有拿到数据，检查网络或经纬度")
        sys.exit(1)

    n = len(times)
    written = 0
    FIELDS = ["time", "temperatureC", "humidityPct", "pressureHpa",
              "cloudCover", "windSpeedMs", "windDirectionDeg", "isDay"]
    KEEP = ("temperatureC", "humidityPct", "pressureHpa",
            "windSpeedMs", "windDirectionDeg", "isDay")

    prev = None
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(FIELDS)
        for i in range(n):
            cur = {
                "time": times[i],
                "temperatureC": h["temperature_2m"][i],
                "humidityPct": h["relative_humidity_2m"][i],
                "pressureHpa": h["pressure_msl"][i],
                "cloudCover": round((h["cloud_cover"][i] or 0) / 100.0, 2),
                "windSpeedMs": h["wind_speed_10m"][i],
                "windDirectionDeg": h["wind_direction_10m"][i],
                "isDay": h["is_day"][i],
            }
            # ★ 缺测用上一个值顶上，绝不能用 continue 跳过：
            #   跳过会让行号与小时序错位，整个「按小时索引」就歪了。
            if prev is not None:
                for k in KEEP:
                    if cur[k] is None:
                        cur[k] = prev[k]
            if cur["temperatureC"] is None or cur["windSpeedMs"] is None:
                continue          # 只可能出现在文件最开头
            prev = cur
            w.writerow([cur[k] for k in FIELDS])
            written += 1

    size = os.path.getsize(out)
    print("✅ 完成：{} 条 -> {} ({:.1f} KB)".format(written, out, size / 1024))
    print()
    print("下一步（在有网的电脑上执行）：")
    print("  scp {} master@192.168.1.100:/tmp/".format(out))
    print("然后在服务器上：")
    print("  sudo mkdir -p /opt/data/weather")
    print("  sudo mv /tmp/{} /opt/data/weather/tongling_weather.csv".format(os.path.basename(out)))
    print("  sudo chown -R master:master /opt/data/weather")


if __name__ == "__main__":
    main()
