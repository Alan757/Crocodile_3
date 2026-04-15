"""
analyzer.py
───────────
技术指标计算 + 四大策略评分
- 头等马：日K持续在60日线上方
- 黑  马：日K在60日线下但OBV上穿均线
- 突破策略：新高突破 / 突破60日线
- 弱转强策略：OBV支撑弱转强 / 60日线支撑弱转强

修正（周线二连阳）：
1) “近几周有过二连阳就行”：在回看窗口内，只要出现任意连续两根已收盘周K为阳线，即为 True
2) “当周周线除周五收线后外，都不能算成一次阳”：仅统计“已完成的周K”
   - 以周五为周线结束（W-FRI），如果本周未到周五收盘，则不把本周计入周K序列
"""

import pandas as pd
import numpy as np

OBV_MA_PERIOD = 20  # OBV 均线周期
MA_PERIOD = 60  # 主均线周期
LOOKBACK_DAYS = 20  # 新高回看天数

WEEKLY_LOOKBACK_WEEKS = 8  # “近几周”窗口：在最近 N 个已收盘周K里找二连阳


# ═══════════════════════════════════════════════
#  OBV 计算
# ═══════════════════════════════════════════════
def calc_obv(close: pd.Series, volume: pd.Series) -> pd.Series:
    """
    量能潮 OBV：
      收盘涨 → +成交量
      收盘跌 → −成交量
      平盘   → 不变
    """
    direction = np.sign(close.diff())  # 1, -1, 0
    direction.iloc[0] = 0
    obv = (direction * volume).cumsum()
    return obv


# ═══════════════════════════════════════════════
#  周线工具：只取“已收盘周K”，并判断近几周是否出现二连阳
# ═══════════════════════════════════════════════
def _get_closed_weekly(df: pd.DataFrame) -> pd.DataFrame:
    """
    生成周线（周五收线，W-FRI），并且只保留“已完成的周K”：
    - 如果最后一根周K的结束日期 > df最后交易日日期，说明本周未收完 → 丢弃最后一根
    """
    if df is None or df.empty:
        return pd.DataFrame()

    dfx = df.copy()
    if not isinstance(dfx.index, pd.DatetimeIndex):
        dfx.index = pd.to_datetime(dfx.index)

    dfx = dfx.sort_index()

    weekly = (
        dfx.resample("W-FRI")
        .agg(
            {
                "Open": "first",
                "High": "max",
                "Low": "min",
                "Close": "last",
                "Volume": "sum",
            }
        )
        .dropna(subset=["Close"])
    )

    if weekly.empty:
        return weekly

    last_daily_date = dfx.index[-1].normalize()
    last_week_end = weekly.index[-1].normalize()

    # 若周线这根的标记日期（周五）还没到，说明本周未收盘，不计入
    if last_week_end > last_daily_date:
        weekly = weekly.iloc[:-1]

    return weekly


def _has_weekly_two_yang(
    weekly: pd.DataFrame, lookback_weeks: int = WEEKLY_LOOKBACK_WEEKS
) -> bool:
    """
    在最近 lookback_weeks 根“已收盘周K”中，是否存在任意连续两周为阳线（Close > Open）
    """
    if weekly is None or weekly.empty:
        return False

    w = weekly.tail(max(lookback_weeks, 2))
    if len(w) < 2:
        return False

    yang = (w["Close"] > w["Open"]).astype(int).values  # 1/0
    # 任意相邻两周同时为阳线
    for i in range(1, len(yang)):
        if yang[i] == 1 and yang[i - 1] == 1:
            return True
    return False


# ═══════════════════════════════════════════════
#  单只 ETF 分析
# ═══════════════════════════════════════════════
def analyze_etf(df: pd.DataFrame, etf_info: dict) -> dict | None:
    if df is None or len(df) < MA_PERIOD + 5:
        return None

    # 确保索引为 DatetimeIndex 且有序（周线/滚动更稳）
    dfx = df.copy()
    if not isinstance(dfx.index, pd.DatetimeIndex):
        dfx.index = pd.to_datetime(dfx.index)
    dfx = dfx.sort_index()

    close = dfx["Close"].copy().ffill().bfill()
    volume = dfx["Volume"].copy().fillna(0)

    if close.iloc[-1] <= 0:
        return None

    # ── 基础指标 ───────────────────────────
    ma60 = close.rolling(MA_PERIOD).mean()
    obv = calc_obv(close, volume)
    obv_ma = obv.rolling(OBV_MA_PERIOD).mean()

    curr_close = close.iloc[-1]
    curr_ma60 = ma60.iloc[-1]
    curr_obv = obv.iloc[-1]
    curr_obv_ma = obv_ma.iloc[-1]

    if pd.isna(curr_ma60) or pd.isna(curr_obv_ma):
        return None

    # ── 周线（只取已收盘周K） ───────────────
    weekly = _get_closed_weekly(dfx)
    weekly_2_yang = _has_weekly_two_yang(weekly, lookback_weeks=WEEKLY_LOOKBACK_WEEKS)

    # ── 连续站上60日线天数 ─────────────────
    above = (close > ma60).values
    days_above = 0
    for v in reversed(above):
        if v:
            days_above += 1
        else:
            break

    # ── OBV 上穿其均线 ────────────────────
    obv_diff = obv - obv_ma
    obv_cross_up = False
    obv_cross_day = 999
    # 注意：i-1 需要 i>=1
    start_i = len(obv_diff) - 1
    end_i = max(len(obv_diff) - 8, 1)
    for i in range(start_i, end_i - 1, -1):
        if obv_diff.iloc[i] > 0 and obv_diff.iloc[i - 1] <= 0:
            obv_cross_up = True
            obv_cross_day = len(obv_diff) - 1 - i  # 0 = 今天
            break

    obv_above_ma = curr_obv > curr_obv_ma

    # ── 放量 ──────────────────────────────
    avg_vol = volume.tail(60).mean()
    max_recent_vol = volume.tail(20).max()
    vol_ratio = max_recent_vol / avg_vol if avg_vol > 0 else 0
    high_volume = vol_ratio > 2.0

    # ── 突破策略 ──────────────────────────
    bk_types: list[str] = []
    # (a) 最近2天新高突破
    if len(close) >= LOOKBACK_DAYS + 2:
        prev_high = close.iloc[-(LOOKBACK_DAYS + 2) : -2].max()
        if close.iloc[-2:].max() > prev_high:
            bk_types.append("新高突破")
    # (b) 突破60日均线（2天内从下方穿越到上方）
    if len(close) >= 4 and not pd.isna(ma60.iloc[-4]):
        if close.iloc[-1] > ma60.iloc[-1] and close.iloc[-3] <= ma60.iloc[-3]:
            bk_types.append("突破60日线")

    # ── 弱转强策略 ────────────────────────
    w2s_types: list[str] = []
    # (a) OBV 支撑弱转强：近期回踩 OBV 均线后重新站上
    if len(obv) >= 20 and len(obv_ma) >= 20:
        recent_gap = obv.iloc[-15:] - obv_ma.iloc[-15:]
        recent_gap_normed = recent_gap / obv_ma.iloc[-15:].abs().replace(0, 1)
        # 曾接近或跌穿 OBV 均线（gap < 2%），现在重新在上方
        if recent_gap_normed.min() < 0.02 and curr_obv > curr_obv_ma:
            # 确认之前是在上方的
            if (obv.iloc[-20:-10] > obv_ma.iloc[-20:-10]).any():
                w2s_types.append("OBV支撑弱转强")

    # (b) 60日均线支撑弱转强：价格回踩60日线后反弹
    if len(close) >= 20 and not pd.isna(ma60.iloc[-15]):
        price_gap = (close.iloc[-10:] - ma60.iloc[-10:]) / ma60.iloc[-10:]
        if price_gap.min() <= 0.03 and price_gap.min() >= -0.03:
            if curr_close > curr_ma60:
                if (close.iloc[-20:-10] > ma60.iloc[-20:-10]).any():
                    w2s_types.append("60日线支撑弱转强")

    # ══════════════════════════════════════
    #  评分
    # ══════════════════════════════════════

    # ── 头等马 ─────────────
    top_score = 0
    if days_above >= 5:
        top_score = min(days_above, 40)
        if weekly_2_yang:
            top_score += 12
        if high_volume:
            top_score += 6
        if obv_above_ma:
            top_score += 6

    # ── 黑马 ───────────────
    dark_score = 0
    is_below_ma60 = curr_close < curr_ma60
    if is_below_ma60 and (obv_cross_up or obv_above_ma):
        dark_score = 15
        if obv_cross_up:
            dark_score += max(0, 10 - obv_cross_day * 2)
        if weekly_2_yang:
            dark_score += 12
        if high_volume:
            dark_score += 6
        gap_pct = (curr_ma60 - curr_close) / curr_ma60
        if gap_pct < 0.05:
            dark_score += 6

    # 特别推荐标签
    tags: list[str] = []
    if weekly_2_yang:
        tags.append(f"近{WEEKLY_LOOKBACK_WEEKS}周曾二连阳(周五收线)")
    if high_volume:
        tags.append(f"近期放量×{vol_ratio:.1f}")
    if obv_above_ma:
        tags.append("OBV↑")

    return {
        "info": etf_info,
        "close": round(float(curr_close), 3),
        "ma60": round(float(curr_ma60), 3),
        "above_ma60": bool(curr_close > curr_ma60),
        "days_above": int(days_above),
        "obv_above": bool(obv_above_ma),
        "obv_cross_up": bool(obv_cross_up),
        "obv_cross_day": int(obv_cross_day),
        "weekly_2y": bool(weekly_2_yang),
        "high_vol": bool(high_volume),
        "vol_ratio": round(float(vol_ratio), 2),
        "top_score": int(top_score),
        "dark_score": int(dark_score),
        "bk_types": bk_types,
        "w2s_types": w2s_types,
        "tags": tags,
        "gap_pct": round(float((curr_close / curr_ma60 - 1) * 100), 2),
    }


# ═══════════════════════════════════════════════
#  批量分析 & 排名
# ═══════════════════════════════════════════════
def analyze_all(matched: list[dict], data_dict: dict) -> list[dict]:
    results = []
    for m in matched:
        tk = m["yf_ticker"]
        if tk not in data_dict:
            continue
        r = analyze_etf(data_dict[tk], m)
        if r:
            results.append(r)
    print(f"[INFO] 分析完成: {len(results)} 个板块")
    return results


def get_rankings(results: list[dict]) -> dict:
    top_horses = sorted(
        [r for r in results if r["top_score"] > 0],
        key=lambda x: x["top_score"],
        reverse=True,
    )[:10]

    dark_horses = sorted(
        [r for r in results if r["dark_score"] > 0],
        key=lambda x: x["dark_score"],
        reverse=True,
    )[:10]

    breakthroughs = [r for r in results if r["bk_types"]]
    weak_to_strongs = [r for r in results if r["w2s_types"]]

    return {
        "top_horses": top_horses,
        "dark_horses": dark_horses,
        "breakthroughs": breakthroughs,
        "weak_to_strongs": weak_to_strongs,
        "total": len(results),
    }
