"""
fetcher.py
──────────
1. 从东方财富获取 概念板块 + 行业板块 列表
2. 从东方财富获取全部场内 ETF 列表
3. 将板块按名称匹配到代表 ETF（按成交额优先）
4. 通过 yfinance 下载 ETF 日线数据（增强容错：失败重试 + 退避）

改动（2026-04-15）：
- match_sectors_to_etfs: 同一只ETF只输出一次（去重），保留“最佳板块匹配”记录
"""

from __future__ import annotations

import re
import time
import random
import requests
import pandas as pd
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor, as_completed

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": "https://quote.eastmoney.com/",
}

# ── 同义词映射：板块关键词 → ETF 可能出现的关键词 ──────────
# 说明：这里的 key 不是“必须完全等于板块名”，而是后面 match 时会用于补充命中。
ALIASES: dict[str, list[str]] = {
    "半导体": ["芯片", "半导体", "集成电路", "IC"],
    "芯片": ["芯片", "半导体", "集成电路", "IC"],
    "人工智能": ["人工智能", "AI", "算力", "大模型"],
    "光伏": ["光伏", "太阳能"],
    "锂电池": ["锂电", "电池", "动力电池"],
    "新能源汽车": ["新能源车", "新能车", "智能汽车", "汽车", "电动车"],
    "机器人": ["机器人", "人形机器人"],
    "军工": ["军工", "国防", "航天", "航空"],
    "医药": ["医药", "医疗", "生物医药", "创新药", "中药"],
    "证券": ["证券", "券商"],
    "白酒": ["白酒", "酒"],
    "地产": ["地产", "房地产"],
    "房地产": ["地产", "房地产"],
    "食品饮料": ["食品", "饮料", "消费"],
    "消费": ["消费", "必选消费", "可选消费"],
    "网络安全": ["网安", "信息安全", "网络安全"],
    "数字经济": ["数字经济", "数据", "信创"],
    "云计算": ["云计算", "云", "IDC", "数据中心"],
    "大数据": ["大数据", "数据"],
    "物联网": ["物联网", "IoT"],
    "有色金属": ["有色", "金属", "铜", "铝", "黄金"],
    "稀土": ["稀土"],
    "储能": ["储能"],
    "碳中和": ["碳中和", "低碳"],
    "跨境支付": ["跨境", "支付", "金融科技"],
    "金融科技": ["金融科技", "支付"],
    "信创": ["信创", "国产软件"],
    "传媒": ["传媒", "影视", "游戏"],
}

# 常见噪声词：出现在ETF/指数名里非常多，但对主题区分度低
NOISE_WORDS = {
    "中国",
    "沪深",
    "上海",
    "深圳",
    "上证",
    "深证",
    "中证",
    "国证",
    "A股",
    "指数",
    "精选",
    "增强",
    "ETF",
    "基金",
    "联接",
    "开放式",
    "交易型",
    "综合",
    "全指",
    "成份",
    "行业",
    "主题",
    "板块",
    "龙头",
    "策略",
    "优选",
    "领先",
    "100",
    "300",
    "500",
    "800",
    "1000",
    "红利",
    "价值",
    "成长",
    "创新",
}

# 更严格一点：避免把“银行ETF”错误地匹配到“银/黄金/白银”等
AMBIGUOUS_SINGLE_CHARS = {"银", "券", "医", "药", "车", "酒", "云", "芯"}


# ═══════════════════════════════════════════════
#  1. 获取东方财富板块列表
# ═══════════════════════════════════════════════
def get_sectors() -> list[dict]:
    """获取东方财富概念板块 (t=3) + 行业板块 (t=2)"""
    sectors: list[dict] = []
    for t, label in [(3, "概念"), (2, "行业")]:
        url = "https://push2.eastmoney.com/api/qt/clist/get"
        params = {
            "pn": 1,
            "pz": 600,
            "po": 1,
            "np": 1,
            "fltt": 2,
            "invt": 2,
            "fid": "f6",
            "fs": f"m:90+t:{t}+f:!50",
            "fields": "f12,f14,f3,f6",
        }
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=20)
            r.raise_for_status()
            data = r.json()
            diff = (data.get("data") or {}).get("diff") or []
            for item in diff:
                sectors.append(
                    {
                        "code": item.get("f12", ""),
                        "name": item.get("f14", "") or "",
                        "type": label,
                        "change_pct": item.get("f3", 0) or 0,
                        "amount": item.get("f6", 0) or 0,  # 板块成交额
                    }
                )
        except Exception as e:
            print(f"[WARN] 获取{label}板块失败: {e}")
        time.sleep(0.35)
    print(f"[INFO] 共获取 {len(sectors)} 个板块")
    return sectors


# ═══════════════════════════════════════════════
#  2. 获取全部 ETF 列表
# ═══════════════════════════════════════════════
def get_etfs() -> list[dict]:
    """从东方财富获取全部场内 ETF（上证 + 深证）"""
    etfs: list[dict] = []
    page = 1
    while page <= 25:
        url = "https://push2.eastmoney.com/api/qt/clist/get"
        params = {
            "pn": page,
            "pz": 500,
            "po": 1,
            "np": 1,
            "fltt": 2,
            "invt": 2,
            "fid": "f6",
            "fs": "b:MK0021,b:MK0022",
            "fields": "f12,f13,f14,f2,f3,f5,f6",
        }
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=20)
            r.raise_for_status()
            diff = (r.json().get("data") or {}).get("diff") or []
            if not diff:
                break

            for item in diff:
                code = str(item.get("f12", "") or "")
                market = item.get("f13", 0) or 0
                name = str(item.get("f14", "") or "")
                amount = item.get("f6", 0) or 0

                # 东财 f13: 1=上交所, 0/others=深交所（实测对基金基本可用）
                suffix = ".SS" if market == 1 else ".SZ"
                etfs.append(
                    {
                        "code": code,
                        "name": name,
                        "market": market,
                        "yf_ticker": f"{code}{suffix}",
                        "amount": float(amount),  # ETF成交额
                    }
                )

            if len(diff) < 500:
                break
            page += 1
        except Exception as e:
            print(f"[WARN] 获取ETF第{page}页失败: {e}")
            break

        time.sleep(0.35)

    print(f"[INFO] 共获取 {len(etfs)} 只ETF")
    return etfs


# ═══════════════════════════════════════════════
#  3. 板块 → ETF 匹配（增强命中率）
# ═══════════════════════════════════════════════
def _normalize_text(s: str) -> str:
    """统一文本：去空白/符号，转大写，保留中文/字母/数字"""
    s = s or ""
    s = s.strip()
    s = re.sub(r"\s+", "", s)
    # 保留中文/字母/数字
    s = re.sub(r"[^\u4e00-\u9fa5A-Za-z0-9]+", "", s)
    return s.upper()


def _clean_etf_name(name: str) -> str:
    """去除ETF名称中常见后缀/修饰词，提高匹配稳定性"""
    s = _normalize_text(name)
    # 常见后缀/词
    for token in [
        "交易型开放式指数基金",
        "交易型开放式",
        "开放式",
        "指数基金",
        "指数",
        "基金",
        "联接",
        "增强",
        "策略",
        "优选",
        "精选",
    ]:
        s = s.replace(_normalize_text(token), "")
    # ETF 本身也去掉，以免干扰
    s = s.replace("ETF", "")
    return s


def _tokenize_cn(s: str) -> list[str]:
    """
    简单中文词元：以2字 bigram 为主，兼容字母数字连续串
    """
    s = _normalize_text(s)
    if not s:
        return []
    tokens: list[str] = []

    # 英文/数字连续串作为 token
    for m in re.finditer(r"[A-Z0-9]{2,}", s):
        tokens.append(m.group(0))

    # 中文 bigrams
    cn = re.sub(r"[A-Z0-9]+", "", s)
    for i in range(len(cn) - 1):
        bg = cn[i : i + 2]
        if bg and bg not in NOISE_WORDS:
            tokens.append(bg)
    return tokens


def _expand_sector_keywords(sector_name: str) -> list[str]:
    """
    给板块名扩展关键词：
    - 本名
    - 去噪后版本
    - 同义词（如果命中 ALIASES 的 key 子串）
    """
    s = _normalize_text(sector_name)
    kws = {s}

    # 如果板块名较长，加入一些子串（3~4字）提高包含命中率
    cn = re.sub(r"[A-Z0-9]+", "", s)
    if len(cn) >= 4:
        kws.add(cn[:4])
    if len(cn) >= 3:
        kws.add(cn[:3])

    # ALIASES: 只要 sector_name 包含 key，就把别名加入
    for k, vs in ALIASES.items():
        nk = _normalize_text(k)
        if nk and nk in s:
            for v in vs:
                kws.add(_normalize_text(v))

    # 去掉明显歧义的单字
    kws = {x for x in kws if not (len(x) == 1 and x in AMBIGUOUS_SINGLE_CHARS)}
    return sorted(kws, key=len, reverse=True)


def _match_score(sector_name: str, etf_name: str) -> float:
    """
    计算板块名 ↔ ETF名 的匹配得分（越大越好）
    规则（按强到弱）：
      1) 板块关键词 直接包含于 ETF清洗名：高分
      2) 同义词命中：中高分
      3) token overlap（bigram/英文串）：中分
      4) 部分子串命中：低分
    """
    s_norm = _normalize_text(sector_name)
    e_clean = _clean_etf_name(etf_name)  # 已 upper

    if not s_norm or not e_clean:
        return 0.0

    score = 0.0

    # 1) 直接包含（最强信号）
    for kw in _expand_sector_keywords(sector_name):
        if kw and len(kw) >= 2 and kw in e_clean:
            score = max(score, 30.0 + min(len(kw), 6) * 4.0)

    # 2) 同义词：如果 sector 本身没直接包含，则补充
    for k, vs in ALIASES.items():
        nk = _normalize_text(k)
        if nk and nk in s_norm:
            for v in vs:
                nv = _normalize_text(v)
                if nv and len(nv) >= 2 and nv in _normalize_text(etf_name):
                    score = max(score, 26.0 + min(len(nv), 6) * 3.0)

    # 3) token overlap
    s_tokens = set(_tokenize_cn(s_norm))
    e_tokens = set(_tokenize_cn(e_clean))
    if s_tokens and e_tokens:
        inter = s_tokens & e_tokens
        overlap = len(inter)
        if overlap > 0:
            score = max(score, 10.0 + overlap * 3.0)

    # 4) 最弱：首字符/短子串命中
    cn = re.sub(r"[A-Z0-9]+", "", s_norm)
    if len(cn) >= 2 and cn[:2] in e_clean:
        score = max(score, 8.0)
    if len(cn) >= 3 and cn[:3] in e_clean:
        score = max(score, 12.0)

    return score


def _is_better_match(a: dict, b: dict) -> bool:
    """
    True 表示 a 比 b 更应该保留（用于 ETF 去重时选最优）
    优先级：
      1) match_score 更高
      2) etf_amount 更大
      3) sector_amount 更大（板块本身热度更高）
      4) sector_name 字典序（稳定输出）
    """
    if b is None:
        return True

    a_score = float(a.get("match_score", 0) or 0)
    b_score = float(b.get("match_score", 0) or 0)
    if a_score != b_score:
        return a_score > b_score

    a_amt = float(a.get("etf_amount", 0) or 0)
    b_amt = float(b.get("etf_amount", 0) or 0)
    if a_amt != b_amt:
        return a_amt > b_amt

    a_samt = float(a.get("sector_amount", 0) or 0)
    b_samt = float(b.get("sector_amount", 0) or 0)
    if a_samt != b_samt:
        return a_samt > b_samt

    return str(a.get("sector_name", "")) < str(b.get("sector_name", ""))


def match_sectors_to_etfs(
    sectors: list[dict],
    etfs: list[dict],
    min_amount: float = 1e6,
    score_threshold: float = 10.0,
    max_candidates: int = 1200,
) -> list[dict]:
    """
    为每个板块寻找成交额/流动性较好的“代表ETF”

    需求：相同 ETF 只输出一次
    - 如果多个板块匹配到同一只ETF，则只保留“最佳匹配”的那条记录
    """
    # 预过滤：优先保留名称含 ETF 的，并要求成交额 >= min_amount
    valid = [
        e
        for e in etfs
        if e.get("amount", 0) >= min_amount and "ETF" in (e.get("name", "").upper())
    ]
    # 若过滤后太少，则放宽：只用成交额
    if len(valid) < 200:
        valid = [e for e in etfs if e.get("amount", 0) >= min_amount]

    # 成交额从高到低（后面同分时优先大成交额）
    valid.sort(key=lambda x: x.get("amount", 0), reverse=True)

    # 仅在 top-N 流动性ETF里找（提升速度）
    candidates = valid[:max_candidates] if len(valid) > max_candidates else valid

    # ETF 去重：key= yf_ticker（更稳定）；如果缺失则退回 code
    best_by_etf: dict[str, dict] = {}

    for sector in sectors:
        sname = sector.get("name", "") or ""
        best = None
        best_score = -1.0

        for etf in candidates:
            sc = _match_score(sname, etf.get("name", ""))
            if sc <= 0:
                continue

            # 评分相同则按成交额优先
            if (sc > best_score) or (
                sc == best_score
                and etf.get("amount", 0) > (best or {}).get("amount", 0)
            ):
                best_score = sc
                best = etf

        if not best or best_score < score_threshold:
            continue

        record = {
            "sector_name": sname,
            "sector_type": sector.get("type", ""),
            "sector_code": sector.get("code", ""),
            "sector_amount": float(sector.get("amount", 0) or 0),
            "etf_code": best.get("code", ""),
            "etf_name": best.get("name", ""),
            "yf_ticker": best.get("yf_ticker", ""),
            "etf_amount": float(best.get("amount", 0) or 0),
            "match_score": round(float(best_score), 2),
        }

        etf_key = record.get("yf_ticker") or record.get("etf_code") or ""
        if not etf_key:
            continue

        prev = best_by_etf.get(etf_key)
        if _is_better_match(record, prev):
            best_by_etf[etf_key] = record

    matched = list(best_by_etf.values())

    # 输出稳定：优先 match_score，再看 etf_amount
    matched.sort(
        key=lambda x: (x.get("match_score", 0), x.get("etf_amount", 0)),
        reverse=True,
    )

    print(
        f"[INFO] 成功匹配 {len(matched)} 个板块→ETF（ETF去重后）"
        f" (阈值={score_threshold}, min_amount={min_amount}, candidates={len(candidates)})"
    )
    return matched


# ═══════════════════════════════════════════════
#  4. yfinance 下载（增强容错：失败重试两次）
# ═══════════════════════════════════════════════
def _standardize_df(df: pd.DataFrame) -> pd.DataFrame | None:
    if df is None or df.empty:
        return None

    # 有些版本会返回多级列，尽量压平
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[-1] for c in df.columns]

    keep_cols = [
        c for c in ["Open", "High", "Low", "Close", "Volume"] if c in df.columns
    ]
    if "Close" not in keep_cols:
        return None

    out = df[keep_cols].copy()
    out = out.dropna(subset=["Close"])
    if "Volume" in out.columns:
        out["Volume"] = out["Volume"].fillna(0)
    return out


def _download_one(
    ticker: str, period: str = "1y", min_rows: int = 80, max_tries: int = 3
):
    """
    下载单只 ETF 的日线数据
    - 失败后再给两次机会（max_tries=3）
    - 指数退避 + jitter
    - 两种方式尝试：Ticker().history 和 yf.download 单票
    """
    last_err = None
    for attempt in range(1, max_tries + 1):
        try:
            t = yf.Ticker(ticker)
            df = t.history(period=period, auto_adjust=True)
            df = _standardize_df(df)
            if df is not None and len(df) >= min_rows:
                return (ticker, df)
        except Exception as e:
            last_err = e

        try:
            df2 = yf.download(
                tickers=ticker,
                period=period,
                interval="1d",
                auto_adjust=True,
                threads=False,
                progress=False,
            )
            df2 = _standardize_df(df2)
            if df2 is not None and len(df2) >= min_rows:
                return (ticker, df2)
        except Exception as e:
            last_err = e

        sleep_s = (0.6 * (2 ** (attempt - 1))) + random.uniform(0.0, 0.35)
        time.sleep(sleep_s)

    if last_err:
        print(
            f"[WARN] yfinance下载失败: {ticker} ({type(last_err).__name__}: {last_err})"
        )
    return (ticker, None)


def download_data(
    tickers: list[str],
    period: str = "1y",
    max_workers: int = 8,
) -> dict[str, pd.DataFrame]:
    """并发下载多只 ETF 的日线数据（单票含重试）"""
    tickers = list(dict.fromkeys([t for t in tickers if t]))  # 去重 + 去空
    data_dict: dict[str, pd.DataFrame] = {}
    failed: list[str] = []

    print(f"[INFO] 开始下载 {len(tickers)} 只ETF数据 (并发={max_workers}) ...")

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futs = {pool.submit(_download_one, t, period): t for t in tickers}
        done_count = 0
        for fut in as_completed(futs):
            ticker, df = fut.result()
            done_count += 1
            if df is not None:
                data_dict[ticker] = df
            else:
                failed.append(ticker)

            if done_count % 50 == 0 or done_count == len(tickers):
                print(
                    f"  ... 已完成 {done_count}/{len(tickers)}  成功={len(data_dict)}  失败={len(failed)}"
                )

    print(f"[INFO] 下载完成: 成功={len(data_dict)}, 失败={len(failed)}")
    if failed:
        print(f"[INFO] 失败示例(最多20): {failed[:20]}")
    return data_dict
