"""
report.py
─────────
生成美观的 HTML 报告（本地打开 / 邮件正文通用）
"""

import datetime


def _badge(text: str, color: str = "#e74c3c", bg: str = "#fff5f5") -> str:
    return (
        f'<span style="display:inline-block;padding:2px 8px;border-radius:4px;'
        f'font-size:12px;margin:1px 2px;color:{color};background:{bg};'
        f'font-weight:600;">{text}</span>'
    )


def _tag_html(tags: list[str]) -> str:
    palette = {
        "周线二连阳": ("#c0392b", "#fff0f0"),
        "OBV↑": ("#27ae60", "#eafaf1"),
    }
    parts = []
    for t in tags:
        if t.startswith("近期放量"):
            parts.append(_badge(t, "#e67e22", "#fef9e7"))
        else:
            c, bg = palette.get(t, ("#555", "#f0f0f0"))
            parts.append(_badge(t, c, bg))
    return " ".join(parts)


def _row(rank: int, r: dict, extra_col: str = "") -> str:
    info = r["info"]
    name = (
        f'{info["sector_name"]}<br><small style="color:#888">{info["etf_name"]}</small>'
    )
    code = info["etf_code"]
    gap = r["gap_pct"]
    gap_color = "#e74c3c" if gap >= 0 else "#27ae60"
    tags = _tag_html(r["tags"])
    extra = ""
    if extra_col:
        extra = f"<td>{extra_col}</td>"

    return f"""<tr>
      <td style="text-align:center;font-weight:700;">{rank}</td>
      <td>{name}</td>
      <td style="text-align:center;">{code}</td>
      <td style="text-align:right;">{r['close']}</td>
      <td style="text-align:right;">{r['ma60']}</td>
      <td style="text-align:right;color:{gap_color};font-weight:600;">{gap:+.2f}%</td>
      {extra}
      <td>{tags}</td>
    </tr>"""


def _section(
    title: str,
    icon: str,
    color: str,
    rows_html: str,
    headers: list[str],
    subtitle: str = "",
) -> str:
    th = "".join(f"<th>{h}</th>" for h in headers)
    sub = (
        f'<p style="margin:4px 0 0;font-size:14px;opacity:0.85;">{subtitle}</p>'
        if subtitle
        else ""
    )
    return f"""
    <div style="background:#fff;border-radius:14px;padding:28px 24px;
                margin:24px 0;box-shadow:0 2px 16px rgba(0,0,0,0.07);">
      <h2 style="margin:0 0 6px;font-size:22px;">
        <span style="margin-right:8px;">{icon}</span>
        <span style="color:{color};">{title}</span>
      </h2>
      {sub}
      <div style="overflow-x:auto;margin-top:16px;">
        <table style="width:100%;border-collapse:collapse;font-size:14px;">
          <thead><tr style="background:#f8f9fa;">{th}</tr></thead>
          <tbody>{rows_html}</tbody>
        </table>
      </div>
    </div>"""


def generate_html(rankings: dict) -> str:
    today = datetime.date.today().strftime("%Y-%m-%d")
    total = rankings["total"]

    # ── 头等马 ──
    top_rows = ""
    for i, r in enumerate(rankings["top_horses"], 1):
        top_rows += _row(i, r)
    top_headers = ["#", "板块 / ETF", "代码", "现价", "60MA", "偏离", "标签"]
    top_section = _section(
        "头等马 TOP 10",
        "🏇",
        "#c0392b",
        top_rows,
        top_headers,
        "日K持续运行在60日均线上方，趋势稳健",
    )

    # ── 黑马 ──
    dk_rows = ""
    for i, r in enumerate(rankings["dark_horses"], 1):
        dk_rows += _row(i, r)
    dk_headers = ["#", "板块 / ETF", "代码", "现价", "60MA", "偏离", "标签"]
    dk_section = _section(
        "黑马 TOP 10",
        "🐴",
        "#2d3436",
        dk_rows,
        dk_headers,
        "日K在60日线下方，但OBV上穿均线，量能先行",
    )

    # ── 突破策略 ──
    bk_rows = ""
    for i, r in enumerate(rankings["breakthroughs"][:15], 1):
        types = " / ".join(r["bk_types"])
        bk_rows += _row(
            i, r, extra_col=f'<span style="font-weight:600;">{types}</span>'
        )
    bk_headers = ["#", "板块 / ETF", "代码", "现价", "60MA", "偏离", "突破类型", "标签"]
    bk_section = _section(
        "突破策略",
        "🚀",
        "#00b894",
        bk_rows,
        bk_headers,
        "最近两天新高突破 或 突破60日均线",
    )

    # ── 弱转强 ──
    ws_rows = ""
    for i, r in enumerate(rankings["weak_to_strongs"][:15], 1):
        types = " / ".join(r["w2s_types"])
        ws_rows += _row(
            i, r, extra_col=f'<span style="font-weight:600;">{types}</span>'
        )
    ws_headers = ["#", "板块 / ETF", "代码", "现价", "60MA", "偏离", "策略类型", "标签"]
    ws_section = _section(
        "弱转强策略",
        "💪",
        "#6c5ce7",
        ws_rows,
        ws_headers,
        "OBV支撑弱转强 或 60日均线支撑弱转强",
    )

    html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>A股板块短线筛选 - {today}</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, 'PingFang SC',
                 'Microsoft YaHei', sans-serif;
    background: #f0f2f5;
    color: #333;
    line-height: 1.6;
  }}
  .container {{ max-width: 960px; margin: 0 auto; padding: 20px 16px; }}
  table th {{
    padding: 10px 12px;
    text-align: left;
    font-weight: 600;
    font-size: 13px;
    color: #555;
    border-bottom: 2px solid #dee2e6;
  }}
  table td {{
    padding: 9px 12px;
    border-bottom: 1px solid #f0f0f0;
    vertical-align: middle;
  }}
  table tbody tr:hover {{ background: #f8f9fa; }}
  small {{ font-size: 12px; }}
  @media (max-width: 640px) {{
    table {{ font-size: 12px; }}
    table th, table td {{ padding: 6px 4px; }}
  }}
</style>
</head>
<body>
<div class="container">

  <!-- Header -->
  <div style="background:linear-gradient(135deg,#1a1a2e 0%,#16213e 50%,#0f3460 100%);
              color:#fff;border-radius:16px;padding:36px 32px;margin-bottom:8px;
              box-shadow:0 4px 24px rgba(0,0,0,0.15);">
    <h1 style="font-size:28px;margin-bottom:8px;">📊 A股板块短线筛选报告</h1>
    <p style="font-size:15px;opacity:0.85;">
      日期：{today}　·　共分析 <b>{total}</b> 个板块ETF
    </p>
    <p style="font-size:13px;opacity:0.65;margin-top:12px;">
      数据来源：东方财富板块列表 + yfinance ETF日线　·　
      推荐标签：周线二连阳 / 近期放量 / OBV向上
    </p>
  </div>

  {top_section}
  {dk_section}
  {bk_section}
  {ws_section}

  <div style="text-align:center;padding:24px;color:#aaa;font-size:12px;">
    本报告由 Python 自动生成，仅供参考，不构成投资建议。
  </div>

</div>
</body>
</html>"""

    return html
