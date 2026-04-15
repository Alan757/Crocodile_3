#!/usr/bin/env python3
"""
main.py — A股板块短线筛选器
────────────────────────────
本地运行 → 输出 report_YYYY-MM-DD.html
GitHub Actions → 发送邮件到 QQ 邮箱（支持多个收件人）
"""

import os
import sys
import datetime
import smtplib
import re
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path

from screener.fetcher import (
    get_sectors,
    get_etfs,
    match_sectors_to_etfs,
    download_data,
)
from screener.analyzer import analyze_all, get_rankings
from screener.report import generate_html

# ═══════════════════════════════════════════════
#  配置
# ═══════════════════════════════════════════════
LOCAL_MODE = os.getenv("LOCAL_MODE", "1") == "1"

SMTP_SERVER = "smtp.qq.com"
SMTP_PORT = 465
SMTP_USER = (os.environ.get("SMTP_USER", "") or "").strip()
SMTP_PASS = (os.environ.get("SMTP_PASS", "") or "").strip()  # QQ邮箱授权码

# TO_EMAIL 支持：a@qq.com,b@qq.com 或 a@qq.com; b@qq.com 或空格分隔
TO_EMAIL_RAW = (os.environ.get("TO_EMAIL", "") or "").strip()


# ═══════════════════════════════════════════════
#  工具：解析/校验邮箱列表
# ═══════════════════════════════════════════════
EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")


def parse_email_list(raw: str) -> list[str]:
    """
    支持多个收件人：
      - 逗号 ,   分号 ;  中文分号；  空格/换行
    会去掉空白并去重，返回合法邮箱列表。
    """
    if not raw:
        return []
    # 统一分隔符为空格
    s = raw.replace("；", ";").replace(",", " ").replace(";", " ")
    parts = [p.strip() for p in s.split() if p.strip()]
    # 去除诸如 "Name <a@qq.com>" 这种格式里的尖括号内容（如果用户误填）
    cleaned = []
    for p in parts:
        p2 = p
        if "<" in p2 and ">" in p2:
            p2 = p2[p2.find("<") + 1 : p2.find(">")].strip()
        cleaned.append(p2)

    # 过滤非法
    ok = []
    for e in cleaned:
        if EMAIL_RE.match(e):
            ok.append(e)

    # 去重（保持顺序）
    seen = set()
    out = []
    for e in ok:
        if e not in seen:
            out.append(e)
            seen.add(e)
    return out


# ═══════════════════════════════════════════════
#  邮件发送
# ═══════════════════════════════════════════════
def send_email(html: str, subject: str) -> bool:
    if not SMTP_USER or not SMTP_PASS:
        print("[ERROR] SMTP_USER / SMTP_PASS 未配置，跳过发送邮件")
        return False

    to_list = parse_email_list(TO_EMAIL_RAW) or (
        [SMTP_USER] if EMAIL_RE.match(SMTP_USER) else []
    )
    if not to_list:
        print(
            "[ERROR] TO_EMAIL 为空或格式不合法，且 SMTP_USER 也不是合法邮箱，无法发送"
        )
        print(f"        TO_EMAIL(raw)={TO_EMAIL_RAW!r}  SMTP_USER={SMTP_USER!r}")
        return False

    # 构建邮件
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = SMTP_USER
    msg["To"] = ", ".join(to_list)  # 头部展示用，实际收件人用 to_list
    msg.attach(MIMEText(html, "html", "utf-8"))

    print(f"[INFO] 准备发送邮件: From={SMTP_USER}  To={to_list}")

    try:
        with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT, timeout=30) as s:
            s.login(SMTP_USER, SMTP_PASS)
            s.sendmail(SMTP_USER, to_list, msg.as_string())
        print(f"[INFO] 邮件已发送成功")
        return True
    except smtplib.SMTPException as e:
        # 把 SMTP 返回码/信息打印清楚
        print(f"[ERROR] 邮件发送失败(SMTPException): {e}")
        return False
    except Exception as e:
        print(f"[ERROR] 邮件发送失败: {type(e).__name__}: {e}")
        return False


# ═══════════════════════════════════════════════
#  主流程
# ═══════════════════════════════════════════════
def main():
    today = datetime.date.today().strftime("%Y-%m-%d")
    print(f"\n{'='*50}")
    print(f"  A股板块短线筛选器  {today}")
    print(f"{'='*50}\n")

    # 1️⃣ 获取板块 & ETF 列表
    print("── Step 1: 获取板块和ETF列表 ──")
    sectors = get_sectors()
    etfs = get_etfs()

    if not sectors:
        print("[ERROR] 未获取到板块数据，退出")
        sys.exit(1)
    if not etfs:
        print("[ERROR] 未获取到ETF数据，退出")
        sys.exit(1)

    # 2️⃣ 匹配板块 → ETF
    print("\n── Step 2: 板块匹配ETF ──")
    matched = match_sectors_to_etfs(sectors, etfs)
    if not matched:
        print("[ERROR] 没有任何板块匹配到ETF，退出")
        sys.exit(1)

    # 打印匹配结果预览
    print(f"  匹配示例（前10个）：")
    for m in matched[:10]:
        print(f"    {m['sector_name']:<12s} → {m['etf_name']:<24s} ({m['yf_ticker']})")

    # 3️⃣ 下载 yfinance 数据
    print("\n── Step 3: 下载ETF日线数据 ──")
    tickers = [m["yf_ticker"] for m in matched]
    data_dict = download_data(tickers, period="1y", max_workers=10)

    if not data_dict:
        print("[ERROR] 未下载到任何数据，退出")
        sys.exit(1)

    # 4️⃣ 分析
    print("\n── Step 4: 技术分析 ──")
    results = analyze_all(matched, data_dict)
    rankings = get_rankings(results)

    print(f"  头等马候选: {len([r for r in results if r.get('top_score', 0) > 0])}")
    print(f"  黑马候选:   {len([r for r in results if r.get('dark_score', 0) > 0])}")
    print(f"  突破策略:   {len(rankings.get('breakthroughs', []))}")
    print(f"  弱转强策略: {len(rankings.get('weak_to_strongs', []))}")

    # 5️⃣ 生成报告
    print("\n── Step 5: 生成报告 ──")
    html = generate_html(rankings)

    if LOCAL_MODE:
        filename = f"report_{today}.html"
        Path(filename).write_text(html, encoding="utf-8")
        print(f"[OK] 报告已保存: {filename}")
    else:
        subject = f"📊 A股板块短线筛选 - {today}"
        ok = send_email(html, subject)
        if not ok:
            # 即使邮件失败，也把报告落地，便于你在 Actions artifacts 里拿到
            filename = f"report_{today}.html"
            Path(filename).write_text(html, encoding="utf-8")
            print(f"[WARN] 邮件失败，已改为保存报告文件: {filename}")

    print(f"\n{'='*50}")
    print("  完成！")
    print(f"{'='*50}\n")


if __name__ == "__main__":
    main()
