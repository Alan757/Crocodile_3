#!/usr/bin/env python3
"""
main.py — A股板块短线筛选器
────────────────────────────
本地运行 → 输出 report_YYYY-MM-DD.html
GitHub Actions → 发送邮件到 QQ 邮箱
"""

import os
import sys
import datetime
import smtplib
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
SMTP_USER = os.environ.get("SMTP_USER", "")
SMTP_PASS = os.environ.get("SMTP_PASS", "")  # QQ邮箱授权码
TO_EMAIL = os.environ.get("TO_EMAIL", "") or SMTP_USER


# ═══════════════════════════════════════════════
#  邮件发送
# ═══════════════════════════════════════════════
def send_email(html: str, subject: str):
    if not SMTP_USER or not SMTP_PASS:
        print("[ERROR] SMTP_USER / SMTP_PASS 未配置，跳过发送邮件")
        return False

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = SMTP_USER
    msg["To"] = TO_EMAIL
    msg.attach(MIMEText(html, "html", "utf-8"))

    try:
        with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT, timeout=30) as s:
            s.login(SMTP_USER, SMTP_PASS)
            s.sendmail(SMTP_USER, [TO_EMAIL], msg.as_string())
        print(f"[INFO] 邮件已发送至 {TO_EMAIL}")
        return True
    except Exception as e:
        print(f"[ERROR] 邮件发送失败: {e}")
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
        print(f"    {m['sector_name']:10s} → {m['etf_name']:20s} ({m['yf_ticker']})")

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

    print(f"  头等马候选: {len([r for r in results if r['top_score']>0])}")
    print(f"  黑马候选:   {len([r for r in results if r['dark_score']>0])}")
    print(f"  突破策略:   {len(rankings['breakthroughs'])}")
    print(f"  弱转强策略: {len(rankings['weak_to_strongs'])}")

    # 5️⃣ 生成报告
    print("\n── Step 5: 生成报告 ──")
    html = generate_html(rankings)

    if LOCAL_MODE:
        # 本地模式：保存 HTML 文件
        filename = f"report_{today}.html"
        Path(filename).write_text(html, encoding="utf-8")
        print(f"[OK] 报告已保存: {filename}")
        print(f"     用浏览器打开即可查看")
    else:
        # GitHub Actions 模式：发邮件
        subject = f"📊 A股板块短线筛选 - {today}"
        send_email(html, subject)

    print(f"\n{'='*50}")
    print("  完成！")
    print(f"{'='*50}\n")


if __name__ == "__main__":
    main()
