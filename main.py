"""
投資アイデア抽出・データ取得・クロスセクション分析 統合ツール

使い方:
    python main.py extract "https://www.youtube.com/watch?v=XXXXX"
    python main.py fetch 7203 6758 9984
    python main.py update
    python main.py analyze
    python main.py status
    python main.py export
    python main.py pipeline "https://www.youtube.com/watch?v=XXXXX" 7203 6758 9984
"""

import argparse
import json
import os
import re
import sqlite3
import subprocess
import sys
import time as time_mod
import warnings
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import numpy as np
import pandas as pd
import yfinance as yf
from scipy import stats

warnings.filterwarnings("ignore", category=FutureWarning)


# ══════════════════════════════════════════════════════════
#  共通定数
# ══════════════════════════════════════════════════════════

BASE_DIR = Path(__file__).parent
DB_DIR = BASE_DIR / "data"
DB_PATH = DB_DIR / "stock_data.db"
CSV_DIR = DB_DIR / "csv"

JP_SUFFIX = ".T"
DEFAULT_START = (datetime.now() - timedelta(days=5 * 365)).strftime("%Y-%m-%d")


# ══════════════════════════════════════════════════════════
#  Phase 1: YouTube → 投資アイデア抽出
# ══════════════════════════════════════════════════════════

KEYWORDS = [
    # 相場・取引手法
    "逆日歩", "空売り", "信用取引", "信用買い", "信用売り", "貸借", "制度信用", "一般信用",
    "損切り", "ロスカット", "利確", "利食い", "ナンピン", "両建て", "ヘッジ",
    "スプレッド", "サヤ取り", "裁定", "アービトラージ",
    "ショート", "ロング", "ポジション", "エントリー", "イグジット",
    # ファンダメンタルズ
    "決算", "業績", "上方修正", "下方修正", "増収", "減収", "増益", "減益",
    "PER", "PBR", "ROE", "ROA", "EPS", "BPS", "配当", "自社株買い", "株主還元",
    "売上高", "営業利益", "経常利益", "純利益", "EBITDA",
    "割安", "割高", "バリュー", "グロース",
    # マクロ・市場
    "金利", "利上げ", "利下げ", "インフレ", "デフレ", "円安", "円高",
    "日経平均", "TOPIX", "マザーズ", "グロース市場", "プライム",
    "出来高", "売買代金", "騰落", "新高値", "新安値",
    "IPO", "上場", "TOB", "MBO", "公募増資",
    # セクター・テーマ
    "半導体", "AI", "生成AI", "DX", "EV", "再エネ", "脱炭素",
    "インバウンド", "防衛", "宇宙", "量子", "バイオ", "医薬",
    # リスク・イベント
    "地政学", "戦争", "制裁", "関税", "規制", "SQ", "MSQ", "メジャーSQ",
    "権利落ち", "権利確定", "ex-dividend",
]

STOCK_CODE_PATTERN = re.compile(r"[1-9]\d{3}(?:\s|　|\.)")
KEYWORD_PATTERN = re.compile("|".join(re.escape(kw) for kw in KEYWORDS), re.IGNORECASE)
MIN_TEXT_LENGTH = 8


def _extract_video_id(url_or_id: str) -> str:
    if len(url_or_id) == 11 and re.match(r"^[A-Za-z0-9_-]+$", url_or_id):
        return url_or_id
    parsed = urlparse(url_or_id)
    if parsed.hostname in ("www.youtube.com", "youtube.com", "m.youtube.com"):
        if parsed.path == "/watch":
            qs = parse_qs(parsed.query)
            if "v" in qs:
                return qs["v"][0]
        elif parsed.path.startswith("/live/"):
            return parsed.path.split("/live/")[1].split("?")[0]
    elif parsed.hostname == "youtu.be":
        return parsed.path.lstrip("/")
    return url_or_id.strip()


def _is_relevant(text: str) -> bool:
    if len(text) < MIN_TEXT_LENGTH:
        return False
    return bool(KEYWORD_PATTERN.search(text) or STOCK_CODE_PATTERN.search(text))


def _extract_matched_keywords(text: str) -> list[str]:
    found = set()
    for m in KEYWORD_PATTERN.finditer(text):
        found.add(m.group())
    for m in STOCK_CODE_PATTERN.finditer(text):
        found.add(m.group().strip())
    return sorted(found)


def _fetch_transcript(video_id: str) -> list[dict]:
    try:
        from youtube_transcript_api import YouTubeTranscriptApi
    except ImportError:
        print("[ERROR] youtube-transcript-api がインストールされていません。")
        return []

    print(f"[INFO] 字幕を取得中... (video_id={video_id})")
    try:
        transcript_list = YouTubeTranscriptApi.list_transcripts(video_id)
        transcript = None
        for lang in ["ja", "en"]:
            try:
                transcript = transcript_list.find_transcript([lang])
                break
            except Exception:
                continue
        if transcript is None:
            try:
                transcript = transcript_list.find_generated_transcript(["ja", "en"])
            except Exception:
                pass
        if transcript is None:
            print("[WARN] 利用可能な字幕が見つかりませんでした。")
            return []
        entries = transcript.fetch()
        print(f"[INFO] 字幕 {len(entries)} 件を取得しました。")
        return [{"time": e.start, "text": e.text} for e in entries]
    except Exception as e:
        print(f"[WARN] 字幕取得に失敗しました: {e}")
        return []


def _fetch_chat_replay(video_id: str) -> list[dict]:
    url = f"https://www.youtube.com/watch?v={video_id}"
    print("[INFO] チャットリプレイを取得中... (yt-dlp)")
    try:
        cmd = [
            sys.executable, "-m", "yt_dlp",
            "--skip-download", "--write-subs",
            "--sub-lang", "live_chat", "--sub-format", "json3",
            "-o", f"_chat_{video_id}", url,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)

        candidates = [
            f"_chat_{video_id}.live_chat.json3",
            f"_chat_{video_id}.live_chat.json",
            f"_chat_{video_id}.ja.json3",
            f"_chat_{video_id}.json3",
            f"_chat_{video_id}.json",
        ]
        chat_file = next((c for c in candidates if os.path.exists(c)), None)

        if chat_file is None:
            print("[WARN] チャットリプレイが見つかりませんでした（ライブ配信でない可能性）。")
            if result.stderr:
                for line in result.stderr.strip().split("\n")[-3:]:
                    print(f"       {line}")
            return []

        with open(chat_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        messages = []
        for event in data.get("events", []):
            seg = event.get("segs")
            if seg:
                text = "".join(s.get("utf8", "") for s in seg).strip()
                if text:
                    messages.append({
                        "time": event.get("tStartMs", 0) / 1000.0,
                        "text": text,
                        "source": "chat",
                    })
        try:
            os.remove(chat_file)
        except OSError:
            pass

        print(f"[INFO] チャットメッセージ {len(messages)} 件を取得しました。")
        return messages
    except FileNotFoundError:
        print("[WARN] yt-dlp が見つかりません。チャット取得をスキップします。")
        return []
    except subprocess.TimeoutExpired:
        print("[WARN] チャット取得がタイムアウトしました。スキップします。")
        return []
    except Exception as e:
        print(f"[WARN] チャット取得中にエラー: {e}")
        return []


def _format_time(seconds: float) -> str:
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    return f"{h:02d}:{m:02d}:{s:02d}" if h > 0 else f"{m:02d}:{s:02d}"


def _merge_nearby(entries: list[dict], window_sec: float = 30.0) -> list[dict]:
    if not entries:
        return []
    merged = []
    cur = {
        "time_start": entries[0]["time"], "time_end": entries[0]["time"],
        "texts": [entries[0]["text"]], "keywords": set(entries[0].get("keywords", [])),
        "source": entries[0].get("source", "subtitle"),
    }
    for entry in entries[1:]:
        if (entry["time"] - cur["time_end"]) <= window_sec and \
           entry.get("source", "subtitle") == cur["source"]:
            cur["time_end"] = entry["time"]
            cur["texts"].append(entry["text"])
            cur["keywords"].update(entry.get("keywords", []))
        else:
            merged.append(cur)
            cur = {
                "time_start": entry["time"], "time_end": entry["time"],
                "texts": [entry["text"]], "keywords": set(entry.get("keywords", [])),
                "source": entry.get("source", "subtitle"),
            }
    merged.append(cur)
    return merged


class IdeaExtractor:
    """YouTube動画から投資アイデアを抽出する。"""

    @staticmethod
    def run(url: str, output: str = "investment_ideas.md", no_chat: bool = False) -> str:
        video_id = _extract_video_id(url)
        print(f"[INFO] 対象動画ID: {video_id}\n")

        transcript = _fetch_transcript(video_id)
        chat = [] if no_chat else _fetch_chat_replay(video_id)

        if not transcript and not chat:
            print("[ERROR] 字幕・チャットともに取得できませんでした。")
            return ""

        # フィルタリング
        relevant = []
        for src_name, src_data in [("subtitle", transcript), ("chat", chat)]:
            for entry in src_data:
                text = entry["text"].strip()
                if _is_relevant(text):
                    relevant.append({
                        "time": entry["time"], "text": text,
                        "keywords": _extract_matched_keywords(text),
                        "source": src_name,
                    })
        relevant.sort(key=lambda x: x["time"])
        entries = _merge_nearby(relevant)
        print(f"\n[INFO] {len(entries)} 件の関連セグメントを検出しました。")

        # Markdown生成
        kw_counts: dict[str, int] = {}
        for e in entries:
            for kw in e["keywords"]:
                kw_counts[kw] = kw_counts.get(kw, 0) + 1

        lines = [
            "# 投資アイデア抽出レポート", "",
            f"- **動画**: https://www.youtube.com/watch?v={video_id}",
            f"- **抽出日時**: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            f"- **抽出件数**: {len(entries)} 件", "",
        ]
        if kw_counts:
            lines += ["## 頻出キーワード", ""]
            for kw, cnt in sorted(kw_counts.items(), key=lambda x: -x[1])[:20]:
                lines.append(f"- **{kw}** ({cnt}回)")
            lines.append("")

        subs = [e for e in entries if e["source"] == "subtitle"]
        chats = [e for e in entries if e["source"] == "chat"]
        if subs:
            lines += ["## 字幕からの抽出", ""]
            for e in subs:
                tags = " ".join(f"`{kw}`" for kw in sorted(e["keywords"]))
                txt = " ".join(e["texts"])[:500]
                lines += [f"### [{_format_time(e['time_start'])}] {tags}", "", f"> {txt}", ""]
        if chats:
            lines += ["## チャットからの抽出", ""]
            for e in chats:
                tags = " ".join(f"`{kw}`" for kw in sorted(e["keywords"]))
                txt = " ".join(e["texts"])[:300]
                lines.append(f"- **[{_format_time(e['time_start'])}]** {tags} — {txt}")
            lines.append("")
        if not entries:
            lines += ["## 結果", "", "投資関連のキーワードに該当する発言は検出されませんでした。", ""]

        with open(output, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
        print(f"[DONE] 結果を {output} に出力しました。")
        return output


# ══════════════════════════════════════════════════════════
#  Phase 2: 株価・ファンダメンタル データパイプライン
# ══════════════════════════════════════════════════════════

class StockDataPipeline:
    """日本株データの取得・保存・差分更新を管理するパイプライン。"""

    def __init__(self, db_path: str | Path | None = None):
        self.db_path = Path(db_path) if db_path else DB_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS price_daily (
                    ticker TEXT NOT NULL, date TEXT NOT NULL,
                    open REAL, high REAL, low REAL, close REAL,
                    adj_close REAL, volume INTEGER,
                    PRIMARY KEY (ticker, date))""")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS fundamentals (
                    ticker TEXT NOT NULL, updated_at TEXT NOT NULL,
                    short_name TEXT, sector TEXT, industry TEXT,
                    market_cap INTEGER, enterprise_value INTEGER,
                    trailing_pe REAL, forward_pe REAL, price_to_book REAL,
                    price_to_sales REAL, dividend_yield REAL, payout_ratio REAL,
                    roe REAL, roa REAL, profit_margins REAL,
                    revenue INTEGER, ebitda INTEGER,
                    total_debt INTEGER, total_cash INTEGER,
                    book_value REAL, shares_outstanding INTEGER,
                    fifty_two_week_high REAL, fifty_two_week_low REAL,
                    PRIMARY KEY (ticker))""")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS update_log (
                    ticker TEXT NOT NULL, last_updated TEXT NOT NULL,
                    rows_added INTEGER DEFAULT 0,
                    PRIMARY KEY (ticker))""")

    def _conn(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    @staticmethod
    def to_yf_ticker(code: str) -> str:
        code = code.strip()
        return code if code.endswith(JP_SUFFIX) else code + JP_SUFFIX

    def _get_last_date(self, ticker: str) -> str | None:
        with self._conn() as conn:
            row = conn.execute("SELECT MAX(date) FROM price_daily WHERE ticker = ?", (ticker,)).fetchone()
        return row[0] if row and row[0] else None

    def _fetch_price(self, code: str, start: str | None = None) -> pd.DataFrame:
        ticker_obj = yf.Ticker(self.to_yf_ticker(code))
        df = ticker_obj.history(start=start or DEFAULT_START, end=None, auto_adjust=False)
        if df.empty:
            return pd.DataFrame()
        df = df.reset_index().rename(columns={
            "Date": "date", "Open": "open", "High": "high", "Low": "low",
            "Close": "close", "Adj Close": "adj_close", "Volume": "volume",
        })
        keep = [c for c in ["date", "open", "high", "low", "close", "adj_close", "volume"] if c in df.columns]
        df = df[keep]
        if "adj_close" not in df.columns:
            df["adj_close"] = df["close"]
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
        df["ticker"] = code
        return df

    def _fetch_fundamentals(self, code: str) -> dict:
        try:
            info = yf.Ticker(self.to_yf_ticker(code)).info or {}
        except Exception:
            info = {}
        if not info:
            return {}
        field_map = {
            "short_name": "shortName", "sector": "sector", "industry": "industry",
            "market_cap": "marketCap", "enterprise_value": "enterpriseValue",
            "trailing_pe": "trailingPE", "forward_pe": "forwardPE",
            "price_to_book": "priceToBook", "price_to_sales": "priceToSalesTrailing12Months",
            "dividend_yield": "dividendYield", "payout_ratio": "payoutRatio",
            "roe": "returnOnEquity", "roa": "returnOnAssets",
            "profit_margins": "profitMargins", "revenue": "totalRevenue",
            "ebitda": "ebitda", "total_debt": "totalDebt", "total_cash": "totalCash",
            "book_value": "bookValue", "shares_outstanding": "sharesOutstanding",
            "fifty_two_week_high": "fiftyTwoWeekHigh", "fifty_two_week_low": "fiftyTwoWeekLow",
        }
        row = {"ticker": code, "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M")}
        for db_col, yf_key in field_map.items():
            row[db_col] = info.get(yf_key)
        return row

    @staticmethod
    def cleanse(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        df = df.copy()
        df = df.drop_duplicates(subset=["ticker", "date"], keep="last").sort_values("date")
        num_cols = ["open", "high", "low", "close", "adj_close"]
        for col in num_cols:
            if col in df.columns:
                df[col] = df[col].ffill()
                df.loc[df[col] <= 0, col] = pd.NA
                df[col] = df[col].ffill()
        if "volume" in df.columns:
            df["volume"] = df["volume"].fillna(0).astype(int)
        return df.dropna(subset=["close"]).reset_index(drop=True)

    def _save_price(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        with self._conn() as conn:
            conn.executemany("""
                INSERT INTO price_daily (ticker, date, open, high, low, close, adj_close, volume)
                VALUES (:ticker, :date, :open, :high, :low, :close, :adj_close, :volume)
                ON CONFLICT(ticker, date) DO UPDATE SET
                    open=excluded.open, high=excluded.high, low=excluded.low,
                    close=excluded.close, adj_close=excluded.adj_close, volume=excluded.volume
            """, df.to_dict("records"))
        return len(df)

    def _save_fundamentals(self, data: dict):
        if not data:
            return
        cols = list(data.keys())
        placeholders = ", ".join(f":{c}" for c in cols)
        updates = ", ".join(f"{c}=excluded.{c}" for c in cols if c != "ticker")
        with self._conn() as conn:
            conn.execute(
                f"INSERT INTO fundamentals ({','.join(cols)}) VALUES ({placeholders}) "
                f"ON CONFLICT(ticker) DO UPDATE SET {updates}", data)

    def _log_update(self, ticker: str, rows_added: int):
        with self._conn() as conn:
            conn.execute("""
                INSERT INTO update_log (ticker, last_updated, rows_added) VALUES (?,?,?)
                ON CONFLICT(ticker) DO UPDATE SET last_updated=excluded.last_updated, rows_added=excluded.rows_added
            """, (ticker, datetime.now().strftime("%Y-%m-%d %H:%M"), rows_added))

    def update(self, codes: list[str], full_refresh: bool = False) -> dict:
        results = {}
        for code in codes:
            code = code.strip().replace(JP_SUFFIX, "")
            print(f"[{code}] 処理中...")
            start = DEFAULT_START
            if not full_refresh:
                last_date = self._get_last_date(code)
                if last_date:
                    next_day = (datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
                    if next_day >= datetime.now().strftime("%Y-%m-%d"):
                        print(f"  株価: 最新データ済み ({last_date})")
                        fdata = self._fetch_fundamentals(code)
                        self._save_fundamentals(fdata)
                        self._log_update(code, 0)
                        results[code] = {"price_rows": 0, "fundamentals": bool(fdata)}
                        continue
                    start = next_day
                    print(f"  株価: {start} 以降を差分取得")
            try:
                df = self._fetch_price(code, start=start)
            except Exception as e:
                print(f"  [ERROR] 株価取得失敗: {e}")
                results[code] = {"price_rows": 0, "fundamentals": False, "error": str(e)}
                continue
            if df.empty:
                print(f"  [WARN] 株価データなし（ティッカー: {self.to_yf_ticker(code)}）")
                results[code] = {"price_rows": 0, "fundamentals": False}
                continue
            df = self.cleanse(df)
            n = self._save_price(df)
            print(f"  株価: {n} 行を保存")
            fdata = self._fetch_fundamentals(code)
            self._save_fundamentals(fdata)
            if fdata:
                pbr = fdata.get("price_to_book")
                mcap = fdata.get("market_cap")
                mcap_str = f"¥{mcap / 1e9:.1f}B" if mcap else "N/A"
                print(f"  ファンダメンタル: PBR={pbr}, 時価総額={mcap_str}")
            self._log_update(code, n)
            results[code] = {"price_rows": n, "fundamentals": bool(fdata)}
            time_mod.sleep(0.5)
        print(f"\n[DONE] {len(codes)} 銘柄の更新完了 → {self.db_path}")
        return results

    def update_all(self) -> dict:
        codes = self.list_tickers()
        if not codes:
            print("[INFO] 登録済み銘柄がありません。")
            return {}
        print(f"[INFO] 登録済み {len(codes)} 銘柄を一括更新します。")
        return self.update(codes)

    def load(self, code: str, start: str | None = None, end: str | None = None) -> pd.DataFrame:
        code = code.strip().replace(JP_SUFFIX, "")
        query = "SELECT * FROM price_daily WHERE ticker = ?"
        params: list = [code]
        if start:
            query += " AND date >= ?"; params.append(start)
        if end:
            query += " AND date <= ?"; params.append(end)
        query += " ORDER BY date"
        with self._conn() as conn:
            df_price = pd.read_sql_query(query, conn, params=params)
            df_fund = pd.read_sql_query("SELECT * FROM fundamentals WHERE ticker = ?", conn, params=[code])
        if df_price.empty:
            return pd.DataFrame()
        if not df_fund.empty:
            fund_row = df_fund.iloc[0].to_dict()
            fund_row.pop("ticker", None); fund_row.pop("updated_at", None)
            for col, val in fund_row.items():
                df_price[col] = val
        return df_price

    def load_all(self, start: str | None = None, end: str | None = None) -> pd.DataFrame:
        frames = [self.load(c, start, end) for c in self.list_tickers()]
        frames = [f for f in frames if not f.empty]
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def list_tickers(self) -> list[str]:
        with self._conn() as conn:
            return [r[0] for r in conn.execute("SELECT DISTINCT ticker FROM price_daily ORDER BY ticker").fetchall()]

    def status(self) -> pd.DataFrame:
        with self._conn() as conn:
            return pd.read_sql_query("""
                SELECT u.ticker, u.last_updated, u.rows_added AS last_rows_added,
                       MIN(p.date) AS first_date, MAX(p.date) AS last_date, COUNT(p.date) AS total_rows
                FROM update_log u LEFT JOIN price_daily p ON u.ticker = p.ticker
                GROUP BY u.ticker ORDER BY u.ticker""", conn)

    def export_csv(self, output_dir: str | Path | None = None):
        out = Path(output_dir) if output_dir else CSV_DIR
        out.mkdir(parents=True, exist_ok=True)
        codes = self.list_tickers()
        for code in codes:
            df = self.load(code)
            if not df.empty:
                path = out / f"{code}.csv"
                df.to_csv(path, index=False, encoding="utf-8-sig")
                print(f"  {path}")
        print(f"[DONE] {len(codes)} ファイルを {out} に出力しました。")

    def delete(self, code: str):
        code = code.strip().replace(JP_SUFFIX, "")
        with self._conn() as conn:
            for table in ["price_daily", "fundamentals", "update_log"]:
                conn.execute(f"DELETE FROM {table} WHERE ticker = ?", (code,))
        print(f"[INFO] {code} のデータを削除しました。")


# ══════════════════════════════════════════════════════════
#  Phase 3: クロスセクション分析 & バックテスト
# ══════════════════════════════════════════════════════════

# 分析パラメータ
TRADING_DAYS_1M = 21
TRADING_DAYS_3M = 63
VOLATILITY_WINDOW = 20
MOMENTUM_WINDOW = 60
ANNUALIZE_FACTOR = 252
QUANTILE_LOW = 1 / 3
QUANTILE_HIGH = 2 / 3
SIGNIFICANCE_LEVEL = 0.05


def _sharpe_ratio(returns: pd.Series, risk_free_annual: float = 0.001) -> float:
    if returns.empty or returns.std() == 0:
        return 0.0
    rf_daily = (1 + risk_free_annual) ** (1 / ANNUALIZE_FACTOR) - 1
    excess = returns - rf_daily
    return float(excess.mean() / excess.std() * np.sqrt(ANNUALIZE_FACTOR))


def _max_drawdown(cum_ret: pd.Series) -> float:
    if cum_ret.empty:
        return 0.0
    return float(((cum_ret - cum_ret.cummax()) / cum_ret.cummax()).min())


def _cohens_d(g1: pd.Series, g2: pd.Series) -> float:
    n1, n2 = len(g1), len(g2)
    if n1 < 2 or n2 < 2:
        return 0.0
    pooled = np.sqrt(((n1 - 1) * g1.var() + (n2 - 1) * g2.var()) / (n1 + n2 - 2))
    return 0.0 if pooled == 0 else float((g1.mean() - g2.mean()) / pooled)


class CrossSectionAnalyzer:
    """PBR × モメンタム/ボラティリティ のクロスセクション分析を実行する。"""

    def __init__(self, db_path: str | Path | None = None):
        self.db_path = Path(db_path) if db_path else DB_PATH
        if not self.db_path.exists():
            print(f"[ERROR] DB が見つかりません: {self.db_path}")
            print("  先に fetch コマンドでデータを取得してください。")
            sys.exit(1)

    def _load_prices(self) -> pd.DataFrame:
        with sqlite3.connect(self.db_path) as conn:
            df = pd.read_sql_query(
                "SELECT ticker, date, close, adj_close, volume FROM price_daily ORDER BY ticker, date", conn)
        df["date"] = pd.to_datetime(df["date"])
        return df

    def _load_fundamentals(self) -> pd.DataFrame:
        with sqlite3.connect(self.db_path) as conn:
            return pd.read_sql_query("SELECT * FROM fundamentals", conn)

    def _compute_features(self, prices: pd.DataFrame, fundamentals: pd.DataFrame) -> pd.DataFrame:
        frames = []
        for _, grp in prices.groupby("ticker"):
            g = grp.sort_values("date").copy()
            g["daily_return"] = g["adj_close"].pct_change()
            g["volatility"] = g["daily_return"].rolling(VOLATILITY_WINDOW).std() * np.sqrt(ANNUALIZE_FACTOR)
            g["momentum"] = g["adj_close"].pct_change(MOMENTUM_WINDOW)
            g["fwd_return_1m"] = g["adj_close"].shift(-TRADING_DAYS_1M) / g["adj_close"] - 1
            g["fwd_return_3m"] = g["adj_close"].shift(-TRADING_DAYS_3M) / g["adj_close"] - 1
            frames.append(g)
        df = pd.concat(frames, ignore_index=True)
        if not fundamentals.empty and "price_to_book" in fundamentals.columns:
            df["pbr"] = df["ticker"].map(fundamentals.set_index("ticker")["price_to_book"].to_dict())
        else:
            df["pbr"] = np.nan
        return df

    @staticmethod
    def _assign_group(series: pd.Series, low: str, high: str, mid: str) -> pd.Series:
        q_l, q_h = series.quantile(QUANTILE_LOW), series.quantile(QUANTILE_HIGH)
        return pd.Series(np.select([series <= q_l, series >= q_h], [low, high], default=mid), index=series.index)

    def _build_matrix(self, df: pd.DataFrame) -> pd.DataFrame:
        valid = df.dropna(subset=["pbr", "volatility", "momentum", "fwd_return_1m", "fwd_return_3m"]).copy()
        if valid.empty:
            return pd.DataFrame()
        valid["pbr_group"] = self._assign_group(valid["pbr"], "低PBR", "高PBR", "中PBR")
        valid["momentum_group"] = self._assign_group(valid["momentum"], "低モメンタム", "高モメンタム", "中モメンタム")
        valid["volatility_group"] = self._assign_group(valid["volatility"], "低ボラ", "高ボラ", "中ボラ")
        return valid

    def _cross_section_stats(self, matrix: pd.DataFrame, col_a: str, col_b: str) -> list[dict]:
        results = []
        for (ga, gb), grp in matrix.groupby([col_a, col_b]):
            if len(grp) < 5:
                continue
            daily_ret = grp["daily_return"].dropna()
            cum_ret = (1 + daily_ret).cumprod()
            results.append({
                "group_a": ga, "group_b": gb, "n_obs": len(grp),
                "fwd_1m_mean": grp["fwd_return_1m"].mean(), "fwd_1m_median": grp["fwd_return_1m"].median(),
                "fwd_3m_mean": grp["fwd_return_3m"].mean(), "fwd_3m_median": grp["fwd_return_3m"].median(),
                "sharpe": _sharpe_ratio(daily_ret),
                "max_dd": _max_drawdown(cum_ret) if not cum_ret.empty else 0.0,
            })
        return results

    @staticmethod
    def _test_significance(matrix: pd.DataFrame, group_col: str, return_col: str) -> list[dict]:
        groups = {lbl: grp[return_col].dropna() for lbl, grp in matrix.groupby(group_col)}
        groups = {k: v for k, v in groups.items() if len(v) >= 5}
        results = []
        if len(groups) < 2:
            return results
        all_g = list(groups.values())
        try: f_s, a_p = stats.f_oneway(*all_g)
        except Exception: f_s, a_p = np.nan, np.nan
        try: kw_s, kw_p = stats.kruskal(*all_g)
        except Exception: kw_s, kw_p = np.nan, np.nan
        results.append({"comparison": f"全群比較 ({return_col})", "test": "One-way ANOVA",
                         "statistic": f_s, "p_value": a_p,
                         "significant": a_p < SIGNIFICANCE_LEVEL if not np.isnan(a_p) else False})
        results.append({"comparison": f"全群比較 ({return_col})", "test": "Kruskal-Wallis",
                         "statistic": kw_s, "p_value": kw_p,
                         "significant": kw_p < SIGNIFICANCE_LEVEL if not np.isnan(kw_p) else False})
        labels = sorted(groups.keys())
        for i in range(len(labels)):
            for j in range(i + 1, len(labels)):
                a_v, b_v = groups[labels[i]], groups[labels[j]]
                try: t_s, t_p = stats.ttest_ind(a_v, b_v, equal_var=False)
                except Exception: t_s, t_p = np.nan, np.nan
                results.append({
                    "comparison": f"{labels[i]} vs {labels[j]} ({return_col})", "test": "Welch's t-test",
                    "statistic": t_s, "p_value": t_p, "cohens_d": _cohens_d(a_v, b_v),
                    "significant": t_p < SIGNIFICANCE_LEVEL if not np.isnan(t_p) else False,
                    "mean_a": a_v.mean(), "mean_b": b_v.mean(),
                })
        return results

    def _fmt_stats_table(self, rows: list[dict]) -> str:
        if not rows:
            return "*データ不足のため算出不可*"
        lines = [
            "| グループA | グループB | N | 1M平均(%) | 1M中央値(%) | 3M平均(%) | 3M中央値(%) | Sharpe | MaxDD(%) |",
            "|---|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
        for r in rows:
            lines.append(
                f"| {r['group_a']} | {r['group_b']} | {r['n_obs']:,} "
                f"| {r['fwd_1m_mean']*100:+.2f} | {r['fwd_1m_median']*100:+.2f} "
                f"| {r['fwd_3m_mean']*100:+.2f} | {r['fwd_3m_median']*100:+.2f} "
                f"| {r['sharpe']:.2f} | {r['max_dd']*100:.1f} |")
        return "\n".join(lines)

    def _fmt_test_table(self, rows: list[dict]) -> str:
        if not rows:
            return "*検定不可（サンプル不足）*"
        lines = [
            "| 比較 | 検定手法 | 統計量 | p値 | 有意 | Cohen's d | 平均A(%) | 平均B(%) |",
            "|---|---|---:|---:|:---:|---:|---:|---:|",
        ]
        for r in rows:
            sig = "**YES**" if r.get("significant") else "no"
            d = f"{r['cohens_d']:.3f}" if "cohens_d" in r else "-"
            ma = f"{r['mean_a']*100:+.2f}" if "mean_a" in r else "-"
            mb = f"{r['mean_b']*100:+.2f}" if "mean_b" in r else "-"
            sv = f"{r['statistic']:.3f}" if not np.isnan(r.get("statistic", np.nan)) else "N/A"
            pv = f"{r['p_value']:.4f}" if not np.isnan(r.get("p_value", np.nan)) else "N/A"
            lines.append(f"| {r['comparison']} | {r['test']} | {sv} | {pv} | {sig} | {d} | {ma} | {mb} |")
        return "\n".join(lines)

    def _fmt_perf(self, rows: list[dict], label: str) -> str:
        if not rows:
            return f"*{label}: データ不足*"
        bs = max(rows, key=lambda x: x["sharpe"])
        wd = min(rows, key=lambda x: x["max_dd"])
        b1 = max(rows, key=lambda x: x["fwd_1m_mean"])
        b3 = max(rows, key=lambda x: x["fwd_3m_mean"])
        return "\n".join([
            f"**{label}**", "",
            "| 指標 | グループ | 値 |", "|---|---|---|",
            f"| 最高Sharpe | {bs['group_a']}/{bs['group_b']} | {bs['sharpe']:.2f} |",
            f"| 最大DD | {wd['group_a']}/{wd['group_b']} | {wd['max_dd']*100:.1f}% |",
            f"| 最高1M | {b1['group_a']}/{b1['group_b']} | {b1['fwd_1m_mean']*100:+.2f}% |",
            f"| 最高3M | {b3['group_a']}/{b3['group_b']} | {b3['fwd_3m_mean']*100:+.2f}% |",
        ])

    def _make_conclusion(self, all_tests: list[dict], all_stats: list[dict]) -> str:
        pairwise = [t for t in all_tests if t["test"] == "Welch's t-test"]
        sig = [t for t in pairwise if t.get("significant")]
        meaningful = [t for t in sig if abs(t.get("cohens_d", 0)) >= 0.3]
        good_sharpe = [s for s in all_stats if s["sharpe"] >= 0.5]
        lines = []
        if not pairwise:
            return "### 判定: データ不足\n\n有効な比較ペアが存在しないため、判定不能です。\nより多くの銘柄を fetch してから再実行してください。"
        n_t, n_s, n_m = len(pairwise), len(sig), len(meaningful)
        lines += [
            "### 検定サマリ", "",
            f"- ペアワイズ比較数: **{n_t}**",
            f"- 統計的有意 (p < {SIGNIFICANCE_LEVEL}): **{n_s} / {n_t}**",
            f"- 実用的効果量 (|d| >= 0.3): **{n_m} / {n_t}**",
            f"- Sharpe >= 0.5 のグループ: **{len(good_sharpe)} / {len(all_stats)}**", "",
        ]
        if n_m >= 1 and len(good_sharpe) >= 1:
            lines += ["### 判定: 統計的優位性あり（条件付き）", "",
                       "以下の組み合わせに限定的なエッジが検出されました。",
                       "ただし、**サンプル外検証・取引コスト控除前**の結果です。", "",
                       "**有意な組み合わせ:**", ""]
            for t in meaningful:
                d_abs = abs(t.get("cohens_d", 0))
                el = "小" if d_abs < 0.5 else ("中" if d_abs < 0.8 else "大")
                lines.append(f"- {t['comparison']}: p={t['p_value']:.4f}, d={t.get('cohens_d',0):+.3f} (効果量: {el})")
        else:
            lines += ["### 判定: 優位性なし。本ロジックは破棄すべき", "",
                       "PBR × モメンタム/ボラティリティの組み合わせにおいて、",
                       "統計的に有意かつ実用的な効果量を持つリターン差は検出されませんでした。", ""]
            if n_s > 0 and n_m == 0:
                lines += ["一部のペアでp値は有意水準を下回ったものの、効果量が小さく (|d| < 0.3)、",
                           "取引コストを考慮すると実用的な収益機会とは言えません。"]
            elif n_s == 0:
                lines += ["いずれのグループ間比較においてもp値が有意水準を超えており、",
                           "フォワードリターンにおける系統的な差異は確認されませんでした。"]
            lines += ["", "**推奨アクション:**", "",
                       "1. 異なるファクター（売上成長率、アクルーアル等）での再検証",
                       "2. ユニバースの拡大", "3. 条件の組み替え（PBR → EV/EBITDA 等）"]
        return "\n".join(lines)

    def run(self, output_path: str = "analysis_report.md") -> str:
        print("[1/5] データ読み込み中...")
        prices = self._load_prices()
        fund = self._load_fundamentals()
        tickers = prices["ticker"].unique().tolist()
        print(f"  銘柄数: {len(tickers)}, 行数: {len(prices):,}")
        if len(tickers) < 2:
            print("[ERROR] 分析には最低2銘柄のデータが必要です。"); sys.exit(1)

        print("[2/5] 特徴量算出中...")
        df = self._compute_features(prices, fund)

        print("[3/5] マトリクス構築中...")
        matrix = self._build_matrix(df)
        if matrix.empty:
            print("[ERROR] 有効データ不足（PBR/モメンタム/ボラティリティが欠損）。"); sys.exit(1)
        print(f"  有効観測数: {len(matrix):,}")

        print("[4/5] 統計検定実行中...")
        pm = self._cross_section_stats(matrix, "pbr_group", "momentum_group")
        pv = self._cross_section_stats(matrix, "pbr_group", "volatility_group")
        tests = []
        for col in ["pbr_group", "momentum_group", "volatility_group"]:
            for ret in ["fwd_return_1m", "fwd_return_3m"]:
                tests.extend(self._test_significance(matrix, col, ret))

        print("[5/5] レポート生成中...")
        dr = f"{matrix['date'].min().strftime('%Y-%m-%d')} ～ {matrix['date'].max().strftime('%Y-%m-%d')}"
        report = "\n".join([
            "# クロスセクション分析 & バックテスト レポート", "",
            f"- **分析実行日**: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            f"- **対象銘柄数**: {len(tickers)}", f"- **銘柄**: {', '.join(tickers)}",
            f"- **観測数**: {len(matrix):,}", f"- **期間**: {dr}", "",
            "## 分析パラメータ", "",
            "| パラメータ | 値 |", "|---|---|",
            "| PBR分類 | 三分位 (低/中/高) |",
            f"| モメンタム | 過去{MOMENTUM_WINDOW}日リターン |",
            f"| ボラティリティ | 過去{VOLATILITY_WINDOW}日σ (年率) |",
            f"| フォワードリターン | 1M({TRADING_DAYS_1M}日) / 3M({TRADING_DAYS_3M}日) |",
            f"| 有意水準 | α = {SIGNIFICANCE_LEVEL} |", "",
            "## 1. PBR × モメンタム", "", self._fmt_stats_table(pm), "",
            "## 2. PBR × ボラティリティ", "", self._fmt_stats_table(pv), "",
            "## 3. 統計的検定結果", "", self._fmt_test_table(tests), "",
            "## 4. パフォーマンス指標", "",
            self._fmt_perf(pm, "PBR × モメンタム"), "",
            self._fmt_perf(pv, "PBR × ボラティリティ"), "",
            "## 5. 結論", "", self._make_conclusion(tests, pm + pv), "",
        ])
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(report)
        print(f"\n[DONE] レポートを {output_path} に出力しました。")
        return report


# ══════════════════════════════════════════════════════════
#  CLI エントリーポイント
# ══════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="投資アイデア抽出・データ取得・分析 統合ツール",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
サブコマンド:
  extract   YouTube動画から投資アイデアを抽出
  fetch     指定銘柄の株価・ファンダメンタルを取得
  update    登録済み全銘柄を差分更新
  analyze   PBR × モメンタム/ボラティリティ分析を実行
  status    各銘柄の更新状況を表示
  export    全銘柄をCSVエクスポート
  pipeline  extract → fetch → analyze を一括実行

例:
  python main.py extract "https://www.youtube.com/watch?v=XXXXX"
  python main.py fetch 7203 6758 9984
  python main.py update
  python main.py analyze
  python main.py pipeline "https://www.youtube.com/watch?v=XXXXX" 7203 6758
""")

    sub = parser.add_subparsers(dest="command")

    # extract
    p_ext = sub.add_parser("extract", help="YouTube → 投資アイデア抽出")
    p_ext.add_argument("url", help="YouTube動画URL or 動画ID")
    p_ext.add_argument("-o", "--output", default="investment_ideas.md")
    p_ext.add_argument("--no-chat", action="store_true")

    # fetch
    p_fetch = sub.add_parser("fetch", help="銘柄の株価・ファンダメンタルを取得")
    p_fetch.add_argument("codes", nargs="+", help="銘柄コード（例: 7203 6758）")
    p_fetch.add_argument("--full-refresh", action="store_true")

    # update
    sub.add_parser("update", help="登録済み全銘柄を差分更新")

    # analyze
    p_ana = sub.add_parser("analyze", help="クロスセクション分析を実行")
    p_ana.add_argument("-o", "--output", default="analysis_report.md")

    # status
    sub.add_parser("status", help="更新状況を表示")

    # export
    sub.add_parser("export", help="全銘柄をCSVエクスポート")

    # pipeline
    p_pipe = sub.add_parser("pipeline", help="extract → fetch → analyze を一括実行")
    p_pipe.add_argument("url", help="YouTube動画URL")
    p_pipe.add_argument("codes", nargs="+", help="銘柄コード")
    p_pipe.add_argument("--no-chat", action="store_true")

    args = parser.parse_args()

    if args.command == "extract":
        IdeaExtractor.run(args.url, args.output, args.no_chat)

    elif args.command == "fetch":
        StockDataPipeline().update(args.codes, full_refresh=args.full_refresh)

    elif args.command == "update":
        StockDataPipeline().update_all()

    elif args.command == "analyze":
        CrossSectionAnalyzer().run(args.output)

    elif args.command == "status":
        df = StockDataPipeline().status()
        print(df.to_string(index=False) if not df.empty else "登録済み銘柄はありません。")

    elif args.command == "export":
        StockDataPipeline().export_csv()

    elif args.command == "pipeline":
        print("=" * 60)
        print("  Phase 1: アイデア抽出")
        print("=" * 60)
        IdeaExtractor.run(args.url, no_chat=args.no_chat)
        print()
        print("=" * 60)
        print("  Phase 2: データ取得")
        print("=" * 60)
        StockDataPipeline().update(args.codes)
        print()
        print("=" * 60)
        print("  Phase 3: 分析")
        print("=" * 60)
        CrossSectionAnalyzer().run()

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
