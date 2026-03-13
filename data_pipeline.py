"""
日本株ヒストリカルデータ取得・管理パイプライン

モジュールとしてインポート:
    from data_pipeline import StockDataPipeline
    pipe = StockDataPipeline()
    pipe.update(["7203", "6758"])       # 差分更新
    df = pipe.load("7203")              # 読み込み
    df = pipe.load_all()                # 全銘柄一括読み込み

CLIとして実行:
    python data_pipeline.py 7203 6758 9984
    python data_pipeline.py --update            # 登録済み銘柄を一括更新
    python data_pipeline.py --export csv        # CSV一括エクスポート
"""

import argparse
import sqlite3
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import yfinance as yf

# ── 定数 ──────────────────────────────────────────────

DB_DIR = Path(__file__).parent / "data"
DB_PATH = DB_DIR / "stock_data.db"
CSV_DIR = DB_DIR / "csv"

# yfinance用の日本株ティッカーサフィックス
JP_SUFFIX = ".T"

# 取得開始日のデフォルト（過去5年）
DEFAULT_START = (datetime.now() - timedelta(days=5 * 365)).strftime("%Y-%m-%d")


# ── パイプライン本体 ──────────────────────────────────

class StockDataPipeline:
    """日本株データの取得・保存・差分更新を管理するパイプライン。"""

    def __init__(self, db_path: str | Path | None = None):
        self.db_path = Path(db_path) if db_path else DB_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    # ── DB初期化 ──────────────────────────────────

    def _init_db(self):
        """SQLiteテーブルを作成する（存在しなければ）。"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS price_daily (
                    ticker      TEXT    NOT NULL,
                    date        TEXT    NOT NULL,
                    open        REAL,
                    high        REAL,
                    low         REAL,
                    close       REAL,
                    adj_close   REAL,
                    volume      INTEGER,
                    PRIMARY KEY (ticker, date)
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS fundamentals (
                    ticker              TEXT NOT NULL,
                    updated_at          TEXT NOT NULL,
                    short_name          TEXT,
                    sector              TEXT,
                    industry            TEXT,
                    market_cap          INTEGER,
                    enterprise_value    INTEGER,
                    trailing_pe         REAL,
                    forward_pe          REAL,
                    price_to_book       REAL,
                    price_to_sales      REAL,
                    dividend_yield      REAL,
                    payout_ratio        REAL,
                    roe                 REAL,
                    roa                 REAL,
                    profit_margins      REAL,
                    revenue             INTEGER,
                    ebitda              INTEGER,
                    total_debt          INTEGER,
                    total_cash          INTEGER,
                    book_value          REAL,
                    shares_outstanding  INTEGER,
                    fifty_two_week_high REAL,
                    fifty_two_week_low  REAL,
                    PRIMARY KEY (ticker)
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS update_log (
                    ticker          TEXT NOT NULL,
                    last_updated    TEXT NOT NULL,
                    rows_added      INTEGER DEFAULT 0,
                    PRIMARY KEY (ticker)
                )
            """)

    def _conn(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    # ── ティッカー変換 ────────────────────────────

    @staticmethod
    def to_yf_ticker(code: str) -> str:
        """銘柄コード → yfinance用ティッカーに変換。"""
        code = code.strip()
        if not code.endswith(JP_SUFFIX):
            code = code + JP_SUFFIX
        return code

    @staticmethod
    def to_code(yf_ticker: str) -> str:
        """yfinanceティッカー → 銘柄コードに戻す。"""
        return yf_ticker.replace(JP_SUFFIX, "")

    # ── 株価取得 ──────────────────────────────────

    def _get_last_date(self, ticker: str) -> str | None:
        """DB上の最新日付を取得する。"""
        with self._conn() as conn:
            row = conn.execute(
                "SELECT MAX(date) FROM price_daily WHERE ticker = ?",
                (ticker,),
            ).fetchone()
        return row[0] if row and row[0] else None

    def _fetch_price(self, code: str, start: str | None = None) -> pd.DataFrame:
        """yfinanceから日足データを取得する。"""
        yf_ticker = self.to_yf_ticker(code)
        ticker_obj = yf.Ticker(yf_ticker)

        if start is None:
            start = DEFAULT_START

        df = ticker_obj.history(start=start, end=None, auto_adjust=False)

        if df.empty:
            return pd.DataFrame()

        # カラム名を正規化
        df = df.reset_index()
        col_map = {
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Adj Close": "adj_close",
            "Volume": "volume",
        }
        df = df.rename(columns=col_map)

        # 必要カラムのみ保持（存在しないカラムは無視）
        keep = [c for c in ["date", "open", "high", "low", "close", "adj_close", "volume"] if c in df.columns]
        df = df[keep]

        # adj_close が無い場合は close をコピー
        if "adj_close" not in df.columns:
            df["adj_close"] = df["close"]

        # date を文字列に
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

        # 銘柄コードを付与
        df["ticker"] = code

        return df

    # ── ファンダメンタル取得 ──────────────────────

    def _fetch_fundamentals(self, code: str) -> dict:
        """yfinanceからファンダメンタル指標を取得する。"""
        yf_ticker = self.to_yf_ticker(code)
        ticker_obj = yf.Ticker(yf_ticker)

        try:
            info = ticker_obj.info
        except Exception:
            info = {}

        if not info:
            return {}

        field_map = {
            "short_name":          "shortName",
            "sector":              "sector",
            "industry":            "industry",
            "market_cap":          "marketCap",
            "enterprise_value":    "enterpriseValue",
            "trailing_pe":         "trailingPE",
            "forward_pe":          "forwardPE",
            "price_to_book":       "priceToBook",
            "price_to_sales":      "priceToSalesTrailing12Months",
            "dividend_yield":      "dividendYield",
            "payout_ratio":        "payoutRatio",
            "roe":                 "returnOnEquity",
            "roa":                 "returnOnAssets",
            "profit_margins":      "profitMargins",
            "revenue":             "totalRevenue",
            "ebitda":              "ebitda",
            "total_debt":          "totalDebt",
            "total_cash":          "totalCash",
            "book_value":          "bookValue",
            "shares_outstanding":  "sharesOutstanding",
            "fifty_two_week_high": "fiftyTwoWeekHigh",
            "fifty_two_week_low":  "fiftyTwoWeekLow",
        }

        row = {"ticker": code, "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M")}
        for db_col, yf_key in field_map.items():
            row[db_col] = info.get(yf_key)

        return row

    # ── クレンジング ──────────────────────────────

    @staticmethod
    def cleanse(df: pd.DataFrame) -> pd.DataFrame:
        """株価データのクレンジング処理。"""
        if df.empty:
            return df

        df = df.copy()

        # 重複行の除去
        df = df.drop_duplicates(subset=["ticker", "date"], keep="last")

        # 日付順ソート
        df = df.sort_values("date")

        # 数値カラムの欠損値を前方補完（休場日のギャップ対応）
        numeric_cols = ["open", "high", "low", "close", "adj_close"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = df[col].ffill()

        # volume の欠損は 0 で埋める
        if "volume" in df.columns:
            df["volume"] = df["volume"].fillna(0).astype(int)

        # 明らかに異常な値（0以下の株価）をNaNに置換し前方補完
        for col in numeric_cols:
            if col in df.columns:
                df.loc[df[col] <= 0, col] = pd.NA
                df[col] = df[col].ffill()

        # それでも残るNaN行を除去
        df = df.dropna(subset=["close"])

        return df.reset_index(drop=True)

    # ── 保存 ──────────────────────────────────────

    def _save_price(self, df: pd.DataFrame) -> int:
        """クレンジング済み株価をSQLiteに保存（UPSERT）。"""
        if df.empty:
            return 0

        with self._conn() as conn:
            rows = df.to_dict("records")
            conn.executemany("""
                INSERT INTO price_daily (ticker, date, open, high, low, close, adj_close, volume)
                VALUES (:ticker, :date, :open, :high, :low, :close, :adj_close, :volume)
                ON CONFLICT(ticker, date) DO UPDATE SET
                    open      = excluded.open,
                    high      = excluded.high,
                    low       = excluded.low,
                    close     = excluded.close,
                    adj_close = excluded.adj_close,
                    volume    = excluded.volume
            """, rows)
        return len(rows)

    def _save_fundamentals(self, data: dict):
        """ファンダメンタル指標をSQLiteに保存（UPSERT）。"""
        if not data:
            return

        cols = list(data.keys())
        placeholders = ", ".join(f":{c}" for c in cols)
        updates = ", ".join(f"{c} = excluded.{c}" for c in cols if c != "ticker")

        with self._conn() as conn:
            conn.execute(f"""
                INSERT INTO fundamentals ({', '.join(cols)})
                VALUES ({placeholders})
                ON CONFLICT(ticker) DO UPDATE SET {updates}
            """, data)

    def _log_update(self, ticker: str, rows_added: int):
        """更新ログを記録する。"""
        with self._conn() as conn:
            conn.execute("""
                INSERT INTO update_log (ticker, last_updated, rows_added)
                VALUES (?, ?, ?)
                ON CONFLICT(ticker) DO UPDATE SET
                    last_updated = excluded.last_updated,
                    rows_added   = excluded.rows_added
            """, (ticker, datetime.now().strftime("%Y-%m-%d %H:%M"), rows_added))

    # ── 公開API ───────────────────────────────────

    def update(self, codes: list[str], full_refresh: bool = False) -> dict:
        """
        指定銘柄のデータを取得・更新する。

        Args:
            codes: 銘柄コードのリスト（例: ["7203", "6758"]）
            full_refresh: True なら全期間を再取得

        Returns:
            {"7203": {"price_rows": 123, "fundamentals": True}, ...}
        """
        results = {}

        for code in codes:
            code = code.strip().replace(JP_SUFFIX, "")
            print(f"[{code}] 処理中...")

            # 差分更新: DB上の最新日の翌日から取得
            start = DEFAULT_START
            if not full_refresh:
                last_date = self._get_last_date(code)
                if last_date:
                    next_day = (datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
                    if next_day >= datetime.now().strftime("%Y-%m-%d"):
                        print(f"  株価: 最新データ済み ({last_date})")
                        # ファンダメンタルのみ更新
                        fdata = self._fetch_fundamentals(code)
                        self._save_fundamentals(fdata)
                        self._log_update(code, 0)
                        results[code] = {"price_rows": 0, "fundamentals": bool(fdata)}
                        continue
                    start = next_day
                    print(f"  株価: {start} 以降を差分取得")

            # 株価取得
            try:
                df = self._fetch_price(code, start=start)
            except Exception as e:
                print(f"  [ERROR] 株価取得失敗: {e}")
                results[code] = {"price_rows": 0, "fundamentals": False, "error": str(e)}
                continue

            if df.empty:
                print(f"  [WARN] 株価データなし（ティッカー確認: {self.to_yf_ticker(code)}）")
                results[code] = {"price_rows": 0, "fundamentals": False}
                continue

            # クレンジング & 保存
            df = self.cleanse(df)
            n = self._save_price(df)
            print(f"  株価: {n} 行を保存")

            # ファンダメンタル取得・保存
            fdata = self._fetch_fundamentals(code)
            self._save_fundamentals(fdata)
            if fdata:
                pbr = fdata.get("price_to_book")
                mcap = fdata.get("market_cap")
                mcap_str = f"¥{mcap / 1e9:.1f}B" if mcap else "N/A"
                print(f"  ファンダメンタル: PBR={pbr}, 時価総額={mcap_str}")

            self._log_update(code, n)
            results[code] = {"price_rows": n, "fundamentals": bool(fdata)}

            # API負荷軽減
            time.sleep(0.5)

        print()
        print(f"[DONE] {len(codes)} 銘柄の更新完了 → {self.db_path}")
        return results

    def update_all(self) -> dict:
        """登録済み全銘柄を差分更新する。"""
        codes = self.list_tickers()
        if not codes:
            print("[INFO] 登録済み銘柄がありません。")
            return {}
        print(f"[INFO] 登録済み {len(codes)} 銘柄を一括更新します。")
        return self.update(codes)

    def load(self, code: str, start: str | None = None, end: str | None = None) -> pd.DataFrame:
        """
        指定銘柄の株価＋ファンダメンタルを結合したDataFrameを返す。

        Args:
            code:  銘柄コード（例: "7203"）
            start: 開始日（例: "2024-01-01"）
            end:   終了日（例: "2024-12-31"）
        """
        code = code.strip().replace(JP_SUFFIX, "")

        query = "SELECT * FROM price_daily WHERE ticker = ?"
        params: list = [code]

        if start:
            query += " AND date >= ?"
            params.append(start)
        if end:
            query += " AND date <= ?"
            params.append(end)

        query += " ORDER BY date"

        with self._conn() as conn:
            df_price = pd.read_sql_query(query, conn, params=params)
            df_fund = pd.read_sql_query(
                "SELECT * FROM fundamentals WHERE ticker = ?",
                conn, params=[code],
            )

        if df_price.empty:
            return pd.DataFrame()

        # ファンダメンタルを結合（全行に同じ値をブロードキャスト）
        if not df_fund.empty:
            fund_row = df_fund.iloc[0].to_dict()
            fund_row.pop("ticker", None)
            fund_row.pop("updated_at", None)
            for col, val in fund_row.items():
                df_price[col] = val

        return df_price

    def load_all(self, start: str | None = None, end: str | None = None) -> pd.DataFrame:
        """全銘柄のデータを一括で読み込む。"""
        codes = self.list_tickers()
        frames = [self.load(c, start, end) for c in codes]
        frames = [f for f in frames if not f.empty]
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    def list_tickers(self) -> list[str]:
        """DB に登録済みの銘柄コード一覧を返す。"""
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT DISTINCT ticker FROM price_daily ORDER BY ticker"
            ).fetchall()
        return [r[0] for r in rows]

    def status(self) -> pd.DataFrame:
        """各銘柄の更新状況を一覧で返す。"""
        with self._conn() as conn:
            df = pd.read_sql_query("""
                SELECT
                    u.ticker,
                    u.last_updated,
                    u.rows_added AS last_rows_added,
                    MIN(p.date)  AS first_date,
                    MAX(p.date)  AS last_date,
                    COUNT(p.date) AS total_rows
                FROM update_log u
                LEFT JOIN price_daily p ON u.ticker = p.ticker
                GROUP BY u.ticker
                ORDER BY u.ticker
            """, conn)
        return df

    def export_csv(self, output_dir: str | Path | None = None):
        """全銘柄のデータを個別CSVファイルとしてエクスポートする。"""
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
        """指定銘柄のデータをDBから削除する。"""
        code = code.strip().replace(JP_SUFFIX, "")
        with self._conn() as conn:
            conn.execute("DELETE FROM price_daily WHERE ticker = ?", (code,))
            conn.execute("DELETE FROM fundamentals WHERE ticker = ?", (code,))
            conn.execute("DELETE FROM update_log WHERE ticker = ?", (code,))
        print(f"[INFO] {code} のデータを削除しました。")


# ── CLI ───────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="日本株ヒストリカルデータ パイプライン",
    )
    parser.add_argument(
        "codes",
        nargs="*",
        help="取得する銘柄コード（例: 7203 6758 9984）",
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help="登録済み全銘柄を差分更新",
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="全期間を再取得（差分ではなくフル取得）",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="各銘柄の更新状況を表示",
    )
    parser.add_argument(
        "--export",
        choices=["csv"],
        help="全銘柄データをCSVエクスポート",
    )
    parser.add_argument(
        "--db",
        type=str,
        default=None,
        help="SQLiteファイルパス（デフォルト: data/stock_data.db）",
    )
    args = parser.parse_args()

    pipe = StockDataPipeline(db_path=args.db)

    if args.status:
        df = pipe.status()
        if df.empty:
            print("登録済み銘柄はありません。")
        else:
            print(df.to_string(index=False))
        return

    if args.export == "csv":
        pipe.export_csv()
        return

    if args.update:
        pipe.update_all()
        return

    if not args.codes:
        parser.print_help()
        print("\n例: python data_pipeline.py 7203 6758 9984")
        sys.exit(1)

    pipe.update(args.codes, full_refresh=args.full_refresh)


if __name__ == "__main__":
    main()
