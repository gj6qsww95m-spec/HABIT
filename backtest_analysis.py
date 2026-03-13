"""
PBR × モメンタム/ボラティリティ クロスセクション分析 & バックテスト

使い方:
    # data_pipeline.py で事前にデータ取得済みであること
    python backtest_analysis.py
    python backtest_analysis.py --db data/stock_data.db --output results.md

モジュールとして:
    from backtest_analysis import CrossSectionAnalyzer
    analyzer = CrossSectionAnalyzer()
    report = analyzer.run()
"""

import argparse
import sqlite3
import sys
import warnings
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

warnings.filterwarnings("ignore", category=FutureWarning)

# ── 定数 ──────────────────────────────────────────────

DB_PATH = Path(__file__).parent / "data" / "stock_data.db"

# 分析パラメータ
TRADING_DAYS_1M = 21     # 1ヶ月 ≈ 21営業日
TRADING_DAYS_3M = 63     # 3ヶ月 ≈ 63営業日
VOLATILITY_WINDOW = 20   # ボラティリティ算出ウィンドウ（日）
MOMENTUM_WINDOW = 60     # モメンタム算出ウィンドウ（日）
ANNUALIZE_FACTOR = 252   # 年率換算係数

# PBR / モメンタム / ボラティリティの分位閾値
QUANTILE_LOW = 1 / 3
QUANTILE_HIGH = 2 / 3

# 統計的有意水準
SIGNIFICANCE_LEVEL = 0.05


# ── ヘルパー ──────────────────────────────────────────

def sharpe_ratio(returns: pd.Series, risk_free_annual: float = 0.001) -> float:
    """日次リターン列からシャープレシオ（年率）を算出。"""
    if returns.empty or returns.std() == 0:
        return 0.0
    rf_daily = (1 + risk_free_annual) ** (1 / ANNUALIZE_FACTOR) - 1
    excess = returns - rf_daily
    return float(excess.mean() / excess.std() * np.sqrt(ANNUALIZE_FACTOR))


def max_drawdown(cumulative_returns: pd.Series) -> float:
    """累積リターン列から最大ドローダウンを算出。"""
    if cumulative_returns.empty:
        return 0.0
    peak = cumulative_returns.cummax()
    dd = (cumulative_returns - peak) / peak
    return float(dd.min())


def cohens_d(group1: pd.Series, group2: pd.Series) -> float:
    """効果量（Cohen's d）を算出。"""
    n1, n2 = len(group1), len(group2)
    if n1 < 2 or n2 < 2:
        return 0.0
    var1, var2 = group1.var(), group2.var()
    pooled_std = np.sqrt(((n1 - 1) * var1 + (n2 - 1) * var2) / (n1 + n2 - 2))
    if pooled_std == 0:
        return 0.0
    return float((group1.mean() - group2.mean()) / pooled_std)


# ── 分析器 ────────────────────────────────────────────

class CrossSectionAnalyzer:
    """PBR × モメンタム/ボラティリティ のクロスセクション分析を実行する。"""

    def __init__(self, db_path: str | Path | None = None):
        self.db_path = Path(db_path) if db_path else DB_PATH
        if not self.db_path.exists():
            print(f"[ERROR] DB が見つかりません: {self.db_path}")
            print("  先に data_pipeline.py でデータを取得してください。")
            sys.exit(1)

    def _load_all_prices(self) -> pd.DataFrame:
        """全銘柄の日足データをロード。"""
        with sqlite3.connect(self.db_path) as conn:
            df = pd.read_sql_query(
                "SELECT ticker, date, close, adj_close, volume FROM price_daily ORDER BY ticker, date",
                conn,
            )
        df["date"] = pd.to_datetime(df["date"])
        return df

    def _load_fundamentals(self) -> pd.DataFrame:
        """ファンダメンタルデータをロード。"""
        with sqlite3.connect(self.db_path) as conn:
            df = pd.read_sql_query("SELECT * FROM fundamentals", conn)
        return df

    # ── 特徴量算出 ────────────────────────────────

    def _compute_features(self, prices: pd.DataFrame, fundamentals: pd.DataFrame) -> pd.DataFrame:
        """
        各銘柄・各日に対して以下を算出:
          - daily_return: 日次リターン
          - volatility:   直近20日のリターン標準偏差（年率換算）
          - momentum:     直近60日のリターン（モメンタム）
          - fwd_return_1m: 1ヶ月後フォワードリターン
          - fwd_return_3m: 3ヶ月後フォワードリターン
          - pbr:          PBR（ファンダメンタルから結合）
        """
        frames = []

        for ticker, grp in prices.groupby("ticker"):
            g = grp.sort_values("date").copy()
            g["daily_return"] = g["adj_close"].pct_change()

            # ボラティリティ（年率）
            g["volatility"] = g["daily_return"].rolling(VOLATILITY_WINDOW).std() * np.sqrt(ANNUALIZE_FACTOR)

            # モメンタム（過去60日リターン）
            g["momentum"] = g["adj_close"].pct_change(MOMENTUM_WINDOW)

            # フォワードリターン（将来リターン）
            g["fwd_return_1m"] = g["adj_close"].shift(-TRADING_DAYS_1M) / g["adj_close"] - 1
            g["fwd_return_3m"] = g["fwd_return_3m"] if "fwd_return_3m" in g.columns else \
                g["adj_close"].shift(-TRADING_DAYS_3M) / g["adj_close"] - 1
            # 上の行はバグ回避のため再計算
            g["fwd_return_3m"] = g["adj_close"].shift(-TRADING_DAYS_3M) / g["adj_close"] - 1

            frames.append(g)

        df = pd.concat(frames, ignore_index=True)

        # PBR を結合
        if not fundamentals.empty and "price_to_book" in fundamentals.columns:
            pbr_map = fundamentals.set_index("ticker")["price_to_book"].to_dict()
            df["pbr"] = df["ticker"].map(pbr_map)
        else:
            df["pbr"] = np.nan

        return df

    # ── グループ分類 ──────────────────────────────

    @staticmethod
    def _assign_group(series: pd.Series, low_label: str, high_label: str, mid_label: str) -> pd.Series:
        """三分位でグループラベルを付与する。"""
        q_low = series.quantile(QUANTILE_LOW)
        q_high = series.quantile(QUANTILE_HIGH)
        conditions = [
            series <= q_low,
            series >= q_high,
        ]
        choices = [low_label, high_label]
        return pd.Series(
            np.select(conditions, choices, default=mid_label),
            index=series.index,
        )

    def _build_matrix(self, df: pd.DataFrame) -> pd.DataFrame:
        """PBR × モメンタム/ボラティリティ のマトリクスを構築。"""
        # 有効なデータのみ
        valid = df.dropna(subset=["pbr", "volatility", "momentum", "fwd_return_1m", "fwd_return_3m"]).copy()

        if valid.empty:
            return pd.DataFrame()

        # PBR グループ
        valid["pbr_group"] = self._assign_group(valid["pbr"], "低PBR", "高PBR", "中PBR")

        # モメンタム グループ
        valid["momentum_group"] = self._assign_group(valid["momentum"], "低モメンタム", "高モメンタム", "中モメンタム")

        # ボラティリティ グループ
        valid["volatility_group"] = self._assign_group(valid["volatility"], "低ボラ", "高ボラ", "中ボラ")

        return valid

    # ── クロスセクション分析 ──────────────────────

    def _cross_section_stats(self, matrix: pd.DataFrame, group_col_a: str, group_col_b: str) -> list[dict]:
        """2軸のグループごとにフォワードリターンの統計を算出する。"""
        results = []

        for (ga, gb), grp in matrix.groupby([group_col_a, group_col_b]):
            n = len(grp)
            if n < 5:
                continue

            fwd_1m = grp["fwd_return_1m"]
            fwd_3m = grp["fwd_return_3m"]
            daily_ret = grp["daily_return"].dropna()

            # 累積リターン（等ウェイト）
            cum_ret = (1 + daily_ret).cumprod()

            results.append({
                "group_a": ga,
                "group_b": gb,
                "n_obs": n,
                "fwd_1m_mean": fwd_1m.mean(),
                "fwd_1m_median": fwd_1m.median(),
                "fwd_1m_std": fwd_1m.std(),
                "fwd_3m_mean": fwd_3m.mean(),
                "fwd_3m_median": fwd_3m.median(),
                "fwd_3m_std": fwd_3m.std(),
                "sharpe": sharpe_ratio(daily_ret),
                "max_dd": max_drawdown(cum_ret) if not cum_ret.empty else 0.0,
                "avg_volatility": grp["volatility"].mean(),
                "avg_momentum": grp["momentum"].mean(),
                "avg_pbr": grp["pbr"].mean(),
            })

        return results

    # ── 統計的検定 ─────────────────────────────────

    @staticmethod
    def _test_significance(matrix: pd.DataFrame, group_col: str, return_col: str) -> list[dict]:
        """
        グループ間のフォワードリターン差に対して統計検定を実施。
        - Welch's t-test（2群間）
        - One-way ANOVA（全群）
        - Kruskal-Wallis 検定（ノンパラメトリック）
        """
        groups_data = {}
        for label, grp in matrix.groupby(group_col):
            vals = grp[return_col].dropna()
            if len(vals) >= 5:
                groups_data[label] = vals

        test_results = []

        if len(groups_data) < 2:
            return test_results

        # ANOVA / Kruskal-Wallis（全群）
        all_groups = list(groups_data.values())
        if len(all_groups) >= 2:
            try:
                f_stat, anova_p = stats.f_oneway(*all_groups)
            except Exception:
                f_stat, anova_p = np.nan, np.nan

            try:
                kw_stat, kw_p = stats.kruskal(*all_groups)
            except Exception:
                kw_stat, kw_p = np.nan, np.nan

            test_results.append({
                "comparison": f"全群比較 ({return_col})",
                "test": "One-way ANOVA",
                "statistic": f_stat,
                "p_value": anova_p,
                "significant": anova_p < SIGNIFICANCE_LEVEL if not np.isnan(anova_p) else False,
            })
            test_results.append({
                "comparison": f"全群比較 ({return_col})",
                "test": "Kruskal-Wallis",
                "statistic": kw_stat,
                "p_value": kw_p,
                "significant": kw_p < SIGNIFICANCE_LEVEL if not np.isnan(kw_p) else False,
            })

        # ペアワイズ t-test（各ペア）
        labels = sorted(groups_data.keys())
        for i in range(len(labels)):
            for j in range(i + 1, len(labels)):
                a_label, b_label = labels[i], labels[j]
                a_vals, b_vals = groups_data[a_label], groups_data[b_label]

                try:
                    t_stat, t_p = stats.ttest_ind(a_vals, b_vals, equal_var=False)
                except Exception:
                    t_stat, t_p = np.nan, np.nan

                d = cohens_d(a_vals, b_vals)

                test_results.append({
                    "comparison": f"{a_label} vs {b_label} ({return_col})",
                    "test": "Welch's t-test",
                    "statistic": t_stat,
                    "p_value": t_p,
                    "cohens_d": d,
                    "significant": t_p < SIGNIFICANCE_LEVEL if not np.isnan(t_p) else False,
                    "mean_a": a_vals.mean(),
                    "mean_b": b_vals.mean(),
                })

        return test_results

    # ── レポート生成 ──────────────────────────────

    def _generate_report(
        self,
        pbr_mom_stats: list[dict],
        pbr_vol_stats: list[dict],
        pbr_tests_1m: list[dict],
        pbr_tests_3m: list[dict],
        mom_tests_1m: list[dict],
        mom_tests_3m: list[dict],
        vol_tests_1m: list[dict],
        vol_tests_3m: list[dict],
        matrix: pd.DataFrame,
        tickers: list[str],
    ) -> str:
        """分析結果のMarkdownレポートを生成する。"""

        lines = []
        lines.append("# クロスセクション分析 & バックテスト レポート")
        lines.append("")
        lines.append(f"- **分析実行日**: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        lines.append(f"- **対象銘柄数**: {len(tickers)}")
        lines.append(f"- **銘柄**: {', '.join(tickers)}")
        lines.append(f"- **観測数 (有効行)**: {len(matrix):,}")
        date_range = f"{matrix['date'].min().strftime('%Y-%m-%d')} ～ {matrix['date'].max().strftime('%Y-%m-%d')}"
        lines.append(f"- **期間**: {date_range}")
        lines.append("")

        # ── パラメータ ──
        lines.append("## 分析パラメータ")
        lines.append("")
        lines.append(f"| パラメータ | 値 |")
        lines.append(f"|---|---|")
        lines.append(f"| PBR分類 | 三分位 (低/中/高) |")
        lines.append(f"| モメンタム算出期間 | 過去{MOMENTUM_WINDOW}営業日リターン |")
        lines.append(f"| ボラティリティ算出期間 | 過去{VOLATILITY_WINDOW}日標準偏差 (年率換算) |")
        lines.append(f"| フォワードリターン | 1ヶ月 ({TRADING_DAYS_1M}日) / 3ヶ月 ({TRADING_DAYS_3M}日) |")
        lines.append(f"| 有意水準 | α = {SIGNIFICANCE_LEVEL} |")
        lines.append("")

        # ── PBR × モメンタム マトリクス ──
        lines.append("## 1. PBR × モメンタム マトリクス")
        lines.append("")
        lines.append(self._format_stats_table(pbr_mom_stats))
        lines.append("")

        # ── PBR × ボラティリティ マトリクス ──
        lines.append("## 2. PBR × ボラティリティ マトリクス")
        lines.append("")
        lines.append(self._format_stats_table(pbr_vol_stats))
        lines.append("")

        # ── 統計的検定結果 ──
        lines.append("## 3. 統計的検定結果")
        lines.append("")

        lines.append("### PBR グループ間")
        lines.append("")
        lines.append(self._format_test_table(pbr_tests_1m + pbr_tests_3m))
        lines.append("")

        lines.append("### モメンタム グループ間")
        lines.append("")
        lines.append(self._format_test_table(mom_tests_1m + mom_tests_3m))
        lines.append("")

        lines.append("### ボラティリティ グループ間")
        lines.append("")
        lines.append(self._format_test_table(vol_tests_1m + vol_tests_3m))
        lines.append("")

        # ── パフォーマンス指標サマリ ──
        lines.append("## 4. パフォーマンス指標サマリ")
        lines.append("")
        lines.append(self._format_performance_summary(pbr_mom_stats, "PBR × モメンタム"))
        lines.append("")
        lines.append(self._format_performance_summary(pbr_vol_stats, "PBR × ボラティリティ"))
        lines.append("")

        # ── 最終判定 ──
        lines.append("## 5. 結論")
        lines.append("")
        conclusion = self._make_conclusion(
            pbr_tests_1m, pbr_tests_3m,
            mom_tests_1m, mom_tests_3m,
            vol_tests_1m, vol_tests_3m,
            pbr_mom_stats, pbr_vol_stats,
        )
        lines.append(conclusion)
        lines.append("")

        return "\n".join(lines)

    @staticmethod
    def _format_stats_table(stat_rows: list[dict]) -> str:
        """統計サマリをMarkdownテーブルにフォーマット。"""
        if not stat_rows:
            return "*データ不足のため算出不可*"

        header = "| グループA | グループB | N | 1M平均(%) | 1M中央値(%) | 3M平均(%) | 3M中央値(%) | Sharpe | MaxDD(%) |"
        sep =    "|---|---|---:|---:|---:|---:|---:|---:|---:|"
        rows = [header, sep]

        for r in stat_rows:
            rows.append(
                f"| {r['group_a']} | {r['group_b']} | {r['n_obs']:,} "
                f"| {r['fwd_1m_mean'] * 100:+.2f} | {r['fwd_1m_median'] * 100:+.2f} "
                f"| {r['fwd_3m_mean'] * 100:+.2f} | {r['fwd_3m_median'] * 100:+.2f} "
                f"| {r['sharpe']:.2f} | {r['max_dd'] * 100:.1f} |"
            )

        return "\n".join(rows)

    @staticmethod
    def _format_test_table(test_rows: list[dict]) -> str:
        """検定結果をMarkdownテーブルにフォーマット。"""
        if not test_rows:
            return "*検定不可（サンプル不足）*"

        header = "| 比較 | 検定手法 | 統計量 | p値 | 有意 | Cohen's d | 平均A(%) | 平均B(%) |"
        sep =    "|---|---|---:|---:|:---:|---:|---:|---:|"
        rows = [header, sep]

        for r in test_rows:
            sig_mark = "**YES**" if r.get("significant") else "no"
            d_str = f"{r['cohens_d']:.3f}" if "cohens_d" in r else "-"
            mean_a = f"{r['mean_a'] * 100:+.2f}" if "mean_a" in r else "-"
            mean_b = f"{r['mean_b'] * 100:+.2f}" if "mean_b" in r else "-"
            stat_val = f"{r['statistic']:.3f}" if not np.isnan(r.get("statistic", np.nan)) else "N/A"
            p_val = f"{r['p_value']:.4f}" if not np.isnan(r.get("p_value", np.nan)) else "N/A"

            rows.append(
                f"| {r['comparison']} | {r['test']} | {stat_val} | {p_val} | {sig_mark} | {d_str} | {mean_a} | {mean_b} |"
            )

        return "\n".join(rows)

    @staticmethod
    def _format_performance_summary(stat_rows: list[dict], label: str) -> str:
        """パフォーマンス指標のサマリテーブル。"""
        if not stat_rows:
            return f"*{label}: データ不足*"

        best_sharpe = max(stat_rows, key=lambda x: x["sharpe"])
        worst_dd = min(stat_rows, key=lambda x: x["max_dd"])
        best_1m = max(stat_rows, key=lambda x: x["fwd_1m_mean"])
        best_3m = max(stat_rows, key=lambda x: x["fwd_3m_mean"])

        lines = [
            f"**{label}**",
            "",
            f"| 指標 | グループ | 値 |",
            f"|---|---|---|",
            f"| 最高シャープレシオ | {best_sharpe['group_a']}/{best_sharpe['group_b']} | {best_sharpe['sharpe']:.2f} |",
            f"| 最大ドローダウン | {worst_dd['group_a']}/{worst_dd['group_b']} | {worst_dd['max_dd'] * 100:.1f}% |",
            f"| 最高1Mリターン | {best_1m['group_a']}/{best_1m['group_b']} | {best_1m['fwd_1m_mean'] * 100:+.2f}% |",
            f"| 最高3Mリターン | {best_3m['group_a']}/{best_3m['group_b']} | {best_3m['fwd_3m_mean'] * 100:+.2f}% |",
        ]
        return "\n".join(lines)

    @staticmethod
    def _make_conclusion(
        pbr_1m, pbr_3m, mom_1m, mom_3m, vol_1m, vol_3m,
        pbr_mom_stats, pbr_vol_stats,
    ) -> str:
        """
        全検定結果を集約し、忖度なしの最終判定を返す。
        有意差なし → 「優位性なし。本ロジックは破棄すべき」
        """

        all_tests = pbr_1m + pbr_3m + mom_1m + mom_3m + vol_1m + vol_3m

        # ペアワイズ t-test のみを判定対象とする
        pairwise = [t for t in all_tests if t["test"] == "Welch's t-test"]
        significant_pairs = [t for t in pairwise if t.get("significant")]

        # 効果量が実用的に意味のあるレベルか（|d| >= 0.3）
        meaningful = [t for t in significant_pairs if abs(t.get("cohens_d", 0)) >= 0.3]

        # シャープレシオが実用水準か
        all_stats = pbr_mom_stats + pbr_vol_stats
        good_sharpe = [s for s in all_stats if s["sharpe"] >= 0.5]

        lines = []

        if not pairwise:
            lines.append("### 判定: データ不足")
            lines.append("")
            lines.append("有効な比較ペアが存在しないため、判定不能です。")
            lines.append("より多くの銘柄を `data_pipeline.py` で取得し、再実行してください。")
            return "\n".join(lines)

        n_total = len(pairwise)
        n_sig = len(significant_pairs)
        n_meaningful = len(meaningful)

        lines.append(f"### 検定サマリ")
        lines.append("")
        lines.append(f"- ペアワイズ比較数: **{n_total}**")
        lines.append(f"- 統計的有意 (p < {SIGNIFICANCE_LEVEL}): **{n_sig} / {n_total}**")
        lines.append(f"- 実用的効果量 (|d| >= 0.3): **{n_meaningful} / {n_total}**")
        lines.append(f"- シャープレシオ >= 0.5 のグループ: **{len(good_sharpe)} / {len(all_stats)}**")
        lines.append("")

        # 判定ロジック
        has_edge = n_meaningful >= 1 and len(good_sharpe) >= 1

        if has_edge:
            lines.append("### 判定: 統計的優位性あり（条件付き）")
            lines.append("")
            lines.append("以下の組み合わせに限定的な統計的エッジが検出されました。")
            lines.append("ただし、**サンプル外検証・取引コスト控除前**の結果であり、")
            lines.append("実運用前には必ずウォークフォワード検証を実施してください。")
            lines.append("")
            lines.append("**有意な組み合わせ:**")
            lines.append("")
            for t in meaningful:
                d_abs = abs(t.get("cohens_d", 0))
                effect_label = "小" if d_abs < 0.5 else ("中" if d_abs < 0.8 else "大")
                lines.append(
                    f"- {t['comparison']}: p={t['p_value']:.4f}, "
                    f"d={t.get('cohens_d', 0):+.3f} (効果量: {effect_label})"
                )
        else:
            lines.append("### 判定: 優位性なし。本ロジックは破棄すべき")
            lines.append("")
            lines.append("PBR × モメンタム/ボラティリティの組み合わせにおいて、")
            lines.append("統計的に有意かつ実用的な効果量を持つリターン差は検出されませんでした。")
            lines.append("")
            if n_sig > 0 and n_meaningful == 0:
                lines.append("一部のペアでp値は有意水準を下回ったものの、効果量が小さく (|d| < 0.3)、")
                lines.append("取引コストを考慮すると実用的な収益機会とは言えません。")
            elif n_sig == 0:
                lines.append("いずれのグループ間比較においてもp値が有意水準を超えており、")
                lines.append("フォワードリターンにおける系統的な差異は確認されませんでした。")
            lines.append("")
            lines.append("**推奨アクション:**")
            lines.append("")
            lines.append("1. 異なるファクター（例: 売上成長率、アクルーアル、インサイダー取引）での再検証")
            lines.append("2. ユニバースの拡大（現在の銘柄数では統計的検出力が不足している可能性）")
            lines.append("3. 条件の組み替え（例: PBR → EV/EBITDA、モメンタム期間の変更）")

        return "\n".join(lines)

    # ── メイン実行 ─────────────────────────────────

    def run(self, output_path: str = "analysis_report.md") -> str:
        """分析を一括実行し、Markdownレポートを出力する。"""

        print("[1/5] データ読み込み中...")
        prices = self._load_all_prices()
        fundamentals = self._load_fundamentals()

        tickers = prices["ticker"].unique().tolist()
        print(f"  銘柄数: {len(tickers)}, 行数: {len(prices):,}")

        if len(tickers) < 2:
            print("[ERROR] 分析には最低2銘柄のデータが必要です。")
            print("  data_pipeline.py で複数銘柄を取得してください。")
            sys.exit(1)

        print("[2/5] 特徴量算出中...")
        df = self._compute_features(prices, fundamentals)

        print("[3/5] マトリクス構築中...")
        matrix = self._build_matrix(df)

        if matrix.empty:
            print("[ERROR] 有効なデータが不足しています（PBR/モメンタム/ボラティリティが欠損）。")
            print("  ファンダメンタルデータが取得済みか確認してください。")
            sys.exit(1)

        print(f"  有効観測数: {len(matrix):,}")

        print("[4/5] 統計検定実行中...")

        # PBR × モメンタム
        pbr_mom_stats = self._cross_section_stats(matrix, "pbr_group", "momentum_group")
        # PBR × ボラティリティ
        pbr_vol_stats = self._cross_section_stats(matrix, "pbr_group", "volatility_group")

        # 検定: PBRグループ間
        pbr_tests_1m = self._test_significance(matrix, "pbr_group", "fwd_return_1m")
        pbr_tests_3m = self._test_significance(matrix, "pbr_group", "fwd_return_3m")

        # 検定: モメンタムグループ間
        mom_tests_1m = self._test_significance(matrix, "momentum_group", "fwd_return_1m")
        mom_tests_3m = self._test_significance(matrix, "momentum_group", "fwd_return_3m")

        # 検定: ボラティリティグループ間
        vol_tests_1m = self._test_significance(matrix, "volatility_group", "fwd_return_1m")
        vol_tests_3m = self._test_significance(matrix, "volatility_group", "fwd_return_3m")

        print("[5/5] レポート生成中...")
        report = self._generate_report(
            pbr_mom_stats, pbr_vol_stats,
            pbr_tests_1m, pbr_tests_3m,
            mom_tests_1m, mom_tests_3m,
            vol_tests_1m, vol_tests_3m,
            matrix, tickers,
        )

        with open(output_path, "w", encoding="utf-8") as f:
            f.write(report)

        print(f"\n[DONE] レポートを {output_path} に出力しました。")
        return report


# ── CLI ───────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="PBR × モメンタム/ボラティリティ クロスセクション分析",
    )
    parser.add_argument(
        "--db", type=str, default=None,
        help="SQLiteファイルパス（デフォルト: data/stock_data.db）",
    )
    parser.add_argument(
        "-o", "--output", type=str, default="analysis_report.md",
        help="出力Markdownファイルパス（デフォルト: analysis_report.md）",
    )
    args = parser.parse_args()

    analyzer = CrossSectionAnalyzer(db_path=args.db)
    analyzer.run(output_path=args.output)


if __name__ == "__main__":
    main()
