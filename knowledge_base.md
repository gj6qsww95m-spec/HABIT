# 投資ファンド コア・マニュアル (Knowledge Base)

> **本ファイルの位置づけ**: 当ファンドの分析基盤に関する唯一の正典（Single Source of Truth）。
> 新規セッション開始時は必ず本ファイルをロードし、前提知識として活用すること。

---

## 1. プロジェクト概要

| 項目 | 内容 |
|---|---|
| **目的** | YouTube上の投資情報を起点に、定量的にエッジを検証し、再現可能な投資戦略を構築する |
| **ワークフロー** | アイデア抽出 → データ取得 → 仮説検証 → 判定 |
| **作業ディレクトリ** | `D:/クロード/株式投資用/` |
| **データ保存先** | `data/stock_data.db` (SQLite) |
| **言語/環境** | Python 3.11+, Windows 11 |

---

## 2. ディレクトリ構成

```
株式投資用/
├── knowledge_base.md          ← 本ファイル（コア・マニュアル）
├── requirements.txt           ← 全依存ライブラリ
├── extract_ideas.py           ← Phase 1: YouTube → 投資アイデア抽出
├── data_pipeline.py           ← Phase 2: 株価・ファンダメンタル取得パイプライン
├── backtest_analysis.py       ← Phase 3: クロスセクション分析 & バックテスト
├── investment_ideas.md        ← 抽出済みアイデア（実行後生成）
├── analysis_report.md         ← 分析レポート（実行後生成）
└── data/
    ├── stock_data.db           ← SQLite（price_daily, fundamentals, update_log）
    └── csv/                    ← CSVエクスポート先
```

---

## 3. 依存ライブラリ

```
youtube-transcript-api>=0.6.1   # YouTube字幕取得
yt-dlp>=2024.1.0                # ライブチャットリプレイ取得
yfinance>=0.2.36                # 日本株 株価・ファンダメンタル取得
pandas>=2.0.0                   # データ操作
scipy>=1.11.0                   # 統計検定
tabulate>=0.9.0                 # テーブル表示
```

インストール: `pip install -r requirements.txt`

---

## 4. Phase 1: 投資アイデア抽出 (`extract_ideas.py`)

### 機能
YouTube動画の字幕およびライブチャットリプレイから、投資関連キーワードを含む発言のみをフィルタリング抽出する。

### 実行方法
```bash
python extract_ideas.py "https://www.youtube.com/watch?v=XXXXX"
python extract_ideas.py --no-chat "XXXXX"       # チャット取得スキップ
python extract_ideas.py -o output.md "XXXXX"    # 出力先指定
```

### 処理フロー
1. `youtube-transcript-api` で字幕取得（日本語手動 → 日本語自動 → 英語の順でフォールバック）
2. `yt-dlp` でライブチャットリプレイを json3 形式で取得
3. 約80個の投資キーワード + 4桁銘柄コード正規表現でフィルタリング
4. 30秒以内の近接発言を統合し文脈を保持
5. `investment_ideas.md` にタイムスタンプ付きで出力

### キーワード体系
| カテゴリ | 代表キーワード |
|---|---|
| 取引手法 | 逆日歩, 空売り, 信用取引, スプレッド, サヤ取り, ヘッジ |
| ファンダメンタルズ | 決算, PER, PBR, ROE, 配当, 自社株買い, EBITDA |
| マクロ・市場 | 金利, 円安, 日経平均, TOPIX, 出来高, IPO, TOB |
| セクター・テーマ | 半導体, AI, EV, バイオ, 防衛, 脱炭素 |
| イベント | SQ, 権利確定, 地政学, 関税 |

### 拡張ポイント
- `KEYWORDS` リストに追記するだけで対応テーマを拡張可能
- `MIN_TEXT_LENGTH`（デフォルト: 8）でノイズ除去閾値を調整可能

---

## 5. Phase 2: データパイプライン (`data_pipeline.py`)

### 機能
yfinance を用いて日本株の日足株価データ + ファンダメンタル指標を取得し、SQLiteに保存・差分更新する。

### 実行方法
```bash
# 銘柄指定で初回取得（過去5年分）
python data_pipeline.py 7203 6758 9984 8306

# 登録済み全銘柄を差分更新（★「データを最新化して」→ これ）
python data_pipeline.py --update

# 全期間フル再取得
python data_pipeline.py --full-refresh 7203

# 更新状況確認
python data_pipeline.py --status

# CSV一括エクスポート
python data_pipeline.py --export csv
```

### モジュールとしての利用
```python
from data_pipeline import StockDataPipeline

pipe = StockDataPipeline()
pipe.update(["7203", "6758"])         # 取得・差分更新
pipe.update_all()                     # 全銘柄一括更新
df = pipe.load("7203", start="2024-01-01")  # DataFrame読み込み
df = pipe.load_all()                  # 全銘柄一括
print(pipe.status())                  # 更新状況
pipe.export_csv()                     # CSV出力
pipe.delete("9999")                   # 銘柄削除
```

### SQLiteスキーマ

**price_daily** (主キー: ticker + date)
| カラム | 型 | 説明 |
|---|---|---|
| ticker | TEXT | 銘柄コード（例: "7203"） |
| date | TEXT | 日付 (YYYY-MM-DD) |
| open / high / low / close | REAL | 四本値 |
| adj_close | REAL | 調整後終値 |
| volume | INTEGER | 出来高 |

**fundamentals** (主キー: ticker)
| カラム | 型 | 説明 |
|---|---|---|
| market_cap | INTEGER | 時価総額 |
| price_to_book | REAL | PBR |
| trailing_pe / forward_pe | REAL | PER (実績/予想) |
| roe / roa | REAL | 自己資本/総資産利益率 |
| dividend_yield | REAL | 配当利回り |
| profit_margins | REAL | 利益率 |
| revenue / ebitda | INTEGER | 売上高 / EBITDA |
| その他 | - | sector, industry, book_value, shares_outstanding 等22指標 |

**update_log** (主キー: ticker)
| カラム | 説明 |
|---|---|
| last_updated | 最終更新日時 |
| rows_added | 最終更新時の追加行数 |

### クレンジング処理
1. 重複行除去（ticker + date で後勝ち）
2. 日付順ソート
3. OHLC欠損値を前方補完（ffill）
4. 出来高欠損を 0 埋め
5. 株価 ≤ 0 の異常値を NaN 化 → 前方補完
6. close が NaN の行を最終除去

### 設計上の注意
- ティッカー変換: コード `7203` → yfinance `7203.T`
- 差分更新: DB上の `MAX(date)` の翌日以降のみ取得
- API負荷軽減: 銘柄間に 0.5 秒スリープ
- デフォルト取得期間: 過去5年

---

## 6. Phase 3: クロスセクション分析 (`backtest_analysis.py`)

### 機能
PBR × モメンタム / ボラティリティの二軸マトリクスを構築し、各グループのフォワードリターンに統計的な差異があるかを検証する。

### 実行方法
```bash
python backtest_analysis.py
python backtest_analysis.py -o results.md --db data/stock_data.db
```

### モジュールとしての利用
```python
from backtest_analysis import CrossSectionAnalyzer

analyzer = CrossSectionAnalyzer()
report = analyzer.run("analysis_report.md")
```

### 分析パラメータ

| パラメータ | 値 | 定数名 |
|---|---|---|
| ボラティリティ算出期間 | 直近20日σ × √252（年率） | `VOLATILITY_WINDOW` |
| モメンタム算出期間 | 過去60日リターン | `MOMENTUM_WINDOW` |
| フォワードリターン（短期） | 1ヶ月 = 21営業日 | `TRADING_DAYS_1M` |
| フォワードリターン（中期） | 3ヶ月 = 63営業日 | `TRADING_DAYS_3M` |
| グループ分類 | 三分位 (33% / 67%) | `QUANTILE_LOW`, `QUANTILE_HIGH` |
| 有意水準 | α = 0.05 | `SIGNIFICANCE_LEVEL` |
| 年率換算係数 | 252 | `ANNUALIZE_FACTOR` |
| 無リスク金利 | 0.1%（年率） | `sharpe_ratio()` 引数 |

### 分析フロー
```
日足データ (SQLite)
  │
  ├── 特徴量算出（銘柄×日ごと）
  │     daily_return, volatility, momentum, fwd_return_1m, fwd_return_3m
  │
  ├── PBR結合（fundamentals テーブルから）
  │
  ├── 三分位グループ分類
  │     PBR:        低PBR / 中PBR / 高PBR
  │     モメンタム:   低 / 中 / 高
  │     ボラティリティ: 低 / 中 / 高
  │
  ├── 3×3 マトリクス統計（2パターン）
  │     ① PBR × モメンタム
  │     ② PBR × ボラティリティ
  │
  └── 統計検定
        ├── One-way ANOVA（全群）
        ├── Kruskal-Wallis（ノンパラメトリック、全群）
        ├── Welch's t-test（ペアワイズ）
        └── Cohen's d（効果量）
```

### パフォーマンス指標
- **シャープレシオ**: (日次超過リターン平均 / 日次超過リターンσ) × √252
- **最大ドローダウン**: 累積リターン系列のピークからの最大下落率

### 判定基準（二重条件、忖度なし）

| 条件 | 閾値 | 根拠 |
|---|---|---|
| 統計的有意性 | p < 0.05 (Welch's t-test) | 帰無仮説棄却 |
| 実用的効果量 | \|Cohen's d\| >= 0.3 | 取引コスト後に意味のある差 |
| リスク調整後収益 | Sharpe >= 0.5（1グループ以上） | 実運用最低水準 |

**判定結果パターン:**
- 全条件充足 → 「統計的優位性あり（条件付き）」＋ ウォークフォワード検証を推奨
- いずれか不足 → **「優位性なし。本ロジックは破棄すべき」**

---

## 7. 標準ワークフロー（End-to-End）

```
Step 1: アイデア抽出
  python extract_ideas.py "https://www.youtube.com/watch?v=XXXXX"
  → investment_ideas.md を確認し、検証したい仮説を特定

Step 2: データ取得
  python data_pipeline.py 7203 6758 9984 8306 6501 4502 9432 6861 7267 8035
  → data/stock_data.db にクレンジング済みデータが保存される

Step 3: データ最新化（以降の定常オペレーション）
  python data_pipeline.py --update

Step 4: 分析実行
  python backtest_analysis.py
  → analysis_report.md に統計的検証結果が出力される

Step 5: 判定確認
  analysis_report.md の「結論」セクションを確認
  - 優位性あり → ウォークフォワード検証へ進む
  - 優位性なし → ロジック破棄、Step 1 に戻り新仮説を探索
```

---

## 8. ショートカットコマンド集

| やりたいこと | コマンド |
|---|---|
| データを最新化して | `python data_pipeline.py --update` |
| 銘柄を追加して | `python data_pipeline.py 新コード` |
| 更新状況を見せて | `python data_pipeline.py --status` |
| 分析を回して | `python backtest_analysis.py` |
| CSVで出して | `python data_pipeline.py --export csv` |
| 動画からアイデア抽出 | `python extract_ideas.py "URL"` |

---

## 9. 技術的な注意事項・既知の制約

### yfinance関連
- 日本株は `.T` サフィックスが必要（自動付与済み）
- `auto_adjust=False` で取得し、`adj_close` を明示的に保持
- yfinance の `info` は稀に空辞書を返す → ファンダメンタル欠損時はスキップ
- レート制限: 銘柄間 0.5 秒のスリープで対処

### 分析関連
- PBRはファンダメンタルテーブルから銘柄単位で結合（時系列変動は未考慮）
- 最低2銘柄がないと分析不可、統計的検出力を確保するには10銘柄以上を推奨
- フォワードリターンは「将来を使う」ため、データ末尾の 21〜63 日分は NaN
- ルックアヘッドバイアスは発生しない設計（shift(-N) で将来値を参照）

### 今後の拡張候補
- ウォークフォワード検証の実装（ローリングウィンドウ）
- 追加ファクター: EV/EBITDA, 売上成長率, アクルーアル, インサイダー取引
- ユニバース拡大（TOPIX500全銘柄等のバルク取得）
- 取引コストモデルの組み込み（片道 10-30bps）
- ポートフォリオ構築モジュール（等ウェイト / リスクパリティ）

---

## 10. 分析実行履歴

| 日付 | 内容 | 結果 |
|---|---|---|
| (初回構築) | PBR × モメンタム/ボラティリティ クロスセクション分析フレームワーク構築 | フレームワーク完成、実データでの検証待ち |

> 分析を実行するたびに、ここに結果サマリを追記すること。

---

## 11. セッション間引き継ぎルール

1. **新規セッション開始時**: 必ず `knowledge_base.md` をロードする
2. **コード修正時**: 本ファイルの該当セクションも同期更新する
3. **分析実行後**: セクション10の実行履歴に結果を追記する
4. **新モジュール追加時**: セクション2のディレクトリ構成とセクション8のコマンド集を更新する
5. **判定基準変更時**: セクション6の判定基準テーブルを更新し、変更理由を記録する

---

*最終更新: 2026-03-13 — 初版作成（Phase 1〜3 フレームワーク構築完了）*
