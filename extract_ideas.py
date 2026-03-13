"""
YouTube動画から投資アイデアの種を抽出するスクリプト

使い方:
    python extract_ideas.py "https://www.youtube.com/watch?v=XXXXX"
    python extract_ideas.py "XXXXX"  (動画IDのみでもOK)
"""

import argparse
import json
import os
import re
import subprocess
import sys
from datetime import datetime
from urllib.parse import parse_qs, urlparse


# ── 設定 ──────────────────────────────────────────────

# 投資関連キーワード（該当行を抽出するためのフィルタ）
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
    # 銘柄コード（4桁数字は後で正規表現で拾う）
]

# 4桁の銘柄コードを検出する正規表現
STOCK_CODE_PATTERN = re.compile(r"[1-9]\d{3}(?:\s|　|\.)")

# キーワードの正規表現パターン（大文字小文字区別なし）
KEYWORD_PATTERN = re.compile(
    "|".join(re.escape(kw) for kw in KEYWORDS),
    re.IGNORECASE,
)

# 最低限の文字数（短すぎるノイズを除外）
MIN_TEXT_LENGTH = 8


# ── ユーティリティ ─────────────────────────────────────

def extract_video_id(url_or_id: str) -> str:
    """URLまたは動画IDから動画IDを抽出する。"""
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

    # フォールバック: そのまま返す
    return url_or_id.strip()


def is_relevant(text: str) -> bool:
    """テキストが投資関連かどうかを判定する。"""
    if len(text) < MIN_TEXT_LENGTH:
        return False
    if KEYWORD_PATTERN.search(text):
        return True
    if STOCK_CODE_PATTERN.search(text):
        return True
    return False


def extract_matched_keywords(text: str) -> list[str]:
    """テキスト中にマッチした投資キーワードを返す。"""
    found = set()
    for m in KEYWORD_PATTERN.finditer(text):
        found.add(m.group())
    # 銘柄コード
    for m in STOCK_CODE_PATTERN.finditer(text):
        found.add(m.group().strip())
    return sorted(found)


# ── 字幕取得 ──────────────────────────────────────────

def fetch_transcript(video_id: str) -> list[dict]:
    """youtube-transcript-api で字幕を取得する。"""
    try:
        from youtube_transcript_api import YouTubeTranscriptApi
    except ImportError:
        print("[ERROR] youtube-transcript-api がインストールされていません。")
        print("  pip install youtube-transcript-api")
        sys.exit(1)

    print(f"[INFO] 字幕を取得中... (video_id={video_id})")
    try:
        # 日本語 → 英語 → 自動生成の順に試行
        transcript_list = YouTubeTranscriptApi.list_transcripts(video_id)

        transcript = None
        # 手動字幕を優先
        for lang in ["ja", "en"]:
            try:
                transcript = transcript_list.find_transcript([lang])
                break
            except Exception:
                continue

        # 自動生成字幕にフォールバック
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


# ── チャットリプレイ取得 ───────────────────────────────

def fetch_chat_replay(video_id: str) -> list[dict]:
    """yt-dlp でライブチャットリプレイを取得する。"""
    url = f"https://www.youtube.com/watch?v={video_id}"
    output_path = f"_chat_{video_id}.json"

    print(f"[INFO] チャットリプレイを取得中... (yt-dlp)")
    try:
        cmd = [
            sys.executable, "-m", "yt_dlp",
            "--skip-download",
            "--write-subs",
            "--sub-lang", "live_chat",
            "--sub-format", "json3",
            "-o", f"_chat_{video_id}",
            url,
        ]
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=120,
        )

        # yt-dlp は拡張子を自動付与するためいくつかのパターンを探す
        candidates = [
            f"_chat_{video_id}.live_chat.json3",
            f"_chat_{video_id}.live_chat.json",
            f"_chat_{video_id}.ja.json3",
            f"_chat_{video_id}.json3",
            output_path,
        ]

        chat_file = None
        for c in candidates:
            if os.path.exists(c):
                chat_file = c
                break

        if chat_file is None:
            print("[WARN] チャットリプレイが見つかりませんでした（ライブ配信でない可能性）。")
            if result.stderr:
                # エラー詳細は冗長なので最後の3行だけ
                lines = result.stderr.strip().split("\n")
                for line in lines[-3:]:
                    print(f"       {line}")
            return []

        messages = []
        with open(chat_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        # json3 形式のパース
        events = data.get("events", [])
        for event in events:
            seg = event.get("segs")
            if seg:
                text = "".join(s.get("utf8", "") for s in seg).strip()
                if text:
                    time_ms = event.get("tStartMs", 0)
                    messages.append({
                        "time": time_ms / 1000.0,
                        "text": text,
                        "source": "chat",
                    })

        # クリーンアップ
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


# ── フィルタリング & 集約 ─────────────────────────────

def format_time(seconds: float) -> str:
    """秒数を HH:MM:SS 形式にフォーマットする。"""
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    if h > 0:
        return f"{h:02d}:{m:02d}:{s:02d}"
    return f"{m:02d}:{s:02d}"


def merge_nearby_entries(entries: list[dict], window_sec: float = 30.0) -> list[dict]:
    """時間的に近い字幕エントリを統合して文脈を保持する。"""
    if not entries:
        return []

    merged = []
    current = {
        "time_start": entries[0]["time"],
        "time_end": entries[0]["time"],
        "texts": [entries[0]["text"]],
        "keywords": set(entries[0].get("keywords", [])),
        "source": entries[0].get("source", "subtitle"),
    }

    for entry in entries[1:]:
        if (entry["time"] - current["time_end"]) <= window_sec and \
           entry.get("source", "subtitle") == current["source"]:
            current["time_end"] = entry["time"]
            current["texts"].append(entry["text"])
            current["keywords"].update(entry.get("keywords", []))
        else:
            merged.append(current)
            current = {
                "time_start": entry["time"],
                "time_end": entry["time"],
                "texts": [entry["text"]],
                "keywords": set(entry.get("keywords", [])),
                "source": entry.get("source", "subtitle"),
            }

    merged.append(current)
    return merged


def filter_and_process(
    transcript: list[dict],
    chat: list[dict],
) -> list[dict]:
    """全テキストから投資関連の発言のみを抽出し、統合する。"""

    relevant = []

    # 字幕の処理
    for entry in transcript:
        text = entry["text"].strip()
        if is_relevant(text):
            keywords = extract_matched_keywords(text)
            relevant.append({
                "time": entry["time"],
                "text": text,
                "keywords": keywords,
                "source": "subtitle",
            })

    # チャットの処理
    for entry in chat:
        text = entry["text"].strip()
        if is_relevant(text):
            keywords = extract_matched_keywords(text)
            relevant.append({
                "time": entry["time"],
                "text": text,
                "keywords": keywords,
                "source": "chat",
            })

    # 時系列ソート
    relevant.sort(key=lambda x: x["time"])

    # 近接エントリを統合
    merged = merge_nearby_entries(relevant)

    return merged


# ── Markdown出力 ──────────────────────────────────────

def generate_markdown(
    entries: list[dict],
    video_id: str,
    output_path: str = "investment_ideas.md",
) -> str:
    """抽出結果を Markdown ファイルとして出力する。"""

    # キーワード集計
    keyword_counts: dict[str, int] = {}
    for entry in entries:
        for kw in entry["keywords"]:
            keyword_counts[kw] = keyword_counts.get(kw, 0) + 1

    top_keywords = sorted(keyword_counts.items(), key=lambda x: -x[1])

    lines = []
    lines.append(f"# 投資アイデア抽出レポート")
    lines.append("")
    lines.append(f"- **動画**: https://www.youtube.com/watch?v={video_id}")
    lines.append(f"- **抽出日時**: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append(f"- **抽出件数**: {len(entries)} 件")
    lines.append("")

    # キーワードサマリ
    if top_keywords:
        lines.append("## 頻出キーワード")
        lines.append("")
        for kw, count in top_keywords[:20]:
            lines.append(f"- **{kw}** ({count}回)")
        lines.append("")

    # 字幕からの抽出
    subtitle_entries = [e for e in entries if e["source"] == "subtitle"]
    chat_entries = [e for e in entries if e["source"] == "chat"]

    if subtitle_entries:
        lines.append("## 字幕からの抽出")
        lines.append("")
        for entry in subtitle_entries:
            time_str = format_time(entry["time_start"])
            kw_tags = " ".join(f"`{kw}`" for kw in sorted(entry["keywords"]))
            combined_text = " ".join(entry["texts"])
            # 長すぎるテキストは切り詰め
            if len(combined_text) > 500:
                combined_text = combined_text[:500] + "..."
            lines.append(f"### [{time_str}] {kw_tags}")
            lines.append("")
            lines.append(f"> {combined_text}")
            lines.append("")

    if chat_entries:
        lines.append("## チャットからの抽出")
        lines.append("")
        for entry in chat_entries:
            time_str = format_time(entry["time_start"])
            kw_tags = " ".join(f"`{kw}`" for kw in sorted(entry["keywords"]))
            combined_text = " ".join(entry["texts"])
            if len(combined_text) > 300:
                combined_text = combined_text[:300] + "..."
            lines.append(f"- **[{time_str}]** {kw_tags} — {combined_text}")
        lines.append("")

    if not entries:
        lines.append("## 結果")
        lines.append("")
        lines.append("投資関連のキーワードに該当する発言は検出されませんでした。")
        lines.append("")

    content = "\n".join(lines)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(content)

    return output_path


# ── メイン ────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="YouTube動画から投資アイデアの種を抽出する",
    )
    parser.add_argument(
        "url",
        help="YouTube動画のURL または 動画ID",
    )
    parser.add_argument(
        "-o", "--output",
        default="investment_ideas.md",
        help="出力ファイルパス (default: investment_ideas.md)",
    )
    parser.add_argument(
        "--no-chat",
        action="store_true",
        help="チャットリプレイの取得をスキップする",
    )
    args = parser.parse_args()

    video_id = extract_video_id(args.url)
    print(f"[INFO] 対象動画ID: {video_id}")
    print()

    # 1. 字幕取得
    transcript = fetch_transcript(video_id)

    # 2. チャットリプレイ取得
    chat = []
    if not args.no_chat:
        chat = fetch_chat_replay(video_id)

    if not transcript and not chat:
        print("[ERROR] 字幕・チャットともに取得できませんでした。")
        print("        動画に字幕が無いか、URLが正しいか確認してください。")
        sys.exit(1)

    # 3. フィルタリング
    print()
    print("[INFO] 投資関連テキストを抽出中...")
    entries = filter_and_process(transcript, chat)
    print(f"[INFO] {len(entries)} 件の関連セグメントを検出しました。")

    # 4. Markdown出力
    output_file = generate_markdown(entries, video_id, args.output)
    print()
    print(f"[DONE] 結果を {output_file} に出力しました。")


if __name__ == "__main__":
    main()
