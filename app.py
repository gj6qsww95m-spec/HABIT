import streamlit as st
import random
import time

# ==========================================
# 1. Page Configuration & Custom CSS (Vibe Coding)
# ==========================================
st.set_page_config(
    page_title="The Habit - MVP",
    page_icon="🔥",
    layout="centered",
    initial_sidebar_state="collapsed",
)

# 極限まで無駄を省いたジョブズ的ダークモードCSS
st.markdown("""
<style>
    /* 全体の背景とフォント */
    .stApp {
        background-color: #000000;
        color: #ffffff;
        font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif;
    }
    
    /* セリフ体（名言用） */
    .serif-text {
        font-family: 'Shippori Mincho', 'Palatino', serif;
        letter-spacing: 0.05em;
    }

    /* ヘッダー周りの非表示 */
    header {visibility: hidden;}
    footer {visibility: hidden;}
    .stDeployButton {display:none;}
    
    /* ストリーク（炎）のスタイル */
    .streak-container {
        text-align: center;
        margin-top: 2rem;
        margin-bottom: 4rem;
        animation: fadeInDown 0.8s ease-out;
    }
    .streak-text {
        color: #ff4757;
        font-size: 1.5rem;
        font-weight: 600;
        letter-spacing: 0.1em;
    }

    /* 習慣タイトルのスタイル */
    .habit-title {
        text-align: center;
        font-size: 3rem;
        font-weight: 700;
        margin-bottom: 4rem;
        letter-spacing: -0.02em;
        animation: fadeIn 1s ease-out;
    }

    /* 完了画面のスタイル */
    .perfect-title {
        text-align: center;
        font-size: 4.5rem;
        font-weight: 300;
        margin-bottom: 1rem;
        color: #ffffff;
        animation: scaleIn 0.6s cubic-bezier(0.16, 1, 0.3, 1);
    }
    .perfect-subtitle {
        text-align: center;
        font-size: 1.25rem;
        color: #888888;
        animation: fadeIn 1.2s ease-out;
    }

    /* ボタンのカスタムスタイリング */
    .stButton>button {
        width: 100%;
        border-radius: 30px;
        border: none;
        transition: all 0.2s ease;
    }
    
    /* Primary Button (完了) */
    div[data-testid="stHorizontalBlock"] > div:nth-child(1) .stButton>button {
        background-color: #ffffff;
        color: #000000;
        font-size: 1.25rem;
        font-weight: 600;
        padding: 1rem 2rem;
        box-shadow: 0 4px 24px rgba(255,255,255,0.2);
    }
    div[data-testid="stHorizontalBlock"] > div:nth-child(1) .stButton>button:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 32px rgba(255,255,255,0.3);
        background-color: #f0f0f0;
        color: #000000;
    }
    div[data-testid="stHorizontalBlock"] > div:nth-child(1) .stButton>button:active {
        transform: translateY(1px) scale(0.98);
    }

    /* Secondary Button (ダウングレード) */
    div[data-testid="stHorizontalBlock"] > div:nth-child(2) .stButton>button {
        background-color: transparent;
        color: #888888;
        font-size: 1rem;
        padding: 0.75rem 1rem;
    }
    div[data-testid="stHorizontalBlock"] > div:nth-child(2) .stButton>button:hover {
        color: #bbbbbb;
        background-color: rgba(255,255,255,0.05);
    }

    /* Tertiary Button (スキップ) */
    div[data-testid="stHorizontalBlock"] > div:nth-child(3) .stButton>button {
        background-color: transparent;
        color: #555555;
        font-size: 0.9rem;
        padding: 0.75rem 1rem;
    }
    div[data-testid="stHorizontalBlock"] > div:nth-child(3) .stButton>button:hover {
        color: #777777;
        background-color: rgba(255,255,255,0.02);
    }

    /* トーストメッセージ（The Mentor）のスタイル */
    @keyframes toastSlideUp {
        0% { transform: translate(-50%, 20px) scale(0.95); opacity: 0; }
        15% { transform: translate(-50%, 0) scale(1); opacity: 1; }
        85% { transform: translate(-50%, 0) scale(1); opacity: 1; }
        100% { transform: translate(-50%, 20px) scale(0.95); opacity: 0; }
    }
    .mentor-toast {
        position: fixed;
        bottom: 2rem;
        left: 50%;
        transform: translateX(-50%);
        background-color: rgba(10, 10, 10, 0.9);
        color: #ffffff;
        padding: 1rem 1.5rem;
        border-radius: 12px;
        border: 1px solid #333333;
        box-shadow: 0 0 40px rgba(255,255,255,0.05);
        text-align: center;
        z-index: 1000;
        backdrop-filter: blur(10px);
        animation: toastSlideUp 5s cubic-bezier(0.16, 1, 0.3, 1) forwards;
        pointer-events: none;
    }

    /* Animations */
    @keyframes fadeInDown {
        from { opacity: 0; transform: translateY(-10px); }
        to { opacity: 1; transform: translateY(0); }
    }
    @keyframes fadeIn {
        from { opacity: 0; }
        to { opacity: 1; }
    }
    @keyframes scaleIn {
        from { opacity: 0; transform: scale(0.9); }
        to { opacity: 1; transform: scale(1); }
    }
</style>
""", unsafe_allow_html=True)

# ==========================================
# 2. Data & State Management
# ==========================================
# スティーブ・ジョブズ風ローカル名言集
JOBS_QUOTES = {
    "completion": [
        "美しさは、そこに不要なものが何一つないときにのみ生まれる。",
        "今日もまた一つ、宇宙に小さな凹みをつくったな。",
        "素晴らしい。最も素晴らしいのは、君がそれを始めたことだ。",
        "1秒の積み重ねが、やがて世界を変えるイノベーションになる。",
        "完璧だ。さあ、次はもっとシンプルに生きよう。"
    ],
    "skip": [
        "休息もまた、デザインの一部だ。美しい余白を持たせよう。",
        "焦る必要はない。イノベーションには充電期間が必要だ。",
        "立ち止まる勇気があるなら、君はまた前に進める。",
        "今日のスキップは、明日のための最も戦略的な意思決定だ。"
    ],
    "downgrade": [
        "小さくてもいい。大事なのは、妥協せずにやり遂げることだ。",
        "制約こそが、我々をクリエイティブにする。",
        "どんなに小さくとも、前進したという事実に変わりはない。",
        "美しい。ハードルを下げるという完璧なデザインだ。"
    ]
}

# 状態の初期化
if 'streak' not in st.session_state:
    st.session_state.streak = 12
if 'is_completed_today' not in st.session_state:
    st.session_state.is_completed_today = False
if 'mentor_message' not in st.session_state:
    st.session_state.mentor_message = None
if 'show_toast' not in st.session_state:
    st.session_state.show_toast = False

HABIT_TITLE = "15分読書"
DOWNGRADE_TITLE = "1分読書（本を開くだけ）"

# ==========================================
# 3. Actions (Callbacks)
# ==========================================
def show_mentor(action_type):
    st.session_state.mentor_message = random.choice(JOBS_QUOTES[action_type])
    st.session_state.show_toast = True

def handle_complete():
    st.session_state.is_completed_today = True
    st.session_state.streak += 1
    show_mentor("completion")

def handle_skip():
    st.session_state.is_completed_today = True
    show_mentor("skip")

def handle_downgrade():
    st.session_state.is_completed_today = True
    st.session_state.streak += 1
    show_mentor("downgrade")

# ==========================================
# 4. Main UI Render
# ==========================================
# ストリーク表示
st.markdown(f'<div class="streak-container"><span class="streak-text">🔥 {st.session_state.streak} Days</span></div>', unsafe_allow_html=True)

# コンテンツエリアの中央寄せ用カラム
col1, col2, col3 = st.columns([1, 10, 1])

with col2:
    if not st.session_state.is_completed_today:
        # メインアクション画面
        st.markdown(f'<h1 class="habit-title">{HABIT_TITLE}</h1>', unsafe_allow_html=True)
        
        st.write("") # スペーサー
        st.write("")
        
        # ボタンの配置（縦並びにするためのカラムコンテナ）
        btn_col1, btn_col2, btn_col3 = st.columns(1)
        
        with btn_col1:
            st.button("完了する", on_click=handle_complete, use_container_width=True)
            st.write("") # スペーサー
        
        with btn_col2:
            st.button(f"忙しい: {DOWNGRADE_TITLE}", on_click=handle_downgrade, use_container_width=True)
        
        with btn_col3:
            st.button("戦略的休息（スキップ）", on_click=handle_skip, use_container_width=True)

    else:
        # 完了画面
        st.markdown('<div class="perfect-title serif-text">Perfect.</div>', unsafe_allow_html=True)
        st.markdown('<div class="perfect-subtitle serif-text">明日のあなたも、きっと美しい。</div>', unsafe_allow_html=True)

        # デバッグ/リセット用（実際には不要だがテスト用に配置）
        # st.write("")
        # if st.button("⏪ Reset for Demo (Secret)"):
        #     st.session_state.is_completed_today = False
        #     st.session_state.show_toast = False
        #     st.rerun()

# ==========================================
# 5. The Mentor Toast (Animation)
# ==========================================
# トースト表示フラグが立っている場合のみCSSアニメーション付きの要素を描画
if st.session_state.show_toast and st.session_state.mentor_message:
    st.markdown(f'''
        <div class="mentor-toast serif-text">
            {st.session_state.mentor_message}
        </div>
    ''', unsafe_allow_html=True)
    
    # 状態をリセットしておく（Streamlitのリロードモデルへの対応。CSSで5秒で消えるアニメにしている）
    st.session_state.show_toast = False
