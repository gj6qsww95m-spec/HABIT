// スティーブ・ジョブズ風の哲学的な名言集（API連携に代わるローカルデータ）
export const jobsQuotes = {
  completion: [
    "美しさは、そこに不要なものが何一つないときにのみ生まれる。",
    "今日もまた一つ、宇宙に小さな凹み（デント）を作ったな。",
    "素晴らしい。最も素晴らしいのは、君がそれを始めたことだ。",
    "1秒の積み重ねが、やがて世界を変えるイノベーションになる。",
    "完璧だ。さあ、次はもっとシンプルに生きよう。"
  ],
  skip: [
    "休息もまた、デザインの一部だ。美しい余白を持たせよう。",
    "焦る必要はない。イノベーションには充電期間が必要だ。",
    "立ち止まる勇気があるなら、君はまた前に進める。",
    "今日のスキップは、明日のための最も戦略的な意思決定だ。"
  ],
  downgrade: [
    "小さくてもいい。大事なのは、妥協せずにやり遂げることだ。",
    "制約こそが、我々をクリエイティブにする。",
    "どんなに小さくとも、前進したという事実に変わりはない。",
    "美しい。ハードルを下げるという完璧なデザインだ。"
  ]
};

export type ActionType = keyof typeof jobsQuotes;

export const getRandomQuote = (type: ActionType): string => {
  const quotes = jobsQuotes[type];
  const randomIndex = Math.floor(Math.random() * quotes.length);
  return quotes[randomIndex];
};
