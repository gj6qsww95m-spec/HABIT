import React, { useState, useCallback, useEffect } from 'react';
import { StyleSheet, View, Text, TouchableOpacity, SafeAreaView, Platform } from 'react-native';
import { StatusBar } from 'expo-status-bar';
import { create } from 'zustand';
import Animated, {
    useSharedValue,
    useAnimatedStyle,
    withTiming,
    withDelay,
    Easing
} from 'react-native-reanimated';

// ==========================================
// 1. Data (Local Quotes)
// ==========================================
const jobsQuotes = {
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

type ActionType = keyof typeof jobsQuotes;

const getRandomQuote = (type: ActionType): string => {
    const quotes = jobsQuotes[type];
    const randomIndex = Math.floor(Math.random() * quotes.length);
    return quotes[randomIndex];
};

// ==========================================
// 2. State Management (Zustand)
// ==========================================
interface Habit {
    id: string;
    title: string;
    downgradeTitle?: string;
    isCompletedToday: boolean;
    streak: number;
}

interface HabitStore {
    habit: Habit;
    completeHabit: () => void;
    skipHabit: () => void;
    downgradeHabit: () => void;
}

const useHabitStore = create<HabitStore>((set) => ({
    habit: {
        id: '1',
        title: '15分読書',
        downgradeTitle: '1分読書（本を開くだけ）',
        isCompletedToday: false,
        streak: 12, // モックの初期ストリーク
    },
    completeHabit: () => set((state) => ({
        habit: {
            ...state.habit,
            isCompletedToday: true,
            streak: state.habit.isCompletedToday ? state.habit.streak : state.habit.streak + 1
        }
    })),
    skipHabit: () => set((state) => ({
        habit: {
            ...state.habit,
            isCompletedToday: true, // スキップでもその日は完了扱いとしストリークを維持
        }
    })),
    downgradeHabit: () => set((state) => ({
        habit: {
            ...state.habit,
            isCompletedToday: true,
            streak: state.habit.isCompletedToday ? state.habit.streak : state.habit.streak + 1
        }
    }))
}));

// ==========================================
// 3. Custom Hooks (The Mentor)
// ==========================================
const useMentor = () => {
    const [toastMessage, setToastMessage] = useState<string | null>(null);
    const [toastVisible, setToastVisible] = useState(false);

    const showMentorMessage = useCallback((action: ActionType) => {
        // ローカル配列から即座に名言を取得
        const message = getRandomQuote(action);
        setToastMessage(message);
        setToastVisible(true);

        // 4秒後に自動で隠す（APIの待ち時間がないため即座にフェードインし、自然に消える）
        setTimeout(() => {
            setToastVisible(false);
        }, 4000);
    }, []);

    const hideMessage = useCallback(() => {
        setToastVisible(false);
    }, []);

    return {
        toastMessage,
        toastVisible,
        showMentorMessage,
        hideMessage
    };
};

// ==========================================
// 4. UI Components (Toast)
// ==========================================
interface ToastProps {
    message: string | null;
    visible: boolean;
}

const Toast: React.FC<ToastProps> = ({ message, visible }) => {
    const opacity = useSharedValue(0);
    const translateY = useSharedValue(20);

    useEffect(() => {
        if (visible && message) {
            // 表示時: ふわっと持ち上がりながら現れる
            opacity.value = withTiming(1, { duration: 600, easing: Easing.out(Easing.exp) });
            translateY.value = withTiming(0, { duration: 600, easing: Easing.out(Easing.exp) });
        } else {
            // 非表示時: 少し遅延させてから静かに消える
            opacity.value = withDelay(
                200,
                withTiming(0, { duration: 500, easing: Easing.in(Easing.ease) })
            );
            translateY.value = withDelay(
                200,
                withTiming(20, { duration: 500, easing: Easing.in(Easing.ease) })
            );
        }
    }, [visible, message, opacity, translateY]);

    const animatedStyle = useAnimatedStyle(() => {
        return {
            opacity: opacity.value,
            transform: [{ translateY: translateY.value }],
        };
    });

    if (!message) return null;

    return (
        <Animated.View style={[styles.toastContainer, animatedStyle]} pointerEvents="none">
            <Text style={styles.toastText}>{message}</Text>
        </Animated.View>
    );
};

// ==========================================
// 5. Main Screen
// ==========================================
const HomeScreen: React.FC = () => {
    const { habit, completeHabit, skipHabit, downgradeHabit } = useHabitStore();
    const { toastMessage, toastVisible, showMentorMessage } = useMentor();

    const handleComplete = () => {
        completeHabit(); // オプティミスティックに状態を即座に更新
        showMentorMessage('completion');
        // TODO: ここでexpo-hapticsなどを呼び出すとさらに良い感触になる
    };

    const handleSkip = () => {
        skipHabit();
        showMentorMessage('skip');
    };

    const handleDowngrade = () => {
        downgradeHabit();
        showMentorMessage('downgrade');
    };

    return (
        <SafeAreaView style={styles.safeArea}>
            <View style={styles.container}>
                {/* ヘッダー部：ストリーク */}
                <View style={styles.header}>
                    <Text style={styles.streakText}>🔥 {habit.streak} Days</Text>
                </View>

                {/* メインの習慣カード空間 */}
                <View style={styles.cardContainer}>
                    {!habit.isCompletedToday ? (
                        <>
                            <Text style={styles.habitTitle}>{habit.title}</Text>

                            {/* 1タップ完了ボタン */}
                            <TouchableOpacity
                                style={styles.primaryButton}
                                activeOpacity={0.8}
                                onPress={handleComplete}
                            >
                                <Text style={styles.primaryButtonText}>完了する</Text>
                            </TouchableOpacity>

                            {/* スマートダウングレード */}
                            <TouchableOpacity
                                style={styles.secondaryButton}
                                activeOpacity={0.6}
                                onPress={handleDowngrade}
                            >
                                <Text style={styles.secondaryButtonText}>
                                    忙しい: {habit.downgradeTitle}
                                </Text>
                            </TouchableOpacity>

                            {/* 戦略的スキップ */}
                            <TouchableOpacity
                                style={styles.tertiaryButton}
                                activeOpacity={0.6}
                                onPress={handleSkip}
                            >
                                <Text style={styles.tertiaryButtonText}>戦略的休息（スキップ）</Text>
                            </TouchableOpacity>
                        </>
                    ) : (
                        <View style={styles.completedContainer}>
                            <Text style={styles.completedTitle}>Perfect.</Text>
                            <Text style={styles.completedSubtitle}>明日のあなたも、きっと美しい。</Text>
                        </View>
                    )}
                </View>

                {/* The Mentor（ジョブズのトースト） */}
                <Toast message={toastMessage} visible={toastVisible} />
            </View>
        </SafeAreaView>
    );
};

// ==========================================
// 6. App Entry Point
// ==========================================
export default function App() {
    return (
        <>
            <StatusBar style="light" />
            <HomeScreen />
        </>
    );
}

// ==========================================
// 7. Styles
// ==========================================
const styles = StyleSheet.create({
    safeArea: {
        flex: 1,
        backgroundColor: '#000000', // 極限のダークモード
    },
    container: {
        flex: 1,
        paddingHorizontal: 24,
        justifyContent: 'center',
        ...Platform.select({
            web: {
                maxWidth: 480,
                marginHorizontal: 'auto',
                width: '100%',
            }
        }),
    },
    header: {
        position: 'absolute',
        top: Platform.OS === 'web' ? 40 : 60,
        alignSelf: 'center',
    },
    streakText: {
        color: '#ff6b6b', // 炎の色
        fontSize: 20,
        fontWeight: '600',
        letterSpacing: 1,
        fontFamily: Platform.OS === 'ios' ? 'San Francisco' : 'System',
    },
    cardContainer: {
        flex: 1,
        justifyContent: 'center',
        alignItems: 'center',
    },
    habitTitle: {
        color: '#ffffff',
        fontSize: 42,
        fontWeight: '700',
        marginBottom: 60,
        letterSpacing: -1,
        fontFamily: Platform.OS === 'ios' ? 'San Francisco' : 'System',
        textAlign: 'center',
    },
    primaryButton: {
        backgroundColor: '#ffffff',
        paddingVertical: 20,
        paddingHorizontal: 40,
        borderRadius: 40,
        width: '100%',
        alignItems: 'center',
        shadowColor: '#ffffff',
        shadowOffset: { width: 0, height: 4 },
        shadowOpacity: 0.3,
        shadowRadius: 16,
        elevation: 8,
        marginBottom: 24,
    },
    primaryButtonText: {
        color: '#000000',
        fontSize: 20,
        fontWeight: '600',
        letterSpacing: 0.5,
    },
    secondaryButton: {
        paddingVertical: 12,
        marginBottom: 16,
    },
    secondaryButtonText: {
        color: '#a1a1aa', // 控えめなグレー
        fontSize: 16,
    },
    tertiaryButton: {
        paddingVertical: 12,
    },
    tertiaryButtonText: {
        color: '#52525b', // さらに控えめなダークグレー
        fontSize: 14,
    },
    completedContainer: {
        alignItems: 'center',
    },
    completedTitle: {
        color: '#ffffff',
        fontSize: 64,
        fontWeight: '300',
        fontFamily: Platform.OS === 'ios' ? 'Palatino' : 'serif',
        marginBottom: 16,
    },
    completedSubtitle: {
        color: '#71717a',
        fontSize: 18,
        fontFamily: Platform.OS === 'ios' ? 'Palatino' : 'serif',
    },
    toastContainer: {
        position: 'absolute',
        bottom: Platform.OS === 'web' ? 40 : 80,
        alignSelf: 'center',
        backgroundColor: '#000000', // 漆黒
        paddingVertical: 16,
        paddingHorizontal: 24,
        borderRadius: 8,
        shadowColor: '#ffffff',
        shadowOffset: { width: 0, height: 4 },
        shadowOpacity: 0.1,
        shadowRadius: 10,
        elevation: 5,
        maxWidth: '85%',
        borderWidth: 1,
        borderColor: '#333333',
        zIndex: 1000,
    },
    toastText: {
        color: '#ffffff', // 白文字
        fontSize: 16,
        fontFamily: Platform.OS === 'ios' ? 'Palatino' : 'serif', // セリフ体
        textAlign: 'center',
        lineHeight: 24,
        letterSpacing: 0.5,
    },
});
