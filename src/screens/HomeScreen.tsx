import React from 'react';
import { StyleSheet, View, Text, TouchableOpacity, SafeAreaView, Platform } from 'react-native';
import { useHabitStore } from '../store/useHabitStore';
import { useMentor } from '../hooks/useMentor';
import { Toast } from '../components/ui/Toast';

export const HomeScreen: React.FC = () => {
    const { habit, completeHabit, skipHabit, downgradeHabit } = useHabitStore();
    const { toastMessage, toastVisible, showMentorMessage } = useMentor();

    const handleComplete = () => {
        completeHabit(); // オプティミスティックに状態を即座に更新
        showMentorMessage('completion');
        // TODO: Здесь вызывать Haptics (expo-haptics)
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
});
