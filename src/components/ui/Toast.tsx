import React, { useEffect } from 'react';
import { StyleSheet, Text, Platform } from 'react-native';
import Animated, {
    useSharedValue,
    useAnimatedStyle,
    withTiming,
    withDelay,
    Easing
} from 'react-native-reanimated';

interface ToastProps {
    message: string | null;
    visible: boolean;
}

export const Toast: React.FC<ToastProps> = ({ message, visible }) => {
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
        <Animated.View style={[styles.container, animatedStyle]} pointerEvents="none">
            <Text style={styles.text}>{message}</Text>
        </Animated.View>
    );
};

const styles = StyleSheet.create({
    container: {
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
    text: {
        color: '#ffffff', // 白文字
        fontSize: 16,
        fontFamily: Platform.OS === 'ios' ? 'Palatino' : 'serif', // セリフ体
        textAlign: 'center',
        lineHeight: 24,
        letterSpacing: 0.5,
    },
});
