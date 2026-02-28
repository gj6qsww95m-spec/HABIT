import { useState, useCallback } from 'react';
import { ActionType, getRandomQuote } from '../utils/jobsQuotes';

export const useMentor = () => {
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
