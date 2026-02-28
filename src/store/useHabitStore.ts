import { create } from 'zustand';

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

export const useHabitStore = create<HabitStore>((set) => ({
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
