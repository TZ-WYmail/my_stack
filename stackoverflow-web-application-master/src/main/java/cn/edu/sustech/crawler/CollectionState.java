package cn.edu.sustech.crawler;


public enum CollectionState {
    NOT_STARTED("Not Started", 0),
    COLLECTING_QUESTIONS("Collecting Questions", 1),
    COLLECTING_ANSWERS("Collecting Answers", 2),
    COLLECTING_QUESTION_COMMENTS("Collecting Question Comments", 3),
    COLLECTING_ANSWER_COMMENTS("Collecting Answer Comments", 4),
    SAVING_TO_DATABASE("Saving to Database", 5),
    COMPLETED("Completed", 6),
    FAILED("Failed", -1),
    PAUSED("Paused", -2);

    private final String description;
    private final int order;

    CollectionState(String description, int order) {
        this.description = description;
        this.order = order;
    }

    // 获取状态描述
    public String getDescription() {
        return description;
    }

    // 获取状态顺序
    public int getOrder() {
        return order;
    }

    // 检查是否为终止状态
    public boolean isTerminal() {
        return this == COMPLETED || this == FAILED;
    }

    // 检查是否可以从当前状态转换到目标状态
    public boolean canTransitionTo(CollectionState target) {
        if (this == target) {
            return true;
        }

        // 失败状态可以转换到任何非终止状态以重试
        if (this == FAILED) {
            return !target.isTerminal();
        }

        // 暂停状态可以恢复到任何非终止状态
        if (this == PAUSED) {
            return !target.isTerminal();
        }

        // 正常状态只能向前推进或转为暂停/失败
        return target == PAUSED ||
                target == FAILED ||
                (target.order > this.order && target.order <= COMPLETED.order);
    }

    // 获取下一个正常状态
    public CollectionState nextState() {
        if (this.isTerminal() || this == PAUSED) {
            return this;
        }

        for (CollectionState state : values()) {
            if (state.order == this.order + 1) {
                return state;
            }
        }
        return this;
    }

    // 获取状态的完成百分比（基于顺序）
    public double getProgressPercentage() {
        if (this.order < 0) {
            return 0.0;
        }
        return (double) this.order / COMPLETED.order * 100.0;
    }

    // 从字符串解析状态
    public static CollectionState fromString(String stateStr) {
        try {
            return valueOf(stateStr.toUpperCase());
        } catch (IllegalArgumentException e) {
            return NOT_STARTED;
        }
    }

    @Override
    public String toString() {
        return description;
    }
}