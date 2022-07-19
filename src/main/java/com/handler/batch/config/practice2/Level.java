package com.handler.batch.config.practice2;

import java.util.Objects;

public enum Level {
    VIP(500_000, null),
    GOLD(500_000, VIP),
    SILVER(300_000, GOLD),
    NORMAL(200_000, SILVER);

    private final int nextAmount;
    private final Level nextLevel;

    Level(int nextAmount, Level nextLevel) {
        this.nextAmount = nextAmount;
        this.nextLevel = nextLevel;
    }

    public static boolean availableLevelUp(Level level, int totalAmount) {
        if (Objects.isNull(level)) {
            return false;
        }

        if (Objects.isNull(level.nextLevel)) {
            return false;
        }

        return totalAmount >= level.nextAmount;
    }

    public static Level getNextLevel(int totalAmount) {
        if (totalAmount >= Level.VIP.nextAmount) {
            return VIP;
        }

        if (totalAmount >= Level.GOLD.nextAmount) {
            return GOLD.nextLevel;
        }

        if (totalAmount >= Level.SILVER.nextAmount) {
            return SILVER.nextLevel;
        }

        if (totalAmount >= Level.NORMAL.nextAmount) {
            return NORMAL.nextLevel;
        }

        return NORMAL;
    }
}
