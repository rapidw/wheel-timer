package io.rapidw.wheeltimer;

public interface TimerTask {

    void run(boolean isExpired, boolean isCancelled);
}
