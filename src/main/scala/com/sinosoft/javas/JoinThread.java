package com.sinosoft.javas;

public class JoinThread {
    volatile int n = 0;

    public void run() {
        this.n = 12;
    }
}
