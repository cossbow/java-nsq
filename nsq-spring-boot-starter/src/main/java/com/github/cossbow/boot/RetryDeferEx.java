package com.github.cossbow.boot;

/**
 * 用于标记正常重试（非错误异常）
 */
public class RetryDeferEx extends RuntimeException {
    private static final long serialVersionUID = -7295539688802200321L;

    /**
     * @param defer 毫秒
     */
    private int defer = -1;     // 按定义值操作

    public RetryDeferEx() {
    }


    public RetryDeferEx(int defer) {
        this.defer = defer;
    }

    public RetryDeferEx(int defer, String message) {
        super(message);
        this.defer = defer;
    }

    public int getDefer() {
        return defer;
    }

    public int getDefer(int defVal) {
        return defer > -1 ? defer : defVal;
    }

    public int getDefer(long defVal) {
        return getDefer((int) defVal);
    }

}
