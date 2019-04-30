package com.lufax.mis.domain;

import com.lufax.mis.utils.DateUtils;

/**
 * 曝光页的浏览量
 *
 * @author sherlock
 * @create 2019/4/24
 * @since 1.0.0
 */
public class PageViewCount {
    private String pageName;
    private long windowEnd;
    private long count;

    public PageViewCount(String pageName, long windowEnd, long count) {
        this.pageName = pageName;
        this.windowEnd = windowEnd;
        this.count = count;
    }

    public long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public String getPageName() {
        return pageName;
    }

    public void setPageName(String pageName) {
        this.pageName = pageName;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "PageViewCount{" +
                "pageName='" + pageName + '\'' +
                ", windowEnd=" + DateUtils.getTimestamp2DateStr(windowEnd) +
                ", count=" + count +
                '}';
    }
}
