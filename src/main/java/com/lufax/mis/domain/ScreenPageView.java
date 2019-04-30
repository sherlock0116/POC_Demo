package com.lufax.mis.domain;

import java.sql.Timestamp;

/**
 * 源数据的JavaBean
 *
 * @author sherlock
 * @create 2019/4/28
 * @since 1.0.0
 */
public class ScreenPageView {

    private String userName;
    private String pageName;
    private String pointType;
    private int click;
    private String title;
    private long userActionTime;

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPageName() {
        return pageName;
    }

    public void setPageName(String pageName) {
        this.pageName = pageName;
    }

    public String getPointType() {
        return pointType;
    }

    public void setPointType(String pointType) {
        this.pointType = pointType;
    }

    public int getClick() {
        return click;
    }

    public void setClick(int click) {
        this.click = click;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public long getUserActionTime() {
        return userActionTime;
    }

    public void setUserActionTime(long userActionTime) {
        this.userActionTime = userActionTime;
    }

    @Override
    public String toString() {
        return "ScreenPageView{" +
                "userName='" + userName + '\'' +
                ", pageName='" + pageName + '\'' +
                ", pointType='" + pointType + '\'' +
                ", click=" + click +
                ", title='" + title + '\'' +
                ", userActionTime=" + userActionTime +
                '}';
    }
}
