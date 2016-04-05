/**
 * @author  cpwang
 * @since 2009-3-1
 */
package com.tencent.isd.lhotse.dao;


/**
 * @author cpwang
 * @since 2012-3-23
 * @modified by cpwang,20120724. Add param1
 */
public class Command implements Comparable<Command> {
    private int                id;
    private String             name;
    private String             taskId;
    private java.util.Date     dateFrom;
    private java.util.Date     dateTo;
    private int                state;
    private String             comment;
    private String             setter;
    private java.sql.Timestamp setTime;
    private String             param1;
    private java.sql.Timestamp startTime;
    private java.sql.Timestamp endTime;


    public Command() {
    }


    public Command(int id) {
        this.id = id;
    }


    public String getName() {
        return name;
    }


    public void setName(String name) {
        this.name = name;
    }


    public String getComment() {
        return comment;
    }


    public void setComment(String comment) {
        this.comment = comment;
    }


    public String getSetter() {
        return setter;
    }


    public void setSetter(String setter) {
        this.setter = setter;
    }


    public String getTaskId() {
        return taskId;
    }


    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }


    public String getParam1() {
        return param1;
    }


    public void setParam1(String param1) {
        this.param1 = param1;
    }


    public int getId() {
        return id;
    }


    public void setId(int id) {
        this.id = id;
    }


    /*
     * (non-Javadoc)
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(Command cmd) {
        if (this.setTime == null) {
            return 1;
        }

        if (cmd.setTime == null) {
            return -1;
        }

        return this.setTime.compareTo(cmd.getSetTime());
    }


    public java.sql.Timestamp getSetTime() {
        return setTime;
    }


    public void setSetTime(java.sql.Timestamp setTime) {
        this.setTime = setTime;
    }


    public java.sql.Timestamp getStartTime() {
        return startTime;
    }


    public void setStartTime(java.sql.Timestamp startTime) {
        this.startTime = startTime;
    }


    public java.sql.Timestamp getEndTime() {
        return endTime;
    }


    public void setEndTime(java.sql.Timestamp endTime) {
        this.endTime = endTime;
    }


    public int getState() {
        return state;
    }


    public void setState(int state) {
        this.state = state;
    }


    public java.util.Date getDateFrom() {
        return dateFrom;
    }


    public void setDateFrom(java.util.Date dateFrom) {
        this.dateFrom = dateFrom;
    }


    public java.util.Date getDateTo() {
        return dateTo;
    }


    public void setDateTo(java.util.Date dateTo) {
        this.dateTo = dateTo;
    }

}
