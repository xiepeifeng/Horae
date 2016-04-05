package com.tencent.isd.lhotse.runner.util;

public class TdwPrivilegeGroup {
	private String userName;
	private String userGroup;
	private String tdwUser;
	private String tdwPasswd;
	private String tdwServerIp;
	private String tdwServerPort;

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getUserGroup() {
		return userGroup;
	}

	public void setUserGroup(String userGroup) {
		this.userGroup = userGroup;
	}

	public String getTdwUser() {
		return tdwUser;
	}

	public void setTdwUser(String tdwUser) {
		this.tdwUser = tdwUser;
	}

	public String getTdwPasswd() {
		return tdwPasswd;
	}

	public void setTdwPasswd(String tdwPasswd) {
		this.tdwPasswd = tdwPasswd;
	}

	public String getTdwServerIp() {
		return tdwServerIp;
	}

	public void setTdwServerIp(String tdwServerIp) {
		this.tdwServerIp = tdwServerIp;
	}

	public String getTdwServerPort() {
		return tdwServerPort;
	}

	public void setTdwServerPort(String tdwServerPort) {
		this.tdwServerPort = tdwServerPort;
	}
}
