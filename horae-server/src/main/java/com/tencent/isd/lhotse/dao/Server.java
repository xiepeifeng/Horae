package com.tencent.isd.lhotse.dao;

import java.util.ArrayList;

import com.tencent.isd.lhotse.proto.LhotseObject.LServer;

/**
 * @author: cpwang
 * @date: 2012-11-15
 */
public class Server {
	private String tag;
	private String type;
	private String host;
	private int port;
	private String service;
	private String userName;
	private String password;
	private String userGroup;
	private String version;
	private int parallelism;
	private int specialReserve;
	private int specialPriority;
	private boolean isGroup;
	private ArrayList<LServer> servers;

	public Server() {
		this.servers = new ArrayList<LServer>();
	}

	public String getTag() {
		return tag;
	}

	public void setTag(String tag) {
		this.tag = tag;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getService() {
		return service;
	}

	public void setService(String service) {
		this.service = service;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getUserGroup() {
		return userGroup;
	}

	public void setUserGroup(String userGroup) {
		this.userGroup = userGroup;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	public int getSpecialReserve() {
		return specialReserve;
	}

	public void setSpecialReserve(int specialReserve) {
		this.specialReserve = specialReserve;
	}

	public void buildServer() {
		if (!isGroup) {
			LServer.Builder lsb = LServer.newBuilder();
			lsb.setTag(tag.trim());
			lsb.setType(type);
			lsb.setHost(host == null ? "" : host);
			lsb.setPort(port);
			lsb.setUserName(userName == null ? "" : userName);
			lsb.setPassword(password == null ? "" : password);
			lsb.setService(service == null ? "" : service);
			lsb.setUserGroup(userGroup == null ? "" : userGroup);
			lsb.setVersion(version == null ? "" : version);
			this.servers.clear();
			this.servers.add(lsb.build());
		}
	}

	public boolean isGroup() {
		return isGroup;
	}

	public void setGroup(boolean isGroup) {
		this.isGroup = isGroup;
	}

	public ArrayList<LServer> getServers() {
		return servers;
	}

	public int getParallelism() {
		return parallelism;
	}

	public void setParallelism(int parallelism) {
		this.parallelism = parallelism;
	}

	public int getSpecialPriority() {
		return specialPriority;
	}

	public void setSpecialPriority(int specialPriority) {
		this.specialPriority = specialPriority;
	}
}
