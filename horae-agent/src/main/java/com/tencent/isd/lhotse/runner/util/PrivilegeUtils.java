package com.tencent.isd.lhotse.runner.util;

import com.tencent.isd.lhotse.runner.AbstractTaskRunner;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class PrivilegeUtils {

	/**
	 * @param args
	 */
	private final static String QUERY_GROUP_HIVESERVER = "select * from tdw.appusergroup where lower(appusergroupname)=?";
	private final static String QUERY_TDW_USER = "select * from tdw.tdwuser where lower(user_name)=?";

	
	public static void init(String pgURL, String pgUser, String pgPasswd, int minPool, int maxPool, int maxSize, int idleTime, AbstractTaskRunner runner){
		String totalURL = pgURL + "?useUnicode=true&characterEncoding=utf-8";
		DBPool.init(totalURL, pgUser,pgPasswd,minPool,maxPool,maxSize,idleTime, runner);
	}
	public static TdwPrivilegeGroup getPrivilegeGroup(String userName, String groupName) throws Exception {
		TdwPrivilegeGroup group = null;
		Connection conn = null;
		PreparedStatement ps = null;
		try {
			conn = DBPool.getBIConnection();
			if(conn == null){
				throw new Exception("pg db pool is full now, retry later");
			}
			ps = conn.prepareStatement(QUERY_GROUP_HIVESERVER);
			String userPassword = null;
			String tdwServer = null;
			String tdwPort = null;
			ps.setString(1, groupName.toLowerCase());
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				String hiveServer = rs.getString("hiveserver");
				if(hiveServer != null){
					String[] serverTuple = hiveServer.split(":");
					if(serverTuple.length >= 2){
						tdwServer = serverTuple[0].trim();
						tdwPort = serverTuple[1].trim();
					}else{
						throw new Exception("format error in appusergroup:hiveserver should be server:port");
					}
				}
			}
			ps = conn.prepareStatement(QUERY_TDW_USER);
			ps.setString(1, "tdw_" + userName.toLowerCase());
			rs = ps.executeQuery();
			while(rs.next()){
				userPassword = rs.getString("passwd");
			}
			if(userPassword == null || StringUtils.isEmpty(userPassword)){
				throw new Exception("user password should not be empty");
			}
			if(tdwServer != null && tdwPort != null && userPassword!= null){
				group = new TdwPrivilegeGroup();
				group.setUserGroup(groupName);
				group.setUserName(userName);
				group.setTdwUser("tdw_" + userName);
				group.setTdwPasswd(userPassword);
				group.setTdwServerIp(tdwServer);
				group.setTdwServerPort(tdwPort);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
            throw new Exception(e.getMessage());
		}
		finally {
			try {
				if (ps != null) {
					ps.close();
				}
				if (conn != null) {
					conn.close();
				}
			}
			catch (Exception e) {

			}
		}
		return group;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
