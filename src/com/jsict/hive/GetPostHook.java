package com.jsict.hive;

import java.io.Serializable;
import java.sql.*;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.AbstractSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.exec.Task;



public class GetPostHook extends AbstractSemanticAnalyzerHook {
	@Override
	public void postAnalyze(HiveSemanticAnalyzerHookContext context,
		      List<Task<? extends Serializable>> rootTasks) throws SemanticException {
		Hive hive = null;
		try {
			hive = context.getHive();
		} catch (HiveException e){
			e.printStackTrace();
			
			throw new RuntimeException(e);
		}
		Set<ReadEntity> inputs = context.getInputs();
		Set<WriteEntity> outputs = context.getOutputs();
		
		Set<String> readTables = new HashSet<String>();
		for(ReadEntity input : inputs) {
			Table table = input.getT();
			if (table != null) {
				if (!table.getDbName().equals("default") ) {
					readTables.add(table.getDbName() + "." + table.getTableName());
				}
			}
		}
		
//		Set<String> writeTables = new HashSet<String>();
//		for(WriteEntity output : outputs) {
//			Table table = output.getT();
//			if (table != null) {
//				writeTables.add(table.getDbName() + "." + table.getTableName());
//			}
//		}

		String userName = SessionState.get().getAuthenticator().getUserName();
		
		if (readTables != null && ! readTables.isEmpty()) {
			Properties allpro = SessionState.getSessionConf().getAllProperties();
			String jdbcURL = allpro.getProperty("javax.jdo.option.ConnectionURL");
			String jdbcUSN = allpro.getProperty("javax.jdo.option.ConnectionUserName");
//			String jdbcPSW = allpro.getProperty("javax.jdo.option.ConnectionPassword");
			String jdbcPSW = "hive";
			String jdbcDRV = allpro.getProperty("javax.jdo.option.ConnectionDriverName");
			
			Connection conn = null;
			Statement stmt = null;
			try {
				Class.forName(jdbcDRV);
				conn = DriverManager.getConnection(jdbcURL, jdbcUSN, jdbcPSW);
				stmt = conn.createStatement();
				String sql;
				for (String str : readTables) {
					sql = "SELECT COUNT(*) AS CNT FROM TBLS T1, TBL_PRIVS T2, DBS T3 WHERE T1.TBL_ID = T2.TBL_ID AND T1.DB_ID = T3.DB_ID AND "
							+ "T3.NAME = \"" + str.split("\\.")[0] + "\" AND T1.TBL_NAME = \"" + str.split("\\.")[1] + "\" AND T3.OWNER_TYPE = \"USER\""
							+ " AND T2.PRINCIPAL_TYPE = \"USER\" AND T2.TBL_PRIV = \"SELECT\" AND T2.PRINCIPAL_NAME = \"" + userName + "\"";
					ResultSet rs = stmt.executeQuery(sql);
					while (rs.next()) {
						int privCount = rs.getInt("CNT");
						if (privCount != 1) {
							throw new SemanticException("No privilege 'Select' found for outputs { table:" +  str.split("\\.")[1] + "}");
						}
					}
				}
				stmt.close();
				conn.close();
			}catch(SQLException se) {
				se.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}finally {
				try {
					if (stmt != null) stmt.close();
				}catch(SQLException se2){
				}
				try {
					if (conn != null) conn.close();
				}catch(SQLException se){
					se.printStackTrace();
				}
			}
		}
	}
}

