package com.jsict.hive;

import javax.security.sasl.AuthenticationException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.auth.PasswdAuthenticationProvider;

public class CustomHiveServer2Auth implements PasswdAuthenticationProvider {	
	private static final Log LOG = LogFactory.getLog(CustomHiveServer2Auth.class);
	private static final String HIVE_SERVER2_CUSTOM_AUTH_PREFIX = "hive.server2.custom.auth.%s";
	
	@Override
	public void Authenticate(String username, String password)
			throws AuthenticationException {
		HiveConf hiveConf = new HiveConf();
		Configuration conf = new Configuration(hiveConf);
		String passwd = conf.get( String.format(HIVE_SERVER2_CUSTOM_AUTH_PREFIX, username) );
		if ( passwd == null || !passwd.equals(password)){
			throw new AuthenticationException("username or password is wrong!");
		}
		
	}







}
