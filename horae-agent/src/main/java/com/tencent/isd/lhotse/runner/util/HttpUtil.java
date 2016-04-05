package com.tencent.isd.lhotse.runner.util;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class HttpUtil {
	public static String doGet(String url) throws ClientProtocolException, IOException {
		DefaultHttpClient httpClient = new DefaultHttpClient();
		HttpGet httpGet = new HttpGet(url);
		HttpResponse response = httpClient.execute(httpGet);
		HttpEntity entity = response.getEntity();

		if (entity != null) {
			BufferedReader reader = null;
			try {
				reader = new BufferedReader(new InputStreamReader(entity.getContent(), "UTF-8"));
				String retVal = reader.readLine();
				return retVal;
			}
			finally {
				if (reader != null) {
					reader.close();
				}
			}
		}
		return null;
	}

}
