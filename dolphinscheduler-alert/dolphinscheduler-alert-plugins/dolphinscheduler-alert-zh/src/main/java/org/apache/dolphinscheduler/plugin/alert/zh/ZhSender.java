/*
 * Aloudata.com Inc.
 * Copyright (c) 2021-2021 All Rights Reserved.
 */

package org.apache.dolphinscheduler.plugin.alert.zh;

import org.apache.dolphinscheduler.alert.api.AlertResult;

import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

/**
 * @author eye.gu@aloudata.com
 * @version 1
 * @date 2021-12-15 21:02
 */
public class ZhSender {

    private String url;

    private static final CloseableHttpClient httpClient = HttpClientBuilder.create().build();

    public ZhSender(String url) {
        this.url = url;
    }

    public AlertResult send(String content) {
        if (content.length() > 2000) {
            content = content.substring(0, 2000);
        }

        try {
            RequestConfig requestConfig = RequestConfig.custom()
                .setSocketTimeout(3000)
                .setConnectTimeout(3000).build();
            HttpGet get = new HttpGet(url + "/api/sendZHGroup?msg=" + URLEncoder.encode(content, StandardCharsets.UTF_8.name()));
            get.setConfig(requestConfig);
            CloseableHttpResponse response = httpClient.execute(get);
            HttpEntity entity = response.getEntity();
            String resp = EntityUtils.toString(entity, "utf-8");
            response.close();
            AlertResult alertResult = new AlertResult();
            alertResult.setStatus("true");
            alertResult.setMessage(resp);
            return alertResult;
        } catch (Exception e) {
            AlertResult alertResult = new AlertResult();
            alertResult.setStatus("false");
            alertResult.setMessage(e.getMessage());
            return alertResult;
        }
    }
}