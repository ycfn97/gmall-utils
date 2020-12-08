package util;

import org.apache.flink.streaming.connectors.elasticsearch6.RestClientFactory;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClientBuilder;

/**
 * className:RestClientFactoryImpl
 * description:
 * author:ChenYajun
 * date:2019-07-10 15:46
 */
public class RestClientFactoryImpl implements RestClientFactory {
    @Override
    public void configureRestClientBuilder(RestClientBuilder restClientBuilder) {
        Header[] headers = new BasicHeader[]{new BasicHeader("Content-Type","application/json")};
        restClientBuilder.setDefaultHeaders(headers); //以数组的形式可以添加多个header
        restClientBuilder.setMaxRetryTimeoutMillis(90000);
    }
}
