package com.tscloud;

import com.tscloud.esplugin.distinct.DistinctBuilder;
import com.tscloud.esplugin.distinct.DistinctResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.Iterator;
import java.util.Map;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 *
 * Created by Administrator on 2016/1/4.
 */
public class DistinctTest {
    private static XContentBuilder buildDoc( Map<String, Object> doc ) throws Exception{
        XContentBuilder xContentBuilder = jsonBuilder().startObject();
        Iterator iterator = doc.entrySet().iterator();
        while(iterator.hasNext()){
            Map.Entry  entry=(Map.Entry)iterator.next();
            Object key=entry.getKey();
            Object value=entry.getValue();
            xContentBuilder.field((String)entry.getKey(),entry.getValue());
        }
        xContentBuilder.endObject();

        return  xContentBuilder;
    }

    public static final void main( String args[] ) throws Exception {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("CloudIndexClientConfig.CLIENT_TRANSPORT_SNIFF", true)
                .put("cluster.name", "truecloud_db_development")
                .build();
        TransportClient client =new TransportClient(settings) ;
        InetSocketTransportAddress host = new InetSocketTransportAddress( "192.168.10.61", 9300 );
        client.addTransportAddress( host );

        //System.out.print( client.prepareSearch("test").setTypes("test").setQuery(QueryBuilders.matchAllQuery()).execute().actionGet() );
        DistinctResponse response = new DistinctBuilder( client ).setIndices("tfs_s").setType("tfs_s").setField("path").setReg("/abc/([^/]*)/*.*").get();
        for ( String str : response.getList() ){
            System.out.println( str );
        }

    }


}
