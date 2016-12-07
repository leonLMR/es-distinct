package com.tscloud.esplugin.distinct;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * Created by Administrator on 2016/1/4.
 */
public class DistinctTransportAction extends HandledTransportAction<DistinctRequest, DistinctResponse> {

    private final Client client;
    private final TransportSearchAction searchAction;
    private final TransportSearchScrollAction scrollAction;

    @Inject
    protected DistinctTransportAction(Settings settings, ThreadPool threadPool, TransportService transportService,
                                      ActionFilters actionFilters,Client client,TransportSearchAction searchAction,
                                      TransportSearchScrollAction scrollAction) {
        super(settings, DistinctAction.NAME , threadPool, transportService, actionFilters);
        this.client = client;
        this.searchAction = searchAction;
        this.scrollAction = scrollAction;
    }

    @Override
    public DistinctRequest newRequestInstance() {
        return new DistinctRequest();
    }

    @Override
    protected void doExecute(final DistinctRequest request, final ActionListener<DistinctResponse> listener) {
        try{
            Pattern pattern = Pattern.compile( request.getReg() );
            List<String> list = new ArrayList<String>();
            SearchResponse scrollResp = client.prepareSearch( request.indices() )
                    .setTypes( request.types() )
                    .setSearchType(SearchType.SCAN)
                    .setScroll(new TimeValue(600000))
                    .setQuery(QueryBuilders.regexpQuery(request.getField(), request.getReg()))
                    .setSize(1000).addField( request.getField() ).execute().actionGet();
            boolean flag = true;
            while ( flag ) {
                scrollResp = client.prepareSearchScroll(scrollResp.getScrollId())
                        .setScroll( new TimeValue(600000) ).execute().actionGet();
                SearchHit[] searchHits = scrollResp.getHits().hits();
                long len = searchHits.length;
                if ( len == 0 ){
                    flag = false;
                }
                for( SearchHit sh : searchHits ){
                    String value = sh.getFields().get( request.getField() ).getValue();
                    if ( value != null ){
                        Matcher m = pattern.matcher(value);
                        if ( m.find() ){
                            String p = m.group( 1 );
                            if ( !list.contains( p ) ){
                                list.add( p );
                            }
                        }
                    }
                }
            }
            listener.onResponse(new DistinctResponse(list));
        } catch ( Exception e ){
            listener.onFailure( e );
        }


        /*SearchRequest scanRequest = new SearchRequest(request)
                .indices(request.indices())
                .types(request.types())
                .searchType( SearchType.SCAN )
                .scroll(new TimeValue(60000));
        SearchSourceBuilder ssb = new SearchSourceBuilder().size( 100 ).query(QueryBuilders.regexpQuery(request.getField(), request.getReg())).field(request.getField());
        scanRequest.source( ssb );
        final Pattern pattern = Pattern.compile( request.getReg() );
        final List<String> list = new ArrayList<String>();
        searchAction.execute( scanRequest, new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
                final String scrollId = searchResponse.getScrollId();
                final boolean[] flag = {true};
                while( flag[0] ){
                    scrollAction.execute( new SearchScrollRequest( scrollId ).scroll( new TimeValue( 60000 ) ), new ActionListener<SearchResponse>() {
                        @Override
                        public void onResponse(SearchResponse scrollResponse) {
                            SearchHit[] searchHits = scrollResponse.getHits().hits();
                            long len = searchHits.length;
                            if ( len == 0 ){
                                flag[0] = false;
                            }
                            for( SearchHit sh : searchHits ){
                                String value = sh.getFields().get( request.getField() ).getValue();
                                if ( value != null ){
                                    Matcher m = pattern.matcher(value);
                                    if ( m.find() ){
                                        String p = m.group( 1 );
                                        if ( !list.contains( p ) ){
                                            list.add( p );
                                        }
                                    }
                                }
                            }
                            listener.onResponse(new DistinctResponse(list));
                        }
                        @Override
                        public void onFailure(Throwable e) {
                            listener.onFailure( e );
                        }
                    });
                }
            }
            @Override
            public void onFailure(Throwable e) {
                listener.onFailure(e);
            }
        });*/
    }
}
