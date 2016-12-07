package com.tscloud.esplugin.distinct;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;

/**
 *
 * Created by Administrator on 2016/1/4.
 */
public class DistinctBuilder extends ActionRequestBuilder<DistinctRequest,DistinctResponse,DistinctBuilder,Client> {

    public DistinctBuilder(Client client) {
        super( client, new DistinctRequest() );
    }

    public DistinctBuilder( Client client, DistinctAction action ) {
        super( client, new DistinctRequest() );
    }

    public DistinctBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    public DistinctBuilder setType(String... types) {
        request.types( types );
        return this;
    }

    public DistinctBuilder setField( String field ){
        request.setField( field );
        return this;
    }

    public DistinctBuilder setReg( String reg ){
        request.setReg( reg );
        return this;
    }

    /**
     * Specifies what type of requested indices to ignore and wildcard indices expressions.
     * <p>
     * For example indices that don't exist.
     */
    public DistinctBuilder setIndicesOptions(IndicesOptions options) {
        request.indicesOptions(options);
        return this;
    }

    /**
     * A comma separated list of routing values to control the shards the action will be executed on.
     */
    public DistinctBuilder setRouting(String routing) {
        request.routing(routing);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<DistinctResponse> listener) {
        client.execute( DistinctAction.INSTANCE, request, listener );
    }
}
