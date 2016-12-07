package com.tscloud.esplugin.distinct;

import org.elasticsearch.action.ClientAction;
import org.elasticsearch.client.Client;

/**
 *
 * Created by Administrator on 2016/1/4.
 */
public class DistinctAction extends ClientAction<DistinctRequest,DistinctResponse,DistinctBuilder> {
    public static final DistinctAction INSTANCE = new DistinctAction();
    public static final String NAME = "indices:data/read/distinct";

    protected DistinctAction() {
        super(NAME);
    }

    @Override
    public DistinctBuilder newRequestBuilder(Client client) {
        return new DistinctBuilder( client);
    }

    @Override
    public DistinctResponse newResponse() {
        return new DistinctResponse();
    }


}
