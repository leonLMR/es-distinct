package com.tscloud.esplugin.distinct;

import org.elasticsearch.action.ActionModule;
import org.elasticsearch.plugins.AbstractPlugin;

/**
 * Created by Administrator on 2016/1/4.
 */
public class DistinctPlugin extends AbstractPlugin {

    @Override
    public String name() {
        return "distinct";
    }

    @Override
    public String description() {
        return "distinct";
    }

    public void onModule(ActionModule actionModule) {
        actionModule.registerAction( DistinctAction.INSTANCE, DistinctTransportAction.class );
    }

}
