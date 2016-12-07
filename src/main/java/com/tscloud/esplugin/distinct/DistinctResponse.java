package com.tscloud.esplugin.distinct;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 *
 * Created by Administrator on 2016/1/4.
 */
public class DistinctResponse extends ActionResponse implements ToXContent {
    private List<String> list;

    public DistinctResponse(){

    }

    public DistinctResponse( List<String> list ){
        this.list = list;
    }

    static final class Fields {
        static final XContentBuilderString RESULT_LIST = new XContentBuilderString("result_list");
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        list = Arrays.asList(in.readStringArray());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray( list.toArray( new String[] {} ) );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        /*builder.startArray( Fields.RESULT_LIST );
        if ( list != null ){
            for ( String str : list ){
                builder.value( str );
            }
        }
        builder.endObject();*/
        builder.field( Fields.RESULT_LIST, list );
        return builder;
    }

    public List<String> getList() {
        return list;
    }

    public void setList(List<String> list) {
        this.list = list;
    }

}
