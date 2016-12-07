package com.tscloud.esplugin.distinct;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;

/**
 *
 * Created by Administrator on 2016/1/4.
 */
public class DistinctRequest extends ActionRequest<DistinctRequest> implements IndicesRequest.Replaceable {

    private String[] indices;
    private String[] types = Strings.EMPTY_ARRAY;
    private String field;
    private String reg;

    @Nullable
    private String routing;

    public static final IndicesOptions DEFAULT_INDICES_OPTIONS = IndicesOptions.strictExpandOpenAndForbidClosed();

    private IndicesOptions indicesOptions = DEFAULT_INDICES_OPTIONS;

    @Override
    public ActionRequestValidationException validate() {
        if ( field == null || reg == null ){
            return new ActionRequestValidationException();
        }
        return null;
    }

    @Override
    public IndicesRequest indices(String[] indices) {
        if (indices == null) {
            throw new ElasticsearchIllegalArgumentException("indices must not be null");
        } else {
            for (int i = 0; i < indices.length; i++) {
                if (indices[i] == null) {
                    throw new ElasticsearchIllegalArgumentException("indices[" + i + "] must not be null");
                }
            }
        }
        this.indices = indices;
        return this;
    }

    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    public DistinctRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    public String routing() {
        return this.routing;
    }

    public DistinctRequest routing(String routing) {
        this.routing = routing;
        return this;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        indices = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
        types = in.readStringArray();
        //byte[] filedBytes = new byte[ in.readInt() ];
        //in.read( filedBytes );
        field = in.readString();
        reg = in.readString();
        routing = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(indices);
        indicesOptions.writeIndicesOptions(out);
        out.writeStringArray(types);
        out.writeString(field);
        out.writeString( reg );
        out.writeOptionalString(routing);

    }

    @Override
    public String toString() {
        return "distinct search indices:" + Arrays.toString(indices) +
                ", types:" + Arrays.toString(types) +
                ", filed:" + field +
                ", reg:" + reg +
                ", routing:" + routing;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getReg() {
        return reg;
    }

    public void setReg(String reg) {
        this.reg = reg;
    }

    public String[] types() {
        return this.types;
    }

    public DistinctRequest types(String... types) {
        this.types = types;
        return this;
    }

}
