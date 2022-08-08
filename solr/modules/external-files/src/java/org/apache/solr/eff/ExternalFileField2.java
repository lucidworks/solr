package org.apache.solr.eff;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.SortField;
import org.apache.solr.common.SolrException;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaAware;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.QParser;
import org.apache.solr.uninverting.UninvertingReader;

import java.io.IOException;
import java.util.Map;

public class ExternalFileField2 extends FieldType implements SchemaAware {
    private String keyFieldName;
    private IndexSchema schema;
    private float defVal;

    @Override
    protected void init(IndexSchema schema, Map<String, String> args) {
        restrictProps(SORT_MISSING_FIRST | SORT_MISSING_LAST);
        keyFieldName = args.remove("keyField");
        String defValS = args.remove("defVal");
        defVal = defValS == null ? 0 : Float.parseFloat(defValS);
        this.schema = schema;
    }

    @Override
    public void write(TextResponseWriter writer, String name, IndexableField f) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public SortField getSortField(SchemaField field,boolean reverse) {
        FileFloatSource2 source = getFileFloatSource2(field);
        return source.getSortField(reverse);
    }

    @Override
    public UninvertingReader.Type getUninversionType(SchemaField sf) {
        return null;
    }

    @Override
    public ValueSource getValueSource(SchemaField field, QParser parser) {
        return getFileFloatSource2(field);
    }

    /**
     * Get a FileFloatSource for the given field, using the datadir from the
     * IndexSchema
     * @param field the field to get a source for
     * @return a FileFloatSource2
     */
    public FileFloatSource2 getFileFloatSource2(SchemaField field) {
        return getFileFloatSource2(field, SolrRequestInfo.getRequestInfo().getReq().getCore().getDataDir());
    }

    /**
     * Get a FileFloatSource2 for the given field.  Call this in preference to
     * getFileFloatSource(SchemaField) if this may be called before the Core is
     * fully initialised (eg in SolrEventListener calls).
     * @param field the field to get a source for
     * @param datadir the data directory in which to look for the external file
     * @return a FileFloatSource2
     */
    public FileFloatSource2 getFileFloatSource2(SchemaField field, String datadir) {
        return new FileFloatSource2(field, getKeyField(), defVal, datadir);
    }

    // If no key field is defined, we use the unique key field
    private SchemaField getKeyField() {
        return keyFieldName == null ?
                schema.getUniqueKeyField() :
                schema.getField(keyFieldName);
    }

    @Override
    public void inform(IndexSchema schema) {
        this.schema = schema;

        if (keyFieldName != null && schema.getFieldType(keyFieldName).isPointField()) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                    "keyField '" + keyFieldName + "' has a Point field type, which is not supported.");
        }
    }
}
