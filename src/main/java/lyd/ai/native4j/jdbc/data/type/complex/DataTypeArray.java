package lyd.ai.native4j.jdbc.data.type.complex;

import lyd.ai.native4j.jdbc.ClickHouseArray;
import lyd.ai.native4j.jdbc.data.DataTypeFactory;
import lyd.ai.native4j.jdbc.misc.Validate;
import lyd.ai.native4j.jdbc.data.IDataType;
import lyd.ai.native4j.jdbc.serializer.BinaryDeserializer;
import lyd.ai.native4j.jdbc.serializer.BinarySerializer;
import lyd.ai.native4j.jdbc.stream.QuotedLexer;
import lyd.ai.native4j.jdbc.stream.QuotedToken;
import lyd.ai.native4j.jdbc.stream.QuotedTokenType;

import java.io.IOException;
import java.sql.Array;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

public class DataTypeArray implements IDataType {
    private final String name;
    private final Array defaultValue;
    private final IDataType elemDataType;
    private final IDataType offsetIDataType;

    public DataTypeArray(String name, IDataType elemDataType, IDataType offsetIDataType) throws SQLException {
        this.name = name;
        this.elemDataType = elemDataType;
        this.offsetIDataType = offsetIDataType;
        this.defaultValue = new ClickHouseArray(new Object[] {elemDataType.defaultValue()});
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public int sqlTypeId() {
        return Types.ARRAY;
    }

    @Override
    public Object defaultValue() {
        return defaultValue;
    }

    @Override
    public Object deserializeTextQuoted(QuotedLexer lexer) throws SQLException {
        QuotedToken token = lexer.next();
        Validate.isTrue(token.type() == QuotedTokenType.OpeningSquareBracket);

        List<Object> elems = new ArrayList<Object>();
        for (; ; ) {
            elems.add(elemDataType.deserializeTextQuoted(lexer));
            token = lexer.next();
            Validate.isTrue(
                token.type() == QuotedTokenType.Comma || token.type() == QuotedTokenType.ClosingSquareBracket);

            if (token.type() == QuotedTokenType.ClosingSquareBracket) {
                return new ClickHouseArray(elems.toArray());
            }
        }
    }

    @Override
    public void serializeBinary(Object data, BinarySerializer serializer) throws SQLException, IOException {
        Validate.isTrue(data instanceof Array, "Expected Array Parameter, but was " + data.getClass().getSimpleName());

        offsetIDataType.serializeBinary(((Object[]) ((Array) data).getArray()).length, serializer);
        elemDataType.serializeBinaryBulk(((Object[]) ((Array) data).getArray()), serializer);
    }

    @Override
    public Object deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        Long offset = (Long) offsetIDataType.deserializeBinary(deserializer);
        return elemDataType.deserializeBinaryBulk(offset.intValue(), deserializer);
    }

    @Override
    public void serializeBinaryBulk(Object[] data, BinarySerializer serializer) throws SQLException, IOException {

        for (Object datum : data) {
            Validate.isTrue(datum instanceof Array,
                "Expected Array Parameter, but was " + datum.getClass().getSimpleName());

            Object[] arrayData = (Object[]) ((Array) datum).getArray();
            offsetIDataType.serializeBinary(arrayData.length, serializer);
        }

        for (Object datum : data) {
            Validate.isTrue(datum instanceof Array,
                "Expected Array Parameter, but was " + datum.getClass().getSimpleName());

            Object[] arrayData = (Object[]) ((Array) datum).getArray();
            elemDataType.serializeBinaryBulk(arrayData, serializer);
        }
    }

    @Override
    public Object[] deserializeBinaryBulk(int rows, BinaryDeserializer deserializer) throws IOException, SQLException {
        ClickHouseArray[] data = new ClickHouseArray[rows];

        Object[] offsets = offsetIDataType.deserializeBinaryBulk(rows, deserializer);
        for (int row = 0, lastOffset = 0; row < rows; row++) {
            Long offset = (Long) offsets[row];
            data[row] = new ClickHouseArray(
                elemDataType.deserializeBinaryBulk(offset.intValue() - lastOffset, deserializer));
            lastOffset = offset.intValue();
        }
        return data;
    }

    public static IDataType createArrayType(QuotedLexer lexer, TimeZone serverZone) throws SQLException {
        Validate.isTrue(lexer.next().type() == QuotedTokenType.OpeningRoundBracket);
        IDataType arrayNestedType = DataTypeFactory.get(lexer, serverZone);
        Validate.isTrue(lexer.next().type() == QuotedTokenType.ClosingRoundBracket);
        return new DataTypeArray("Array(" + arrayNestedType.name() + ")",
            arrayNestedType, DataTypeFactory.get("UInt64", serverZone));
    }
}
