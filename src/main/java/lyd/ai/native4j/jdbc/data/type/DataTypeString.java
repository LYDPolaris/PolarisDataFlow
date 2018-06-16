package lyd.ai.native4j.jdbc.data.type;

import lyd.ai.native4j.jdbc.data.IDataType;
import lyd.ai.native4j.jdbc.misc.Validate;
import lyd.ai.native4j.jdbc.stream.QuotedLexer;
import lyd.ai.native4j.jdbc.stream.QuotedTokenType;
import lyd.ai.native4j.jdbc.serializer.BinaryDeserializer;
import lyd.ai.native4j.jdbc.serializer.BinarySerializer;
import lyd.ai.native4j.jdbc.stream.QuotedToken;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Types;

public class DataTypeString implements IDataType {

    @Override
    public String name() {
        return "String";
    }

    @Override
    public int sqlTypeId() {
        return Types.VARCHAR;
    }

    @Override
    public Object defaultValue() {
        return "";
    }

    @Override
    public void serializeBinary(Object data, BinarySerializer serializer) throws SQLException, IOException {
        Validate.isTrue(data instanceof String, "Expected String Parameter, but was " + data.getClass().getSimpleName());

        serializer.writeStringBinary((String) data);
    }

    @Override
    public Object deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        return deserializer.readStringBinary();
    }

    @Override
    public void serializeBinaryBulk(Object[] data, BinarySerializer serializer) throws SQLException, IOException {
        for (Object datum : data) {
            serializeBinary(datum, serializer);
        }
    }

    @Override
    public Object[] deserializeBinaryBulk(int rows, BinaryDeserializer deserializer)
        throws SQLException, IOException {
        String[] data = new String[rows];
        for (int row = 0; row < rows; row++) {
            data[row] = deserializer.readStringBinary();
        }
        return data;
    }

    @Override
    public Object deserializeTextQuoted(QuotedLexer lexer) throws SQLException {
        QuotedToken token = lexer.next();
        Validate.isTrue(token.type() == QuotedTokenType.StringLiteral, "Expected String Literal.");
        return token.data();
    }

}
