package lyd.ai.native4j.jdbc.data.type;

import lyd.ai.native4j.jdbc.misc.Validate;
import lyd.ai.native4j.jdbc.serializer.BinaryDeserializer;
import lyd.ai.native4j.jdbc.serializer.BinarySerializer;
import lyd.ai.native4j.jdbc.data.IDataType;
import lyd.ai.native4j.jdbc.stream.QuotedLexer;
import lyd.ai.native4j.jdbc.stream.QuotedToken;
import lyd.ai.native4j.jdbc.stream.QuotedTokenType;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Types;

public class DataTypeInt16 implements IDataType {

    private static final Short DEFAULT_VALUE = 0;
    private final String name;

    public DataTypeInt16(String name) {
        this.name = name;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public int sqlTypeId() {
        return Types.SMALLINT;
    }

    @Override
    public Object defaultValue() {
        return DEFAULT_VALUE;
    }

    @Override
    public void serializeBinary(Object data, BinarySerializer serializer) throws SQLException, IOException {
        Validate.isTrue(data instanceof Byte || data instanceof Short,
            "Expected Short Parameter, but was " + data.getClass().getSimpleName());

        serializer.writeShort(((Number) data).shortValue());
    }

    @Override
    public Object deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        return deserializer.readShort();
    }

    @Override
    public void serializeBinaryBulk(Object[] data, BinarySerializer serializer) throws SQLException, IOException {
        for (Object datum : data) {
            serializeBinary(datum, serializer);
        }
    }

    @Override
    public Object[] deserializeBinaryBulk(int rows, BinaryDeserializer deserializer) throws SQLException, IOException {
        Short[] data = new Short[rows];
        for (int row = 0; row < rows; row++) {
            data[row] = deserializer.readShort();
        }
        return data;
    }

    @Override
    public Object deserializeTextQuoted(QuotedLexer lexer) throws SQLException {
        QuotedToken token = lexer.next();
        Validate.isTrue(token.type() == QuotedTokenType.Number, "Expected Number Literal.");
        return Short.valueOf(token.data());
    }

}
