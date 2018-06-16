package lyd.ai.native4j.jdbc.data.type;

import lyd.ai.native4j.jdbc.data.IDataType;
import lyd.ai.native4j.jdbc.misc.Validate;
import lyd.ai.native4j.jdbc.serializer.BinaryDeserializer;
import lyd.ai.native4j.jdbc.serializer.BinarySerializer;
import lyd.ai.native4j.jdbc.stream.QuotedLexer;
import lyd.ai.native4j.jdbc.stream.QuotedToken;
import lyd.ai.native4j.jdbc.stream.QuotedTokenType;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Types;

public class DataTypeInt8 implements IDataType {

    private static final Byte DEFAULT_VALUE = 0;
    private final String name;

    public DataTypeInt8(String name) {
        this.name = name;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public int sqlTypeId() {
        return Types.TINYINT;
    }

    @Override
    public Object defaultValue() {
        return DEFAULT_VALUE;
    }

    @Override
    public void serializeBinary(Object data, BinarySerializer serializer) throws SQLException, IOException {
        Validate.isTrue(data instanceof Byte, "Expected Byte Parameter, but was " + data.getClass().getSimpleName());
        serializer.writeByte((Byte) data);
    }

    @Override
    public Byte deserializeBinary(BinaryDeserializer deserializer) throws IOException {
        return deserializer.readByte();
    }

    @Override
    public void serializeBinaryBulk(Object[] data, BinarySerializer serializer) throws SQLException, IOException {
        for (Object datum : data) {
            serializeBinary(datum, serializer);
        }
    }

    @Override
    public Byte[] deserializeBinaryBulk(int rows, BinaryDeserializer deserializer) throws IOException {
        Byte[] data = new Byte[rows];
        for (int row = 0; row < rows; row++) {
            data[row] = deserializer.readByte();
        }
        return data;
    }

    @Override
    public Object deserializeTextQuoted(QuotedLexer lexer) throws SQLException {
        QuotedToken token = lexer.next();
        Validate.isTrue(token.type() == QuotedTokenType.Number, "Expected Number Literal.");
        return Byte.valueOf(token.data());
    }
}
