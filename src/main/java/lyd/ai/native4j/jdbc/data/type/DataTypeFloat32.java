package lyd.ai.native4j.jdbc.data.type;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Types;

import lyd.ai.native4j.jdbc.data.IDataType;
import lyd.ai.native4j.jdbc.misc.Validate;
import lyd.ai.native4j.jdbc.stream.QuotedLexer;
import lyd.ai.native4j.jdbc.stream.QuotedTokenType;
import lyd.ai.native4j.jdbc.serializer.BinaryDeserializer;
import lyd.ai.native4j.jdbc.serializer.BinarySerializer;
import lyd.ai.native4j.jdbc.stream.QuotedToken;

public class DataTypeFloat32 implements IDataType {

    private static final Float DEFAULT_VALUE = 0.0F;

    @Override
    public String name() {
        return "Float32";
    }

    @Override
    public int sqlTypeId() {
        return Types.FLOAT;
    }

    @Override
    public Object defaultValue() {
        return DEFAULT_VALUE;
    }

    @Override
    public void serializeBinary(Object data, BinarySerializer serializer) throws SQLException, IOException {
        Validate.isTrue(data instanceof Float, "Expected Float Parameter, but was " + data.getClass().getSimpleName());

        serializer.writeFloat((Float) data);
    }

    @Override
    public Object deserializeBinary(BinaryDeserializer deserializer) throws IOException {
        return deserializer.readFloat();
    }

    @Override
    public void serializeBinaryBulk(Object[] data, BinarySerializer serializer) throws SQLException, IOException {
        for (Object datum : data) {
            serializeBinary(datum, serializer);
        }
    }

    @Override
    public Object[] deserializeBinaryBulk(int rows, BinaryDeserializer deserializer) throws IOException {
        Float[] data = new Float[rows];
        for (int row = 0; row < rows; row++) {
            data[row] = deserializer.readFloat();
        }
        return data;
    }

    @Override
    public Object deserializeTextQuoted(QuotedLexer lexer) throws SQLException {
        QuotedToken token = lexer.next();
        Validate.isTrue(token.type() == QuotedTokenType.Number, "Expected Number Literal.");
        return Float.valueOf(token.data());
    }

}
