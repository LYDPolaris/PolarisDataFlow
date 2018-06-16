package lyd.ai.native4j.jdbc.data.type.complex;

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
import java.util.TimeZone;

public class DataTypeFixedString implements IDataType {

    private final int n;
    private final String name;
    private final String defaultValue;

    public DataTypeFixedString(String name, int n) {
        this.n = n;
        this.name = name;

        char[] data = new char[n];
        for (int i = 0; i < n; i++) {
            data[i] = '\u0000';
        }
        this.defaultValue = new String(data);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public int sqlTypeId() {
        return Types.VARCHAR;
    }

    @Override
    public Object defaultValue() {
        return defaultValue;
    }

    @Override
    public Object deserializeTextQuoted(QuotedLexer lexer) throws SQLException {
        QuotedToken token = lexer.next();
        Validate.isTrue(token.type() == QuotedTokenType.StringLiteral, "Expected String Literal.");
        return token.data();
    }

    @Override
    public void serializeBinary(Object data, BinarySerializer serializer) throws SQLException, IOException {
        Validate.isTrue(data instanceof String, "Expected String Parameter, but was " + data.getClass().getSimpleName());
        serializer.writeBytes(((String) data).getBytes());
    }

    @Override
    public Object deserializeBinary(BinaryDeserializer deserializer) throws SQLException, IOException {
        return new String(deserializer.readBytes(n));
    }

    @Override
    public void serializeBinaryBulk(Object[] data, BinarySerializer serializer) throws SQLException, IOException {
        for (Object datum : data) {
            serializeBinary(datum, serializer);
        }
    }

    @Override
    public Object[] deserializeBinaryBulk(int rows, BinaryDeserializer deserializer) throws SQLException, IOException {
        String[] data = new String[rows];
        for (int row = 0; row < rows; row++) {
            data[row] = new String(deserializer.readBytes(n));
        }
        return data;
    }

    public static IDataType createFixedStringType(QuotedLexer lexer, TimeZone serverZone) throws SQLException {
        Validate.isTrue(lexer.next().type() == QuotedTokenType.OpeningRoundBracket);
        QuotedToken fixedStringN = lexer.next();
        Validate.isTrue(fixedStringN.type() == QuotedTokenType.Number);
        Validate.isTrue(lexer.next().type() == QuotedTokenType.ClosingRoundBracket);
        Integer bytes = Integer.valueOf(fixedStringN.data());
        return new DataTypeFixedString("FixedString(" + bytes + ")", bytes);
    }
}
