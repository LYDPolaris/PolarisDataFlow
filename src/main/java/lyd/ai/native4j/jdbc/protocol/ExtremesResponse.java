package lyd.ai.native4j.jdbc.protocol;

import java.io.IOException;
import java.sql.SQLException;

import lyd.ai.native4j.jdbc.connect.PhysicalInfo;
import lyd.ai.native4j.jdbc.data.Block;
import lyd.ai.native4j.jdbc.serializer.BinaryDeserializer;
import lyd.ai.native4j.jdbc.serializer.BinarySerializer;

public class ExtremesResponse extends RequestOrResponse {

    private final String name;
    private final Block block;

    ExtremesResponse(String name, Block block) {
        super(ProtocolType.RESPONSE_Extremes);
        this.name = name;
        this.block = block;
    }

    @Override
    public void writeImpl(BinarySerializer serializer) throws IOException {
        throw new UnsupportedOperationException("ExtremesResponse Cannot write to Server.");
    }

    public static ExtremesResponse readFrom(BinaryDeserializer deserializer, PhysicalInfo.ServerInfo info)
        throws IOException, SQLException {
        return new ExtremesResponse(deserializer.readStringBinary(), Block.readFrom(deserializer, info));
    }
}
