package lyd.ai.native4j.jdbc.protocol;

import lyd.ai.native4j.jdbc.data.Block;
import lyd.ai.native4j.jdbc.serializer.BinarySerializer;

import java.io.IOException;
import java.sql.SQLException;

public class DataRequest extends RequestOrResponse {
    public static final DataRequest EMPTY = new DataRequest("");

    private final String name;
    private final Block block;

    public DataRequest(String name) {
        this(name, new Block());
    }

    public DataRequest(String name, Block block) {
        super(ProtocolType.REQUEST_DATA);
        this.name = name;
        this.block = block;
    }

    @Override
    public void writeImpl(BinarySerializer serializer) throws IOException, SQLException {
        serializer.writeStringBinary(name);

        serializer.maybeEnableCompressed();
        block.writeTo(serializer);
        serializer.maybeDisenableCompressed();
    }

}
