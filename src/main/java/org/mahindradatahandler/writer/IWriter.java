package org.mahindradatahandler.writer;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface IWriter {
    void write(ByteBuffer data) throws IOException;

    void close() throws IOException;
}
