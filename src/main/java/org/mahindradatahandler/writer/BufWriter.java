package org.mahindradatahandler.writer;

import java.io.IOException;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class BufWriter implements IWriter {

    private final FileOutputStream fos;
    private final FileChannel fileChannel;

    public BufWriter(String filePath) throws IOException {
        File file = new File(filePath);
        if (!file.exists()) {
            file.createNewFile();
        }
        this.fos = new FileOutputStream(file, true);
        this.fileChannel = this.fos.getChannel();
    }

    @Override
    public void write(ByteBuffer data) throws IOException {
        fileChannel.write(data);
    }

    @Override
    public void close() throws IOException {
        fileChannel.close();
        fos.close();
    }
}