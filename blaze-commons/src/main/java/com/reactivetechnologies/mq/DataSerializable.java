package com.reactivetechnologies.mq;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

public interface DataSerializable extends Serializable {

	/**
     * Writes object fields to output stream
     *
     * @param out output
     * @throws IOException
     */
    void writeData(DataOutput out) throws IOException;

    /**
     * Reads fields from the input stream
     *
     * @param in input
     * @throws IOException
     */
    void readData(DataInput in) throws IOException;
}
