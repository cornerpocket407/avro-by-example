package com.opower.avro.example;

import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

// In a real application, you would not use PersonV1, but it is imported to make
// the writing part of this example easier.
import com.opower.avro.example.model.PersonV1;
import com.opower.avro.example.model.Person;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Demonstrates how to write and read using different Avro schemas.
 */
public class WriteAndReadWithDifferentSchemas {
    public static void main(String[] args) throws Exception {
        // Write some PersonV1 objects to a byte array
        System.out.println("Writing data to byte[]");
        byte[] dataArray = writeData();
        System.out.printf("Wrote %d bytes%n", dataArray.length);

        // Read in Person objects from the byte array of PersonV1 objects
        System.out.println("Reading data from byte[]");
        readData(dataArray);
    }

    private static byte[] writeData() throws IOException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().binaryEncoder(bout, null);
        DatumWriter<PersonV1> writer = new SpecificDatumWriter<PersonV1>(PersonV1.class);
        PersonV1 person = PersonV1.newBuilder().
                setUsername("tom").
                setPassword("password!").
                setJoinedOn(System.currentTimeMillis()).
                build();
        System.out.println(person);
        writer.write(person, encoder);
        encoder.flush();

        return bout.toByteArray();
    }

    private static void readData(byte[] dataArray) throws IOException {
        Decoder decoder = DecoderFactory.get().binaryDecoder(dataArray, null);
        DatumReader<Person> reader = new SpecificDatumReader<Person>(PersonV1.SCHEMA$, Person.SCHEMA$);
        System.out.println(reader.read(null, decoder));
    }
}
