package com.github.huntersherms.pojo2parquet

import org.apache.avro.reflect.ReflectData
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetFileWriter
import java.io.File
import org.apache.parquet.hadoop.metadata.CompressionCodecName.GZIP
import com.fasterxml.jackson.dataformat.avro.schema.AvroSchemaGenerator
import com.fasterxml.jackson.dataformat.avro.AvroFactory
import com.fasterxml.jackson.dataformat.avro.AvroGenerator
import com.fasterxml.jackson.dataformat.avro.AvroMapper
import com.fasterxml.jackson.dataformat.avro.AvroSchema
import org.apache.avro.Schema
import org.apache.avro.file.DataFileReader
import org.apache.avro.file.SeekableByteArrayInput
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.BinaryEncoder
import org.apache.avro.io.EncoderFactory
import java.io.ByteArrayOutputStream


class Pojo2Parquet<T>(private val clazz: Class<T>) {

    /**
     * Converts a Collection of POJOs to a temp parquet file and returns it.
     *
     * Uses reflection so that providing an Avro schema is not necessary.
     */
    fun pojos2Parquet(pojos: Collection<T>): File {

        assert(pojos.isNotEmpty())

        val file = File.createTempFile("parquet", ".gzip")
        val schema = ReflectData.AllowNull.get().getSchema(clazz)

        AvroParquetWriter.builder<T>(Path(file.toURI()))
                .withSchema(schema) // generate nullable fields
                .withDataModel(ReflectData.get())
                .withCompressionCodec(GZIP)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build().use({ writer ->
                    pojos.forEach { pojo -> writer.write(pojo) }
                })

        return file
    }

    /**
     * Converts a parquet file to a list of POJOs.
     *
     * Uses reflection so that providing an Avro schema is not necessary.
     */
    fun parquet2Pojos(file: File): List<T> {

        val pojos = mutableListOf<T>()
        AvroParquetReader.builder<T>(Path(file.toURI()))
                .withDataModel(ReflectData(clazz.classLoader))
                .disableCompatibility() // always use this (since this is a new project)
                .build().use({ reader ->
                    var pojo = reader.read()
                    while (null != pojo) {
                        pojos.add(pojo)
                        pojo = reader.read()
                    }
                })

        return pojos
    }

    /**
     * Use in place of pojos2Parquet if your POJOs are Jackson annotated and you want the derived parquet column names
     * to follow your annotations rather than your POJO property names, or optionally provide your own AvroMapper with
     * your own configuration in place.
     */
    fun jacksonAnnotatedPojos2Parquet(pojos: Collection<T>, mapper: AvroMapper = getAvroMapper()): File {

        assert(pojos.isNotEmpty())

        val file = File.createTempFile("parquet", ".gzip")

        //Derive our schema using Jackson
        val schema = getAvroSchema(mapper)

        val avroWriter = mapper.writer(AvroSchema(schema))

        AvroParquetWriter.builder<GenericRecord>(Path(file.toURI()))
                .withSchema(schema)
                .withCompressionCodec(GZIP)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build().use({ writer ->
                    pojos.forEach { pojo ->
                        DataFileReader.openReader(
                                SeekableByteArrayInput(avroWriter.writeValueAsBytes(pojo)),
                                GenericDatumReader<GenericRecord>(schema)).forEach { record ->
                                    writer.write(record)
                                }
                    }
                })

        return file
    }

    /**
     * Use in place of parquet2Pojos if your POJOs are Jackson annotated and you want Jackson to manage the mapping
     * to your POJOs.
     */
    fun parquet2JacksonAnnotatedPojos(file: File, mapper: AvroMapper = getAvroMapper()): List<T> {

        //Derive our schema using Jackson
        val schema = getAvroSchema(mapper)

        val avroReader = mapper.reader(AvroSchema(schema)).forType(clazz)

        val datumWriter = GenericDatumWriter<GenericRecord>(schema)

        val encoderFactory = EncoderFactory.get()

        val pojos = mutableListOf<T>()

        AvroParquetReader.builder<GenericRecord>(Path(file.toURI()))
                .disableCompatibility()
                .withDataModel(GenericData.get())
                .build().use({ reader ->
                    var pojo = reader.read()
                    var bos: ByteArrayOutputStream
                    var encoder: BinaryEncoder? = null
                    while (null != pojo) {
                        bos = ByteArrayOutputStream()
                        encoder = encoderFactory.directBinaryEncoder(bos, encoder)
                        datumWriter.write(pojo, encoder)
                        pojos.add(avroReader.readValue(bos.toByteArray()))
                        pojo = reader.read()
                    }
                })

        return pojos
    }

    /**
     * Get an AvroMapper meeting the requirements of these Jackson methods. Useful if you want to set some default properties on
     * your mapper such as defaulting to snake case.
     *
     * mapper.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);
     */
    fun getAvroMapper(): AvroMapper {

        return AvroMapper(AvroFactory().enable(AvroGenerator.Feature.AVRO_FILE_OUTPUT))
    }

    /**
     * Use Jackson to derive the Avro Schema based on Jackson annotations if provided.
     */
    private fun getAvroSchema(mapper: AvroMapper): Schema {
        val gen = AvroSchemaGenerator()
        mapper.acceptJsonFormatVisitor(clazz, gen)
        return gen.generatedSchema.avroSchema
    }
}