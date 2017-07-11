package com.github.huntersherms.pojo2parquet

import org.apache.avro.reflect.ReflectData
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetReader
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetFileWriter
import java.io.File
import org.apache.parquet.hadoop.metadata.CompressionCodecName.GZIP;

class Pojo2Parquet<T>(val clazz: Class<T>) {

    private val conf = Configuration()

    /**
     * Converts a list of Pojos to a temp parquet file and returns it
     */
    fun pojos2Parquet(pojos: List<T>): File {

        assert(pojos.isNotEmpty())

        val file = File.createTempFile("parquet", ".gzip")
        val schema = ReflectData.AllowNull.get().getSchema(clazz)

        AvroParquetWriter.builder<T>(Path(file.toURI()))
                .withSchema(schema) // generate nullable fields
                .withDataModel(ReflectData.get())
                .withConf(conf)
                .withCompressionCodec(GZIP)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build().use({ writer ->

                    for (pojo in pojos) {
                        writer.write(pojo)
                    }
        })
        return file
    }

    /**
     * Converts a parquet file to a list of Pojos
     */
    fun parquet2Pojos(file: File): List<T> {

        val pojos = mutableListOf<T>()
        AvroParquetReader.builder<T>(Path(file.toURI()))
                .withDataModel(ReflectData(clazz.getClassLoader()))
                .disableCompatibility() // always use this (since this is a new project)
                .withConf(conf)
                .build().use({ reader ->

                    var pojo = reader.read()
                    while (null != pojo) {
                        pojos.add(pojo)
                        pojo = reader.read()
                    }
        })
        return pojos
    }
}