package com.github.huntersherms.pojo2parquet

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.dataformat.avro.AvroGenerator
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class Pojo2ParquetTest {

    private val readerWriter = Pojo2Parquet(CrewMember::class.java)

    private val firefly = mutableListOf(
            CrewMember("Malcolm", "Reynolds", 49, 1234567890),
            CrewMember("Zoe", "Washburne", 33, 2345678901),
            CrewMember("Kaylee", "Frye", null, 3456789012)
    )

    @Test
    fun pojos2ParquetTestIAndOut() {

        val file = readerWriter.pojos2Parquet(firefly)
        file.deleteOnExit()

        val pojos = readerWriter.parquet2Pojos(file)

        assertEquals(firefly, pojos)
    }

    @Test
    fun jacksonAnnotatedPojo2ParquetTestIAndOut() {

        val file = readerWriter.jacksonAnnotatedPojos2Parquet(firefly)
        file.deleteOnExit()

        val pojos = readerWriter.parquet2JacksonAnnotatedPojos(file)

        assertEquals(firefly, pojos)
    }

    @Test
    fun getAvroMapperForcesFileInput() {

        assertTrue(readerWriter.getAvroMapper().factory.isEnabled(AvroGenerator.Feature.AVRO_FILE_OUTPUT))
    }
}

data class CrewMember(@get:JsonProperty("first_name") val firstName: String = "",
                      @get:JsonProperty("last_name") val lastName: String = "",
                      @get:JsonProperty("age") val age: Int? = null,
                      @get:JsonProperty("id") val id: Long? = null)