package com.github.huntersherms.pojo2parquet

import com.fasterxml.jackson.annotation.JsonProperty
import org.junit.Test
import kotlin.test.assertEquals

class Pojo2ParquetTest {

    private val firefly = mutableListOf(
            CrewMember("Malcolm", "Reynolds", 49),
            CrewMember("Zoe", "Washburne", 33),
            CrewMember("Kaylee", "Frye", null)
    )

    @Test
    fun pojos2ParquetTestIAndOut() {

        val readerWriter = Pojo2Parquet(CrewMember::class.java)

        val file = readerWriter.pojos2Parquet(firefly)
        file.deleteOnExit()

        val pojos = readerWriter.parquet2Pojos(file)

        assertEquals(firefly, pojos)
    }

    @Test
    fun jacksonAnnotatedPojo2ParquetTestIAndOut() {

        val readerWriter = Pojo2Parquet(CrewMember::class.java)

        val file = readerWriter.jacksonAnnotatedPojos2Parquet(firefly)
        file.deleteOnExit()

        val pojos = readerWriter.parquet2JacksonAnnotatedPojos(file)

        assertEquals(firefly, pojos)
    }
}

data class CrewMember(@get:JsonProperty("first_name") val firstName: String = "",
                      @get:JsonProperty("last_name") val lastName: String = "",
                      @get:JsonProperty("age") val age: Int? = null)