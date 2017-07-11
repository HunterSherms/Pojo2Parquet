package com.github.huntersherms.pojo2parquet

import org.junit.Test
import kotlin.test.assertEquals

class Pojo2ParquetTest {

    val firefly = mutableListOf(
            CrewMember("Malcolm", "Reynolds", 49),
            CrewMember("Zoe", "Washburne", 33),
            CrewMember("Kaylee", "Frye", null)
    )

    @Test
    fun Pojo2ParquetTestIAndOut() {

        val readerWriter = Pojo2Parquet(CrewMember::class.java)

        val file = readerWriter.pojos2Parquet(firefly)
        file.deleteOnExit()

        val pojos = readerWriter.parquet2Pojos(file)

        assertEquals(firefly, pojos)
    }
}

data class CrewMember(val firstName: String = "", val lastName: String = "", val age: Int? = null)
