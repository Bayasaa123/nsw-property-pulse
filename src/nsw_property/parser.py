"""Parser for NSW Valuer General bulk property sales files (.DAT)."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SALE_KEY = ["district_code", "property_id", "sale_counter", "_source_file"]

def field(i:int):
    """i deh talbariig avna (0 -ees ehelj toolno). Hooson bol null bolgono."""
    value = F.trim(F.col("fields")[i])
    return F.when(value =="", None).otherwise(value)


def split_records(raw: DataFrame) -> DataFrame:
    """Mur buriig talbaruudad huvaaj, muriin turliig nemne."""
    return(
        raw
        .withColumn("fields", F.split("value", ";"))
        .withColumn("record_type", F.col("fields")[0])
    )

def parse_sales(records: DataFrame) -> DataFrame:
    """B muruud -> borluulalt burt neg mur."""
    return(
        records
        .where(F.col("record_type") == "B")
        .select(
            field(1).alias("district_code"),
            field(2).cast("bigint").alias("property_id"),
            field(3).cast("int").alias("sale_counter"),
            F.to_timestamp(field(4), "yyyyMMdd HH:mm").alias("download_ts"),
            field(6).alias("unit_number"),
            field(7).alias("house_number"),
            field(8).alias("street_name"),
            field(9).alias("suburb"),
            field(10).alias("postcode"),
            field(11).cast("double").alias("area"),
            field(12).alias("area_type"),
            F.to_date(field(13), "yyyyMMdd").alias("contract_date"),
            F.to_date(field(14), "yyyyMMdd").alias("settlement_date"),
            field(15).cast("bigint").alias("purchase_price"),
            field(17).alias("nature_of_property"),
            field(18).alias("primary_purpose"),
            field(23).alias("dealing_number"),
            "_source_file"
        )
        .withColumn(
            "area_sqm",
            F.when(F.col("area_type") == "H", F.col("area") * 10000)
            .otherwise(F.col("area")),
        )
        .withColumn(
            "address",
            F.concat_ws(" ", "unit_number", "house_number", "street_name"),
        )
    )

def parse_legal(records: DataFrame) -> DataFrame:
    """C мөрүүд -> газрын эрх зүйн дугаар."""
    return (
        records
        .where(F.col("record_type") == "C")
        .select(
            field(1).alias("district_code"),
            field(2).cast("bigint").alias("property_id"),
            field(3).cast("int").alias("sale_counter"),
            field(5).alias("legal_description"),
            "_source_file",
        )
    )


def parse_parties(records: DataFrame) -> DataFrame:
    """D мөрүүд -> борлуулалт бүрийн худалдагч, худалдан авагчийн тоо."""
    return (
        records
        .where(F.col("record_type") == "D")
        .select(
            field(1).alias("district_code"),
            field(2).cast("bigint").alias("property_id"),
            field(3).cast("int").alias("sale_counter"),
            field(5).alias("party_type"),
            "_source_file",
        )
        .groupBy(*SALE_KEY)
        .agg(
            F.count_if(F.col("party_type") == "P").alias("purchaser_count"),
            F.count_if(F.col("party_type") == "V").alias("vendor_count"),
        )
    )


def reconcile(records: DataFrame) -> DataFrame:
    """Файл бүрийн Z мөрийн тоог бодит мөрийн тоотой харьцуулна."""
    actual = records.groupBy("_source_file").agg(
        F.count("*").alias("actual_total"),
        F.count_if(F.col("record_type") == "B").alias("actual_b"),
        F.count_if(F.col("record_type") == "C").alias("actual_c"),
        F.count_if(F.col("record_type") == "D").alias("actual_d"),
    )

    expected = records.where(F.col("record_type") == "Z").select(
        "_source_file",
        field(1).cast("int").alias("expected_total"),
        field(2).cast("int").alias("expected_b"),
        field(3).cast("int").alias("expected_c"),
        field(4).cast("int").alias("expected_d"),
    )

    all_match = (
        (F.col("expected_total") == F.col("actual_total"))
        & (F.col("expected_b") == F.col("actual_b"))
        & (F.col("expected_c") == F.col("actual_c"))
        & (F.col("expected_d") == F.col("actual_d"))
    )

    return (
        actual
        .join(expected, "_source_file", "left")
        .withColumn("is_complete", F.coalesce(all_match, F.lit(False)))
    )

def build_sales(records: DataFrame) -> DataFrame:
    """B + C + D -> борлуулалт бүрт нэг бүрэн мөр."""
    return (
        parse_sales(records)
        .join(parse_legal(records), SALE_KEY, "left")
        .join(parse_parties(records), SALE_KEY, "left")
    )

def add_quality_flags(sales: DataFrame) -> DataFrame:
    """Borluulalt burt ugugdliin chanariin shalgalt nemne"""
    issues = F.array_compact(F.array(
        F.when(F.col("property_id").isNull(), F.lit("missing_property_id")),
        F.when(F.col("dealing_number").isNull(), F.lit("missing_dealing_number")),
        F.when(F.col("contract_date").isNull(), F.lit("missing_contract_date")),
        F.when(F.col("purchase_price").isNull(), F.lit("missing_price")),
        F.when(
            F.col("settlement_date") < F.col("contract_date"),
            F.lit("settlement_before_contract"),
        ),
    ))

    return (
        sales
        .withColumn("dq_issues", issues)
        .withColumn("is_valid", F.size("dq_issues") == 0)
    )