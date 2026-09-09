from pyspark import pipelines as dp
from pyspark.sql.types import StringType, StructField, StructType

TARGET_TABLE = "dp_static_location_lookup"


MIDDLE_EARTH_CITY_LOOKUP = [
    ("Minas Tirith", "Gondor", "GO", "Gondor", "Westron"),
    ("Osgiliath", "Gondor", "GO", "Gondor", "Westron"),
    ("Pelargir", "Gondor", "GO", "Gondor", "Westron"),
    ("Dol Amroth", "Gondor", "GO", "Gondor", "Westron/Sindarin"),
    ("Edoras", "Rohan", "RO", "Rohan", "Rohirric"),
    ("Helm's Deep", "Rohan", "RO", "Rohan", "Rohirric"),
    ("Aldburg", "Rohan", "RO", "Rohan", "Rohirric"),
    ("Dunharrow", "Rohan", "RO", "Rohan", "Rohirric"),
    ("Isengard", "Isengard", "IS", "Rohan", "Westron"),
    ("Hobbiton", "The Shire", "SH", "Eriador", "Westron"),
    ("Bywater", "The Shire", "SH", "Eriador", "Westron"),
    ("Michel Delving", "The Shire", "SH", "Eriador", "Westron"),
    ("Tuckborough", "The Shire", "SH", "Eriador", "Westron"),
    ("Bree", "Bree-land", "BR", "Eriador", "Westron"),
    ("Combe", "Bree-land", "BR", "Eriador", "Westron"),
    ("Archet", "Bree-land", "BR", "Eriador", "Westron"),
    ("Rivendell", "Rivendell", "RV", "Eriador", "Sindarin"),
    ("Grey Havens", "Lindon", "LI", "Eriador", "Sindarin"),
    ("Fornost", "Arnor", "AR", "Eriador", "Westron"),
    ("Annúminas", "Arnor", "AR", "Eriador", "Westron"),
    ("Tharbad", "Enedwaith", "EN", "Enedwaith", "Westron"),
    ("Esgaroth", "Dale", "DA", "Rhovanion", "Westron"),
    ("Dale", "Dale", "DA", "Rhovanion", "Westron"),
    ("Erebor", "Erebor", "ER", "Rhovanion", "Khuzdul"),
    ("Caras Galadhon", "Lothlórien", "LO", "Rhovanion", "Sindarin"),
    ("Thranduil's Halls", "Woodland Realm", "WR", "Rhovanion", "Sindarin"),
]

CITY_LOOKUP_SCHEMA = StructType(
    [
        StructField("city", StringType()),
        StructField("realm_name", StringType()),
        StructField("realm_code", StringType()),
        StructField("region", StringType()),
        StructField("language_region", StringType()),
    ]
)


dp.create_streaming_table(
    name=TARGET_TABLE,
    comment="Static Middle-earth city reference data, seeded in code (26 rows).",
)


@dp.append_flow(target=TARGET_TABLE, name="dp_static_location_lookup_seed", once=True)
def dp_static_location_lookup_seed():
    """Seed the city lookup once from the in-code list.

    Returns:
        Batch DataFrame with one row per Middle-earth city, columns city,
        realm_name, realm_code, region and language_region.
    """
    return spark.createDataFrame(MIDDLE_EARTH_CITY_LOOKUP, CITY_LOOKUP_SCHEMA)
