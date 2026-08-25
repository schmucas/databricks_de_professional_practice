"""
Silver | dp_static_location_lookup | declarative track

Mirrors : src/classic_approach/silver/static/silver_static_location_lookup.ipynb
Source  : hardcoded seed list (no upstream system)
Target  : dp_static_location_lookup  (small reference table)

Parity notes
  - 26 Middle-earth cities
  - columns: city, realm_name, realm_code, region, language_region
  - values are identical to the classic notebook, copy the list verbatim
  - streaming table seeded by a one-shot append flow, so the seed lands exactly
    once and later runs leave it alone, which is what the classic INITIAL_RUN
    flag buys by hand

This is the only silver table with no bronze parent. It exists so
dp_dim_location can enrich the raw city strings coming off orders.
"""

from pyspark import pipelines as dp
from pyspark.sql.types import StringType, StructField, StructType

# --- config -----------------------------------------------------------------
# silver is the pipeline default schema, so silver targets are named bare.
TARGET_TABLE = "dp_static_location_lookup"


# --- seed data --------------------------------------------------------------
MIDDLE_EARTH_CITY_LOOKUP = [
    # city, realm_name, realm_code, region, language_region
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


# --- target -----------------------------------------------------------------
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


# Paradigm note
#   Classic guards this table behind an INITIAL_RUN flag so a normal run does not
#   clobber it; the declarative equivalent is a once=True append flow, which the
#   pipeline itself remembers having run, instead of a job parameter a human has
#   to remember to flip. The trade is that editing the seed list has no effect
#   until a full refresh, where the classic notebook only needs the flag set.
