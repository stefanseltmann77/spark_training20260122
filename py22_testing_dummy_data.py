from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql import Row

schema_beer = StructType([StructField("BeerID", IntegerType(), True),
                          StructField("Name", StringType(), True),
                          StructField("URL", StringType(), True),
                          StructField("Style", StringType(), True),
                          StructField("StyleID", IntegerType(), True),
                          StructField("Size", DoubleType(), True),
                          StructField("OG", DoubleType(), True),
                          StructField("FG", DoubleType(), True),
                          StructField("ABV", DoubleType(), True),
                          StructField("IBU", DoubleType(), True),
                          StructField("Color", DoubleType(), True),
                          StructField("BoilSize", DoubleType(), True),
                          StructField("BoilTime", IntegerType(), True),
                          StructField("BoilGravity", StringType(), True),
                          StructField("Efficiency", DoubleType(), True),
                          StructField("MashThickness", StringType(), True),
                          StructField("SugarScale", StringType(), True),
                          StructField("BrewMethod", StringType(), True),
                          StructField("PitchRate", StringType(), True),
                          StructField("PrimaryTemp", StringType(), True),
                          StructField("PrimingMethod", StringType(), True),
                          StructField("PrimingAmount", StringType(), True)])

beer_rows = [
    Row(BeerID=1, Name='Vanilla Cream Ale', URL='/homebrew/recipe/view/1633/vanilla-cream-ale', Style='Cream Ale',
        StyleID=45, Size=21.77, OG=1.055, FG=1.013, ABV=5.48, IBU=17.65, Color=4.83, BoilSize=28.39, BoilTime=75,
        BoilGravity='1.038', Efficiency=70.0, MashThickness='N/A', SugarScale='Specific Gravity',
        BrewMethod='All Grain', PitchRate='N/A', PrimaryTemp='17.78', PrimingMethod='corn sugar',
        PrimingAmount='4.5 oz'),
    Row(
        BeerID=2, Name='Southern Tier Pumking clone', URL='/homebrew/recipe/view/16367/southern-tier-pumking-clone',
        Style='Holiday/Winter Special Spiced Beer', StyleID=85, Size=20.82, OG=1.083, FG=1.021, ABV=8.16, IBU=60.65,
        Color=15.64, BoilSize=24.61, BoilTime=60, BoilGravity='1.07', Efficiency=70.0, MashThickness='N/A',
        SugarScale='Specific Gravity', BrewMethod='All Grain', PitchRate='N/A', PrimaryTemp='N/A', PrimingMethod='N/A',
        PrimingAmount='N/A'),
    Row(
        BeerID=3, Name='Zombie Dust Clone - EXTRACT', URL='/homebrew/recipe/view/5920/zombie-dust-clone-extract',
        Style='American IPA', StyleID=7, Size=18.93, OG=1.063, FG=1.018, ABV=5.91, IBU=59.25, Color=8.98,
        BoilSize=22.71, BoilTime=60, BoilGravity='N/A', Efficiency=70.0, MashThickness='N/A',
        SugarScale='Specific Gravity', BrewMethod='extract', PitchRate='N/A', PrimaryTemp='N/A', PrimingMethod='N/A',
        PrimingAmount='N/A')]
