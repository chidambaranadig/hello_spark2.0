import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._

val fireIncidentsFields = Array( StructField("IncidentNumber", IntegerType, true),
StructField("ExposureNumber", IntegerType, true),
StructField("Address", StringType, true),
StructField("IncidentDate", StringType, true),
StructField("CallNumber", IntegerType, true),
StructField("AlarmDtTm", StringType, true),
StructField("ArrivalDtTm", StringType, true),
StructField("CloseDtTm", StringType, true),
StructField("City", StringType, true),
StructField("Zipcode", IntegerType, true),
StructField("Battalion", StringType, true),
StructField("StationArea", IntegerType, true),
StructField("Box", IntegerType, true),
StructField("SuppressionUnits", IntegerType, true),
StructField("SuppressionPersonnel", IntegerType, true),
StructField("EMSUnits", IntegerType, true),
StructField("EMSPersonnel", IntegerType, true),
StructField("OtherUnits", IntegerType, true),
StructField("OtherPersonnel", IntegerType, true),
StructField("FirstUnitOnScene", StringType, true),
StructField("EstimatedPropertyLoss", StringType, true),
StructField("EstimatedContentsLoss", StringType, true),
StructField("FireFatalities", IntegerType, true),
StructField("FireInjuries", IntegerType, true),
StructField("CivilianFatalities", IntegerType, true),
StructField("CivilianInjuries", IntegerType, true),
StructField("NumberofAlarms", IntegerType, true),
StructField("PrimarySituation", StringType, true),
StructField("MutualAid", StringType, true),
StructField("ActionTakenPrimary", StringType, true),
StructField("ActionTakenSecondary", StringType, true),
StructField("ActionTakenOther", StringType, true),
StructField("DetectorAlertedOccupants", StringType, true),
StructField("PropertyUse", StringType, true),
StructField("AreaofFireOrigin", StringType, true),
StructField("IgnitionCause", StringType, true),
StructField("IgnitionFactorPrimary", StringType, true),
StructField("IgnitionFactorSecondary", StringType, true),
StructField("HeatSource", StringType, true),
StructField("ItemFirstIgnited", StringType, true),
StructField("HumanFactorsAssociatedwithIgnition", StringType, true),
StructField("StructureType", StringType, true),
StructField("StructureStatus", StringType, true),
StructField("FloorofFireOrigin", StringType, true),
StructField("FireSpread", StringType, true),
StructField("NoFlameSpead", StringType, true),
StructField("Numberoffloorswithminimumdamage", IntegerType, true),
StructField("Numberoffloorswithsignificantdamage", IntegerType, true),
StructField("Numberoffloorswithheavydamage", IntegerType, true),
StructField("Numberoffloorswithextremedamage", IntegerType, true),
StructField("DetectorsPresent", StringType, true),
StructField("DetectorType", StringType, true),
StructField("DetectorOperation", StringType, true),
StructField("DetectorEffectiveness", StringType, true),
StructField("DetectorFailureReason", StringType, true),
StructField("AutomaticExtinguishingSystemPresent", StringType, true),
StructField("AutomaticExtinguishingSytemType", StringType, true),
StructField("AutomaticExtinguishingSytemPerfomance", StringType, true),
StructField("AutomaticExtinguishingSytemFailureReason", StringType, true),
StructField("NumberofSprinklerHeadsOperating", StringType, true),
StructField("SupervisorDistrict", StringType, true),
StructField("NeighborhoodDistrict", StringType, true),
StructField("Location", StringType, true))

val fireIncidentsSchema = StructType(fireIncidentsFields)

val fireCallsFields = Array(StructField("CallNumber", IntegerType, true),
StructField("UnitID", StringType, true),
StructField("IncidentNumber", IntegerType, true),
StructField("CallType", StringType, true),
StructField("CallDate", StringType, true),
StructField("WatchDate", StringType, true),
StructField("ReceivedDtTm", StringType, true),
StructField("EntryDtTm", StringType, true),
StructField("DispatchDtTm", StringType, true),
StructField("ResponseDtTm", StringType, true),
StructField("OnSceneDtTm", StringType, true),
StructField("TransportDtTm", StringType, true),
StructField("HospitalDtTm", StringType, true),
StructField("CallFinalDisposition", StringType, true),
StructField("AvailableDtTm", StringType, true),
StructField("Address", StringType, true),
StructField("City", StringType, true),
StructField("ZipcodeofIncident", IntegerType, true),
StructField("Battalion", StringType, true),
StructField("StationArea", StringType, true),
StructField("Box", StringType, true),
StructField("OriginalPriority", StringType, true),
StructField("Priority", StringType, true),
StructField("FinalPriority", IntegerType, true),
StructField("ALSUnit", BooleanType, true),
StructField("CallTypeGroup", StringType, true),
StructField("NumberofAlarms", IntegerType, true),
StructField("UnitType", StringType, true),
StructField("Unitsequenceincalldispatch", IntegerType, true),
StructField("FirePreventionDistrict", StringType, true),
StructField("SupervisorDistrict", StringType, true),
StructField("NeighborhoodDistrict", StringType, true),
StructField("Location", StringType, true),
StructField("RowID", StringType, true))

val fireCallsSchema = StructType(fireCallsFields)

val sqlContext = new SQLContext(sc)


val fireIncidents = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(fireIncidentsSchema).load("/home/cnadig/Developer/Fire_Incidents.csv")

val fireCalls = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(fireCallsSchema).load("/home/cnadig/Developer/Fire_Department_Calls_for_Service.csv")

fireIncidents.repartition(16).createOrReplaceTempView("FireIncidentsView")

fireCalls.repartition(16).createOrReplaceTempView("FireCallsView")

spark.catalog.cacheTable("FireIncidentsView")
spark.catalog.cacheTable("FireCallsView")

spark.table("FireCallsView").count()



val fireIncidentsByNeighborhood = fireIncidents.groupBy("NeighborhoodDistrict").count().orderBy(desc("count")).collect().foreach(println)
val fireCallsByNeighborhood = fireCalls.groupBy("NeighborhoodDistrict").count.orderBy(desc("count")).collect().foreach(println)


val joined = fireCalls.join(fireIncidents, fireCalls.col("IncidentNumber") === fireIncidents.col("IncidentNumber"))