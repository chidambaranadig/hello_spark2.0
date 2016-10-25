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
  StructField("Zipcode", StringType, true),
  StructField("Battalion", StringType, true),
  StructField("StationArea", StringType, true),
  StructField("Box", StringType, true),
  StructField("SuppressionUnits", StringType, true),
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
  StructField("Numberoffloorswithminimumdamage", StringType, true),
  StructField("Numberoffloorswithsignificantdamage", StringType, true),
  StructField("Numberoffloorswithheavydamage", StringType, true),
  StructField("Numberoffloorswithextremedamage", StringType, true),
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
val fireIncidents2 = fireIncidents.repartition(16)



val fireCalls = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").schema(fireCallsSchema).load("/home/cnadig/Developer/Fire_Department_Calls_for_Service.csv")
val fireCalls2 = fireCalls.repartition(16)

val dateFormat1 = "MM/dd/yyyy"
val dateFormat2 = "MM/dd/yyyy hh:mm:ss aa"

// Some columns are reformatted from StringType to Unix_Timestamp
val fireCallsDF = fireCalls2.withColumn("CallDateTS", unix_timestamp(fireCalls("CallDate"), dateFormat1).cast("timestamp")).drop("CallDate")
  .withColumn("WatchDateTS", unix_timestamp(fireCalls("WatchDate"), dateFormat1).cast("timestamp")).drop("WatchDate")
  .withColumn("ReceivedDtTmTS", unix_timestamp(fireCalls("ReceivedDtTm"), dateFormat2).cast("timestamp")).drop("ReceivedDtTm")
  .withColumn("EntryDtTmTS", unix_timestamp(fireCalls("EntryDtTm"), dateFormat2).cast("timestamp")).drop("EntryDtTm")
  .withColumn("DispatchDtTmTS", unix_timestamp(fireCalls("DispatchDtTm"), dateFormat2).cast("timestamp")).drop("DispatchDtTm")
  .withColumn("ResponseDtTmTS", unix_timestamp(fireCalls("ResponseDtTm"), dateFormat2).cast("timestamp")).drop("ResponseDtTm")
  .withColumn("OnSceneDtTmTS", unix_timestamp(fireCalls("OnSceneDtTm"), dateFormat2).cast("timestamp")).drop("OnSceneDtTm")
  .withColumn("TransportDtTmTS", unix_timestamp(fireCalls("TransportDtTm"), dateFormat2).cast("timestamp")).drop("TransportDtTm")
  .withColumn("HospitalDtTmTS", unix_timestamp(fireCalls("HospitalDtTm"), dateFormat2).cast("timestamp")).drop("HospitalDtTm")
  .withColumn("AvailableDtTmTS", unix_timestamp(fireCalls("AvailableDtTm"), dateFormat2).cast("timestamp")).drop("AvailableDtTm")

// Some columns are reformatted from StringType to Unix_Timestamp
val fireIncidentsDF = fireIncidents2.withColumn("IncidentDateTS",unix_timestamp(fireIncidents("IncidentDate"),dateFormat1).cast("timestamp")).drop("IncidentDate")
  .withColumn("AlarmDtTmTS",unix_timestamp(fireIncidents("AlarmDtTm"),dateFormat2).cast("timestamp")).drop("AlarmDtTm")
  .withColumn("ArrivalDtTmTS",unix_timestamp(fireIncidents("ArrivalDtTm"),dateFormat2).cast("timestamp")).drop("ArrivalDtTm")
  .withColumn("CloseDtTmTS",unix_timestamp(fireIncidents("CloseDtTm"),dateFormat2).cast("timestamp")).drop("CloseDtTm")

//fireCallsDF.createOrReplaceTempView("FireCallsView")
//fireIncidentsDF.createOrReplaceTempView("FireIncidentsView")

fireCallsDF.cache
fireIncidentsDF.cache

// Number of Fire Incidents Per Year
val fireIncidentsByYear = fireIncidentsDF.select(year(fireIncidentsDF("IncidentDateTS"))).groupBy("year(IncidentDateTS)").count().orderBy("year(IncidentDateTS)")

// Number of Calls Made to the Fire Department Per Year
val fireCallsByYear = fireCallsDF.select(year(fireCallsDF("CallDateTS"))).groupBy("year(CallDateTS)").count().orderBy("year(CallDateTS)").withColumnRenamed("count","count2")

val callsByYear = fireIncidentsByYear.join(fireCallsByYear, fireIncidentsByYear("year(IncidentDateTS)")===fireCallsByYear("year(CallDateTS)"),"outer").orderBy("year(CallDateTS)")
  .select("year(CallDateTS)","count","count2")
  .withColumnRenamed("count","FireIncidents")
  .withColumnRenamed("count2","FireCalls")
  .withColumnRenamed("year(CallDateTS)","Year")

val fireIncidentsByNeighborhood = fireIncidentsDF.groupBy("NeighborhoodDistrict").count.orderBy(desc("count"))

val fireCallsByNeighborhood = fireCallsDF.groupBy("NeighborhoodDistrict").count.orderBy(desc("count"))

val fireCallsByType = fireCallsDF.groupBy("CallType").count().orderBy(desc("count"))

val fireCallsByMonth2016 = fireCallsDF.filter(year(fireCallsDF("CallDateTS"))==2016)

val callType = fireCalls.groupBy("CallType").count().orderBy(desc("count")).collect().foreach(println)

val heatSource = fireIncidentsDF.groupBy("HeatSource").count().orderBy(desc("count")).collect().foreach(println)
