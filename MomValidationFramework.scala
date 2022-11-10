import com.amazon.eider.edx.EdxTsvReader
import org.apache.spark.sql.functions._

// Generate Threshold Table For POC
val thresholdTableSchema = Seq("pipeline","module","country_code","metricType","metricThreshold")
val thresholdData = Seq(
                    ("GAM_GSF_VCPU","GSF_VCPU_Final_Transform","US","Cost",5),
                    ("GAM_GSF_VCPU","GSF_VCPU_Final_Transform","UK","Cost",15),
                    ("GAM_GSF_VCPU","GSF_VCPU_Final_Transform","US","Unit",6),
                    ("GAM_GSF_VCPU","GSF_VCPU_Final_Transform","UK","Unit",12),
                    ("GAM_GSF_VCPU","GSF_VCPU_Final_Transform","US","Hour",8),
                    ("GAM_GSF_VCPU","GSF_VCPU_Final_Transform","UK","Hour",16),
                    ("FAM_FYP","FAM_FYP_Final_Transform","US","Cost",10),
                    ("FAM_FYP","FAM_FYP_Final_Transform","UK","Cost",10)
                  )
                  
val thresholdTableRdd = spark.sparkContext.parallelize(thresholdData)
val thresholdTable = thresholdTableRdd.toDF(thresholdTableSchema:_*)

print("Threshold Master Table")
thresholdTable.show(20,truncate=false)

////////////////////////////////////////////
// List of Inputs Passed from Map Reduce Job
////////////////////////////////////////////
val inputDimension = "REGION_ID,COUNTRY_ID"
val inputMetric = "VARIABLE_COST_USD,VARIABLE_COST_LOCAL,ALLOCATION_UNITS_FCLM"
val inputMetricType = "Cost,Cost,Unit"
val inputCountryField = "COUNTRY_ID"
val inputPipeline = "GAM_GSF_VCPU"
val inputModule = "GSF_VCPU_Final_Transform"
val inputFilterCondition = "1=1"
val inputDefaultThreshold = 100
val inputCommonPath = "arn:amazon:edx:iad::manifest/nova-prod/gsf-vcpu/step5-unallocated-cost/"
val inputCurrentMonth = "[2022-10-31]"
val inputPreviousMonth = "[2022-09-30]"
////////////////////////////////////////////
// List of Inputs Passed from Map Reduce Job
////////////////////////////////////////////

// Constant to define prefixes for fields to denote current month metrix, previous month metrix, difference of metrix across two months & percentage of difference
val currentMonthPrefix = "current_month_"
val previousMonthPrefix = "previous_month_"
val diffCurrentPreviousPrefix = "diff_monthly_"
val diffPercentCurrentPreviousPrefix = "percent_diff_monthly_"
val costThresholdField = "Cost_Threshold"
val unitThresholdField = "Unit_Threshold"
val hourThresholdField = "Hour_Threshold"

////////////////////////////////////////////
// Mutable variable to store custom  message for success/exception
////////////////////////////////////////////

var messageHeader=""
var messageDetails=""
var generatedMessage = ""

//////////////////////////////////////
// Input Parameters Pipeline, Module Validation Part
//////////////////////////////////////

// Check Whether Input Pipeline, Module is present in Configuration else Raise an Exception & Exit
thresholdTable.createOrReplaceTempView("threshold_data")
val thresholdTableDataIsExist = spark.sql("select * from threshold_data where pipeline='"+inputPipeline+"' and module='"+inputModule+"'")

if (thresholdTableDataIsExist.count == 0){
  
  messageHeader = "Exception Encountered"
  messageDetails = "Input Pipeline "+inputPipeline+" and Module "+inputModule+" are missing in Configuration"
  generatedMessage =  generateMessage(
                messageHeader,
                inputPipeline,
                inputModule,
                inputDimension,
                inputMetric,
                inputMetricType,
                inputCountryField,
                inputFilterCondition,
                inputdDefaultThreshold.toString,
                inputCommonPath,
                inputCurrentMonth,
                inputPreviousMonth,
                messageDetails
              )
  
  throw new Exception(generatedMessage)            
              
}

// Convert Input Metric Type to List and generate a SQL Filter for Validation
val MetricTypeList: List[String] = inputMetricType.split(",").map(_.trim).toList
val MetricTypeFilter = MetricTypeList.mkString("('","','","')")

// Filter On the Threshold Data
// Apply the Filter passed from Map Reduce Task to reduce Threshold data for Configuration
thresholdTable.createOrReplaceTempView("threshold_data")
val thresholdTableDataFilter = spark.sql("select country_code AS "+inputCountryField+",metricType,metricThreshold from threshold_data where pipeline='"+inputPipeline+"' and module='"+inputModule+"' and metricType IN "+MetricTypeFilter+"")

// Check Whether Pipeline,Module,Metric Type Filed is present in in Configuration or not
// Raise a exception & Exit the process

if (thresholdTableDataFilter.count == 0){
  print("Input MetricType "+inputMetricType+" are missing in Configuration")
  
  messageHeader = "Exception Encountered"
  messageDetails = "Input Pipeline "+inputPipeline+" and Module "+inputModule+" and MetricType "+inputMetricType+" combination are missing in Configuration"
  generatedMessage =  generateMessage(
                messageHeader,
                inputPipeline,
                inputModule,
                inputDimension,
                inputMetric,
                inputMetricType,
                inputCountryField,
                inputFilterCondition,
                inputdDefaultThreshold.toString,
                inputCommonPath,
                inputCurrentMonth,
                inputPreviousMonth,
                messageDetails
              )
  
  throw new Exception(generatedMessage)      
  
}

// Calculate Denormalized Table from Configuration To Join Back with Aggregated Dataset
thresholdTableDataFilter.createOrReplaceTempView("threshold_data_filtered")
val thresholdTableDataDenormalised = spark.sql("""
                                                SELECT
                                                  """+inputCountryField+"""
                                                  ,MAX(CASE WHEN metricType = 'Cost' THEN metricThreshold ELSE 0 END) AS """+costThresholdField+"""
                                                  ,MAX(CASE WHEN metricType = 'Unit' THEN metricThreshold ELSE 0 END) AS """+unitThresholdField+"""
                                                  ,MAX(CASE WHEN metricType = 'Hour' THEN metricThreshold ELSE 0 END) AS """+hourThresholdField+"""
                                                  from threshold_data_filtered
                                                GROUP BY
                                                  """+inputCountryField
                                                )

//print("Threshold Table For Given Input Parameter")
//thresholdTableDataDenormalised.show(20,truncate=false)    

//////////////////////////////////////
// MOM SQL Generation Population Part
//////////////////////////////////////


// Convert Input String to Scala List
val dimensionList: List[String] = inputDimension.split(",").map(_.trim).toList
val metricList: List[String] = inputMetric.split(",").map(_.trim).toList

// Check Whether Country Filed is present in dimension list or not
// Raise a exception & Exit the process
if (!dimensionList.contains(inputCountryField)){
  
  messageHeader = "Exception Encountered"
  messageDetails = "Input Country Field "+inputCountryField+" is missing from "+inputDimension
  generatedMessage =  generateMessage(
                messageHeader,
                inputPipeline,
                inputModule,
                inputDimension,
                inputMetric,
                inputMetricType,
                inputCountryField,
                inputFilterCondition,
                inputdDefaultThreshold.toString,
                inputCommonPath,
                inputCurrentMonth,
                inputPreviousMonth,
                messageDetails
              )
  
  throw new Exception(generatedMessage) 
  
}

// Generate a HashMap based on Metric Field Name vs Metric Field Type
val mapMetricToMetricType = (metricList zip MetricTypeList).toMap

// Concatenate List with List of Dimension + Metric Fields to be retrieved from Source data
val fieldList = dimensionList ++ metricList

// Create Individual Lists having Field names generated with prefixes to use later for data manipulation
// val commonDimensionList = dimensionList.map(x => x)

// Create Individual Lists having Field names generated with prefixes to use later for data manipulation
// eg : Current_Month_<FieldName>, Previous_Month_<FieldName>

val currDimensionList=dimensionList.map(x => currentMonthPrefix+x)
val currMetricList=metricList.map(x => currentMonthPrefix+x)
val currFieldList=currDimensionList ++ currMetricList

val prevDimensionList=dimensionList.map(x => previousMonthPrefix+x)
val prevMetricList=metricList.map(x => previousMonthPrefix+x)
val prevFieldList=prevDimensionList ++ prevMetricList

// Create Metric Lists having Field names generated with prefixes to use later for data manipulation
// eg : diff_monthly_<FieldName>
val diffMetricList=metricList.map(x => diffCurrentPreviousPrefix+x)

val edx = new EdxTsvReader(spark,sc)

// EDX path to read data for previous month
val readPreviousMonth = inputCommonPath+inputPreviousMonth
val readPreviousMonthRawData = edx.read(readPreviousMonth)

// Apply the Filter passed from Map Reduce Task to reduce input data for previous month
readPreviousMonthRawData.createOrReplaceTempView("prev_raw_data")
val readPreviousMonthFilterData = spark.sql("select * from prev_raw_data where "+inputFilterCondition)

if (readPreviousMonthFilterData.count == 0){
  
  messageHeader = "Exception Encountered"
  messageDetails = "Exception Message - No Data is present for Previous Month"
  generatedMessage =  generateMessage(
                messageHeader,
                inputPipeline,
                inputModule,
                inputDimension,
                inputMetric,
                inputMetricType,
                inputCountryField,
                inputFilterCondition,
                inputdDefaultThreshold.toString,
                inputCommonPath,
                inputCurrentMonth,
                inputPreviousMonth,
                messageDetails
              )
  
  throw new Exception(generatedMessage)            
              
}

val readPreviousMonthFinalData = readPreviousMonthFilterData.select(fieldList.map(x => col(x)):_*)
val readPreviousMonthAggregateData = readPreviousMonthFinalData
.groupBy(
  dimensionList
  .map(x => col(x)):_*)
  .agg(metricList.map( y => y -> "sum")
  .toMap
  ).toDF(prevFieldList:_*)

// EDX path to read data for current month
val readCurrentMonth = inputCommonPath+inputCurrentMonth
val readCurrentMonthRawData = edx.read(readCurrentMonth)

// Apply the Filter passed from Map Reduce Task to reduce input data for current month
readCurrentMonthRawData.createOrReplaceTempView("curr_raw_data")
val readCurrentMonthFilterData = spark.sql("select * from curr_raw_data where "+inputFilterCondition)

if (readCurrentMonthFilterData.count == 0){
  
  messageHeader = "Exception Encountered"
  messageDetails = "Exception Message - No Data is present for Current Month"
  generatedMessage =  generateMessage(
                messageHeader,
                inputPipeline,
                inputModule,
                inputDimension,
                inputMetric,
                inputMetricType,
                inputCountryField,
                inputFilterCondition,
                inputdDefaultThreshold.toString,
                inputCommonPath,
                inputCurrentMonth,
                inputPreviousMonth,
                messageDetails
              )
  
  throw new Exception(generatedMessage)            
              
}

// Generate the aggregate data based on input dimension and metric field list
val readCurrentMonthFinalData = readCurrentMonthFilterData.select(fieldList.map(x => col(x)):_*)
val readCurrentMonthAggregateData = readCurrentMonthFinalData
.groupBy(
  dimensionList
  .map(x => col(x)):_*)
  .agg(metricList.map( y => y -> "sum")
  .toMap
  ).toDF(currFieldList:_*)

// Generate the joining condition based on the dimension fields from both current and previous month data
val joinExpression = dimensionList
.map{
  case (x) => readCurrentMonthAggregateData(currentMonthPrefix+x) === readPreviousMonthAggregateData(previousMonthPrefix+x)
}.reduce(_ && _) 

val generateCurrentPreviousCombineAggregateData = readCurrentMonthAggregateData.join(readPreviousMonthAggregateData,joinExpression,"left")

// Calculate the difference for metrics for previous and current month and calculate percentage
val selectDimensionList = dimensionList
.map(
  x => coalesce(col(currentMonthPrefix+x),col(previousMonthPrefix+x)).as(x) 
)

val selectMetricList = (currMetricList ++ prevMetricList)
.map(
  y => coalesce(col(y),lit("0")).as(y)
)

val calculatedDiffList = metricList
.map(
  z => (
    coalesce(col(currentMonthPrefix+z),lit("0"))
    -
    coalesce(col(previousMonthPrefix+z),lit("0"))
    ).as(diffCurrentPreviousPrefix+z) 
  )

val calculatedDiffPercentList = metricList.map(w =>  
  (
    round
    (
        ( 
          (coalesce(col(currentMonthPrefix+w),lit(0))-coalesce(col(previousMonthPrefix+w),lit(0)))
          /
          coalesce(col(previousMonthPrefix+w),lit(0))
        ) * lit(100)
    ,2)
  ).as (diffPercentCurrentPreviousPrefix+w) 
)

val selectList =  selectDimensionList ++ selectMetricList ++ calculatedDiffList ++ calculatedDiffPercentList
val generateCurrentPreviousCombineAggregatePercentData = generateCurrentPreviousCombineAggregateData.select(selectList:_*)

// Create a Dataframe with only DImension List and Percent Difference Fields For Threshold Analysis
val FinalDimensionList = dimensionList.map(z => col(z).as(z))
val DiffPercentList = metricList.map(z => col(diffPercentCurrentPreviousPrefix+z) )
val selectiveOutputList =  FinalDimensionList ++ DiffPercentList

val generateCurrentPreviousCombineAggregatePercentSelectiveData = generateCurrentPreviousCombineAggregatePercentData.select(selectiveOutputList:_*)

print("Aggregated Result Based On Dimension & Metric. Shows the Percent Difference For every dimension Combination")
generateCurrentPreviousCombineAggregatePercentSelectiveData.show(30,truncate=false)

// Create A Dataframe to Combine Calculated Percent Difference and Threshold
// Drop the Country Field from Configuration from Resultant dataset
// Fill default threshold values which are not configured in configuration

val generateCurrentPreviousCombineAggregatePercentSelectiveWithThresholdData = generateCurrentPreviousCombineAggregatePercentSelectiveData
.join(
  thresholdTableDataDenormalised,
  generateCurrentPreviousCombineAggregatePercentSelectiveData(inputCountryField) === thresholdTableDataDenormalised(inputCountryField),
  "left"
  )
.drop(thresholdTableDataDenormalised(inputCountryField))
.na.fill(inputDefaultThreshold,Array(costThresholdField))
.na.fill(inputDefaultThreshold,Array(unitThresholdField))
.na.fill(inputDefaultThreshold,Array(hourThresholdField))

print("Percent result Along With Threshold Defined in configuration in flat structure")
generateCurrentPreviousCombineAggregatePercentSelectiveWithThresholdData.show(30,truncate=false)

// Generate the Dynamic Filter For each Metric Column Based On it's Metric Type So that appropriate filter is generated to generate only threshold breached records
val percentMetricToMetricTypeFilter = metricList
.map(
  p => col(diffPercentCurrentPreviousPrefix+p) > col(mapMetricToMetricType(p)+"_Threshold")
  )
.reduce(_ || _)

// Create a dataframe with the aggregated records having percentage higher than threshold value
val generateFinalThresholdBreachedData = generateCurrentPreviousCombineAggregatePercentSelectiveWithThresholdData.filter(percentMetricToMetricTypeFilter)

print("List of Dimension Combinations with Threshold Breached Records")
generateFinalThresholdBreachedData.show(30,truncate=false)

// If No Record breached Threshold, send an appropriate message
if (generateFinalThresholdBreachedData.count == 0){
  
  messageHeader = "Successfull Validation"
  messageDetails = "No Threshold Breached Records have been identifed"
  generatedMessage =  generateMessage(
                  messageHeader,
                  inputPipeline,
                  inputModule,
                  inputDimension,
                  inputMetric,
                  inputMetricType,
                  inputCountryField,
                  inputFilterCondition,
                  inputdDefaultThreshold.toString,
                  inputCommonPath,
                  inputCurrentMonth,
                  inputPreviousMonth,
                  messageDetails
                )  
                
  print(generatedMessage)
  
}
else{
  
  val resultHeader = generateFinalThresholdBreachedData.columns.mkString("|")
  
  // Generate Final Threshold Breached Result Set for reporting
  val resultDetails=generateFinalThresholdBreachedData
  .withColumn("Id",lit(1))
  .withColumn(
      "Concatenated_Field",
      concat_ws("|",generateFinalThresholdBreachedData.columns.map(m => col(m)):_*)
   )
  .select(col("Id"),col("Concatenated_Field"))
  .groupBy(col("Id"))
  .agg(
    collect_list(col("Concatenated_Field"))
    .alias("Concatenated_Row_Column")
    )
  .withColumn("Concatenated_Output", concat_ws("\n",col("Concatenated_Row_Column")))
  .select("Concatenated_Output").collect.mkString(" ")
  
  messageHeader = "Unsuccessfull Validation"
  messageDetails = resultHeader+"\n"+resultDetails
  generatedMessage =  generateMessage(
                messageHeader,
                inputPipeline,
                inputModule,
                inputDimension,
                inputMetric,
                inputMetricType,
                inputCountryField,
                inputFilterCondition,
                inputdDefaultThreshold.toString,
                inputCommonPath,
                inputCurrentMonth,
                inputPreviousMonth,
                messageDetails
              )
              
  print(generatedMessage)
}


// Function To Generate Exception/Intermediate/Success in a common template
def generateMessage (
                      messageHeader: String,
                      inputPipeline: String,
                      inputModule: String,
                      inputDimension: String,
                      inputMetric: String,
                      inputMetricType: String,
                      inputCountryField: String,
                      inputFilterCondition: String,
                      inputdDefaultThreshold: String,
                      inputCommonPath: String,
                      inputCurrentMonth: String,
                      inputPreviousMonth: String,
                      messageDetails: String
                      ): (String) = {
  
  var finalMessage: String = ""
  finalMessage="\n" +" **** "+messageHeader+" **** : " + "\n" +
  "\n" + "--Input Pipeline **** "+inputPipeline +
  "\n" + "--Input Module **** "+inputModule +
  "\n" + "--Input Dimension List **** "+inputDimension +
  "\n" + "--Input Metric List **** "+inputMetric +
  "\n" + "--Input Metric Type List **** "+inputMetricType +
  "\n" + "--Input Country Field **** "+inputCountryField +
  "\n" + "--Input Filter Condition **** "+inputFilterCondition +
  "\n" + "--Input Default Threshold **** "+inputdDefaultThreshold +
  "\n" + "--Input Common Path **** "+inputCommonPath +
  "\n" + "--Input Current Dataset Date **** "+inputCurrentMonth +
  "\n" + "--Input Previous Dataset Date **** "+inputPreviousMonth +
  "\n" + "\n" + messageDetails
  
  finalMessage
}
