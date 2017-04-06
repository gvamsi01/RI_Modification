import sys,pandas as pd, numpy as np
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import HiveContext
from pyspark.sql import functions as func

# -- Parameters
DT = sys.argv[1]
DB_NM1 = sys.argv[2]
DB_NM2 = sys.argv[3]
frequency = sys.argv[4]

print "========================================================================"
print "DT: " + DT + " <> DB: " + DB_NM1
print "========================================================================"

conf = SparkConf().setAppName("RI-Modifications" + "-" + DT)
sc = SparkContext(conf=conf)

hivecontext = HiveContext(sc)

hivecontext.sql(""" SET spark.sql.shuffle.partitions=100 """)
hivecontext.sql(""" SET fs.s3.canned.acl = BucketOwnerFullControl """)
hivecontext.sql(""" SET spark.sql.parquet.mergeSchema = FALSE """)
hivecontext.sql(""" SET parquet.enable.summary-metadata = FALSE """)
hivecontext.sql(""" SET spark.sql.parquet.filterPushdown = TRUE """)
hivecontext.sql(""" SET spark.sql.autoBroadcastJoinThreshold = 50000000 """)
hivecontext.sql(""" SET parquet.metadata.read.parallelism = 100 """)
hivecontext.sql(""" SET spark.serializer = org.apache.spark.serializer.KryoSerializer """)
hivecontext.sql(""" SET spark.speculation=false """)
hivecontext.sql(""" SET spark.sql.parquet.output.committer.class=org.apache.spark.sql.parquet.DirectParquetOutputCommitter """)

########### Establishing the spark content ###########
hivecontext = HiveContext(sc)

########### Extracting reserved istances (consumption) data ################
consumption_data = hivecontext.sql(""" select productname, 
      year(usagestartdate) as yr, 
      month(usagestartdate) as mon, 
      day(usagestartdate) as dy, 
      hour(usagestartdate) as hr,
      region, availabilityzone, 
      lower(os) as os, case 
      when instancetype is NULL then 'm1.small' 
      else instancetype end as instancetype, 
      sum(usagequantity) as consumption,
      sum(unblendedcost) as dollar 
      from """+DB_NM1+""".aws_cost_enriched_raw_actuals 
      where 
      lower(productname) = 'amazon elastic compute cloud'
      and (unix_timestamp(usageenddate) - unix_timestamp(usagestartdate)) <= 3600 
      and to_date(usagestartdate) <= FROM_UNIXTIME(unix_timestamp('"""+DT+"""' ,'yyyyMMdd'),'yyyy-MM-dd')
      and to_date(usagestartdate) > date_sub(FROM_UNIXTIME(unix_timestamp('"""+DT+"""' ,'yyyyMMdd'),'yyyy-MM-dd') , """+frequency+""")
      and purchaseoption = 'reservedinstance' 
      group by productname, 
      year(usagestartdate),
      month(usagestartdate), 
      day(usagestartdate), 
      hour(usagestartdate), 
      region, availabilityzone, 
      os,instancetype 
      order by yr, mon, dy, hr, 
      region, availabilityzone, os, instancetype asc """)
#consumption_data.show(5)

######### Extracting active instances data #############
active_full_data = hivecontext.sql(""" select account_id, reserved_instance_id, offering_type, 
       region, availability_zone, 
       case 
       when lower(product_description) like '%windows%' then 'windows' 
       when lower(product_description) like '%linux%' then 'linux' 
       else product_description end as os, 
       trim(instance_type) as instancetype, 
       sum(instance_count) as active 
       from """+DB_NM2+""".aws_ri_list 
       where state = 'active' 
       group by account_id, reserved_instance_id, offering_type, region, availability_zone, 
       product_description, instance_type 
       order by account_id, reserved_instance_id, offering_type, region, 
       availability_zone, product_description, instance_type asc """)

active_data = active_full_data.select("region","availability_zone","os","instancetype","active")
active_data = active_data.groupby(["region","availability_zone","os","instancetype"]).agg(func.sum('active').alias('active'))       

#active_data.show(5)

############### Data Transformation logic begins #####################
##### merging active & reserved instances to calculate unused RI #####
combined_data = consumption_data.join(active_data, ["region","os","instancetype"], "left" )

##### extracting regional instances & instances with combinations of os, availabilityzone & region ######
combined_data = combined_data.filter( (combined_data.availabilityzone == combined_data.availability_zone ) | 
                           (combined_data.region == combined_data.availability_zone ) )

###### dropping duplicated records if exists combined_data #####
combined_data = combined_data.drop_duplicates()

####### counting number of times the regional RI record is repeating ###############
count_records = combined_data.groupBy(combined_data.productname,combined_data.yr,combined_data.mon,combined_data.dy,combined_data.hr,combined_data.region,
                           combined_data.availability_zone,combined_data.os,combined_data.instancetype).count()


####### Aggregating combined data #########                           
merge_data = combined_data.groupby(["productname","yr","mon","dy","hr","region","availability_zone","os","instancetype"]).\
             agg(func.sum('dollar').alias('dollar'), func.sum('consumption').alias('consumption'), func.sum('active').alias('active') )

             
###### calculating the RI difference (unused RI) ########
merge_data = merge_data.join(count_records,["productname","yr","mon","dy","hr","region","availability_zone","os","instancetype"],"left")
merge_data = merge_data.withColumn('active', func.col("active")/func.col("count"))
merge_data = merge_data.withColumn("unused_RI", func.col("active") - func.col("consumption"))
merge_data = merge_data.withColumn("unused_RI", func.when(merge_data["unused_RI"] < 0, 0).otherwise(merge_data["unused_RI"]))
merge_data = merge_data.drop('count')


################### Data aggergation per day & per month ##########
####### Aggregating unused reserved instances per day #########
merge_data_day = merge_data.groupby('productname','yr','mon','dy','region','availability_zone','os','instancetype').\
    agg(func.count('hr').alias('hrcount'),
    func.sum('dollar').alias('dollarsum'),
    func.sum('consumption').alias('consumptionsum'),
    func.sum('active').alias('activesum'), 
    func.sum('unused_RI').alias('unused_RItotal'),
    func.sum(func.when(func.col("unused_RI")>0,1).otherwise(0)).alias('unused_RItotal_hrs')).\
    orderBy(["region","availability_zone","os","instancetype","yr","mon","dy"])
    
merge_data_day = merge_data_day.withColumn('Non_usage_hr_prop_day',func.col("unused_RItotal_hrs")*100/func.col("hrcount"))

#merge_data_day.show(15)

####### Aggreating unused reserved instances per month #######
merge_data_month = merge_data.groupby('productname','region','availability_zone','os','instancetype').\
    agg(func.count('hr').alias('hrcount'),
    func.sum('dollar').alias('dollarsum'),
    func.sum('consumption').alias('consumptionsum'),
    func.sum('active').alias('activesum'), 
    func.sum('unused_RI').alias('unused_RItotal'),
    func.sum(func.when(func.col("unused_RI")>0,1).otherwise(0)).alias('unused_RItotal_hrs')).\
    orderBy(["region","availability_zone","os","instancetype"])
    
merge_data_month = merge_data_month.withColumn('Non_usage_hr_prop_month',func.col("unused_RItotal_hrs")*100/func.col("hrcount"))
#merge_data_month.show(15)

######## immplementing the 80 - 20% rule #############
### These unused RI's in merge_data_month_sort can be used for on-demand manipulation
### The data is arranged in the ascending order of highest non_usage_proportion throughout the month
merge_data_month_sort = merge_data_month.filter('Non_usage_hr_prop_month >= 80').\
                        orderBy(["dollarsum","Non_usage_hr_prop_month"],ascending = [0,0])

ondemand_data = hivecontext.sql(""" select productname, 
      year(usagestartdate) as ondemand_yr, 
      month(usagestartdate) as ondmenad_mon, 
      day(usagestartdate) as ondemand_days, 
      hour(usagestartdate) as ondemand_hrs,
      region, availabilityzone, 
      lower(os) as os, case 
      when instancetype is NULL then 'm1.small' 
      else instancetype end as ondemand_instancetype, 
      sum(usagequantity) as quantity_used, 
      sum(unblendedcost) as dollar_spent 
      from """+DB_NM1+""".aws_cost_enriched_raw_actuals 
      where 
      lower(productname) = 'amazon elastic compute cloud' 
      and (unix_timestamp(usageenddate) - unix_timestamp(usagestartdate)) <= 3600 
      and to_date(usagestartdate) <= FROM_UNIXTIME(unix_timestamp('"""+DT+"""' ,'yyyyMMdd'),'yyyy-MM-dd')
      and to_date(usagestartdate) > date_sub(FROM_UNIXTIME(unix_timestamp('"""+DT+"""' ,'yyyyMMdd'),'yyyy-MM-dd') , """+frequency+""")
      and purchaseoption = 'on-demand' 
      and instr(lower(itemdescription),'instance') > 0 and 
      not instr(itemdescription,'Mbps')>0 
      and measure = 'Hour' 
      and os != 'N/A' 
      group by productname, 
      year(usagestartdate),
      month(usagestartdate), 
      day(usagestartdate), 
      hour(usagestartdate), 
      region, availabilityzone, 
      os,instancetype 
      order by ondemand_yr, ondmenad_mon, ondemand_days, 
      ondemand_hrs,region, availabilityzone, os, instancetype asc """)

####### Aggregating on-demand data per day #######
ondemand_data_day = ondemand_data.groupby("productname","ondemand_yr", "ondmenad_mon", "ondemand_days","region","availabilityzone","os","ondemand_instancetype").\
    agg(func.count('ondemand_hrs').alias('ondemand_hrs'),
            func.sum('dollar_spent').alias('dollar_spent'),
            func.sum('quantity_used').alias('quantity_used')).\
    orderBy(["region","availabilityzone","os","ondemand_instancetype","ondemand_yr", "ondmenad_mon", "ondemand_days"])
    
ondemand_data_day = ondemand_data_day.withColumn("Demand_hr_day_prop",func.col("ondemand_hrs")*100/24)

####### Aggregating on-demand data per month ######
ondemand_data_month = ondemand_data.groupby("productname","region","availabilityzone","os","ondemand_instancetype").\
    agg(func.count('ondemand_hrs').alias('ondemand_hrs'),
        func.sum('dollar_spent').alias('dollar_spent'),
        func.sum('quantity_used').alias('quantity_used'),
        func.countDistinct('ondemand_days').alias('ondemand_days')).\
    orderBy(["productname","region","availabilityzone","os","ondemand_instancetype"])
    
ondemand_data_month = ondemand_data_month.withColumn("Demand_hr_month_prop", 
                                                     (func.col("ondemand_hrs")*100)/ (func.col("ondemand_days")*24))
                                                     
############ sorting wrt to dollar spent & demand proportion ############
ondemand_data_month = ondemand_data_month.orderBy(["dollar_spent","Demand_hr_month_prop"],ascending = [0,0])

######### joining on-demand data with possible RI availability data based on product name, region, os ###########
modified_RI= merge_data_month_sort.join(ondemand_data_month,["productname","region","os"],"left")

######### filtering records whose ondemand instance matching with unused RI instance family
modified_RI = modified_RI.withColumn("isin",func.when(func.col("ondemand_instancetype").substr(1,2) == func.col("instancetype").substr(1,2),1).
              otherwise(0)).filter("isin=1").drop("isin")

######### No of CPU's required to make an instance for both on-demand & available RI's #############
modified_RI = modified_RI.withColumn("unused_instance_CPU", func.when(func.split(func.col("instancetype"),'[.]+').getItem(1) == "large",2).\
                                        when(func.split(func.col("instancetype"),'[.]+').getItem(1) == "xlarge",4).\
                                        when(func.split(func.col("instancetype"),'[.]+').getItem(1) == "2xlarge",8).\
                                        when(func.split(func.col("instancetype"),'[.]+').getItem(1) == "4xlarge",16).\
                                        when(func.col("instancetype") == "c4.8xlarge",36).\
                                        when(func.split(func.col("instancetype"),'[.]+').getItem(1) == "8xlarge",32).\
                                        when(func.col("instancetype") == "m4.10xlarge",40).\
                                        when(func.split(func.col("instancetype"),'[.]+').getItem(1) == "16xlarge",64).\
                                        when(func.split(func.col("instancetype"),'[.]+').getItem(1) == "32xlarge",128).\
                                        when( (func.col("instancetype") == "m1.medium") | (func.col("instancetype") == "t2.nano") 
                                        | (func.col("instancetype") == "t2.small") | (func.col("instancetype") == "m3.medium"),1).\
                                        when(func.split(func.col("instancetype"),'[.]+').getItem(1) == "medium",2).
                                        otherwise(1))


modified_RI = modified_RI.withColumn("ondemand_instance_CPU", func.when(func.split(func.col("ondemand_instancetype"),'[.]+').getItem(1) == "large",2).\
                                        when(func.split(func.col("ondemand_instancetype"),'[.]+').getItem(1) == "xlarge",4).\
                                        when(func.split(func.col("ondemand_instancetype"),'[.]+').getItem(1) == "2xlarge",8).\
                                        when(func.split(func.col("ondemand_instancetype"),'[.]+').getItem(1) == "4xlarge",16).\
                                        when(func.col("ondemand_instancetype") == "c4.8xlarge",36).\
                                        when(func.split(func.col("ondemand_instancetype"),'[.]+').getItem(1) == "8xlarge",32).\
                                        when(func.col("ondemand_instancetype") == "m4.10xlarge",40).\
                                        when(func.split(func.col("ondemand_instancetype"),'[.]+').getItem(1) == "16xlarge",64).\
                                        when(func.split(func.col("ondemand_instancetype"),'[.]+').getItem(1) == "32xlarge",128).\
                                        when( (func.col("ondemand_instancetype") == "m1.medium") | (func.col("ondemand_instancetype") == "t2.nano") 
                                        | (func.col("ondemand_instancetype") == "t2.small") | (func.col("ondemand_instancetype") == "m3.medium"),1).\
                                        when(func.split(func.col("ondemand_instancetype"),'[.]+').getItem(1) == "medium",2).
                                        otherwise(1))

######### Average no of unused & on-demand instances ###########
modified_RI = modified_RI.withColumn("Avg_unused_instances", func.round(func.col("unused_RItotal")/func.col("hrcount")))
modified_RI = modified_RI.withColumn("Avg_ondemand_instances", func.round(func.col("quantity_used")/func.col("ondemand_hrs")))

######### Average no of CPU's available & needed for unused RI & on-demand regions ###########
modified_RI = modified_RI.withColumn("Avg_unused_CPU_available", func.col("Avg_unused_instances")*func.col("unused_instance_CPU"))
modified_RI = modified_RI.withColumn("Avg_ondemand_CPU_needed", func.col("Avg_ondemand_instances")*func.col("ondemand_instance_CPU"))

######### No of instances to be shifted & determined on the basis of CPU's required & available ############
modified_RI = modified_RI.withColumn("Instances_tobe_shifted", func.when(func.col("Avg_ondemand_CPU_needed") > func.col("Avg_unused_CPU_available"),
                                                                         func.round(func.col("Avg_unused_CPU_available")/func.col("unused_instance_CPU"))).
                                                               otherwise(func.round(func.col("Avg_ondemand_CPU_needed")/func.col("unused_instance_CPU"))))
                                        
########## No of on-demand instances reduced after shifting instances ###################
modified_RI = modified_RI.withColumn("Ondemand_instances_reduced", func.when(func.col("Avg_ondemand_CPU_needed") > func.col("Avg_unused_CPU_available"),
                                                                        func.round(func.col("Avg_unused_CPU_available")/func.col("ondemand_instance_CPU"))).
                                                               otherwise(func.round(func.col("Avg_ondemand_CPU_needed")/func.col("ondemand_instance_CPU"))))
                                        
########## No of on-demand instances increased after shifting #############
modified_RI = modified_RI.withColumn("Ondemand_instances_increased", func.round(func.col("Instances_tobe_shifted")*func.col("unused_instance_CPU")/func.col("ondemand_instance_CPU")))
               
########### Calculating the savings generated  ###################
modified_RI = modified_RI.withColumn("Savings", func.col("Instances_tobe_shifted")*func.col("ondemand_hrs")*(func.col("dollar_spent") /func.col("quantity_used")))

modified_RI = modified_RI.filter("Avg_unused_instances >0")
modified_RI = modified_RI.filter("Ondemand_instances_reduced > 0")
modified_RI = modified_RI.filter("Instances_tobe_shifted >= 1")

####################### generating the possible down-grading & up-grading of instances ##########
modified_RI = modified_RI.groupby(["productname", "region", "availability_zone", "os", "instancetype","availabilityzone","ondemand_instancetype" ]).\
                agg(func.sum('hrcount').alias('hrcount'),
                        func.sum('dollarsum').alias('dollarsum'),
                        func.sum('consumptionsum').alias('consumptionsum'),
                        func.sum('activesum').alias('activesum'),
                        func.sum('unused_RItotal').alias('unused_RItotal'),
                        func.sum('unused_RItotal_hrs').alias('unused_RItotal_hrs'),
                        func.sum('Non_usage_hr_prop_month').alias('Non_usage_hr_prop_month'),
                        func.sum('ondemand_hrs').alias('ondemand_hrs'),
                        func.sum('quantity_used').alias('quantity_used'),
                        func.sum('dollar_spent').alias('dollar_spent'),
                        func.sum('ondemand_days').alias('ondemand_days'),
                        func.sum('Demand_hr_month_prop').alias('Demand_hr_month_prop'),
                        func.sum('unused_instance_CPU').alias('unused_instance_CPU'),
					  func.sum('ondemand_instance_CPU').alias('ondemand_instance_CPU'),
                        func.sum('Avg_unused_instances').alias('Avg_unused_instances'),
                        func.sum('Avg_ondemand_instances').alias('Avg_ondemand_instances'),
                        func.sum('Avg_unused_CPU_available').alias('Avg_unused_CPU_available'),
                        func.sum('Avg_ondemand_CPU_needed').alias('Avg_ondemand_CPU_needed'),
                        func.sum('Instances_tobe_shifted').alias('Instances_tobe_shifted'),
                        func.sum('Ondemand_instances_reduced').alias('Ondemand_instances_reduced'),
                        func.sum('Ondemand_instances_increased').alias('Ondemand_instances_increased'),
                        func.sum('Savings').alias('Savings'),
                        ).orderBy(["productname", "instancetype","region", "availability_zone", "os",  "Savings"])

modified_RI = modified_RI.withColumn("check",func.lit(0))
                                              
####################### generating the possible down-grading & up-grading of instances ##########
modified_RI_pandas = modified_RI.toPandas()
modified_RI_pandas = modified_RI_pandas.iloc[::-1,:].reset_index(drop=True)
######################### Complete list of suggestions have been generated ################################				

######################### Extracting the best combinations out of the suggested ############################
modified_RI_pandas_copy = modified_RI_pandas.copy()
z= len(modified_RI_pandas_copy)
DATA = pd.DataFrame()
sumins = 0

for i in range(1, z-1):
    if ((sumins <= modified_RI_pandas_copy.loc[i - 1, "Avg_unused_instances"]) & (modified_RI_pandas_copy.loc[i - 1, "check"] == 0) &
            (modified_RI_pandas_copy.loc[i, "availability_zone"] == modified_RI_pandas_copy.loc[i - 1, "availability_zone"]) &
            (modified_RI_pandas_copy.loc[i, "os"] == modified_RI_pandas_copy.loc[i - 1, "os"]) &
            (modified_RI_pandas_copy.loc[i, "instancetype"] == modified_RI_pandas_copy.loc[i - 1, "instancetype"]) &
            (modified_RI_pandas_copy.loc[i, "Instances_tobe_shifted"] >= 1)):

        if ((sumins + modified_RI_pandas_copy.loc[i - 1, "Instances_tobe_shifted"]) < modified_RI_pandas_copy.loc[i - 1, "Avg_unused_instances"]):
            DATA = DATA.append(modified_RI_pandas_copy.iloc[i - 1,])
            sumins = sumins + modified_RI_pandas_copy.loc[i - 1, "Instances_tobe_shifted"]

            if (modified_RI_pandas_copy.loc[i - 1, "Ondemand_instances_reduced"] == modified_RI_pandas_copy.loc[i - 1, "Avg_ondemand_instances"]):
                modified_RI_pandas_copy.loc[((modified_RI_pandas_copy["os"] == modified_RI_pandas_copy.loc[i - 1, "os"]) &
                               (modified_RI_pandas_copy["availabilityzone"] == modified_RI_pandas_copy.loc[i - 1, "availabilityzone"]) &
                               (modified_RI_pandas_copy["ondemand_instancetype"] == modified_RI_pandas_copy.loc[i - 1, "ondemand_instancetype"])), "check"] = 1
            else:
                modified_RI_pandas_copy.loc[((modified_RI_pandas_copy["os"] == modified_RI_pandas_copy.loc[i - 1, "os"]) &
                               (modified_RI_pandas_copy["availabilityzone"] == modified_RI_pandas_copy.loc[i - 1, "availabilityzone"]) &
                               (modified_RI_pandas_copy["ondemand_instancetype"] == modified_RI_pandas_copy.loc[i - 1, "ondemand_instancetype"])),"New_demand"] = \
                    modified_RI_pandas_copy["Avg_ondemand_instances"] - modified_RI_pandas_copy.loc[i - 1, "Ondemand_instances_reduced"]

                modified_RI_pandas_copy["Avg_ondemand_CPU_needed"] = modified_RI_pandas_copy["Avg_ondemand_instances"] * modified_RI_pandas_copy["ondemand_instance_CPU"]

                modified_RI_pandas_copy["Instances_tobe_shifted"] = np.where(modified_RI_pandas_copy["Avg_ondemand_CPU_needed"] > modified_RI_pandas_copy["Avg_unused_CPU_available"],
                                                                  modified_RI_pandas_copy["Avg_unused_CPU_available"] / modified_RI_pandas_copy["unused_instance_CPU"],
                                                                  modified_RI_pandas_copy["Avg_ondemand_CPU_needed"] / modified_RI_pandas_copy["unused_instance_CPU"])

                modified_RI_pandas_copy["Instances_tobe_shifted"] = (modified_RI_pandas_copy["Instances_tobe_shifted"]).apply(np.round)

                modified_RI_pandas_copy["Ondemand_instances_reduced"] = np.where(modified_RI_pandas_copy["Avg_ondemand_CPU_needed"] > modified_RI_pandas_copy["Avg_unused_CPU_available"],
                                                                    modified_RI_pandas_copy["Avg_unused_CPU_available"] / modified_RI_pandas_copy["ondemand_instance_CPU"],
                                                                    modified_RI_pandas_copy["Avg_ondemand_CPU_needed"] / modified_RI_pandas_copy["ondemand_instance_CPU"])

                modified_RI_pandas_copy["Ondemand_instances_reduced"] = (modified_RI_pandas_copy["Ondemand_instances_reduced"]).apply(np.round)

                modified_RI_pandas_copy["Ondemand_instances_increased"] = modified_RI_pandas_copy["Instances_tobe_shifted"] * modified_RI_pandas_copy["unused_instance_CPU"] / modified_RI_pandas_copy["ondemand_instance_CPU"]
                
                modified_RI_pandas_copy["Ondemand_instances_increased"] = (modified_RI_pandas_copy["Ondemand_instances_increased"]).apply(np.round)
                
                modified_RI_pandas_copy["Savings"] = (modified_RI_pandas_copy["Instances_tobe_shifted"])*(modified_RI_pandas_copy["ondemand_hrs"])*(modified_RI_pandas_copy["dollar_spent"])/(modified_RI_pandas_copy["quantity_used"])
                

                continue
        else:
            continue
    elif (modified_RI_pandas_copy.loc[i - 1, "check"] == 1):
        continue

    elif (i == 1 | i == z - 1):
        DATA = DATA.append(modified_RI_pandas_copy.iloc[i,])

    else:
        sumins = 0
        continue

######### Changing on-demand availability zone name tag #############
DATA = DATA.reset_index()
for i in range(0,len(DATA)) :
    if ( DATA.loc[i,"region"] == DATA.loc[i,"availability_zone"]):
        DATA.loc[i,"availabilityzone"] = DATA.loc[i,"availability_zone"]
    else:
        continue

################################### Completion of best combination extraction ############################

################################### Extracting data required for business modifications ###################

data_dump = DATA.loc[:,["productname","region","availability_zone","instancetype","Avg_unused_instances","os","availabilityzone",
                        "ondemand_instancetype","Avg_ondemand_instances","Instances_tobe_shifted","Ondemand_instances_increased","Savings"]]
data_dump.columns = ["productname","region","unused_ri_availabilityzone","unused_ri_instancetype","avg_unused_instances","os",
                     "ondemand_availabilityzone","ondemand_instancetype","avg_ondemand_instances","instances_tobe_shifted","ondemand_instances_increased","savings"]

########## Generating an external table for suggestions ############ 
tmp_df = hivecontext.createDataFrame(data_dump)
tmp_df.registerTempTable("refdate")
hivecontext.sql("""DROP TABLE IF EXISTS temp.refdate_"""+DT+""" """)
hivecontext.sql("select * from refdate").saveAsTable("""temp.refdate_"""+DT+""" """)
hivecontext.sql("""INSERT OVERWRITE TABLE cloudcost_public.ri_modification_suggestions_daily PARTITION (dt='"""+DT+"""')  
                   select productname, region, unused_ri_availabilityzone, os,
                   unused_ri_instancetype, avg_unused_instances, ondemand_availabilityzone,
                   ondemand_instancetype, avg_ondemand_instances, instances_tobe_shifted, ondemand_instances_increased, savings
                   from temp.refdate_"""+DT+""" """)
############# External table generation completed ################

###### Individual instances shifting ################# 
##### adding dummy columns to active_full_data for extracting individual instances shifting ###################### 
active_full_data = active_full_data.withColumn("check", func.lit(0))
active_full_data = active_full_data.withColumn("active_instances_tobe_shifted", func.lit(0))
active_full_data = active_full_data.withColumn("active_check", func.col("active"))
 
 
########## Converting active_full_data to pandas 
active_full_data_pandas = active_full_data.toPandas()

reserve_instance_id_shift = pd.merge(data_dump,active_full_data_pandas, left_on = ["region","unused_ri_availabilityzone","os","unused_ri_instancetype"],
                                     right_on = ["region", "availability_zone", "os", "instancetype"],how = "left")

x= reserve_instance_id_shift.copy()
sumins = 0
y = len(x)
for i in range(1,y):

    if((x.loc[i-1,"check"] == 0) & (x.loc[i-1,"instances_tobe_shifted"] <= x.loc[i-1,"active_check"]) &
       (x.loc[i,"reserved_instance_id"] == x.loc[i-1,"reserved_instance_id"]) &
       (x.loc[i,"region"] == x.loc[i-1,"region"]) & 
       (x.loc[i,"availability_zone"] == x.loc[i-1,"availability_zone"]) &
       (x.loc[i-1,"active_check"] >0)):
        
        x.loc[i-1,"active_instances_tobe_shifted"] = x.loc[i-1,"instances_tobe_shifted"]
        sumins = sumins + x.loc[i-1,"instances_tobe_shifted"]
        x.loc[i,"active_check"] = x.loc[i,"active_check"] - sumins 
        
        x.loc[((x["os"] == x.loc[i-1, "os"]) &
              (x["ondemand_availabilityzone"] == x.loc[i-1 , "ondemand_availabilityzone"]) &
              (x["ondemand_instancetype"] == x.loc[i-1 , "ondemand_instancetype"])), "check"] = 1
    
    
    
    elif((x.loc[i-1,"check"] == 0) & (x.loc[i-1,"instances_tobe_shifted"] <= x.loc[i-1,"active_check"]) &
         (x.loc[i-1,"active_check"] >0)):
        
        x.loc[i-1,"active_instances_tobe_shifted"] = x.loc[i-1,"instances_tobe_shifted"]
        sumins = 0
        x.loc[((x["os"] == x.loc[i-1, "os"]) &
              (x["ondemand_availabilityzone"] == x.loc[i-1 , "ondemand_availabilityzone"]) &
              (x["ondemand_instancetype"] == x.loc[i-1 , "ondemand_instancetype"])), "check"] = 1
    
    else:
        sumins=0
        continue


x = x.loc[x["active_instances_tobe_shifted"] > 0]

data_dump_instances =  x.loc[:,["productname","reserved_instance_id", "account_id" ,"offering_type", "instancetype", "region", 
                                "availability_zone" ,"os" ,"active", "ondemand_availabilityzone","ondemand_instancetype","active_instances_tobe_shifted","savings" ]]

############ Writing to an external table ############## 
tmp_df_instances = hivecontext.createDataFrame(data_dump_instances)
tmp_df_instances.registerTempTable("refdate_instances")
hivecontext.sql("""DROP TABLE IF EXISTS temp.refdate_instances_"""+DT+""" """)
hivecontext.sql("select * from refdate_instances").saveAsTable("""temp.refdate_instances_"""+DT+""" """)
hivecontext.sql("""INSERT OVERWRITE TABLE cloudcost_public.active_instances_shifting_daily PARTITION (dt='"""+DT+"""')  
                   select productname, reserved_instance_id, account_id, offering_type,
                   instancetype, region, availability_zone, os,active,ondemand_availabilityzone,
                   ondemand_instancetype, active_instances_tobe_shifted, savings 
                   from temp.refdate_instances_"""+DT+""" """)