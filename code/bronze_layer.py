####################
# Spark
####################
# Import
import sys
sys.path.insert(1, '/opt/airflow/dags/repo/CommonFunctions')
from shared_spark_config import *
import os
import socket
from os.path import abspath
import s3fs
import pyspark
from pyspark import SparkFiles
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, Row, SQLContext

# Environment
os.environ['PYSPARK_PYTHON'] = "/opt/bitnami/python/bin/python3"
os.environ['PYSPARK_DRIVER_PYTHON'] = "/opt/bitnami/python/bin/python3"
warehouse_location = abspath('spark-warehouse')

# Connection
hostname = socket.gethostname()
pod_ip_address = socket.gethostbyname(hostname)
print(f"Hostname: {hostname}")
print(f"POD IP Address: {pod_ip_address}")

# Session
spark = get_shared_spark_session(appName="Spark_Bronze_layer")

# Defining file system
s3 = s3fs.S3FileSystem(anon=False, client_kwargs={'aws_access_key_id':'accesskey', 'aws_secret_access_key':'secretkey', 'endpoint_url': 'http://minio.acumen.svc.cluster.local:9000'})

# Context
sc = spark.sparkContext
sqlContext = SQLContext(sc)

# Adding files to the Spark executors
sc.addFile('s3a://spark/landingzone/xsd-schemas/xsdv1.xsd')
sc.addFile('s3a://spark/landingzone/xsd-schemas/xsdv2.xsd')


####################
# Imports
####################

from logging_requirements import *
import sys
from pyspark.sql.column import Column, _to_java_column
from pyspark.sql.types import _parse_datatype_json_string, StructType, ArrayType, StringType, LongType, DoubleType, BooleanType, StructField,StructType, TimestampType, DateType, IntegerType
from pyspark.sql.functions import struct, array, transform, schema_of_json, from_json, to_json, col, unbase64, explode, col, lit, startswith, collect_list, sort_array, concat_ws, when, regexp_replace, regexp_extract, row_number
from pyspark.sql.window import Window
from datetime import datetime
import datetime
import time
import re


####################
# Wrapper functions for Spark-XML
####################

extracontextjson_ext_from_xml_other = {}
@logging_decorator(extradurationjson=None, extracontextjson=extracontextjson_ext_from_xml_other, extraauditjson=None)
def ext_from_xml_other(xml_column, schema, options={"rowValidationXSDPath":"xsdv2.xsd", "excludeAttribute":"False", "columnNameOfCorruptRecord": "corruptRecords"}, BatchDate=None): 
    extracontextjson_ext_from_xml_other["BatchDate"] = BatchDate
    java_column = _to_java_column(xml_column.cast('string'))
    java_schema = spark._jsparkSession.parseDataType(schema.json())
    scala_map = spark._jvm.org.apache.spark.api.python.PythonUtils.toScalaMap(options)
    jc = spark._jvm.com.databricks.spark.xml.functions.from_xml(
        java_column, java_schema, scala_map)
    return Column(jc)

extracontextjson_ext_from_xml_205 = {}
@logging_decorator(extradurationjson=None, extracontextjson=extracontextjson_ext_from_xml_205, extraauditjson=None)
def ext_from_xml_205(xml_column, schema, options={"rowValidationXSDPath":"xsdv1.xsd", "excludeAttribute":"False", "columnNameOfCorruptRecord": "corruptRecords"}, BatchDate=None): 
    extracontextjson_ext_from_xml_205["BatchDate"] = BatchDate
    java_column = _to_java_column(xml_column.cast('string'))
    java_schema = spark._jsparkSession.parseDataType(schema.json())
    scala_map = spark._jvm.org.apache.spark.api.python.PythonUtils.toScalaMap(options)
    jc = spark._jvm.com.databricks.spark.xml.functions.from_xml(
        java_column, java_schema, scala_map)
    return Column(jc)

extracontextjson_ext_schema_of_xml_df = {}
@logging_decorator(extradurationjson=None, extracontextjson=extracontextjson_ext_schema_of_xml_df, extraauditjson=None)
def ext_schema_of_xml_df(df, options={}, BatchDate=None):
    extracontextjson_ext_schema_of_xml_df["BatchDate"] = BatchDate
    assert len(df.columns) == 1
    
    scala_options = spark._jvm.PythonUtils.toScalaMap(options)
    java_schema = getattr(getattr(spark._jvm.com.databricks.spark.xml, "package$"), "MODULE$").schema_of_xml_df(df._jdf, scala_options)
        
    return _parse_datatype_json_string(java_schema.json())


####################
# Unfolding the single message into multiple dataframes
####################

extracontextjson_unfolding_singlemessage = {}
@logging_decorator(extradurationjson=None, extracontextjson=extracontextjson_unfolding_singlemessage, extraauditjson=None)
def unfolding_singlemessage(df, main_keys=None, BatchDate=None):
    """This function recursively goes through the XML file and expands the structure of the elements into columns and dataframes.

    Parameters:
        df (dataframe): Dataframe to call the function on
        main_keys (array): Columns that should be included in the underlying dataframes 
        BatchDate (string): For logging purposes
        
    Returns:
        df (dataframe): Unfolded dataframe
    """

    # Log the BatchDate
    extracontextjson_unfolding_singlemessage["BatchDate"] = BatchDate

    possible_arrays = [
            "max-Patient_identification_name",
            "max-Patient_contact_address",
            "max-Patient_contact_telecom",
            "max-Patient_entourage_contactPerson",
            "max-Patient_entourage_contactPerson_name",
            "max-Patient_entourage_contactPerson_telecom",
            "max-Patient_careProviders_hcparty",
            "max-Patient_careProviders_hcparty_id",
            "max-Patient_careProviders_hcparty_type",
            "max-Patient_careProviders_hcparty_name",
            "max-Patient_careProviders_hcparty_adress",
            "max-Patient_careProviders_hcparty_telecom",
            "max-Patient_physiology_chronicPathologies_disease",
            "max-Patient_physiology_chronicPathologies_allergy",
            "max-Patient_physiology_chronicPathologies_homeOxygenTherapy",
            "hcparty_id",
            "hcparty_type",
            "hcparty_name",
            "hcparty_address",
            "hcparty_telecom",
            "product_description_magistralPreparation_compound",
            "product_dispensation_administrationInstructions_site",
            "product_dispensation_administrationInstructions_dayperiod",
            "product_pharmaceuticalCare",
            "product_pharmaceuticalCare_productInformation",
            "product_pharmaceuticalCare_drugRelatedProblem",
            "product_pharmaceuticalCare_drugRelatedProblem_cause",
            "product_pharmaceuticalCare_drugRelatedProblem_intervention",
            "product_pharmaceuticalCare_drugRelatedProblem_result",
            "pharmaceuticalCareActivities_drugRelatedProblem",
            "pharmaceuticalCareActivities_drugRelatedProblem_cause",
            "pharmaceuticalCareActivities_drugRelatedProblem_intervention",
            "pharmaceuticalCareActivities_drugRelatedProblem_result",
            "metaDataList_metaData",
            "dispensedForSamePrescription",
            "dispensedWithoutPrescription"
    ]

    # Check which main_keys are in the columns of the current dataframe
    for key in main_keys:
        if key not in df.columns:
            main_keys.remove(key)

    # Iteration memory
    structs_columns_list = [] # List of StructType's that will be unfolded this operation
    structs_columns_list_processed = [] # List of StructType's that are processed in this operation
    array_columns_list = [] # List of ArrayType's that will be exploded this iteration
    attributes_to_drop = [] # List of columns that must be dropped in the resulting dataframe

    # Check column datatypes
    for column_name in df.columns:
        column_data_type = df.schema[column_name].dataType

        # StructType
        if isinstance(column_data_type, StructType):
            if column_name in possible_arrays:
                array_columns_list.append(column_name)
            else:
                structs_columns_list_add = []
                for c in df.select(column_name+'.*').columns:
                    if ":" not in str(c):
                        structs_columns_list_add.append(col(column_name+'.'+c).alias(column_name+'_'+c))
                        structs_columns_list_processed.append(column_name)
                    else:
                        attributes_to_drop.append(str("`"+c+"`"))
                structs_columns_list = structs_columns_list + structs_columns_list_add

        # ArrayType
        elif isinstance(column_data_type, ArrayType):
            array_columns_list.append(column_name)
            
        # Other Types
        else:
            if column_name in possible_arrays:
                array_columns_list.append(column_name)
            else:
                if ":" not in column_name:
                    structs_columns_list = structs_columns_list + [col(column_name)]
                else:
                    attributes_to_drop.append(str("`"+column_name+"`"))


    if len(array_columns_list) > 0:
        for array_name in array_columns_list:
            array_data_type = df.schema[array_name].dataType
            if not isinstance(array_data_type, ArrayType):
                if isinstance(array_data_type, StructType):
                    df = df.withColumn(array_name, array(col(array_name)))
                else:
                    df = df.withColumn(array_name, array(struct(col(array_name).alias(array_name.split('_')[-1])))) 

    if len(structs_columns_list_processed) > 0:
        df = df.select(*structs_columns_list, *array_columns_list) 
        df = unfolding_singlemessage(df, main_keys, BatchDate=BatchDate)

    # Sort the columns inside the dataframe to union in a later stage
    df = df.select(sorted(df.columns))

    # Avoid duplicate dGUID column in 'product' dataframe
    if 'product_dispensationGUID' in df.columns:
        df = df.drop('product_dispensationGUID')

    # Renaming columns by exluding the parent path: 'path_to_element' becomes 'element'
    for column in df.columns:
        df = df.withColumnRenamed(column, column.split("_")[-1])

    # Dropping certain unnecessary attributes
    df = df.drop(*attributes_to_drop)
    for column in df.columns:
        column_data_type = df.schema[column].dataType
        if isinstance(column_data_type, StructType):
            fieldnames = df[column].fieldNames()
            for fieldname in fieldnames:
                if ':' in fieldname:
                    attributes_to_drop.append(str(f"`{fieldname}`"))
            for field in attributes_to_drop:
                df = df.withColumn(column, col(column).dropFields(field))
        elif isinstance(column_data_type, ArrayType):
            fieldnames = df.schema[column].dataType.elementType.fieldNames()
            for fieldname in fieldnames:
                if ':' in fieldname:
                    attributes_to_drop.append(str(f"`{fieldname}`"))
            for field in attributes_to_drop:
                df = df.withColumn(column, transform(col(column), lambda x: x.dropFields(field)))
    

    return df


#######################
# Creating a unique compounds_ID for magistral preparation compounds
#######################

extracontextjson_add_id_to_compounds_df = {}
@logging_decorator(extradurationjson=None, extracontextjson=extracontextjson_add_id_to_compounds_df, extraauditjson=None)
def add_id_to_compounds_df(df, BatchDate=None):
    """Function to adapt the magprep_compounds df such that it includes a new column (compounds_ID) which contains a new ID consisting of all compound ID's (medicinal products and substances).
    
    Parameters:
        df (dataframe): The dataframe of magrep_compounds
        BatchDate (string): For logging purposes

    Returns:
        df (dataframe): The adapted dataframe of magrep_compounds which includes the new compounds_ID column
    """
    # Logging
    extracontextjson_add_id_to_compounds_df["BatchDate"] = BatchDate

    try:
        result_df = df.withColumn("concatenated_values", when(col("compound.substance.substancecode").isNotNull(), concat_ws("|", col("compound.substance.substancecode"), col("compound.quantity.decimal"), col("compound.quantity.unit"))).otherwise(concat_ws("|",col("compound.medicinalproduct.intendedProduct"), col("compound.quantity.decimal"), col("compound.quantity.unit"))))
        grouped_df = result_df.orderBy(["compound.order"]).groupBy(["messageID","dispensationGUID"]).agg(collect_list("concatenated_values").alias("compounds_ID_array"))
        final_df = grouped_df.withColumn("compoundsID",concat_ws("||", "compounds_ID_array")).drop("compounds_ID_array")
        final_df = df.join(final_df, on=["messageID","dispensationGUID"], how="inner")
    except:
        final_df = df

    return final_df
    

#######################
# Writing a dataframe to parquet file
#######################

extracontextjson_write_to_parquet = {}
@logging_decorator(extradurationjson=None, extracontextjson=extracontextjson_write_to_parquet, extraauditjson=None)
def write_to_parquet(df, path, BatchDate=None):
    """This function writes a dataframe as a parquet file to a certain path.

    Parameters:
        df (dataframe): Dataframe to call the function on
        path (string): Path where the dataframe needs to be written to
        BatchDate (string): For logging purposes
    """

    # Logging
    extracontextjson_write_to_parquet["BatchDate"] = BatchDate
    extracontextjson_write_to_parquet["filename"] = path

    df.write.mode('overwrite').parquet(path)

extracontextjson_write_to_table = {}
@logging_decorator(extradurationjson=None, extracontextjson=extracontextjson_write_to_table, extraauditjson=None)
def write_to_table(df, database, table_name, BatchDate=None):
    """This function writes a dataframe as a table to a certain path.

    Parameters:
        df (dataframe): Dataframe to call the function on
        database (string): Database to write to
        table_name (string): Name of the table to be created/updated
        BatchDate (string): For logging purposes
    """

    # Logging
    extracontextjson_write_to_table["BatchDate"] = BatchDate
    extracontextjson_write_to_table["database"] = database
    extracontextjson_write_to_table["table_name"] = table_name

    spark.sql('CREATE DATABASE IF NOT EXISTS ' + database)
    spark.sql('USE ' + database)
    destination = database + '.' + table_name
    df.write.mode('overwrite').saveAsTable(destination)

#######################
# Check which columns are present in a dataframe
#######################

extracontextjson_columns_to_include = {}
@logging_decorator(extradurationjson=None, extracontextjson=extracontextjson_columns_to_include, extraauditjson=None)
def columns_to_include(df, path, col_list, BatchDate=None):
    """Returns the columns of the col_list that are in the dataframe on a specific level.
    
    Parameters:
        df (dataframe): Dataframe to check
        path (string): Specifies the depth in the hierarchy of the nested structtype columns
        col_list (array): List of column names
    
    Returns:
        columns (array): List of columns that are present in the dataframe
        column_names (array): Names of the columns that are present in the dataframe
    """

    # Logging
    extracontextjson_columns_to_include["BatchDate"] = BatchDate

    column_names = col_list
    columns = [path+'.'+col for col in column_names]
    schema = df[[path]].schema[0].dataType.fieldNames()

    for col in column_names.copy():
        if col not in schema:
            columns.remove(path+'.'+col)
            column_names.remove(col) 
    
    return columns, column_names


#######################
# Struct Merger
#######################
import pyspark.sql.functions as F

def unpack_dataframe_paths(schema, parent, level, arrays, arrays_schema, structs_schema, combined_schema, structure):
    """Returns a list of all the paths inside the nested dataframe.
    
    Parameters:
        schema (string): 
        parent (string):
        level (int):
        arrays (array): Memory that is passed on each iteration.
        arrays_schema (dict): Memory that is passed on each iteration.
        structs_schema (dict): Memory that is passed on each iteration.
        combined_schema (dict): Memory that is passed on each iteration.
        structure (array): Memory that is passed on each iteration.
    
    Returns:
        structure (array): Array of strings that represent all the underlying paths of the dataframe

    """
    for field in schema:
        datatype = field.dataType
        nullable = field.nullable
        name = field.name #.replace('v1:','')
        newname = parent+'.'+name
    
        if isinstance(datatype,StructType): 
            
            newfield = [level,newname,parent,'StructType()',nullable]
            structure.append(newfield)
            structs_schema[newname]=datatype
            combined_schema[newname]=datatype

            unpack_dataframe_paths(datatype, newname, level+1, arrays, arrays_schema, structs_schema, combined_schema, structure)
        
        elif isinstance(datatype,ArrayType): 
            
            arrays.append(newname)
            arrays_schema[newname] = datatype.elementType
            combined_schema[newname]=datatype.elementType


            newfield = [level,newname,parent,'ArrayType()',nullable]
            structure.append(newfield)

            if isinstance(datatype.elementType, StructType):
                unpack_dataframe_paths(datatype.elementType,newname,level+1, arrays, arrays_schema, structs_schema, combined_schema, structure)
            
        
        else:
            newfield = [level,newname,parent,str(datatype),nullable]
            structure.append(newfield)

    return structure

def struct_merger(lstOfDifferences, df, colSuperList):
    """Returns a df that contains the columns of the listOfDifferences inside its struct schema.

    Parameters:
        lstOfDifferences (array): List of the column names that are not in df but should be added.
        df (dataframe): Dataframe where the columns need to be added.
        colSuperList (array): List that contains all the columns of the two dataframes that need to be unioned.
    
    Returns:
        df (dataframe): Dataframe with the added columns
    """

    Arrays = [X[1:].replace('$$$ArrayType()','') for X in colSuperList if  'ArrayType()' in X]
    Arrays.sort()
    for column in lstOfDifferences:
        name,type = column.split('$$$')
        StructName = name[1:].split('.')[0]
        StructField = name[1:].split('.')[-1]
        FieldPath = name[1:].split(f'.{StructField}')[0]
        FieldPathWithoutStruct = name.split(f'.{StructName}')[1][1:]
        Full = name[1:]
        newFieldPath = FieldPath
        arrays = [Array for Array in Arrays if Array in Full]
        stringToGetRid = StructName
        if type == 'StructType()' and f'{name}$$$ArrayType()' in colSuperList:
            continue                
        else:
                evalString = f"df.withColumn('{StructName}',"
                brackets = 1
                if StructName in arrays:
                    evalString = evalString + f'transform(col("{StructName}"),lambda LambdaFunctionThatNeedsToBeReplaced: LambdaFunctionThatNeedsToBeReplaced'
                    brackets = brackets+1
                else:
                    evalString = evalString + f'col("{StructName}")'
                for i,array in enumerate(arrays):
                    if array == StructName:
                        stringToGetRid= array
                        # brackets = brackets - 1
                        continue
                    if array in column and array != name[1:]:
                        stringToGetRid= array
                        if i>0:
                            array = array.replace(arrays[i-1],'')
                        else:
                            array = array.replace(StructName,'')
                        # brackets = brackets+2
                        if 'LambdaFunctionThatNeedsToBeReplaced' in evalString:
                            evalString = evalString + f".withField('{array[1:]}',transform(LambdaFunctionThatNeedsToBeReplaced." + '.'.join([f"getField('{x}')" for x in array[1:].split('.')]) + f', lambda LambdaFunctionThatNeedsToBeReplaced{i}: LambdaFunctionThatNeedsToBeReplaced{i}'
                        else:
                            evalString = evalString + f".withField('{array[1:]}',transform(col('{StructName}')." + '.'.join([f"getField('{x}')" for x in array[1:].split('.')]) + f', lambda LambdaFunctionThatNeedsToBeReplaced{i}: LambdaFunctionThatNeedsToBeReplaced{i}'
                if Full.replace(stringToGetRid,'')[0:1] == '.':
                    leftOver = Full.replace(stringToGetRid,'')[1:]
                else:
                    leftOver = Full.replace(stringToGetRid,'')
                if type == 'ArrayType()' and f'{name}$$$StructType()' in colSuperList:
                    if 'transform' in evalString:
                        evalString = evalString + f".withField('{leftOver}',F.array({evalString.split(':')[-1][1:]}." + '.'.join([f"getField('{x}')" for x in  leftOver.split('.')]) + ")))" #+ brackets*')'
                    else:
                        evalString = evalString + f".withField('{leftOver}',F.array(col('{StructName}.{leftOver}')))" # + brackets*')'
                else:
                    evalString = evalString + f".withField('{leftOver}',lit(None).cast({type}))" #+ brackets*')'
                evalString = evalString.replace("'",'"')
                evalString = evalString + ')'*(evalString.count('(')-evalString.count(')'))
                df = eval(evalString)
    return df

###############################
# Execution of the bronze layer
###############################

extracontextjson_bronze_layer_master = {}
@logging_decorator(extradurationjson=None, extracontextjson=extracontextjson_bronze_layer_master, extraauditjson=None)
def bronze_layer_master(input_path, output_path, source):
    """Master function that reads in the files of an input_path and performs the necessary operations based on the other functions inside the bronze layer.

    Parameters:
        input_path (string): Location of the json files to be processed
        output_path (string): Location where the parquet files need to be written to
        source (string): If the input is a single message or something else
    """
    # Logging
    BatchDate = str(datetime.datetime.now())
    extracontextjson_bronze_layer_master["BatchDate"] = BatchDate
    
    if source == 'single-message':
        drop_duplicates_cols = ['messageID', 'sessionID', 'dispensationGUID']
        df = spark.read.format("json").option('inferTimestamp','False').option('inferSchema','False').load(input_path)

        df = df.withColumn('smc_decoded', unbase64(df["smc"]).cast("string")) 
        df = df.withColumn('smc_decoded', regexp_replace('smc_decoded', 'ns2:', ''))
        df = df.withColumn('smc_decoded', regexp_replace('smc_decoded', 'ns3:', ''))
        df = df.withColumn('smc_decoded', regexp_replace('smc_decoded', 'ns4:', ''))

        # Starting timer for XSD validation logging
        start_time = time.time()
        start_datetime = datetime.datetime.now()
        start_date_time_unix_format = time.mktime(start_datetime.timetuple())

        # Extracting the schema of the single message XML
        xmlSchema = ext_schema_of_xml_df(df.select("smc_decoded"), BatchDate=BatchDate) 
        xmlSchema = eval(str(xmlSchema).replace("StructField('cnk', LongType(), True)", "StructField('cnk', StringType(), True)"))
        xmlSchema = eval(str(xmlSchema)[:-2] + ",StructField('corruptRecords',StringType(),True)])")


        # ----------
        # Adapting the extracted xmlSchema to contain the correct data types according to the XSD file -> In functie gieten?

        structschema = str(xmlSchema)

        # TO DO: same for v2
        with s3.open('s3a://spark/landingzone/xsd-schemas/xsdv1.xsd', 'r') as f:
            xsd_file = f.read()

        xsd_element_pattern = r'<xsd:element name="([^"]*)" type="([^"]*)"'
        xsd_elements = re.findall(xsd_element_pattern, xsd_file)

        xsd_types_pattern = r'<xsd:simpleType name="([^"]*)">[\n\r\s]+<xsd:restriction base="([^"]*)">'
        xsd_types = re.findall(xsd_types_pattern, xsd_file)

        correct_xsd_simpletypes = {}
        for name, base in xsd_types:
            base = base.replace("xsd:", "").replace("smoa:", "").replace("string", "StringType()").replace("int", "IntegerType()").replace("positiveInteger", "IntegerType()").replace("boolean", "BooleanType()").replace("double", "DoubleType()").replace("decimal", "DoubleType()")
            if base in ["BooleanType()", "StringType()", "IntegerType()", "DoubleType()"]:
                correct_xsd_simpletypes[name] = base

        correct_xsd_types = {}
        for name, type in xsd_elements:
            type = type.replace("xsd:", "").replace("smoa:", "").replace("string", "StringType()").replace("int", "IntegerType()").replace("positiveInteger", "IntegerType()").replace("boolean", "BooleanType()").replace("double", "DoubleType()").replace("decimal", "DoubleType()")
            if type in correct_xsd_simpletypes:
                type = correct_xsd_simpletypes[type]
            if type in ["BooleanType()", "StringType()", "IntegerType()", "DoubleType()"]:
                correct_xsd_types[name] = type

        structschema_field_pattern = r"StructField\('([^']*)',\s*([^()]*\(\)),\s*True\)"
        structschema_fields = re.findall(structschema_field_pattern, structschema)

        for name, type in structschema_fields:
            if name in correct_xsd_types:
                if type != correct_xsd_types[name]:
                    structschema = structschema.replace(f"StructField('{name}', {type}, True)", f"StructField('{name}', {correct_xsd_types[name]}, True)")

        xmlSchema = eval(str(structschema))

        # Ending timer for XSD validation logging
        end_time = time.time()
        end_datetime = datetime.datetime.now()
        duration = end_time - start_time
  
        # XSD validation with multiple SMC versions
        smc_version_pattern = r"<version>\s*(.*)\s*<\/version>"
        df = df.withColumn('smcversionpre', regexp_extract(df["smc_decoded"], smc_version_pattern, 1))
        parsed_df = df.withColumn("parsed", when(col('smcversionpre').isin(['2.0.1', '2.0.2', '2.0.3', '2.0.4' ,'2.0.5']), ext_from_xml_205(col("smc_decoded"), xmlSchema, BatchDate=BatchDate)).otherwise(ext_from_xml_other(col("smc_decoded"), xmlSchema, BatchDate=BatchDate)))

        # Ending timer for XSD validation logging
        end_time = time.time()
        end_datetime = datetime.datetime.now()
        duration = end_time - start_time

        # Writing corrupt records to a seperate json file
        corruptedRecords_df=parsed_df.where('parsed.corruptrecords is not null') 
        corruptedRecords_df.write.mode('overwrite').json(output_path + "corruptedRecords.json")

        parsed_df = parsed_df.where('parsed.corruptrecords is null') 

        # ----------
        # Logging: XSD Validation DUURT HEEL LANG
        
        # # Non corrupted message ID and version
        # non_corrupted_message_ids = parsed_df.select('parsed.unsigned.header.messageID').rdd.flatMap(lambda x: x).collect()
        # non_corrupted_message_versions = parsed_df.select('parsed.unsigned.header.version').rdd.flatMap(lambda x: x).collect()
        
        # # Corrupted message ID and version patterns to search for in corruptedrecords column
        # pattern_messageID = r"<messageID>\s*(.*)\s*<\/messageID>"
        # pattern_version = r"<header>\s*<version>\s*(.*)\s*<\/version>"
        
        # # Corrupted message ID
        # corruptedRecords_df_with_messageID = corruptedRecords_df.withColumn("messageID", regexp_extract(corruptedRecords_df["parsed.corruptRecords"], pattern_messageID, 1))
        # corrupted_message_ids = corruptedRecords_df_with_messageID.select('messageID').rdd.flatMap(lambda x: x).collect()

        # # Corrupted message version
        # corruptedRecords_df_with_version = corruptedRecords_df.withColumn("version", regexp_extract(corruptedRecords_df["parsed.corruptRecords"], pattern_version, 1))
        # corrupted_message_versions = corruptedRecords_df_with_version.select('version').rdd.flatMap(lambda x: x).collect()

        # # Log message
        # auditjson = {}
        # durationjson = [{"proc": "xsdvalidation", "start": str(start_datetime), "diff": duration}]
        # contextjson = {}
        # contextjson["xmlschema"] = str(xmlSchema)
        # contextjson["BatchDate"] = BatchDate
        # if '.' in input_path:
        #     input_path_changed = '/' + input_path.replace(':', '').replace('*', '')
        #     input_path_filename = input_path_changed.split('/')[-1]
        #     contextjson["processed_files"] = input_path_filename
        # else:
        #     input_path_changed = '/' + input_path.replace(':', '').replace('*', '')
        #     contextjson["processed_files"] = os.listdir(input_path_changed)
        # contextjson["corruptrecords"] = list(set(corrupted_message_ids))
        # if len(non_corrupted_message_versions) == 0:
        #     smc_version_list = str(list(set(corrupted_message_versions)))
        # else:
        #     smc_version_list = str(list(set(non_corrupted_message_versions)))
        # contextjson["smc_version"] = smc_version_list
        
        # auditjson["duration"] = {"duration": durationjson, "initial": str(start_datetime)}
        # auditjson["context"] = contextjson
        # auditjson["auditinfo"] = {"created": str(start_datetime), "createdTime": start_date_time_unix_format, "audittype": "audit"}
        # logger.info("datalakeaudit:" + json.dumps(auditjson))


        # ----------
        # Initialization of the dataframes

        parsed_df.write.parquet('s3a://spark/bronze/parsed_df.parquet', mode="overwrite")
        parsed_df = spark.read.parquet('s3a://spark/bronze/parsed_df.parquet')
        s3.rm('s3a://spark/bronze/parsed_df.parquet')
        
        # Initial columns
        patient_columns = [str(c) for c in parsed_df.columns if str(c).startswith('patient')]
        key_columns = ['operation',
            'parsed.unsigned.header.messageCreateDate', \
            'parsed.unsigned.header.messageID', \
            'parsed.unsigned.eventFolder.events.event.sessionDateTime',
            'parsed.unsigned.eventFolder.events.event.id', 
            'parsed.unsigned.eventFolder.events.event.PharmacyID.nihiiPharmacyNumber', \
            ] + patient_columns
        key_columns_name = [col.split('.')[-1] for col in key_columns]

        event_columns,event_columns_names = columns_to_include(parsed_df,'parsed.unsigned.eventFolder.events.event',['pharmaceuticalCareActivities','metaDataList'], BatchDate=BatchDate)

        # dispensedForSamePrescription
        dispenses_ForSamePrescription = parsed_df[key_columns+['parsed.unsigned.eventFolder.events.event.dispensedForSamePrescription']+event_columns]
        if not isinstance(dispenses_ForSamePrescription.schema['dispensedForSamePrescription'].dataType, ArrayType):
            dispenses_ForSamePrescription = dispenses_ForSamePrescription.withColumn('dispensedForSamePrescription', array(col('dispensedForSamePrescription')))
        dispenses_ForSamePrescription = dispenses_ForSamePrescription.select(*key_columns_name, explode('dispensedForSamePrescription').alias('dispensedForSamePrescription'), *event_columns_names)
        disp_columns,disp_columns_names = columns_to_include(dispenses_ForSamePrescription,'dispensedForSamePrescription',['hcparty'], BatchDate=BatchDate)
        dispenses_ForSamePrescription = dispenses_ForSamePrescription[[*key_columns_name, 'dispensedForSamePrescription.product', *disp_columns,*event_columns_names]]
        try:
            dispenses_ForSamePrescription = dispenses_ForSamePrescription.select(*key_columns_name, explode('product').alias('product'), *disp_columns_names, *event_columns_names)
        except:
            dispenses_ForSamePrescription = dispenses_ForSamePrescription.select(*key_columns_name, col('product').alias('product'), *disp_columns_names, *event_columns_names)
        dispenses_ForSamePrescription = dispenses_ForSamePrescription.select(*key_columns_name, 'product.dispensationGUID', 'product', *disp_columns_names, *event_columns_names).withColumn('dispensedWithPrescription', lit(True))
    

        # dispensedWithoutPrescription
        dispenses_WithoutPrescription = parsed_df[key_columns+['parsed.unsigned.eventFolder.events.event.dispensedWithoutPrescription']+event_columns].filter('dispensedWithoutPrescription is not null')
        try:
            dispenses_WithoutPrescription = dispenses_WithoutPrescription.select(*key_columns_name, explode('dispensedWithoutPrescription.product').alias('product'), *event_columns_names).select(*key_columns_name, 'product', 'product.dispensationGUID', *event_columns_names).withColumn('dispensedWithPrescription', lit(False))
        except:
            dispenses_WithoutPrescription = dispenses_WithoutPrescription.select(*key_columns_name, col('dispensedWithoutPrescription.product').alias('product'), *event_columns_names).select(*key_columns_name, 'product', 'product.dispensationGUID', *event_columns_names).withColumn('dispensedWithPrescription', lit(False))
        
        
        # ----------
        # Struct Merger for the struct schemas of the product columns -> Vervangen door functie aanroep?

        # Dispenses ForSamePrescription
        structure = unpack_dataframe_paths(dispenses_ForSamePrescription.schema,'',1, arrays=[], arrays_schema={}, structs_schema={}, combined_schema={}, structure=[])
        dispensedForSamePrescriptionCols = [f'{column[1]}$$${column[3]}' for column in structure]

        # Dispenses WithoutPrescription
        structure = unpack_dataframe_paths(dispenses_WithoutPrescription.schema,'',1, arrays=[], arrays_schema={}, structs_schema={}, combined_schema={}, structure=[])
        dispensedWithoutPrescriptionCols = [f'{column[1]}$$${column[3]}' for column in structure]

        colsSuperSet = set(dispensedForSamePrescriptionCols + dispensedWithoutPrescriptionCols)
        colSuperList = list(colsSuperSet)
        colSuperList.sort()

        evalString = ''
        previousdots = 0
        addToSame = [column for column in colSuperList if column not in dispensedForSamePrescriptionCols]
        addToSame = [column for column in addToSame if column[1:].split('.')[0] in dispenses_ForSamePrescription.columns]
        addToWithout = [column for column in colSuperList if column not in dispensedWithoutPrescriptionCols]
        addToWithout = [column for column in addToWithout if column[1:].split('.')[0] in dispenses_WithoutPrescription.columns]

        dispenses_ForSamePrescription = struct_merger(addToSame, dispenses_ForSamePrescription, colSuperList)
        dispenses_WithoutPrescription = struct_merger(addToWithout, dispenses_WithoutPrescription, colSuperList)

        # ----------


        # Union them
        dispenses = dispenses_ForSamePrescription.unionByName(dispenses_WithoutPrescription, allowMissingColumns=True).withColumnRenamed('id', 'sessionID')
        
        dispenses.write.parquet('s3a://spark/bronze/dispenses_df.parquet', mode="overwrite")
        dispenses = spark.read.parquet('s3a://spark/bronze/dispenses_df.parquet')
        s3.rm('s3a://spark/bronze/dispenses_df.parquet')

        # Output 'messages' dataframe
        dfs_to_create = ['hcparty', 'product', 'metaDataList', 'pharmaceuticalCareActivities']
        dispenses = dispenses.withColumn('patientConcat', concat_ws("||", *patient_columns))
        dispenses_output = dispenses.drop(*dfs_to_create, *patient_columns).withColumn('BatchDate', lit(BatchDate))
        file_path = output_path + 'smc_messages.parquet'
        write_to_parquet(dispenses_output.dropDuplicates(drop_duplicates_cols), file_path, BatchDate=BatchDate)
        # write_to_table(dispenses_output.dropDuplicates(*drop_duplicates_cols), 'bronze', 'messages', BatchDate=BatchDate)

        main_keys = ['operation','sessionID','sessionDateTime', 'messageID', 'messageCreateDate', 'dispensationGUID']

        df_to_process = {}
        for df in dfs_to_create:
            if df in dispenses.columns:
                if df == 'product':
                    df_to_process[df] =  dispenses[[*main_keys, df, 'patient', 'patientConcat', 'dispensedWithPrescription', 'nihiiPharmacyNumber']]
                else:  
                    df_to_process[df] =  dispenses[[*main_keys, df]]


        # Seperate df for patient information
        df_patient = dispenses.select('patientConcat', *patient_columns).withColumn('BatchDate', lit(BatchDate))
        file_path = output_path + 'smc_patient.parquet'
        write_to_parquet(df_patient.dropDuplicates(), file_path, BatchDate=BatchDate)

        for df in df_to_process:
            df_to_process[df] = unfolding_singlemessage(df_to_process[df], main_keys=main_keys, BatchDate=BatchDate).withColumn('BatchDate', lit(BatchDate))

            if df == 'product': 
                df_to_process[df] = add_id_to_compounds_df(df_to_process[df], BatchDate=BatchDate) 
            
            filename = 'smc_' + df.replace(':','__') # Is die replace wel nodig?
            file_path = output_path + filename + '.parquet'

            write_to_parquet(df_to_process[df].dropDuplicates(drop_duplicates_cols), file_path, BatchDate=BatchDate)

    else:
        df = spark.read.json(input_path)
        filename = input_path.split("/")[-1].split(".")[0]
        file_path = output_path + filename
      
        write_to_parquet(df, file_path, BatchDate=BatchDate)

# bronze_layer_master("s3a://spark/landingzone/single-messages/202309/20230903/*.annon.gz", "s3a://spark/bronze/20230903/", "single-message")

for i in ['20230905']: # ['20230901', '20230902', '20230904', '20230905', '20230906']:
     input = 's3a://spark/landingzone/single-messages/202309/' + i + '/*.annon.gz'
     output = 's3a://spark/bronze/' + i + '/'
     bronze_layer_master(input, output, "single-message")

# INPUT VAN DELTA TABEL, FILTER OP PHARMACARE