create or replace schema snowflake_schema;

create or replace database snowflake_database;


 select * from automobile_data;

 create or replace sequence new_sqe start = 1 increment = 1
 CREATE OR REPLACE TABLE automobile_data (
  ---  id INT deafault autoincrement start =1 increment= 1,  -- Auto-incrementing column in Snowflake
     id INT DEFAULT new_sqe.nextval,
    symboling INT,
    normalized_losses INT,
    make VARCHAR(50),
    fuel_type VARCHAR(10),
    aspiration VARCHAR(10),
    num_of_doors VARCHAR(10),
    body_style VARCHAR(20),
    drive_wheels VARCHAR(10),
    engine_location VARCHAR(10),
    wheel_base FLOAT,
    length FLOAT,
    width FLOAT,
    height FLOAT,
    curb_weight INT,
    engine_type VARCHAR(20),
    num_of_cylinders VARCHAR(20),
    engine_size INT,
    fuel_system VARCHAR(20),
    bore FLOAT,
    stroke FLOAT,
    compression_ratio FLOAT,
    horsepower INT,
    peak_rpm INT,
    city_mpg INT,
    highway_mpg INT,
    price FLOAT,
   PRIMARY KEY (id)  -- Optional, if 'id' needs to be unique
);



 copy into automobile_data
 from@my_stage
 file_format=my_file_format
 pattern='.*Automobile_data.csv2.gz*'
 on_error='continue'
 force=True

  SELECT 
    l.query_id,
    l.status AS load_status,
    l.error_message,
    c.column_name
FROM 
    information_schema.load_history l
JOIN 
    information_schema.columns c 
    ON l.automobile_data = c.automobile_data
WHERE 
    l.table_name = 'automobile_data' 
    AND l.status = 'PARTIALLY_LOADED'
    AND l.error_message LIKE '%first_error_column_name%'
ORDER BY 
    l.query_id DESC;
list@my_stage

 select * from automobile_data

 create or replace schema curated_schema;




 
 // create task on automobile_data
desc table  snowflake_database.snowflake_schema.automobile_data


SELECT 
    new_sqe.NEXTVAL::INT AS new_id, 
    t.$1::INT AS symboling,
    t.$2::INT AS normalized_losses,
    t.$3::string AS make,
    t.$4::VARCHAR AS fuel_type,
    t.$5::VARCHAR AS aspiration,
    t.$6::VARCHAR AS num_of_doors,
    t.$7::VARCHAR AS body_style,
    t.$8::VARCHAR AS drive_wheels,
    t.$9::VARCHAR AS engine_location,
    t.$10::FLOAT AS wheel_base,
    t.$11::FLOAT AS length,
    t.$12::FLOAT AS width,
    t.$13::FLOAT AS height,
    t.$14::INT AS curb_weight,
    t.$15::VARCHAR AS engine_type,
    t.$16::VARCHAR AS num_of_cylinders,
    t.$17::INT AS engine_size,
    t.$18::VARCHAR AS fuel_system,
    t.$19::FLOAT AS bore,
    t.$20::FLOAT AS stroke,
    t.$21::FLOAT AS compression_ratio,
    t.$22::INT AS horsepower,
    t.$23::INT AS peak_rpm,
    t.$24::INT AS city_mpg,
    t.$25::INT AS highway_mpg,
    t.$26::FLOAT AS price
FROM 
    @my_stage (FILE_FORMAT => 'my_file_format') t;

    truncate automobile_data

insert into automobile_data

    SELECT 
    new_sqe.NEXTVAL::INT AS id, 
    t.$1::INT AS symboling,
    t.$2::INT AS normalized_losses,
    t.$3::STRING AS make,
    t.$4::STRING AS fuel_type,
    t.$5::STRING AS aspiration,
    t.$6::STRING AS num_of_doors,
    t.$7::STRING AS body_style,
    t.$8::STRING AS drive_wheels,
    t.$9::STRING AS engine_location,
    t.$10::FLOAT AS wheel_base,
    t.$11::FLOAT AS length,
    t.$12::FLOAT AS width,
    t.$13::FLOAT AS height,
    t.$14::INT AS curb_weight,
    t.$15::STRING AS engine_type,
    t.$16::STRING AS num_of_cylinders,
    t.$17::INT AS engine_size,
    t.$18::STRING AS fuel_system,
    t.$19::FLOAT AS bore,
    t.$20::FLOAT AS stroke,
    t.$21::FLOAT AS compression_ratio,
    t.$22::INT AS horsepower,
    t.$23::INT AS peak_rpm,
    t.$24::INT AS city_mpg,
    t.$25::INT AS highway_mpg,
    t.$26::FLOAT AS price
FROM 
    @my_stage/Automobile_data.csv2.gz (FILE_FORMAT => 'my_file_format') AS t;



// create task



    SELECT 
        id,
        SYMBOLING,
        NORMALIZED_LOSSES,
        MAKE,
        FUEL_TYPE,
        ASPIRATION,
        NUM_OF_DOORS,
        BODY_STYLE,
        DRIVE_WHEELS,
        ENGINE_LOCATION,
        WHEEL_BASE,
        LENGTH,
        WIDTH,
        HEIGHT,
        CURB_WEIGHT,
        ENGINE_TYPE,
        NUM_OF_CYLINDERS,
        ENGINE_SIZE,
        FUEL_SYSTEM,
        BORE,
        STROKE,
        COMPRESSION_RATIO,
        HORSEPOWER,
        PEAK_RPM,
        CITY_MPG,
        HIGHWAY_MPG,
        PRICE,
        METADATA$ACTION,
        METADATA$ISUPDATE
    FROM 
        snowflake_database.snowflake_schema.auto_stream
) AS stream
ON target.id = stream.id
WHEN MATCHED AND stream.METADATA$ACTION = 'INSERT' AND stream.METADATA$ISUPDATE = 'FALSE' THEN
    UPDATE SET
        target.SYMBOLING = stream.SYMBOLING,
        target.NORMALIZED_LOSSES = stream.NORMALIZED_LOSSES,
        target.MAKE = stream.MAKE,
        target.FUEL_TYPE = stream.FUEL_TYPE,
        target.ASPIRATION = stream.ASPIRATION,
        target.NUM_OF_DOORS = stream.NUM_OF_DOORS,
        target.BODY_STYLE = stream.BODY_STYLE,
        target.DRIVE_WHEELS = stream.DRIVE_WHEELS,
        target.ENGINE_LOCATION = stream.ENGINE_LOCATION,
        target.WHEEL_BASE = stream.WHEEL_BASE,
        target.LENGTH = stream.LENGTH,
        target.WIDTH = stream.WIDTH,
        target.HEIGHT = stream.HEIGHT,
        target.CURB_WEIGHT = stream.CURB_WEIGHT,
        target.ENGINE_TYPE = stream.ENGINE_TYPE,
        target.NUM_OF_CYLINDERS = stream.NUM_OF_CYLINDERS,
        target.ENGINE_SIZE = stream.ENGINE_SIZE,
        target.FUEL_SYSTEM = stream.FUEL_SYSTEM,
        target.BORE = stream.BORE,
        target.STROKE = stream.STROKE,
        target.COMPRESSION_RATIO = stream.COMPRESSION_RATIO,
        target.HORSEPOWER = stream.HORSEPOWER,
        target.PEAK_RPM = stream.PEAK_RPM,
        target.CITY_MPG = stream.CITY_MPG,
        target.HIGHWAY_MPG = stream.HIGHWAY_MPG,
        target.PRICE = stream.PRICE
WHEN NOT MATCHED THEN
    INSERT (
        id, SYMBOLING, NORMALIZED_LOSSES, MAKE, FUEL_TYPE, ASPIRATION, NUM_OF_DOORS,
        BODY_STYLE, DRIVE_WHEELS, ENGINE_LOCATION, WHEEL_BASE, LENGTH, WIDTH, HEIGHT, 
        CURB_WEIGHT, ENGINE_TYPE, NUM_OF_CYLINDERS, ENGINE_SIZE, FUEL_SYSTEM, BORE, 
        STROKE, COMPRESSION_RATIO, HORSEPOWER, PEAK_RPM, CITY_MPG, HIGHWAY_MPG, PRICE
    ) VALUES (
        stream.id, stream.SYMBOLING, stream.NORMALIZED_LOSSES, stream.MAKE, stream.FUEL_TYPE, stream.ASPIRATION,
        stream.NUM_OF_DOORS, stream.BODY_STYLE, stream.DRIVE_WHEELS, stream.ENGINE_LOCATION, stream.WHEEL_BASE,
        stream.LENGTH, stream.WIDTH, stream.HEIGHT, stream.CURB_WEIGHT, stream.ENGINE_TYPE, stream.NUM_OF_CYLINDERS,
        stream.ENGINE_SIZE, stream.FUEL_SYSTEM, stream.BORE, stream.STROKE, stream.COMPRESSION_RATIO, 
        stream.HORSEPOWER, stream.PEAK_RPM, stream.CITY_MPG, stream.HIGHWAY_MPG, stream.PRICE
    );

    /// create or replace curated_table
   CREATE OR REPLACE TABLE snowflake_database.curated_schema.curated_automobile_data
CLONE snowflake_database.snowflake_schema.automobile_data;


// creat stream on the table 
create or replace stream curated_stream on TABLE  SNOWFLAKE_DATABASE.CURATED_SCHEMA.curated_automobile_data
append_only =TRUE



// CREATE STREAM FOR THE LANDING ZONE 
create or replace stream LANING_stream on TABLE  SNOWFLAKE_DATABASE.SNOWFLAKE_SCHEMA.automobile_data
append_only =TRUE

//CREATE TABLE IN THE COMSUMPTION ZONE 
CREATE OR REPLACE TABLE snowflake_database.comsumption_schema.automobile_consumption_zone AS
SELECT 
    id ,
    MAKE,
    FUEL_TYPE,
    ASPIRATION,
    NUM_OF_DOORS,
    BODY_STYLE,
    DRIVE_WHEELS,
    ENGINE_LOCATION,
    CURB_WEIGHT,
    ENGINE_TYPE,
    NUM_OF_CYLINDERS,
    ENGINE_SIZE,
    FUEL_SYSTEM,
    BORE,
    STROKE,
    COMPRESSION_RATIO,
    HORSEPOWER,
    PEAK_RPM,
    CITY_MPG,
    HIGHWAY_MPG,
    PRICE,
    SUM(NORMALIZED_LOSSES) AS SUM_OF_NORMALIZED_LOSSES,
    SUM(CASE WHEN HORSEPOWER != 0 THEN PRICE / HORSEPOWER ELSE NULL END) AS PRICE_PER_HORSEPOWER,
    SUM(CASE WHEN CURB_WEIGHT != 0 THEN PRICE / CURB_WEIGHT ELSE NULL END) AS "Price_per_Curb_Weight",
    (SUM(CITY_MPG) + SUM(HIGHWAY_MPG)) / 2 AS Fuel_Efficiency,
    SUM(CASE WHEN CURB_WEIGHT != 0 THEN HORSEPOWER / CURB_WEIGHT ELSE NULL END) AS Engine_Power_to_Weight_Ratio,
    SUM(CASE WHEN ENGINE_SIZE != 0 THEN PRICE / ENGINE_SIZE ELSE NULL END) AS PRICE_PER_ENGINE_SIZE,
    SUM(CASE WHEN COMPRESSION_RATIO !=0 THEN PRICE/COMPRESSION_RATIO ELSE NULL END) AS TOTAL_COMPRESSION_RATION,
    SUM(CASE WHEN PEAK_RPM !=0 THEN PRICE/PEAK_RPM ELSE NULL END ) AS PRICE_PER_PEAK,
    sum(case when length !=0 then price/length else null end ) as price_per_length,
    sum(case when HEIGHT !=0 then price/HEIGHT else null end ) as  price_per_height,
    sum(case when width !=0 then price/width else null end ) as  price_per_width
FROM 
    SNOWFLAKE_DATABASE.CURATED_SCHEMA.CURATED_AUTOMOBILE_DATA
GROUP BY 
    id,
    MAKE,
    FUEL_TYPE,
    ASPIRATION,
    NUM_OF_DOORS,
    BODY_STYLE,
    DRIVE_WHEELS,
    ENGINE_LOCATION,
    WHEEL_BASE,
    CURB_WEIGHT,
    ENGINE_TYPE,
    NUM_OF_CYLINDERS,
    ENGINE_SIZE,
    FUEL_SYSTEM,
    BORE,
    STROKE,
    COMPRESSION_RATIO,
    HORSEPOWER,
    PEAK_RPM,
    CITY_MPG,
    HIGHWAY_MPG,
    PRICE; 


/create task on consumption zone
CREATE OR REPLACE TASK consumption_task
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '2 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('snowflake_database.curated_schema.curated_stream')
AS
MERGE INTO snowflake_database.comsumption_schema.automobile_consumption_zone AS target
USING (
    SELECT 
        id,
        MAKE,
        FUEL_TYPE,
        ASPIRATION,
        NUM_OF_DOORS,
        BODY_STYLE,
        DRIVE_WHEELS,
        ENGINE_LOCATION,
        CURB_WEIGHT,
        ENGINE_TYPE,
        NUM_OF_CYLINDERS,
        ENGINE_SIZE,
        FUEL_SYSTEM,
        BORE,
        STROKE,
        COMPRESSION_RATIO,
        HORSEPOWER,
        PEAK_RPM,
        CITY_MPG,
        HIGHWAY_MPG,
        PRICE,
        SUM(NORMALIZED_LOSSES) AS SUM_OF_NORMALIZED_LOSSES,
        SUM(CASE WHEN HORSEPOWER != 0 THEN PRICE / HORSEPOWER ELSE NULL END) AS PRICE_PER_HORSEPOWER,
        SUM(CASE WHEN CURB_WEIGHT != 0 THEN PRICE / CURB_WEIGHT ELSE NULL END) AS Price_per_Curb_Weight,
        (SUM(CITY_MPG) + SUM(HIGHWAY_MPG)) / 2 AS Fuel_Efficiency,
        SUM(CASE WHEN CURB_WEIGHT != 0 THEN HORSEPOWER / CURB_WEIGHT ELSE NULL END) AS Engine_Power_to_Weight_Ratio,
        SUM(CASE WHEN ENGINE_SIZE != 0 THEN PRICE / ENGINE_SIZE ELSE NULL END) AS PRICE_PER_ENGINE_SIZE,
        SUM(CASE WHEN COMPRESSION_RATIO != 0 THEN PRICE / COMPRESSION_RATIO ELSE NULL END) AS TOTAL_COMPRESSION_RATIO,
        SUM(CASE WHEN PEAK_RPM != 0 THEN PRICE / PEAK_RPM ELSE NULL END) AS PRICE_PER_PEAK,
        SUM(CASE WHEN LENGTH != 0 THEN PRICE / LENGTH ELSE NULL END) AS PRICE_PER_LENGTH,
        SUM(CASE WHEN HEIGHT != 0 THEN PRICE / HEIGHT ELSE NULL END) AS PRICE_PER_HEIGHT,
        SUM(CASE WHEN WIDTH != 0 THEN PRICE / WIDTH ELSE NULL END) AS PRICE_PER_WIDTH,
        METADATA$ACTION AS metadata_action,
        METADATA$ISUPDATE AS metadata_isupdate
    FROM 
        SNOWFLAKE_DATABASE.CURATED_SCHEMA.CURATED_AUTOMOBILE_DATA
) AS curated_stream
ON target.id = curated_stream.id
WHEN MATCHED AND curated_stream.metadata_action = 'INSERT' AND curated_stream.metadata_isupdate = FALSE THEN
    UPDATE SET
        target.MAKE = curated_stream.MAKE,
        target.FUEL_TYPE = curated_stream.FUEL_TYPE,
        target.ASPIRATION = curated_stream.ASPIRATION,
        target.NUM_OF_DOORS = curated_stream.NUM_OF_DOORS,
        target.BODY_STYLE = curated_stream.BODY_STYLE,
        target.DRIVE_WHEELS = curated_stream.DRIVE_WHEELS,
        target.ENGINE_LOCATION = curated_stream.ENGINE_LOCATION,
        target.CURB_WEIGHT = curated_stream.CURB_WEIGHT,
        target.ENGINE_TYPE = curated_stream.ENGINE_TYPE,
        target.NUM_OF_CYLINDERS = curated_stream.NUM_OF_CYLINDERS,
        target.ENGINE_SIZE = curated_stream.ENGINE_SIZE,
        target.FUEL_SYSTEM = curated_stream.FUEL_SYSTEM,
        target.BORE = curated_stream.BORE,
        target.STROKE = curated_stream.STROKE,
        target.COMPRESSION_RATIO = curated_stream.COMPRESSION_RATIO,
        target.HORSEPOWER = curated_stream.HORSEPOWER,
        target.PEAK_RPM = curated_stream.PEAK_RPM,
        target.CITY_MPG = curated_stream.CITY_MPG,
        target.HIGHWAY_MPG = curated_stream.HIGHWAY_MPG,
        target.PRICE = curated_stream.PRICE,
        target.SUM_OF_NORMALIZED_LOSSES = curated_stream.SUM_OF_NORMALIZED_LOSSES,
        target.PRICE_PER_HORSEPOWER = curated_stream.PRICE_PER_HORSEPOWER,
        target.Price_per_Curb_Weight = curated_stream.Price_per_Curb_Weight,
        target.Fuel_Efficiency = curated_stream.Fuel_Efficiency,
        target.Engine_Power_to_Weight_Ratio = curated_stream.Engine_Power_to_Weight_Ratio,
        target.PRICE_PER_ENGINE_SIZE = curated_stream.PRICE_PER_ENGINE_SIZE,
        target.TOTAL_COMPRESSION_RATIO = curated_stream.TOTAL_COMPRESSION_RATIO,
        target.PRICE_PER_PEAK = curated_stream.PRICE_PER_PEAK,
        target.PRICE_PER_LENGTH = curated_stream.PRICE_PER_LENGTH,
        target.PRICE_PER_HEIGHT = curated_stream.PRICE_PER_HEIGHT,
        target.PRICE_PER_WIDTH = curated_stream.PRICE_PER_WIDTH
WHEN NOT MATCHED THEN 
    INSERT (
        id, MAKE, FUEL_TYPE, ASPIRATION, NUM_OF_DOORS, BODY_STYLE, DRIVE_WHEELS,
        ENGINE_LOCATION, CURB_WEIGHT, ENGINE_TYPE, NUM_OF_CYLINDERS, ENGINE_SIZE,
        FUEL_SYSTEM, BORE, STROKE, COMPRESSION_RATIO, HORSEPOWER, PEAK_RPM,
        CITY_MPG, HIGHWAY_MPG, PRICE, SUM_OF_NORMALIZED_LOSSES, PRICE_PER_HORSEPOWER,
        Price_per_Curb_Weight, Fuel_Efficiency, Engine_Power_to_Weight_Ratio,
        PRICE_PER_ENGINE_SIZE, TOTAL_COMPRESSION_RATIO, PRICE_PER_PEAK,
        PRICE_PER_LENGTH, PRICE_PER_HEIGHT, PRICE_PER_WIDTH
    ) VALUES (
        curated_stream.id, curated_stream.MAKE, curated_stream.FUEL_TYPE, curated_stream.ASPIRATION,
        curated_stream.NUM_OF_DOORS, curated_stream.BODY_STYLE, curated_stream.DRIVE_WHEELS,
        curated_stream.ENGINE_LOCATION, curated_stream.CURB_WEIGHT, curated_stream.ENGINE_TYPE,
        curated_stream.NUM_OF_CYLINDERS, curated_stream.ENGINE_SIZE, curated_stream.FUEL_SYSTEM,
        curated_stream.BORE, curated_stream.STROKE, curated_stream.COMPRESSION_RATIO,
        curated_stream.HORSEPOWER, curated_stream.PEAK_RPM, curated_stream.CITY_MPG,
        curated_stream.HIGHWAY_MPG, curated_stream.PRICE, curated_stream.SUM_OF_NORMALIZED_LOSSES,
        curated_stream.PRICE_PER_HORSEPOWER, curated_stream.Price_per_Curb_Weight,
        curated_stream.Fuel_Efficiency, curated_stream.Engine_Power_to_Weight_Ratio,
        curated_stream.PRICE_PER_ENGINE_SIZE, curated_stream.TOTAL_COMPRESSION_RATIO,
        curated_stream.PRICE_PER_PEAK, curated_stream.PRICE_PER_LENGTH,
        curated_stream.PRICE_PER_HEIGHT, curated_stream.PRICE_PER_WIDTH
    );
 
