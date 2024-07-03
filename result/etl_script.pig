data = LOAD 'pig_data1.txt' AS (brand:chararray,
               date:chararray,
               model:chararray,
               agent:chararray,
               country:chararray,
               price:int,
               code:chararray);

data_notnull = FILTER data BY brand is not null and model is not null and agent is not null;
price_up = filter data_notnull by price > 1000;

reordered = FOREACH price_up GENERATE code,
               UPPER(TRIM(brand)),
               model,
               date,
               country,
               price;

STORE reordered INTO 'car1';



--delete $ sign for price --

data2 = LOAD 'pig_data2.txt' USING PigStorage(',')
        AS (brand:chararray,
            date:chararray,
            model:chararray,
            agent:chararray,
            country:chararray,
            price:chararray,
            code:chararray);

data_notnull = FILTER data2 BY brand is not null and model is not null and agent is not null;
unique = DISTINCT data_notnull;

/*
price_up = filter data_notnull by price > 1000;
*/

reordered = FOREACH unique GENERATE code,
               brand,
               UPPER(TRIM(model)),
               REPLACE(date,'-','/'),
               country,
               SUBSTRING(price, 1,8);

--STORE reordered INTO 'hdfs://localhost/data/pig/car_table2';
STORE reordered INTO 'car2';
