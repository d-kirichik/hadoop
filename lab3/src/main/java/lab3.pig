FlightData = load 'sample.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER') AS
	(year:int,quarter:int,month:int,day_of_month:int,day_of_week:int,
	fl_date:chararray,unique_carrier:chararray,airline_id:int,carrier:chararray,
	tail_num:chararray,fl_num:chararray,origin_airport_id:int,origin_airport_seq_id:int,
	origin_city_market_id:int,dest_airport_id:int,wheels_on:chararray,arr_time:chararray,
	arr_delay:float,arr_delay_new:float,cancelled:float,cancellation_code:chararray,air_time:float,distance:float);
AirportData = load 'airports.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER') AS (code:chararray,description:chararray);
JoinedTable = JOIN AirportData BY (int)code, FlightData BY dest_airport_id;
GrouppedTable = GROUP JoinedTable BY description;
result = FOREACH GrouppedTable GENERATE group AS description, MIN(JoinedTable.arr_delay) AS min_arr_delay,AVG(JoinedTable.arr_delay) AS avg_arr_delay,MAX(JoinedTable.arr_delay) AS max_arr_delay;
STORE result INTO 'output_pig' USING PigStorage(',');

/*
Количестов отменннеых рейсов из точки А в точку В и наоборот
создать пару. позиция неважно
сумма отмененных рейсов для пары
сортировка
*/
