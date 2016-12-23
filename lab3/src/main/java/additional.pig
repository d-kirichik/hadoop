FlightData = load 'sample.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER') AS
	(year:int,quarter:int,month:int,day_of_month:int,day_of_week:int,
	fl_date:chararray,unique_carrier:chararray,airline_id:int,carrier:chararray,
	tail_num:chararray,fl_num:chararray,origin_airport_id:int,origin_airport_seq_id:int,
	origin_city_market_id:int,dest_airport_id:int,wheels_on:chararray,arr_time:chararray,
	arr_delay:float,arr_delay_new:float,cancelled:float,cancellation_code:chararray,air_time:float,distance:float);

cancelledFlightThere = FILTER FlightData BY cancelled == 1.0 AND origin_airport_id < dest_airport_id;

airportPairThere = FOREACH cancelledFlightThere GENERATE (origin_airport_id, dest_airport_id) AS key;

cancelledFlightBack = FILTER FlightData BY cancelled == 1.0 AND dest_airport_id < origin_airport_id;

airportPairBack = FOREACH cancelledFlightBack GENERATE (dest_airport_id, origin_airport_id) AS key;

totalGroupped = COGROUP airportPairThere BY key, airportPairBack BY key;

counted = FOREACH totalGroupped GENERATE group AS airport_pair, (COUNT(airportPairThere) + COUNT(airportPairBack)) AS numCancelled;

countedOrdered = ORDER counted BY numCancelled;

STORE countedOrdered INTO 'additional_pig' USING PigStorage(',');
