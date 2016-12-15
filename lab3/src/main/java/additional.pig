FlightData = load 'sample.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER') AS
	(year:int,quarter:int,month:int,day_of_month:int,day_of_week:int,
	fl_date:chararray,unique_carrier:chararray,airline_id:int,carrier:chararray,
	tail_num:chararray,fl_num:chararray,origin_airport_id:int,origin_airport_seq_id:int,
	origin_city_market_id:int,dest_airport_id:int,wheels_on:chararray,arr_time:chararray,
	arr_delay:float,arr_delay_new:float,cancelled:float,cancellation_code:chararray,air_time:float,distance:float);

cancelledFlight = FILTER FlightData BY cancelled == 1.0 AND origin_airport_id < dest_airport_id;

airportPair = FOREACH cancelledFlight GENERATE origin_airport_id, dest_airport_id;

grouppedAirportPair = GROUP airportPair BY (origin_airport_id, dest_airport_id);	

counted = FOREACH grouppedAirportPair GENERATE group AS airport_pair, COUNT(airportPair) AS num_cancelled;

counted_ordered = ORDER counted BY num_cancelled;

STORE counted_ordered INTO 'additional_pig' USING PigStorage(',');
