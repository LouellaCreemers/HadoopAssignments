ordersCSV = LOAD '/user/maria_dev/diplomacy/orders.csv'  
USING PigStorage(',') AS 
(game_id:chararray, 
unit_id:chararray, 
unit_order:chararray, 
location:chararray, 
target:chararray, 
target_dest:chararray, 
success:int, 
reason:chararray, 
turn_num:chararray);

targetFilteredList = FILTER ordersCSV BY (target matches '.*Holland$*.');
groupedList = GROUP targetFilteredList BY (location, target);
unorderedList = FOREACH groupedList GENERATE FLATTEN(group), COUNT(targetFilteredList);
orderedList = ORDER unorderedList BY location ASC;

DUMP orderedList;