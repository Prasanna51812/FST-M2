--Load input file from HDFS
inputEpisodeFiles = LOAD 'hdfs:///user/javeed/project' USING PigStorage('\t') AS ( name:chararray,line:chararray);

--FILTER inputFile BY name;
groupByName = GROUP inputEpisodeFiles BY name;

totalnamesCount = FOREACH groupByName GENERATE $0 as name, COUNT($1) as no_of_dialogue_lines;

--Sorting
nameordered = ORDER totalnamesCount BY no_of_dialogue_lines DESC;

--Delete output folder
rmf hdfs:///user/javeed/project/PigOutput

--Store the output
STORE nameordered INTO 'hdfs:///user/javeed/project/PigOutput';
