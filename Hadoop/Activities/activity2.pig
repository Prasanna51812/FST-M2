-- Load the input file from HDFS
inputFile = LOAD 'hdfs:///user/javeed/input.txt' AS (line:chararray);
-- Tokenize the lines in the file
words = FOREACH inputFile GENERATE FLATTEN(TOKENIZE(line)) AS word;
-- Group words by word
grpd = GROUP words BY word;
-- count the total number of words [REDUCE]
totalCount = FOREACH grpd GENERATE $0, COUNT($1);

--Store the output
STORE totalCount INTO 'hdfs:///user/javeed/PigOutput';
