1.根据俄罗斯选取信息（csv）得出以下结果，并且可以在AWS EMR上运行。
a. 得出每个地区的选举人数量
b. 得出每个地区选举人数量的中位数
2. Word Count. 根据马克吐温的文章，按照以下要求选择出单词，并且按照a.b的要求得出结果。
• Removing the endings “'” (single quote), “--”, “-”, “'s”, “ly”, “ed”, “ing”, “ness”, “)“, “_”, “;”,
“?”, “!”, “,”, “:”.
• Convert all words to lower case
• Remove leading “‘“ (single quote), “”” (double quote), “(“ and “_”.
a. 得出每个词及每个词的频率。（key:word value:count）.并且按照降序排列，结果应只有一个文件。
b. 无规则词组频率计算（key:词组 value:词组频率）.每两个相邻单词为一个词组，但是以下情况为一个词组：
“a cat a rat a bat”: "a cat"和 "cat a"为同一个词组。 每一行行末尾和一下行行首组成一个词组。
注意事项：每当map时候，文件被分布到各个服务器上，这时候每个小文件的尾和一下个文件的首应该组成一个词组。

3. 底下是美国2012年的税收表。按照州，邮编和税收水平分类。共分为6个税收水平。下表是一个示例：
FIPS STATE zipcode AGI_STUB N1 MARS1 MARS2 MARS4
1 AL 35004 1 1600.0000 990.0000 270.0000 300.0000
1 AL 35004 2 1310.0000 570.0000 400.0000 290.0000
1 AL 35004 3 900.0000 280.0000 500.0000 100.0000
1 AL 35004 4 590.0000 70.0000 490.0000 30.0000
1 AL 35004 5 480.0000 30.0000 440.0000 0.0000
1 AL 35004 6 50.0000 0.0000 50.0000 0.0000
The second column STATE give the abbreviation for the state. The third column indicates
the zipcode. The fourth column, AGI_STUB, gives the adjusted gross income given in
numbers 1-6 with the meaning listed below.
1 - $1 to $25,000
2 - $25,000 to $50,000
3 - $50,000 to $75,000
4 - $75,000 tor $100,000
5 - $100,000 to $200,000
6 - $200,000 or more

The fourth column, N1, givens the number of returns in the income level from the indicated
zipcode. So there were 1600 tax returns from zip code 35004 with income between $1 and
$25,000.

a. Write a Hadoop program to find the total number of tax returns filed in each state in
each category. Again the program needs two command line argument, the first the input
directory and the second the output directory. The input will be a file as described
above. The output will be a file(s) sorted by state. Each state will have the six levels of
income with the total of tax returns in each category.
（编写一个hadoop程序找出每个周每个交税水平的总税收。并且以州名和税收水平排序 如下图所示：）
AL 1 889920
AL 2 491150
AL 3 491150
AL 4 160160
AL 5 160160
AL 6 44840


There is five directory contains five questions jar files.
The output directory in each directory has the result.

To run every single program, you need to enter in each directory in order to run
jar file using following command.
For example: cd 2.1_WordCountSort/

1.1
hadoop jar vc.jar Russia2011/ gradeOutput
2.1 
hadoop jar wc.jar MarkTwain.txt gradeOutput
2.2 
hadoop jar wpc.jar MarkTwain.txt gradeOutput
3.  
hadoop jar taxreturn.jar 12taxData.csv gradeOutput

Every jar file is configured with main class. So only submit jar file, input files.
When adding steps,  setup arguments  with input directory and output directory.
