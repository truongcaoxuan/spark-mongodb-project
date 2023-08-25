# Stackoverflow's user behavior analysis project

## I. Download Dataset

Download [Dataset](https://drive.google.com/drive/folders/1uq4TNKlSE-a_UuVSUSidartcUtRGmS30)

## II. Guide to install MongoDB

* Install MongoDB Community Edition on Windows - *a flexible document data model along with support for ad-hoc queries, secondary indexing, and real-time aggregations to provide powerful ways to access and analyze data.*: <https://www.mongodb.com/docs/v5.0/tutorial/install-mongodb-on-windows/>
* Install MongoDB Shell - *is the quickest way to connect to (and work with) MongoDB* : <https://www.mongodb.com/docs/mongodb-shell/install/#std-label-mdb-shell-install>
* Install MongoDB Compass (GUI) - *Easily explore and manipulate database with Compass, the GUI for MongoDB*: <https://www.mongodb.com/docs/compass/master/install/>

Edit **`Path`** variable from environment variable and add new values below

```path
C:\Program Files\MongoDB\Server\6.0\bin
```

```path
C:\ProgramFiles\MongoDB\mongosh\
```

## III. Run command

### 1. Import data to mongodb

Open Command Prompt and run command below:

```mongoimport
mongoimport --type csv -d STODB -c Answers   --headerline --drop --file Answers.csv
```

```mongoimport
mongoimport --type csv -d STODB -c Questions --headerline --drop --file Questions.csv
```

### 2. Run command project

```spark
spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 UserBehaviorAnalysis.py
```

## III.  Preview result in code

### Requirement 1: Count programing languages (Java, Python, C++, C#, Go, Ruby, Javascript, PHP, HTML, CSS, SQL)

*TODO: use regex to extract the programming languages that appeared in each question. Then use Aggregation operations to sum each language.*

```result
+-------------------+-----+
|Programing Language|count|
+-------------------+-----+
|                 C#|25037|
|                C++|18918|
|         Javascript|11740|
|                CSS|22656|
|               HTML|55131|
|                PHP|36538|
|                SQL|63072|
|                 Go|42706|
|               Ruby| 7337|
|             Python|21578|
|               Java|65142|
+-------------------+-----+
```

### Requirement 2: Get 20 domain mostly used

*TODO: Use regex to extract urls, then apply some string processing to `get domain names`, and finally use Aggregation to group.*

```result
+-----------------------+-----+
|Domain                 |count|
+-----------------------+-----+
|i.stack.imgur.com      |38955|
|jsfiddle.net           |21593|
|github.com             |10518|
|stackoverflow.com      |6012 |
|pastebin.com           |3181 |
|en.wikipedia.org       |2875 |
|gist.github.com        |1688 |
|codepen.io             |1571 |
|i.imgur.com            |1306 |
|developer.android.com  |1277 |
|code.google.com        |1189 |
|imgur.com              |917  |
|docs.oracle.com        |916  |
|developers.google.com  |888  |
|localhost              |788  |
|jsbin.com              |684  |
|example.com            |681  |
|developers.facebook.com|611  |
|developer.apple.com    |565  |
|maxcdn.bootstrapcdn.com|532  |
+-----------------------+-----+
only showing top 20 rows
```

### Requirement 3: Get score of each user in each day

*TODO: Need to know how many `points the User will get on a certain day`.*

```result
+-----------+------------+-----+----------+
|OwnerUserId|CreationDate|Score|TotalScore|
+-----------+------------+-----+----------+
|          1|  2008-08-04|    7|         7|
|          1|  2008-08-14|    2|         9|
|          1|  2008-08-17|    2|        11|
|          1|  2008-08-28|   11|        22|
|          1|  2008-11-26|   10|        32|
|          1|  2008-12-22|   11|        43|
|          1|  2008-12-30|    3|        46|
|          1|  2009-01-08|   20|        66|
|          1|  2009-06-07|   55|       121|
|          1|  2009-07-19|    4|       125|
|          1|  2009-10-08|   22|       147|
|          1|  2009-10-08|   28|       175|
|          1|  2009-11-21|   32|       207|
|          1|  2009-12-03|    4|       211|
|          1|  2010-04-23|    1|       212|
|          1|  2012-02-24|   78|       290|
|          3|  2008-11-16|    0|         0|
|          3|  2009-01-09|    4|         4|
|          3|  2009-01-12|   13|        17|
|          3|  2009-02-09|   49|        66|
+-----------+------------+-----+----------+
only showing top 20 rows
```

### Requirement 4: Get score of each user in range of time

*TODO: Calculates the `total score` obtained by the User asking a question over a period of time. For example, you want to calculate how many points users get from January 1, 2008 to January 1, 2009 from asking questions.*

```result
+-----------+----------+
|OwnerUserId|TotalScore|
+-----------+----------+
|          1|        46|
|          3|         0|
|          4|         8|
|          5|       158|
|          9|        13|
|         13|       334|
|         17|        70|
|         20|        21|
|         22|         6|
|         23|        27|
|         25|        10|
|         26|        63|
|         27|         9|
|         29|       237|
|         33|       225|
|         34|        29|
|         35|        55|
|         36|        32|
|         37|        13|
|         39|        67|
+-----------+----------+
only showing top 20 rows
```

### Requirement 5: Find questions with more than 5 answers

If the `question has more than 5 answers`, it will be counted as good. You will need to find out how many questions are being counted as good.

To accomplish this, you will need to use Join operations to merge data from Answers with Collections, then use Aggregation operations to group, calculate how many answers each question has. Finally, use the filter() function to filter out questions with more than 5 answers.

Note: Because the operation can take a lot of time, please use the Bucket Join mechanism to partition the data first.

Results will need to be sorted by question ID

```result
+--------------------+----------+------------+-----+-----------+-----+--------------------+--------+--------------+
|                Body|ClosedDate|CreationDate|   Id|OwnerUserId|Score|               Title|ParentId|Number_answers|
+--------------------+----------+------------+-----+-----------+-----+--------------------+--------+--------------+
|<p>I would like t...|      null|  2008-08-03|  650|        143|   79|Automatically upd...|     650|             6|
|<p>I'm setting up...|      null|  2008-08-04| 1390|         60|   18|Is Windows Server...|    1390|             6|
|<P>I want to be a...|      null|  2008-08-05| 2300|        193|   10|How do I traverse...|    2300|             6|
|<p>How do I page ...|      null|  2008-08-05| 2840|        383|   32|Paging SQL Server...|    2840|             6|
|<p>I have a very ...|      null|  2008-08-06| 3470|        383|   12|How do I Transfor...|    3470|             6|
|<p>I'm pretty new...|      null|  2008-08-08| 5880|        721|    6|Are there any neg...|    5880|             6|
|<P>I've been usin...|      null|  2008-08-08| 6440|        560|   17|.NET 3.5 Redistri...|    6440|             6|
|<p>I have a ASP.N...|      null|  2008-08-09| 6530|        233|    7|Where should I pu...|    6530|             6|
|<p>What tools wou...|      null|  2008-08-10| 7190|        371|    9|Setting up Contin...|    7190|             6|
|<p>Is there a gen...|      null|  2008-08-11| 8140|        277|   14|Suggestions for A...|    8140|             6|
|<p>I've been play...|      null|  2008-08-13|10300|        203|   11|Validating posted...|   10300|             6|
|<p>So, the answer...|2015-10-29|  2008-08-14|10600|       1219|    6|What's the best d...|   10600|             6|
|<p>Does anyone us...|      null|  2008-08-14|10610|        805|    8|Pre-built regular...|   10610|             6|
|<p>I'm guessing i...|      null|  2008-08-14|11200|         26|    1|T-Sql date format...|   11200|             6|
|<p>My license for...|      null|  2008-08-14|11520|       1327|    3|What are the list...|   11520|             6|
|<p>I've got some ...|      null|  2008-08-14|11690|        337|    6|How can I get Uni...|   11690|             6|
|<p>I know this si...|      null|  2008-08-19|15470|        692|   15|How do I get rid ...|   15470|             6|
|<p>I've seen proj...|      null|  2008-08-19|16320|       1463|    3|Should DB layer m...|   16320|             6|
|<p>I was writing ...|      null|  2008-08-19|16460|       1801|    2|Removing N items ...|   16460|             6|
|<p>Admittedly thi...|      null|  2008-08-19|16940|        230|    1|VS 2008 - Detacha...|   16940|             6|
+--------------------+----------+------------+-----+-----------+-----+--------------------+--------+--------------+
only showing top 20 rows

+--------------------+----------+------------+--------+-----------+-----+--------------------+--------+--------------+
|                Body|ClosedDate|CreationDate|      Id|OwnerUserId|Score|               Title|ParentId|Number_answers|
+--------------------+----------+------------+--------+-----------+-----+--------------------+--------+--------------+
|<p>This is defini...|      null|  2009-01-02|  406760|      22656|  363|What's your most ...|  406760|           408|
|<p>This is a <a h...|2010-06-10|  2008-09-01|   38210|       1944|  338|What non-programm...|   38210|           316|
|<p>I want to see ...|      null|  2008-08-23|   23930|       1337|   64|Factorial Algorit...|   23930|           129|
|<p>Visual Studio ...|      null|  2008-09-19|  100420|       9611|  182|Hidden Features o...|  100420|           100|
|<p>I always thoug...|      null|  2008-09-02|   40480|       4315| 3613|Is Java "pass-by-...|   40480|            69|
|<p>If you had to ...|2012-09-13|  2009-01-29|  490420|       9931|  142|Favorite (Clever)...|  490420|            67|
|<p>What add-in/se...|2011-09-20|  2008-09-19|  106340|      17176|   81|What is your favo...|  106340|            61|
|<p>C# desktop app...|      null|  2010-01-28| 2155930|      65393|  644|Fixing "The break...| 2155930|            59|
|<blockquote>\n  <...|2011-03-29|  2008-10-22|  226970|      28722|   70|What's the  best ...|  226970|            55|
|<p>I mean, is the...|2011-11-08|  2008-10-14|  202750|      68336|   47|Is there a human ...|  202750|            51|
|<p>I've heard a l...|      null|  2009-08-02| 1218390|     106140| 1129|What is your most...| 1218390|            50|
|<p>In every insta...|      null|  2013-06-11|17054000|    1489990|  344|"cannot resolve s...|17054000|            49|
|<p>As far as I ca...|      null|  2008-08-23|   24270|       2131|  126|What's the point ...|   24270|            45|
|<p>PHP treats all...|      null|  2008-10-06|  173400|       5291|  502|How to check if P...|  173400|            43|
|<p>What are the b...|2012-07-31|  2008-11-08|  274230|      31649|   58|What are the best...|  274230|            43|
|<p>I have an obje...|      null|  2009-04-08|  728360|      49695| 1520|How do I correctl...|  728360|            42|
|<p>I have this <c...|      null|  2009-06-17| 1009160|      68920|   58|Reverse the order...| 1009160|            41|
|<p>I have breakpo...|      null|  2008-09-15|   64790|       8761|   68|Why aren't my bre...|   64790|            40|
|<h2>Syntax</h2>\n...|      null|  2008-10-08|  182630|      20946|  508|jQuery Tips and T...|  182630|            40|
|<p>I recently poi...|      null|  2010-08-05| 3412730|     389236|   18|Code-golf: Output...| 3412730|            40|
+--------------------+----------+------------+--------+-----------+-----+--------------------+--------+--------------+
only showing top 20 rows
```

### Requirement 6: Find active users

A User to be counted as Active will need to satisfy one of the following requirements:

* There are `more than 50 answers` or the `total score obtained when the answer is greater than 500`.

```result
+------------+--------------+-------------------+
|answerUserId|number_answers|total_answers_score|
+------------+--------------+-------------------+
|     1760502|             1|                549|
|     1687195|             1|                557|
|      610231|             1|                558|
|     1792483|             1|                620|
|      216222|             1|                633|
|      275993|             1|                644|
|     1496935|             1|                684|
|       47544|             1|                712|
|      527768|             1|                719|
|      804978|             1|                791|
|     2090682|             1|                796|
|      233202|             1|                831|
|     2421200|             1|                838|
|      635960|             1|                889|
|      132867|             1|               1010|
|       14250|             1|               1059|
|     1426193|             1|               2419|
|      279452|             2|                523|
|     1418484|             2|                552|
|       24179|             2|                596|
+------------+--------------+-------------------+
only showing top 20 rows

+------------+--------------+-------------------+
|answerUserId|number_answers|total_answers_score|
+------------+--------------+-------------------+
|     1818488|            67|                 11|
|      979580|            58|                 17|
|      108207|            61|                 19|
|     5015238|           116|                 19|
|     5391065|            77|                 21|
|      644603|            51|                 22|
|     3629249|           130|                 22|
|      794226|            51|                 23|
|     4786421|            53|                 23|
|     4746376|            58|                 23|
|     2413201|            96|                 24|
|     2385479|            51|                 25|
|     2293560|            89|                 25|
|     2046598|            51|                 26|
|      560435|            51|                 26|
|     3667257|            51|                 26|
|     2081982|            51|                 26|
|     5425825|            68|                 26|
|     3193455|            58|                 27|
|     3874768|            51|                 28|
+------------+--------------+-------------------+
only showing top 20 rows
```

* There were `more than 5 answers the same day` the question was created.

```result
+-----------+---------------------------------+
|OwnerUserId|number_answers_same_date_question|
+-----------+---------------------------------+
|         35|                                6|
|         37|                                6|
|         40|                                6|
|         55|                                6|
|         56|                                6|
|        103|                                6|
|        120|                                6|
|        143|                                6|
|        192|                                6|
|        199|                                6|
|        202|                                6|
|        276|                                6|
|        318|                                6|
|        322|                                6|
|        323|                                6|
|        438|                                6|
|        459|                                6|
|        475|                                6|
|        476|                                6|
|        483|                                6|
+-----------+---------------------------------+
only showing top 20 rows
```
