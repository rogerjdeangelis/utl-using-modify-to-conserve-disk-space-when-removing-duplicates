# utl-using-modify-to-conserve-disk-space-when-removing-duplicates
Using modify to conserve disk space when removing duplicates.  

    Using modify to conserve disk space when removing duplicates

    For infrequent dups this should be very fast.

    This is a usefull techinque, especiall when disk space is limited.
    Reminds me a little of data normization;

    see Paul Dorfman's paper
    https://tinyurl.com/yba7kxqy
    https://www.sas.com/content/dam/SAS/support/en/sas-global-forum-proceedings/2018/2426-2018.pdf

    github
    https://tinyurl.com/y9blowrf
    https://github.com/rogerjdeangelis/utl-using-modify-to-conserve-disk-space-when-removing-duplicates

    INPUT
    =====

    Keep only the most recent date grouped by Test_id and part_id

    WORK.HAVE total obs=20
                                                                                       RULES
     RID TEST_ID PART_ID  DATE SMOKER  LIFESTYLE    RESULT AGE HEIGHT WEIGHT ALCOHOL | Keep latest by Test and Part
                                                                                     |
                                                                                     |
       1    A       2    21256   N     Sedentary      98    45   162    57     102   | A 2 -------+ Drop
       2    A       5    21260   N     Semi-active    97    47   174    72     111   |            |
       3    A       4    21259   N     Semi-active    93    42   180    70      76   |            |
       4    A       9    21265   N     Sedentary      82    47   186    78      41   | A 9-+ Drop |
       5    A       8    21263   Y     Sedentary      75    42   167    69      54   |     |      |
       6    A       1    21255   N     Semi-active    70    35   194    79     116   |     | DUPS | DUPS
       7    A       3    21258   N     Couch-potato   66    58   171    64      27   |     |      |
       8    A       9    21266   N     Sedentary      64    47   186    78      41   | A 9-+ KEEP |
       9    A       9    21264   N     Sedentary      57    47   186    78      41   | A 9-+ Drop |
      10    A       6    21261   Y     Fitness        55    38   174    68      19   |            |
      11    A       7    21262   Y     Semi-active    54    38   193    81      16   |            |
      12    A       2    21257   N     Sedentary      51    45   162    57     102   | A 2 -------+ KEEP(LATEST)
      13    B       5    21259   N     Semi-active    99    47   174    72     111   | B 5-+ Drop
      14    B       6    21261   Y     Fitness        98    38   174    68      19   |     |
      15    B       3    21257   N     Couch-potato   96    58   171    64      27   |     | DUPS
      16    B       1    21255   N     Semi-active    86    35   194    79     116   |     |
      17    B       5    21261   N     Semi-active    73    47   174    72     111   | B 5-+ KEEP(LATEST)
      18    B       4    21258   N     Semi-active    60    42   180    70      76   |
      19    B       2    21256   N     Sedentary      57    45   162    57     102   |
      20    B       7    21262   Y     Semi-active    53    38   193    81      16   |

    EXAMPLE OUTPUT
    --------------


     WORK.HAVE total obs=16

      RID TEST_ID PART_ID   DATE SMOKER  LIFESTYLE    RESULT AGE HEIGHT WEIGHT ALCOHOL

                                                                                       1 Dropped
        2    A       5     21260   N     Semi-active    97    47   174    72     111
        3    A       4     21259   N     Semi-active    93    42   180    70      76
                                                                                       4 Dropped
        5    A       8     21263   Y     Sedentary      75    42   167    69      54
        6    A       1     21255   N     Semi-active    70    35   194    79     116
        7    A       3     21258   N     Couch-potato   66    58   171    64      27
        8    A       9     21266   N     Sedentary      64    47   186    78      41
                                                                                       9 Dropped
       10    A       6     21261   Y     Fitness        55    38   174    68      19
       11    A       7     21262   Y     Semi-active    54    38   193    81      16
       12    A       2     21257   N     Sedentary      51    45   162    57     102
                                                                                      13 Dropped
       14    B       6     21261   Y     Fitness        98    38   174    68      19
       15    B       3     21257   N     Couch-potato   96    58   171    64      27
       16    B       1     21255   N     Semi-active    86    35   194    79     116
       17    B       5     21261   N     Semi-active    73    47   174    72     111
       18    B       4     21258   N     Semi-active    60    42   180    70      76
       19    B       2     21256   N     Sedentary      57    45   162    57     102
       20    B       7     21262   Y     Semi-active    53    38   193    81      16


    PROCESS
    =======

    * note we dropped all the satelite variables - saving disk space;
    proc sort data=have(keep=Rid Test_ID Part_ID Date) out=key_RID ;
      by Test_ID Part_ID Date;
    run ;

    /*

     BY TEST_ID PART_ID DATE

     WORK.KEY_RID total obs=20

      RID    TEST_ID    PART_ID     DATE

        6       A          1       21255
        1       A          2       21256
       12       A          2       21257
        7       A          3       21258
        3       A          4       21259
        2       A          5       21260
      ...
    */

    data dup_RID (keep=RID) ;
       set key_RID ;
       by Test_ID Part_ID ;
       if not last.Part_ID ;
    run ;

    /*
     record IDS to be delete

     WORK.DUP_RID total obs=4

      Obs    RID

       1       1
       2       9
       3       4
       4      13

     It may be faster to sort this on RID
     but it is not needed
    */

    data have ;
      set dup_RID ;
      modify have point=RID ;
      remove ;
    run;quit;



    OUTPUT
    ======

     see above

    *                _               _       _
     _ __ ___   __ _| | _____     __| | __ _| |_ __ _
    | '_ ` _ \ / _` | |/ / _ \   / _` |/ _` | __/ _` |
    | | | | | | (_| |   <  __/  | (_| | (_| | || (_| |
    |_| |_| |_|\__,_|_|\_\___|   \__,_|\__,_|\__\__,_|

    ;

    data have (sortedBy = Test_ID Result) ;
     retain rid;
     informat Test_ID $1. Part_ID $1. Date yymmdd10. Smoker $1. Lifestyle $12. ;
     input rid Test_ID Part_ID Date Result Age Height Weight Smoker Alcohol Lifestyle;
    cards4 ;
    01 A 2 2018-03-13 98 45 162 57 N 102 Sedentary
    02 A 5 2018-03-17 97 47 174 72 N 111 Semi-active
    03 A 4 2018-03-16 93 42 180 70 N 76 Semi-active
    04 A 9 2018-03-22 82 47 186 78 N 41 Sedentary
    05 A 8 2018-03-20 75 42 167 69 Y 54 Sedentary
    06 A 1 2018-03-12 70 35 194 79 N 116 Semi-active
    07 A 3 2018-03-15 66 58 171 64 N 27 Couch-potato
    08 A 9 2018-03-23 64 47 186 78 N 41 Sedentary
    09 A 9 2018-03-21 57 47 186 78 N 41 Sedentary
    10 A 6 2018-03-18 55 38 174 68 Y 19 Fitness nut
    11 A 7 2018-03-19 54 38 193 81 Y 16 Semi-active
    12 A 2 2018-03-14 51 45 162 57 N 102 Sedentary
    13 B 5 2018-03-16 99 47 174 72 N 111 Semi-active
    14 B 6 2018-03-18 98 38 174 68 Y 19 Fitness nut
    15 B 3 2018-03-14 96 58 171 64 N 27 Couch-potato
    16 B 1 2018-03-12 86 35 194 79 N 116 Semi-active
    17 B 5 2018-03-18 73 47 174 72 N 111 Semi-active
    18 B 4 2018-03-15 60 42 180 70 N 76 Semi-active
    19 B 2 2018-03-13 57 45 162 57 N 102 Sedentary
    20 B 7 2018-03-19 53 38 193 81 Y 16 Semi-active
    ;;;;
    run ; quit;


