# utl-using-modify-to-conserve-disk-space-when-removing-duplicates
Using modify to conserve disk space when removing duplicates.  

    Using modify to conserve disk space when removing duplicates 
    
    Nice comparison of HASH, DOW and MODIFY solutions on end of this message
    Bartosz Jablonski <yabwon@GMAIL.COM>

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
    
    
    *____             _
    | __ )  __ _ _ __| |_ ___  ___ ____
    |  _ \ / _` | '__| __/ _ \/ __|_  /
    | |_) | (_| | |  | || (_) \__ \/ /
    |____/ \__,_|_|   \__\___/|___/___|

    ;

    Hi Paul, Roger, and SAS-Lers

    Roger, thanks for the hint and great lecture reference!

    Paul, after reading the paper and your comments (since my copy of hash-book is still in shipping and I can't read examples 10.4 and 11.16) I decided to play with the hash tables and write myself a code that extracts duplicates in one datastep (lets cal

    SAS-Lers, I wrote some code and I would like to share (maybe you will consider it useful :-).

    As the first step I tried to prepare test data as close as I could to the paper's requirements, so:
    - I have ~80M rows in the table,
    - with 3 processing keys (Test_ID, Part_ID, and Date with 48 bytes of length),
    - I have ~11% of duplicated Part_IDs for each Test_ID,
    - one thing I wasn't sure was the number of unique values Test_IDs in total so I assumed 8 (so one Test_ID spans across ~10M obs.),

    /* test data set */
    libname t "E:\SAS_WORK_5400\test";
    data t.tests (sortedBy = Test_ID Result) ;
    length Test_ID Part_ID $ 20 Date 8;
    format Test_ID Part_ID $20. Date date11.;

    do Test_ID =
      "A0000000000000000000"
    , "B0000000000000000000"
    , "C0000000000000000000"
    , "D0000000000000000000"
    , "E0000000000000000000"
    , "F0000000000000000000"
    , "G0000000000000000000"
    , "H0000000000000000000"
    ;
      T_ID = "Z0000000000000000000"; /* variable for additional testing */
      Result = 3e7;
      do _N_ = 1 to 1e7 - 1;
        Part_ID = substr(Test_ID,1,1) || put(_N_, z19.);
        Date = '30sep2018'd/*today()*/ - int(ranuni(17)*6*365);
        Result + (-1);
        output;
      end;
      do _N_ = 1 to 1e7 - 1 by 10; /* duplicates */
        Part_ID = substr(Test_ID,1,1) || put(_N_, z19.);
        Date = '30sep2018'd/*today()*/ - int(ranuni(17)*6*365);
        Result + (-1);
        output;
      end;
      do _N_ = 1 to 1e7 - 1 by 50; /* some additional duplicates */
        Part_ID = substr(Test_ID,1,1) || put(_N_, z19.);
        Date = '30sep2018'd/*today()*/ - int(ranuni(17)*6*365);
        Result + (-1);
        output;
      end;

    end;
    run;

    Code uses 4 hashes and 2 DoW-loops and goes like that (I hope comments are ok):

    data _null_;
    if 0 then set t.tests(keep=Test_ID Part_ID date); length RID 8; /* setup variables' lengths */

    if _N_ = 1 then
    do; /* first hash is only for duplicates' RIDs,
           it is declared once for whole data step and
           will produce dataset used in modify statement
        */
    declare hash RIDs(ordered:'y');
    RIDs.DefineKey('RID');
    RIDs.DefineData('RID');
    RIDs.DefineDone();
    RIDs.clear();
    end;

    /* hashes 2nd, 3rd, and 4th are re-built for every iteration of main loop */
    /* second hash stores all Part_IDs to verify
       if a given Part_ID is duplicated or not
    */
    declare hash Part_IDs(hashexp:20);
    Part_IDs.DefineKey('Part_ID');
    Part_IDs.DefineDone();
    Part_IDs.clear();
    /* third hash stores only duplicated Part_IDs
       and is used in second DoW-loop to extract
       data of duplicated records
    */
    declare hash Part_IDs_dup(hashexp:20);
    Part_IDs_dup.DefineKey('Part_ID');
    Part_IDs_dup.DefineDone();
    Part_IDs_dup.clear();
    declare hiter Part_IDs_dup_I("Part_IDs_dup");

    /* first DoW-loop by values of Test_IDs
       it finds duplicated Part_IDs
    */
    do until(last.Test_ID);
     set t.tests(keep=Test_ID Part_ID date);
     by Test_ID;

     if 0 = Part_IDs.check() then do; _rc_ = Part_IDs_dup.add(); end; /* Part_ID is duplicated */
                             else do; _rc_ = Part_IDs.add();     end; /* first occurrence of a given Part_ID */
    end;
    _rc_ = Part_IDs.clear(); /* release memory since it is no longer needed */

    /* fourth hash stores all data for duplicates
       which are required to "un-duplicate" the dataset
    */
    declare hash Part_IDs2(hashexp:20, multidata:'y');
    Part_IDs2.DefineKey('Part_ID');
    Part_IDs2.DefineData('Part_ID', 'RID', 'date');
    Part_IDs2.DefineDone();
    Part_IDs2.clear();

    /* second DoW-loop works like this:
       if a given Part_ID is on the duplicates' list (i.e. inside Part_IDs_dup)
       then collect the data required, i.e. Date and RID
    */
    do until(last.Test_ID);
     set t.tests(keep=Test_ID Part_ID date) end = EOF curobs=curobs; /* curobs indicates the number of current observation (i.e. RID),  */
     by Test_ID;

     if 0 = Part_IDs_dup.check() then
        do;
            RID = curobs;
            _rc_ = Part_IDs2.add();
        end;
    end;


    /* for each duplicated Part_ID we want to store
       RIDs of "bad" records in the first hash,
       so as a firs step we...
    */
    do while(Part_IDs_dup_I.next()=0); /* ...loop through duplicated Part_IDs...*/

        max_dt =.; format max_dt date11.;
        min_co =.;
        _rc_ = Part_IDs2.RESET_DUP();
        do while(Part_IDs2.do_over() eq 0);
            max_dt = max_dt<>date; /* ... and we get the newest date */
        end;

        _rc_ = Part_IDs2.RESET_DUP();
        do while(Part_IDs2.do_over() eq 0);
            if max_dt = date then /* in case of doubled dates (two records with the same date)... */
            do;
                min_co = min_co<>-RID; /* ...select one with smallest RID (i.e. with max Result [since dataset is ordered by descending values of Result]) */
            end;
        end;

        _rc_ = Part_IDs2.RESET_DUP();
        do while(Part_IDs2.do_over() eq 0);
            /* eventually:
               exclude records with not the newest date
               or
               that with latest date but with not the smallest RID
            */
            if (max_dt ^= date) or (max_dt = date and -min_co ne RID) then RIDs.add(); /*output;*/
        end;
    end;

    _rc_ = Part_IDs.clear();
    _rc_ = Part_IDs2.clear();
    _rc_ = Part_IDs_dup.clear();

    if EOF then do; RIDs.output(dataset:'dup_RID'); stop; end;
    run;

    Additionally I did some comparisons of execution with code from Paul's paper
    /*
    data add_RID / view=add_RID ;
     set t.Tests (keep=Test_ID Part_ID date) ;
     RID = _N_ ;
    run ;
    proc sort data=add_RID out=key_RID ;
     by Test_ID Part_ID Date ;
    run ;
    data dup_RID (keep=RID) ;
     set key_RID ;
     by Test_ID Part_ID ;
     if not last.Part_ID ;
    run ;
    proc sort data=dup_RID ;
     by RID ;
    run ;
    */

    I did it on SAS machine with:
    -CPUCOUNT 4
    -THREADS
    -MEMSIZE 8G
    -REALMEMSIZE 6G
    -SORTSIZE 4G
    -SUMSIZE 4G
    the input dataset was on standard HDD, and WORK on SSD.

    Observations I have are following:
    1) Hash approach is longer to write and harder for inexperienced users (but it is good - since it is a chance to learn something new!)
    2) Hash worked ~100sec. while "Proc Sort et. all" was ~150sec.
    3) Hash used about 52% of the RAM that Sort used (2232447.45k vs. 4247607.45k, sortsize was set to 4G, so SAS used as much as could)
    4) Hash uses ~75M of Work space while Sort ~4.8G

    5) as Paul wrote in the paper, that RAM might be a bottle neck for hash solution, I did some additional test (in which used T_ID variable from input dataset instead of Test_ID) so there was only one big BY group (~80M obs.), in that case hash worked ~2

    All the best :-)
    Bart

    sob., 29 wrz 2018 o 18:25 Paul Dorfman <sashole@bellsouth.net> napisal(a):
    Roger,

    Thanks for you comments - all you're saying is true. To add a couple of tidbits:

    This trick was born out of sheer practical necessity while doing work for a client. The task was to clean up an ETL process pre-aggregating stuff for online reporting. The corporate DW from which the data were extracted was, uh, dusty; and so there wer
    his thread).

    Each contained a small number (relative to the total obs) of dupes by the DW insertion date and surrogate key. They needed to be killed before the processes could go forward, so the sort -> dedup - resort had been being applied to every such data set,
    be run overnight to make the data available for analysis every day in the morning, and it simply would not fit in the production window. The other factor was constantly running out of utility sort work disk space and having to deal with the system peop

    After examining the entire ETL (similar for each extract), it was clear that the narrowest bottleneck was in killing the dupes, so I concocted this method and tested it. Turned out, it was able to reduce the dupe killing time to about 20 minutes for al
    uch smaller than 1 TB, particularly if there're many to deal with. Needless to say, I encapsulated the thing in a macro parameterized to account for a number of scenarios - for example, the client wanted the option of saving the dupes separately for fu
    can easily ideate its essence).

    On a different note, the same can be done in a single DATA step without explicit sorting at all by using a hash table to identify the dupes to kill using a variation of the program 10.4 (or 11.16) shown in our hash book. I didn't opt for it at the time
    rint. Besides, the multi-step sort-keys-only solution was already doing the job just fine.

    Best regards
    Paul Dorfman


    --




