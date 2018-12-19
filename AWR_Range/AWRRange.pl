#!/usr/bin/perl
#
# Copyright (c) 2001, 2004, Oracle. All rights reserved.
#
#    NAME
#      AWRRange.pl - AWRRange 
#
#    INPUT PARAMETER
#      -u : Username 
#      -p : Password
#      -d : Connect as SYSDBA
#      -c : SQL*Net    
#      -t : AWR Format ( TEXT | HTML )
#      -i : Instance   ( 1-n | 1,2,3,x | ALL )
#      -b : SnapID Begin 
#      -e : SnapID End
#      -s : Start Time ( YYYYMMDDHH24MI )
#      -f : End Time   ( YYYYMMDDHH24MI )
#      -g : Generate Global AWR Report ( awrgrpti ) ( Available only from 11.2 )
#      -a : aggregate hourly
#      -o : output directory 
#      -z : dbid 
#
#    DESCRIPTION
#      Extract all AWR Report for a given Instance in a given Range of snapshot.
#      It's possible to executes this script interactively by inserting parameter
#      or use command line parameters.
#
#    OUTPUT:
#      AWR Report for a given Instance in a given Range of snapshot.
#
#    NOTES
#      Sample :
#      - Extract all AWR from ALL instances in the time range  28-Apr-2013 00:00 , 28-Apr-2013 23:00
#        ./AWRRange.sh -u system -p oracle -c "localhost:1521/o11203" -t TEXT -i ALL -s 201304280000 -f 201304282300 
#
#      - Extract all AWR from instances 1,2,3 in the time range  28-Apr-2013 00:00 , 28-Apr-2013 23:00
#        ./AWRRange.sh -u system -p oracle -c "localhost:1521/o11203" -t TEXT -i "1,2,3" -s 201304280000 -f 201304282300 
#
#      - Extract all AWR from ALL instances in the snap range  845 - 854
#        ./AWRRange.sh -u system -p oracle -c "localhost:1521/o11203" -t TEXT -i ALL -b 845 -e 854
#
#      - Extract all AWR from ALL instances, connecting as SYSDBA to local instances 
#        ./AWRRange.sh -d -t TEXT -i ALL -b 845 -e 847
#
#      - Extract all AWR from ALL instances in the snap range  845 - 854, connecting as SYSDBA to specifided instance. 
#        ./AWRRange.sh -d -u sys -p oracle -c "localhost:1521/o11203" -t TEXT -i ALL -b 845 -e 847
#
#      - Extract all AWR from ALL instances in the time range  05-may-2013 00:00 , 05-May-2013 01:00 from
#        remote database 
#        ./AWRRange.sh -u co15731 -p test -c "neta-scan-pr2.services.intranet:1521/GPBR2P" -t TEXT -i ALL -s 201305050000 -f 201305050100
#
#      - Extract Global AWR Report for ALL instances in the time range  10-jun-2013 00:00 , 11-Jun-2013 00:00 
#        and aggregate hourly
#        ./AWRRange.sh -u co15731 -p test -c "neta-scan-pr.services.intranet:1521/GPBR1P" -t TEXT -i ALL -s 201306100000 -f 201306110000 -g -a
#
#
#    TODO : 
#       - add default for dbid.
#
#    MODIFIED   (MM/DD/YY)
#    fbozzo      12/10/09 - Creation
#    fbozzo      02/27/13 - Add Instance Parameter 
#    fbozzo      03/11/13 - Add Command Line Parameter
#    fbozzo      06/11/13 - Add Global Report
#    fbozzo      06/12/13 - Add Usage
#    fbozzo      04/14/14 - Add database ID support

use strict;
use DBI;
use Getopt::Std;

################################################################################
# Global Variable 
################################################################################
my $lda;

my $username = "";
my $password = "";
my $address  = "";
my $oraRelease = "";
my $interactive = "Y";

my $mode = 0;
my @fetch_row;

my $dbid;
my $tempdbid;
my $dbName;
my %instList;
my $instNumInput;
my $instName;
my $incr;
my $incrFormat;
my $instListFilter;
my $awrFormat = "";
my $beginSnap=-1;
my $endSnap=-1;
my $numDays=-1;
my $max_snap_time;
my (%snapData);
my (%snapEndTime);
my (%snapTime2ID);
my $outDir = "";

################################################################################
# Usage 
################################################################################
sub usage()
    {
        print STDERR << "EOF";

    Generate AWR Report in a given time range. It's possible enter all parameters interactively  (except -g -a -s -f -o ) or 
    specify parameters on command line.

    usage: $0 [-hdga] [-u username] [-p password] [-c connect string] [-t TEXT|HTML] [-i instance] [-b begin snapID] [-e end snapID] [-s Start Time] [-f End Time] [-o Output Dir] [-z dbid ]

      -u : Username 
      -p : Password
      -d : Connect as SYSDBA
      -c : SQL*Net    
      -t : AWR Format ( TEXT | HTML )
      -i : Instance   ( 1-n | 1,2,3,x | ALL )
      -b : SnapID Begin 
      -e : SnapID End
      -s : Start Time ( YYYYMMDDHH24MI )
      -f : End Time   ( YYYYMMDDHH24MI )
      -g : Generate Global AWR Report ( awrgrpti ) ( Available only from 11.2 )
      -a : aggregate hourly
      -o : output directory 
      -z : dbid

    example: 
      - Extract all AWR from ALL instances in the time range  28-Apr-2013 00:00 , 28-Apr-2013 23:00
        ./AWRRange.sh -u system -p oracle -c "localhost:1521/o11203" -t TEXT -i ALL -s 201304280000 -f 201304282300 

      - Extract all AWR from instances 1,2,3 in the time range  28-Apr-2013 00:00 , 28-Apr-2013 23:00
        ./AWRRange.sh -u system -p oracle -c "localhost:1521/o11203" -t TEXT -i "1,2,3" -s 201304280000 -f 201304282300 

      - Extract all AWR from ALL instances in the snap range  845 - 854
        ./AWRRange.sh -u system -p oracle -c "localhost:1521/o11203" -t TEXT -i ALL -b 845 -e 854

      - Extract all AWR from ALL instances, connecting as SYSDBA to local instances 
        ./AWRRange.sh -d -t TEXT -i ALL -b 845 -e 847

      - Extract all AWR from ALL instances in the snap range  845 - 854, connecting as SYSDBA to specifided instance. 
        ./AWRRange.sh -d -u sys -p oracle -c "localhost:1521/o11203" -t TEXT -i ALL -b 845 -e 847

      - Extract all AWR from ALL instances in the time range  05-may-2013 00:00 , 05-May-2013 01:00 from
        remote database 
        ./AWRRange.sh -u co15731 -p test -c "neta-scan-pr2.services.intranet:1521/GPBR2P" -t TEXT -i ALL -s 201305050000 -f 201305050100

      - Extract Global AWR Report for ALL instances in the time range  10-jun-2013 00:00 , 11-Jun-2013 00:00 
        and aggregate hourly
        ./AWRRange.sh -u co15731 -p test -c "neta-scan-pr.services.intranet:1521/GPBR1P" -t TEXT -i ALL -s 201306100000 -f 201306110000 -g -a

EOF
        exit;
    }


################################################################################
# Function 
################################################################################

sub execSQL() {
  my $stm=$_[0];

  my $cur = $lda->prepare($stm)
    or die "prepare($stm): $DBI::errstr\n";
  $cur->execute()
    or die "cur->execute(): $DBI::errstr\n";

  return $cur;
}

################################################################################
# Verify Oracle relase 
################################################################################
sub checkRelease() {
  my $sql;
  my $release;


  $sql    = "declare";
  $sql   .= "  ret varchar2(10);";
  $sql   .= "begin";
  $sql   .= "  ret:=DBMS_DB_VERSION.VERSION||'.'||DBMS_DB_VERSION.RELEASE;";
  $sql   .= "  select ret into :OUTVER from dual;";
  $sql   .= "end;";
 
  my $hnd = $lda->prepare($sql);
  $hnd->bind_param_inout( ":OUTVER", \$release, 10);  
  $hnd->execute(); 
 
  return $release;
}

################################################################################
# MAIN
################################################################################

#
# Input Parameter
#
getopts('gdahu:p:c:t:i:b:e:s:f:o:z:') or usage();
our($opt_g, $opt_u, $opt_h,$opt_p, $opt_d,$opt_c, $opt_t, $opt_i, $opt_b, $opt_e,$opt_s,$opt_f, $opt_a, $opt_o,$opt_z);
usage() if $opt_h;
if ($opt_g eq ''  &&
    $opt_u eq ''  && 
    $opt_h eq ''  &&
    $opt_p eq ''  &&
    $opt_d eq ''  &&
    $opt_c eq ''  &&
    $opt_t eq ''  &&
    $opt_i eq ''  &&
    $opt_b eq ''  &&
    $opt_e eq ''  &&
    $opt_s eq ''  &&
    $opt_f eq ''  &&
    $opt_a eq ''  &&
    $opt_o eq ''  &&
    $opt_z eq '' ) {
  $interactive="Y";
} else {
  $interactive="N";
}
#
# Start 
#
print "\n";
print "*** Generate AWR Reports. *** \n";
print "\n";
print " Insert Connection Parameter : \n";

#
# Database Username
#
print "   DB Username : ";
if ( $opt_d != 1 ) { 
  if ( $opt_u eq '' )  {
    $username = <STDIN>;
    chomp $username;
  } else {
    $username = $opt_u;
    print "", $username,"\n";
  } 
} else {
 $username = $opt_u;
 print "SYSDBA\n";
}

#
# Database Password
#
print "      Password : ";
if ( $opt_d != 1 ) { 
  if ( $opt_p eq '' ) { 
    $password = <STDIN>;
    chomp $password;
  } else {
    $password = $opt_p;
    print "********";
  }
} else {
  $password =$opt_p;
  print "********";
}
print "\n";

#
# Connection String
#
# Value allowed  : <tnalias>                       : TNS*Alias
#                  <blank>                         : connect to local database
#                  <scanname>:<port>/<servicename> : conect to remote databsae
#
print " ConnectString : ";
if ( $opt_d != 1 ) { 
  if ( $opt_c eq '' ) { 
    $address = <STDIN>;
    chomp $address;
  } else {
    $address = $opt_c;
    print $address;
  }
} else {
  $address=$opt_c;
}
print "\n";


# 
# Connect mode to connect as SYSDBA
#
if ( $username && $password && $address && $opt_d != 1 ) {
  $mode=0;
} else {
  $mode=2;
}

#
# Connect
#
$lda = DBI->connect('dbi:Oracle:', "$username@".$address, "$password",
    {ora_session_mode => $mode, PrintError => 0, RaiseError => 0, AutoCommit => 0})
    or die "Could not connect to $username/$address: $DBI::errstr\n";

################################################################################ 
# Check Version
################################################################################ 

$oraRelease=checkRelease();


################################################################################ 
# Step
################################################################################ 
if ( defined($opt_a) ) {
  $incr = 1;
  $incrFormat = "yyyymmddhh24";
} else {
  $incr = 1;
  $incrFormat = "yyyymmddhh24mi";
}

#print "Increment : ".$incr." Increment Format : ".$incrFormat;

################################################################################ 
# AWR Output Format
################################################################################
print "\n";
print "Specify the Report Type\n";
print "~~~~~~~~~~~~~~~~~~~~~~~\n";
print "Enter 'HTML' for an HTML report, or 'TEXT' for plain text\n";
print "Defaults to 'text'\n";

if ( $opt_t eq '' ) {
  while ( $awrFormat !~ /text/i && $awrFormat !~ /html/i ) {
    print "Enter value for report_type: ";
    $awrFormat = <STDIN>;
    chomp $awrFormat;
    if ( $awrFormat eq '' ) {
      $awrFormat = "text";
    }
  }
} else {
  if ( $opt_t =~ /text/i || $opt_t =~ /html/i ) {
    $awrFormat = $opt_t;
    print $opt_t;
  } else {
    die "Wrong type parameter!\n\n";
  }
}
print "\n";






################################################################################
# Current Instance
################################################################################
my $sql = "select d.dbid            dbidi ";
$sql   .= "     , d.name            db_name ";
$sql   .= "     , i.instance_number inst_num ";
$sql   .= "     , i.instance_name   inst_name ";
$sql   .= "  from v\$database d, ";
$sql   .= "       v\$instance i ";
$sql   .= "  order by 1,2,3";

my $cur = &execSQL($sql);

print "\n";
print "Current Instance\n";
print "~~~~~~~~~~~~~~~~\n";
print "   DB Id    DB Name      Inst Num Instance    \n";
print "----------- ------------ -------- ------------\n";

while(my $record = $cur->fetchrow_hashref){
  $dbid = $record->{'DBIDI'};
  $dbName = $record->{'DB_NAME'};
  $instListFilter = $record->{'INST_NUM'};
  $instName = $record->{'INST_NAME'};

  printf "%+11u %-12s %+8u %+12s \n",$record->{'DBIDI'}, 
                                     $record->{'DB_NAME'}, 
                                     $record->{'INST_NUM'} , 
                                     $record->{'INST_NAME'};
}
warn "Data fetching terminated early by error: $DBI::errstr\n"
    if $DBI::err;



################################################################################
# Instance in workload
################################################################################
$sql = qq { select distinct
                   (case when cd.dbid = wr.dbid and
                              cd.name = wr.db_name and
                              ci.instance_number = wr.instance_number and
                              ci.instance_name   = wr.instance_name
                         then '* '
                    else '  '
                    end) || wr.dbid   dbbid
                 , wr.instance_number instt_num
                 , wr.db_name         dbb_name
                 , wr.instance_name   instt_name
                 , wr.host_name       host
              from dba_hist_database_instance wr, v\$database cd, v\$instance ci
             order by 1,2,3};


print "\n";
print "Instances in this Workload Repository schema\n";
print "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n";
print "\n";
print "   DB Id     Inst Num DB Name      Instance     Host \n";
print "------------ -------- ------------ ------------ ------------\n";

my $cur = &execSQL($sql);

while(my $record = $cur->fetchrow_hashref){
  # $instList{$record->{'INSTT_NUM'}} = $record->{'INSTT_NAME'};

  printf "%+12s %+8u %+12s %+12s %-12s\n",$record->{'DBBID'},
                                          $record->{'INSTT_NUM'},
                                          $record->{'DBB_NAME'},
                                          $record->{'INSTT_NAME'},
                                          $record->{'HOST'};
}
warn "Data fetching terminated early by error: $DBI::errstr\n"
    if $DBI::err;

################################################################################ 
# Choose Database 
################################################################################

print "\n";
print "Specify Database ID \n";
print "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ \n";
print "Entering the database id from which extract report \n";
print "Pressing <return> for current database [$dbid]  \n";
print "\n";

print "\n";


if ( $opt_z eq ''  &&  $interactive eq 'Y' ) {
  print "Enter value for database id : ";
  $tempdbid = <STDIN>;
  if ( $tempdbid == '' ) {
    $tempdbid = $dbid;
  }
  chomp $tempdbid;
  print "use DBID specified [ $tempdbid ]";
} else {
  if ( $opt_z eq '' ) {
    print "use DBID of current database [ $dbid ]";
    $tempdbid = $dbid;  
  } else {
    print "use DBID specified  [ $opt_z ]";
    $tempdbid = $opt_z;
  }  
}



$sql = qq { select distinct
                   (case when cd.dbid = wr.dbid and
                              cd.name = wr.db_name and
                              ci.instance_number = wr.instance_number and
                              ci.instance_name   = wr.instance_name
                         then '* '
                    else '  '
                    end) || wr.dbid   dbbid
                 , wr.instance_number instt_num
                 , wr.db_name         dbb_name
                 , wr.instance_name   instt_name
                 , wr.host_name       host
              from dba_hist_database_instance wr, v\$database cd, v\$instance ci
             where wr.dbid = $tempdbid
             order by 1,2,3};

my $cur = &execSQL($sql);

while(my $record = $cur->fetchrow_hashref){
  $instList{$record->{'INSTT_NUM'}} = $record->{'INSTT_NAME'};
  $dbid = $tempdbid;

  $dbName = $record->{'DBB_NAME'};
  $instListFilter = $record->{'INSTT_NUM'};
  $instName = $record->{'INSTT_NAME'};

}
warn "Data fetching terminated early by error: $DBI::errstr\n"
    if $DBI::err;

print "\n";

################################################################################
# Available Instances
################################################################################
print "\n";
print "Specify the number of instance \n";
print "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ \n";
print "Entering the number of instance from which extract report \n";
print "Pressing <return> for current instance or ALL for all instances : \n";
print "\n";

if ( $opt_i eq '' ) {
  print "Enter value for instance_number : ";
  $instNumInput = <STDIN>;
  if ( $instNumInput == '' ) {
    $instNumInput = $instListFilter;
  }
  chomp $instNumInput;
} else {
  $instNumInput = $opt_i;
  print $opt_i;
}


print "\n";
#print "InstNumInput :".$instNumInput."\n";



if ( $instNumInput != '' || !($instNumInput =~ /all/i ) ) {
  $instListFilter = $instNumInput;
} else {
  if ( $instNumInput =~ /all/i ) {
    $instListFilter = "";
    while ( (my $LInst, my$instName) = each %instList ) {
      $instListFilter .= $LInst.",";
    }
    $instListFilter = substr $instListFilter,0,-1;
  }
}
#print "InstListFilter :".$instListFilter."\n";

################################################################################
# Num Days
################################################################################
print "\n";
print "Specify the number of days of snapshots to choose from \n";
print "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ \n";
print "Entering the number of days (n) will result in the most recent \n";
print "(n) days of snapshots being listed.  Pressing <return> without \n";
print "specifying a number lists all completed snapshots. \n";
print "\n";

if ( $opt_b eq '' && $opt_e eq '' && $opt_s eq '' && $opt_f eq '' ) {
  while ( $numDays < 0 ) {
    print "Enter value for num_days: ";
    $numDays = <STDIN>;
    chomp $numDays;
    print "\n";
  }
} 

if ( $numDays == '' ) {
  $numDays = 3.14;
}

################################################################################
# Max Snap Time
################################################################################
$sql = qq{select to_char(max(end_interval_time),'dd/mm/yyyy') MAX_SNAP_TIME
            from dba_hist_snapshot
           where instance_number in ( $instListFilter )
             and dbid            = $dbid};

my $cur = &execSQL($sql);

while(my $record = $cur->fetchrow_hashref){
  $max_snap_time = $record->{'MAX_SNAP_TIME'};
}
warn "Data fetching terminated early by error: $DBI::errstr\n"
    if $DBI::err;

################################################################################
# Snapshoots 
################################################################################
#
# StartTime / EndTime conversion to BEGIN SNAP ID / END SNAP ID.
#
if ( $opt_s ne '' and $opt_f ne '' ) {
  #printf "begin_snap -> %+12u end_snap -> %+12u\n",$opt_s,$opt_f;
  my $min_snap;
  my $max_snap;

# For global report we must modify filter instance_number in ( strings ) 
    $sql = qq{ select min(s.snap_id) min_snap_id
                    , max(s.snap_id) max_snap_id
               from dba_hist_snapshot s
                  , dba_hist_database_instance di
              where s.dbid              = $dbid 
                and di.dbid             = $dbid
                and s.instance_number in ( $instListFilter )
                and di.dbid             = s.dbid
                and di.instance_number  = s.instance_number
                and di.startup_time     = s.startup_time
                and s.end_interval_time >= to_date($opt_s,'YYYYMMDDHH24MI') 
                and s.begin_interval_time <= to_date($opt_f,'YYYYMMDDHH24MI')
              order by db_name, snap_id, instance_name};

  my $cur = &execSQL($sql);

  while(my $record = $cur->fetchrow_hashref){
    $min_snap = $record->{'MIN_SNAP_ID'};
    $max_snap = $record->{'MAX_SNAP_ID'};
  }
  warn "Data fetching terminated early by error: $DBI::errstr\n"
    if $DBI::err;
  
  if ( $min_snap > 0 and $max_snap > 0 ) {
    $opt_b = $min_snap;
    $opt_e = $max_snap;
  } else {
    die "Invalid Start and/or End Timestamp";
  }

  #printf "begin_snap -> %+8u end_snap -> %+8u\n",$opt_b,$opt_e;
}

if ( $opt_b eq '' && $opt_e eq '' ) {
  $sql = qq{ select to_char(s.startup_time,'dd Mon "at" HH24:mi:ss')  instart_fmt
                , di.instance_name                                  inst_name
                , di.db_name                                        db_name
                , s.snap_id                                         snap_id
                , to_char(s.end_interval_time,'dd Mon YYYY HH24:mi') snapdat
                , to_char(s.end_interval_time,'YYYYMMDDHH24mi') end_time
                , s.snap_level                                      lvl
             from dba_hist_snapshot s
                , dba_hist_database_instance di
            where s.dbid              = $dbid 
              and di.dbid             = $dbid
              and s.instance_number in ( $instListFilter )
              and di.dbid             = s.dbid
              and di.instance_number  = s.instance_number
              and di.startup_time     = s.startup_time
              and s.end_interval_time >= decode( $numDays
                                           , 0   , to_date('31-JAN-9999','DD-MON-YYYY')
                                           , 3.14, s.end_interval_time
                                           , to_date('$max_snap_time','dd/mm/yyyy') - ($numDays-1))
            order by db_name, snap_id, instance_name};
} else {
#
# BEGIN SNAP ID / END SNAP ID.
#
    $sql = qq{ select to_char(s.startup_time,'dd Mon "at" HH24:mi:ss')  instart_fmt
                  , di.instance_name                                  inst_name
                  , di.db_name                                        db_name
                  , s.snap_id                                         snap_id
                  , to_char(s.end_interval_time,'dd Mon YYYY HH24:mi') snapdat
                  , to_char(s.end_interval_time,'YYYYMMDDHH24mi') end_time
                  , s.snap_level                                      lvl
               from dba_hist_snapshot s
                  , dba_hist_database_instance di
              where s.dbid              = $dbid 
                and di.dbid             = $dbid
                and s.instance_number in ( $instListFilter )
                and di.dbid             = s.dbid
                and di.instance_number  = s.instance_number
                and di.startup_time     = s.startup_time
                and s.snap_id between $opt_b and $opt_e 
              order by db_name, snap_id, instance_name};
}
my $cur = &execSQL($sql);

print "\n";
print "Listing ";
if ( $numDays == 3.14) {
  print "all Completed Snapshots\n";
} else {
  print "the last $numDays days of Completed Snapshots\n";
}


print "\n";
print "\n";
print "Instance     DB Name        Snap Id    Snap Started    Level\n";
print "------------ ------------ --------- ------------------ -----\n";

while(my $record = $cur->fetchrow_hashref){
  printf "%+12s %+12s %+9u %+18s %-5s\n",$record->{'INST_NAME'},
                                         $record->{'DB_NAME'},
                                         $record->{'SNAP_ID'},
                                         $record->{'SNAPDAT'},
                                         $record->{'LVL'};

  $snapData{$record->{'SNAP_ID'}} = $record->{'SNAPDAT'};
  $snapEndTime{$record->{'SNAP_ID'}} = $record->{'END_TIME'};
  
}
warn "Data fetching terminated early by error: $DBI::errstr\n"
    if $DBI::err;


################################################################################
# Choose Snap
################################################################################
print "\n";
print "Specify the Begin and End Snapshot Ids\n";
print "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n";

if ( $opt_b eq '' ) {
  while ( !defined($beginSnap) || $beginSnap == '' || $beginSnap < 0 ) {
    print "Enter value for begin_snap: ";
    $beginSnap = <STDIN>;
    chomp $beginSnap;
    print "\n";
  }
} else {
  $beginSnap = $opt_b;
  #print $beginSnap . "\n";
}
print "Begin Snapshot Id specified: " . $beginSnap . "\n";
print "\n";


if ( $opt_e eq '' ) {
  while ( !defined($endSnap) || $endSnap == '' || $endSnap < 0 ) {
    print "Enter value for end_snap: ";
    $endSnap = <STDIN>;
    chomp $endSnap;
    print "\n";
  }
} else {
  $endSnap = $opt_e;
  #print $opt_e . "\n";
} 

print "End   Snapshot Id specified: " . $endSnap . "\n";
print "\n";


################################################################################
# Gen Report 
################################################################################

print "\nGenerating AWR Reports : \n";
print "Instance     Begin SnapId End SnapId   Begin Snap Date    End Snap Date      Report Name                                   \n";
print "------------ ------------ ------------ ------------------ ------------------ ----------------------------------------------\n";

if ( defined($opt_o) ) {
  if ( substr $instListFilter,0,-1 eq '/' ) {
    $outDir=$opt_o;
  } else {
    $outDir=$opt_o.'/';
  }
}

if ( $opt_g != 1 ) {
  foreach my $inst (split(',', $instListFilter)) {
      my $instName = $instList{$inst};
      $sql = qq{select snap_id, next_snap
                from
                (  
                  select s.snap_id
                       , lead(s.snap_id) over (order by db_name, instance_name, snap_id) next_snap
                       , s.startup_time
                       , lead(s.startup_time) over (order by db_name, instance_name, snap_id) next_start
                    from ( select DBID
                                , INSTANCE_NUMBER
                                , SNAP_ID
                                , END_INTERVAL_TIME
                                , STARTUP_TIME
                                , min(snap_id) over (partition by DBID,INSTANCE_NUMBER,  to_char(END_INTERVAL_TIME ,'$incrFormat')) min_snap
                             from dba_hist_snapshot 
                            where dbid              = $dbid
                              and instance_number   = $inst 
                              and snap_id           between $beginSnap and $endSnap
                         )  s 
                       , dba_hist_database_instance di
                   where di.dbid             = $dbid
                     and di.instance_number  = $inst
                     and di.dbid             = s.dbid
                     and di.instance_number  = s.instance_number
                     and di.startup_time     = s.startup_time
                     and s.snap_id           = s.min_snap
                ) s where s.startup_time = s.next_start
                      and SNAP_ID<NEXT_SNAP 
                order by snap_id};
  
  
      $cur = &execSQL($sql);
  
  
      while(my $record = $cur->fetchrow_hashref){
        my $repSql = qq{ select output 
                           from table(DBMS_WORKLOAD_REPOSITORY.AWR_REPORT_$awrFormat(
                                             $dbid, 
                                             $inst,
                                             $record->{'SNAP_ID'},
                                             $record->{'NEXT_SNAP'},
                                             8))};
        
        my $rep = &execSQL($repSql);
  
        my $reportName;
        if ( $awrFormat =~ /text/i ) {
          $reportName=$outDir."awrrpt\_$dbName\_$inst\_$snapEndTime{$record->{'SNAP_ID'}}\_$snapEndTime{$record->{'NEXT_SNAP'}}.txt";
        } else {
          $reportName=$outDir."awrrpt\_$dbName\_$inst\_$snapEndTime{$record->{'SNAP_ID'}}\_$snapEndTime{$record->{'NEXT_SNAP'}}.html";
        }
  
        open AWRREPORT , ">$reportName";
  
        printf "%-12s %+12s %+12s %-18s %-18s %-30s\n",$instName,
                                                       $record->{'SNAP_ID'},  
                                                       $record->{'NEXT_SNAP'},
                                                       $snapData{$record->{'SNAP_ID'}},
                                                       $snapData{$record->{'NEXT_SNAP'}},
                                                       $reportName;
  
  
        while ( my @fetch_row = $rep->fetchrow_array() ) {
          print AWRREPORT $fetch_row[0] . "\n";
        }
  
        close AWRREPORT;
      }
      warn "Data fetching terminated early by error: $DBI::errstr\n"
          if $DBI::err;
  }
} else {
  if ( $oraRelease eq '11.2' ) {
      my $instName = "GLOBAL";
      $sql = qq{select distinct snap_id, next_snap
                from
                (  
                  select s.snap_id
                       , lead(s.snap_id) over (order by db_name, instance_name, snap_id) next_snap
                       , s.startup_time
                       , lead(s.startup_time) over (order by db_name, instance_name, snap_id) next_start
                    from ( select DBID
                                , INSTANCE_NUMBER
                                , SNAP_ID
                                , END_INTERVAL_TIME
                                , STARTUP_TIME
                                , min(snap_id) over (partition by DBID,INSTANCE_NUMBER,  to_char(END_INTERVAL_TIME ,'$incrFormat')) min_snap
                             from dba_hist_snapshot 
                            where dbid              = $dbid
                              and instance_number   in ($instListFilter)
                              and snap_id           between $beginSnap and $endSnap
                         )  s 
                       , dba_hist_database_instance di
                   where di.dbid             = $dbid
                     and di.dbid             = s.dbid
                     and di.instance_number  = s.instance_number
                     and di.startup_time     = s.startup_time
                     and s.snap_id           = s.min_snap
                ) s where s.startup_time = s.next_start 
                      and SNAP_ID<NEXT_SNAP 
                order by snap_id};

      #print "\n".$sql;

      $cur = &execSQL($sql);

      while(my $record = $cur->fetchrow_hashref){
        my $repSql = qq{ select output 
                           from table(DBMS_WORKLOAD_REPOSITORY.AWR_GLOBAL_REPORT_$awrFormat(
                                             $dbid, 
                                             '$instListFilter',
                                             $record->{'SNAP_ID'},
                                             $record->{'NEXT_SNAP'},
                                             8))};

        my $rep = &execSQL($repSql);

        my $reportName;
        if ( $awrFormat =~ /text/i ) {
          $reportName=$outDir."awrGrpt\_$dbName\_$instName\_$snapEndTime{$record->{'SNAP_ID'}}\_$snapEndTime{$record->{'NEXT_SNAP'}}.txt";
        } else {
          $reportName=$outDir."awrGrpt\_$dbName\_$instName\_$snapEndTime{$record->{'SNAP_ID'}}\_$snapEndTime{$record->{'NEXT_SNAP'}}.html";
        }

        open AWRREPORT , ">$reportName";

        printf "%-12s %+12s %+12s %-18s %-18s %-30s\n",$instName,
                                                       $record->{'SNAP_ID'},
                                                       $record->{'NEXT_SNAP'},
                                                       $snapData{$record->{'SNAP_ID'}},
                                                       $snapData{$record->{'NEXT_SNAP'}},
                                                       $reportName;


        while ( my @fetch_row = $rep->fetchrow_array() ) {
          print AWRREPORT $fetch_row[0] . "\n";
        }

        close AWRREPORT;
      }
      warn "Data fetching terminated early by error: $DBI::errstr\n"
          if $DBI::err;
  } else {
    print "\nGlobal REPORT is available from 11.2 onward \n";
  }
}

print "\n";
print "\n";


$lda->disconnect;


