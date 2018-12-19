## Usage
##
## ./AWRRange.sh -u eora006 -p acssupport#123 -c "172.20.1.81:1051/HHMLPROD" -t HTML -i 1 -s 201810310900 -f 201810311230

export PERL5LIB=$ORACLE_HOME/perl/lib:$ORACLE_HOME/perl/lib/site_perl
export LD_LIBRARY_PATH=$ORACLE_HOME/lib32:$ORACLE_HOME/lib:$ORACLE_HOME/network/lib32:$ORACLE_HOME/network/lib:$ORACLE_HOME/perl/lib
$ORACLE_HOME/perl/bin/perl AWRRange.pl "$@"

