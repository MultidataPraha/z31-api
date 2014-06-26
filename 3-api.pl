#!/exlibris/aleph/a21_1/product/bin/perl

###########################################################
#
# (c) 2013, 2014 MULTIDATA Praha spol. s r.o.
#
# cash-api.pl v076 aleph z31 api (TEST)
#
###########################################################

use strict;
use warnings;
use diagnostics;
use utf8;
binmode STDOUT, ":utf8";
use CGI;
use File::Basename;

#
##use CGI qw(-utf8);
use Crypt::OpenSSL::RSA;

use File::Slurp;
use MIME::Base64;
use DBI;

#use URI::Escape;
use POSIX qw(strftime);
use Time::HiRes q/gettimeofday/;
use XML::Simple qw(:strict);

my $q = CGI->new;
print $q->header('text/xml; charset=utf-8');

my $script_name = basename( $q->script_name(), ".pl" ) if $q->script_name();
$script_name = basename( $0, ".pl" ) unless $script_name;

######################################################################
#
#

my %config;
my %param;

die "missing DOCUMENT_ROOT environment variable" unless $ENV{'DOCUMENT_ROOT'};
$ENV{'alephe_tab'} = $ENV{'DOCUMENT_ROOT'} . '/../../tab';

my $config_file = $ENV{'alephe_tab'} . '/' . $script_name . '.cfg';

die "cannot open config file $config_file: $!"
    unless read_config($config_file);

my $usr_library
    = $config{'usr_library'} ? $config{'usr_library'} : $ENV{'usr_library'};

my $pw_library
    = $config{'pw_library'} ? $config{'pw_library'} : $ENV{'pw_library'};

my $key_file = $config{'api_public_key'};

$ENV{'aleph_db'}    = $config{'aleph_db'}    if $config{'aleph_db'};
$ENV{'ORACLE_HOME'} = $config{'ORACLE_HOME'} if $config{'ORACLE_HOME'};
$ENV{'LOGDIR'}      = $config{'LOGDIR'}      if $config{'LOGDIR'};
die "missing LOGDIR cfg / env" unless $ENV{'LOGDIR'};
$ENV{'NLS_LANG'} = $config{'NLS_LANG'} if $config{'NLS_LANG'};
$ENV{'NLS_LANG'} = 'American_America.UTF8' unless $ENV{'NLS_LANG'};

my $ignore_digest = $config{'api_ignore_digest'};
$ignore_digest = 0 unless defined $ignore_digest;

my $max_time_diff = $config{'api_max_time_diff'};
$max_time_diff = 300 unless defined $max_time_diff;

#$config{'Z31_SUB_LIBRARY'} = substr( $config{'Z31_SUB_LIBRARY'}, 0, 5 )
#    if $config{'Z31_SUB_LIBRARY'};
#$config{'Z31_PAYMENT_TARGET'} = substr( $config{'Z31_PAYMENT_TARGET'}, 0, 20 )
#    if $config{'Z31_PAYMENT_TARGET'};

$config{'Z31_TYPE'} =~ s/^([0-9]{1,4}).*/$1/ if $config{'Z31_TYPE'};

$config{'Z31_PAYMENT_MODE'} = substr( $config{'Z31_PAYMENT_MODE'}, 0, 2 )
    if $config{'Z31_PAYMENT_MODE'};

$config{'Z31_DESCRIPTION'} = substr( $config{'Z31_DESCRIPTION'}, 0, 300 )
    if $config{'Z31_DESCRIPTION'};

######################################################################
#
# GET / POST params
#

my ( $dbh, $sql, $sth );
my $z303_rec_key;

my $api_response;

my $adm_library;

eval {
    my $logfile = $ENV{"LOGDIR"} . '/' . $script_name . '.log';
    open( LOGFILE, ">>" . $logfile )
        || die $api_response->{'diag'} = "cannot write log: $logfile: $!";
    die $api_response->{'diag'} if defined $api_response->{'diag'};
    die "missing aleph_db cfg / env"    unless $ENV{'aleph_db'};
    die "missing ORACLE_HOME cfg / env" unless $ENV{'ORACLE_HOME'};

    binmode( LOGFILE, ":unix" );
    open( STDERR, ">&LOGFILE" );
    get_and_check_params();
    $adm_library                  = $param{'adm'};
    $config{'Z31_PAYMENT_TARGET'} = $adm_library;
    $config{'Z31_SUB_LIBRARY'}    = $adm_library;

    die "missing usr_library cfg\n" unless defined $usr_library;
    die "wrong usr_library cfg\n"
        unless $usr_library =~ /^[a-z]{3}[0-9]{2}$/i;

    $dbh = DBI->connect( 'dbi:Oracle:' . $ENV{"aleph_db"},
        'aleph', 'aleph',
        { RaiseError => 1, PrintError => 1, AutoCommit => 0, Warn => 1 } );

    check_libs();

    check_time_diff();
    find_z303_rec_key();

          $param{'op'} =~ /^list$/i  ? api_list()
        : $param{'op'} =~ /^open$/i  ? api_open()
        : $param{'op'} =~ /^close$/i ? api_close()
        :                              api_error();

    print_xml_response();

#########################################################################################

} or do {

#########################################################################################

    $param{'op'} = "error";
    $@ = "unknown error" unless $@;
    $api_response->{'diag'} = "check logfile for error description"
        unless $api_response->{'diag'};

    $api_response->{'diag'} =~ s/\n/ /g;
    print_xml_response();
    chomp $@;
    $@ =~ s/\n/#/g;

    write_log($@);

};

exit;

#########################################################################################
#
# list
#
#########################################################################################

sub api_list {

    $sql = <<EOT;
       select /*+ DYNAMIC_SAMPLING(2) ALL_ROWS */
         sum(Z31_SUM)
       from ${adm_library}.z31
       where Z31_REC_KEY like ? || '%'
         and Z31_STATUS = 'O'
         and Z31_CREDIT_DEBIT = 'D'
         and Z31_SUM > 0
EOT

    $sth = $dbh->prepare($sql);

    $sth->execute($z303_rec_key);

    my ($z31_sum) = $sth->fetchrow_array();

    $z31_sum = 0 unless $z31_sum;

    $sql = <<EOT;
       update ${adm_library}.z31
       set Z31_PAYMENT_IDENTIFIER = ?
       where Z31_REC_KEY like ? || '%'
         and Z31_STATUS = 'O'
         and Z31_CREDIT_DEBIT = 'D'
         and Z31_SUM > 0
EOT

    $sth = $dbh->prepare($sql);
    $sth->execute( $param{'time'}, $z303_rec_key );
    $dbh->commit;

    $sql = <<EOT;
       select /*+ DYNAMIC_SAMPLING(2) ALL_ROWS */
         Z31_REC_KEY,Z31_SUM+0,nvl(Z31_DESCRIPTION,'[empty]'),Z31_PAYMENT_IDENTIFIER
       from ${adm_library}.z31
       where Z31_REC_KEY like ? || '%'
         and Z31_STATUS = 'O'
         and Z31_CREDIT_DEBIT = 'D'
         and Z31_SUM > 0
       order by Z31_REC_KEY
EOT

    $sth = $dbh->prepare($sql);

    $sth->execute($z303_rec_key);

    my @response_array;

    while ( my @row = $sth->fetchrow_array() ) {
        push(
            @response_array,
            {   key         => $row[0],
                amount      => $row[1],
                description => $row[2],
                pid         => $row[3]
            }
        );
    }

    $api_response = { 'tr' => [@response_array] };
    $api_response->{'total'} = $z31_sum;

    write_log("list|$param{'id'}|$z303_rec_key|$z31_sum|$param{'time'}");

} ## end sub api_list

#########################################################################################
#
# open
#
#########################################################################################

sub api_open {

    die "missing pw_library cfg\n" unless defined $pw_library;
    die "wrong pw_library cfg\n" unless $pw_library =~ /^[a-z]{3}[0-9]{2}$/i;

    $sql = <<EOT;
       select ${pw_library}.last_record_sequence.nextval
       from dual
EOT

    my $print_error = $dbh->{PrintError};
    $dbh->{PrintError} = 0;

    eval {
        $sth = $dbh->prepare($sql);
        $sth->execute();
    }
        or die "pw_library.last_record_sequence not found in ${pw_library}\n";
    $dbh->{PrintError} = $print_error;
    my ($z31_sequence) = $sth->fetchrow_array();

# Z31_REC_KEY, Z31_DATE_X, Z31_STATUS, Z31_SUB_LIBRARY, Z31_ALPHA, Z31_TYPE, Z31_CREDIT_DEBIT, Z31_SUM,
# Z31_VAT_SUM, Z31_NET_SUM, Z31_PAYMENT_DATE_KEY, Z31_PAYMENT_CATALOGER, Z31_PAYMENT_TARGET, Z31_PAYMENT_IP, Z31_PAYMENT_RECEIPT_NUMBER, Z31_PAYMENT_MODE,
# Z31_PAYMENT_IDENTIFIER, Z31_DESCRIPTION, Z31_KEY, Z31_KEY_TYPE, Z31_TRANSFER_DEPARTMENT, Z31_TRANSFER_DATE, Z31_TRANSFER_NUMBER, Z31_RECALL_TRANSFER_STATUS,
# Z31_RECALL_TRANSFER_DATE, Z31_RECALL_TRANSFER_NUMBER, Z31_RELATED_Z31_KEY, Z31_RELATED_Z31_KEY_TYPE, Z31_REQUESTER_NAME, Z31_UPD_TIME_STAMP, Z31_PAYMENT_IP_V6, Z31_NOTE,

    $sql = <<EOT;
       insert into ${adm_library}.z31
       values(?,?,'O',?,'L',?,'D',?,
              ?,?,?,'API1',?,NULL,NULL,?,
              ?,?,NULL,NULL,NULL,NULL,NULL,NULL,
              NULL,NULL,NULL,NULL,NULL,?,NULL,NULL)
EOT

    $sth = $dbh->prepare($sql);
    my $yyyymmdd = strftime '%Y%m%d', localtime;

    # Z31_REC_KEY = User ID + YYYYMMDD + last_record_sequence

    my $z31_rec_key
        = sprintf( "%12s%8u%07u", $z303_rec_key, $yyyymmdd, $z31_sequence );
    my $z31_sum              = sprintf( "%014u", $param{'amount'} );
    my $z31_vat_sum          = sprintf( "%014u", 0 );
    my $z31_payment_date_key = sprintf( "%012u", 0 );
    my $z31_upd_time_stamp   = get_time_stamp_15();

    my $inserted = $sth->execute(
        $z31_rec_key,                  $yyyymmdd,
        $config{'Z31_SUB_LIBRARY'},    $config{'Z31_TYPE'},
        $z31_sum,                      $z31_vat_sum,
        $z31_sum,                      $z31_payment_date_key,
        $config{'Z31_PAYMENT_TARGET'}, $config{'Z31_PAYMENT_MODE'},
        $param{'time'},                $config{'Z31_DESCRIPTION'},
        $z31_upd_time_stamp
    );

    $dbh->commit;

    write_log(
        "open|$param{'id'}|$z31_sequence|$z31_rec_key|$param{'amount'}|$param{'time'}"
    ) if $inserted;

    $sql = <<EOT;
       select Z31_REC_KEY,Z31_SUM+0,nvl(Z31_DESCRIPTION,'[empty]'),Z31_PAYMENT_IDENTIFIER
       from ${adm_library}.z31
       where Z31_REC_KEY = ?
       and Z31_STATUS = 'O'
       and Z31_CREDIT_DEBIT = 'D'
       and Z31_SUM > 0
EOT

    $sth = $dbh->prepare($sql);

    $sth->execute($z31_rec_key);

    my @row = $sth->fetchrow_array();

    $api_response->{'tr'}->{'key'}         = $row[0];
    $api_response->{'tr'}->{'amount'}      = $row[1];
    $api_response->{'tr'}->{'description'} = $row[2];
    $api_response->{'tr'}->{'pid'}         = $row[3];

} ## end sub api_open

#########################################################################################
#
# close
#
#########################################################################################

sub api_close {

    my $z31_payment_date_key = strftime( q/%Y%m%d%H%M/, localtime() );

    $sql = <<EOT;
update ${adm_library}.z31
set
 Z31_PAYMENT_CATALOGER = 'API2',
 Z31_PAYMENT_DATE_KEY = ?
where Z31_STATUS = 'O'
and Z31_CREDIT_DEBIT = 'D'
and Z31_REC_KEY like ? || '%'
and Z31_SUM > 0
EOT

    if ( $param{'tr'} =~ /^[0-9]{10}$/ ) {
        $sql .= " and Z31_PAYMENT_IDENTIFIER = ?";
    }
    elsif ( $param{'tr'} !~ /^ALL$/ ) {
        $sql .= " and Z31_REC_KEY = ?";
    }

    #        $sql .= ' and Z31_REC_KEY = ?' unless $param{'tr'} =~ /^ALL$/;

    $sth = $dbh->prepare($sql);

    if ( $param{'tr'} =~ /^ALL$/ ) {
        $sth->execute( $z31_payment_date_key, $z303_rec_key );
    }
    else {
        $sth->execute( $z31_payment_date_key, $z303_rec_key, $param{'tr'} );
    }

    $api_response->{'diag'} = "no transaction record found"
        unless $sth->rows > 0;
    die $api_response->{'diag'} if defined $api_response->{'diag'};

    $sql = <<EOT;
select sum(Z31_SUM)
from ${adm_library}.z31
where Z31_STATUS = 'O'
and Z31_CREDIT_DEBIT = 'D'
and Z31_PAYMENT_CATALOGER = 'API2'
and Z31_PAYMENT_DATE_KEY = ?
and Z31_REC_KEY like ? || '%'
EOT

    #    $sql .= ' and Z31_REC_KEY = ?' unless $param{'tr'} =~ /^ALL$/;
    if ( $param{'tr'} =~ /^[0-9]{10}$/ ) {
        $sql .= " and Z31_PAYMENT_IDENTIFIER = ?";
    }
    elsif ( $param{'tr'} !~ /^ALL$/ ) {
        $sql .= " and Z31_REC_KEY = ?";
    }

    $sth = $dbh->prepare($sql);

    if ( $param{'tr'} =~ /^ALL$/ ) {
        $sth->execute( $z31_payment_date_key, $z303_rec_key );
    }
    else {
        $sth->execute( $z31_payment_date_key, $z303_rec_key, $param{'tr'} );
    }

    my ($tr_amount) = $sth->fetchrow_array();

    if ( $tr_amount != $param{'amount'} ) {
        $api_response->{'requested_amount'} = $param{'amount'};
        $api_response->{'current_amount'}   = $tr_amount;
        $api_response->{'diag'}
            = "requested amount differs from current amount";
        die $api_response->{'diag'};
    }

    $sth = $dbh->prepare($sql);

    $sql = <<EOT;
update ${adm_library}.z31
set
 Z31_PAYMENT_IP = ?,
 Z31_STATUS = 'C',
 Z31_PAYMENT_CATALOGER = 'API3',
 Z31_PAYMENT_IDENTIFIER = ?
where Z31_STATUS = 'O'
and Z31_CREDIT_DEBIT = 'D'
and Z31_PAYMENT_CATALOGER = 'API2'
and Z31_PAYMENT_DATE_KEY = ?
and Z31_REC_KEY like ? || '%'
and Z31_SUM > 0
EOT

    #    $sql .= ' and Z31_REC_KEY = ?' unless $param{'tr'} =~ /^ALL$/;
    if ( $param{'tr'} =~ /^[0-9]{10}$/ ) {
        $sql .= " and Z31_PAYMENT_IDENTIFIER = ?";
    }
    elsif ( $param{'tr'} !~ /^ALL$/ ) {
        $sql .= " and Z31_REC_KEY = ?";
    }

    $sth = $dbh->prepare($sql);

    if ( $param{'tr'} =~ /^ALL$/ ) {
        $sth->execute(
            $ENV{'REMOTE_ADDR'},   $param{'time'},
            $z31_payment_date_key, $z303_rec_key
        );
    }
    else {
        $sth->execute(
            $ENV{'REMOTE_ADDR'}, $param{'time'}, $z31_payment_date_key,
            $z303_rec_key,       $param{'tr'}
        );
    }

    my $closed = $sth->rows;
    $dbh->commit;

    write_log(
        "close|$param{'id'}|$z31_payment_date_key|$z303_rec_key|$param{'amount'}|$param{'time'}|$closed"
    ) if $closed;

    $api_response->{'amount'} = $tr_amount;
    $api_response->{'pid'}    = $param{'time'};

}

sub api_error {
    $api_response->{'diag'} = "unknown op";
    die "unknown op";
}

sub append_escape {
    my ( $key, $value ) = @_;
    return join( '=', $key, uri_escape($value) );
}

sub get_time3 {
    my ( $seconds, $microseconds ) = gettimeofday;
    return strftime( q/%FT%T/, localtime($seconds) )
        . sprintf( ".%03d", $microseconds / 1000 );
}

sub get_time_stamp_15 {
    my ( $seconds, $microseconds ) = gettimeofday;
    return strftime( q/%Y%m%d%H%M%S/, localtime($seconds) )
        . sprintf( "%1.1u", $microseconds / 100000 );
} ## end sub get_time_stamp_15

sub read_config {
    my $file = shift;

    #    open( IN, "<", $file ) || die "cannot open config file: $file\n";
    open( IN, "<", $file ) || return;
    while (<IN>) {
        if (/^[;!\#]/) {
            next;
        }
        if (/^\s*([a-z][a-z0-9_]*)\s*=\s*(.*?)\s*$/i) {
            $config{ ($1) } = $2;
        }
    } ## end while (<IN>)
    close IN;
} ## end sub read_config

sub write_log {
    print LOGFILE get_time3(), " ", shift(@_), "\n"
        || die "cannot write log: $!";
}

sub find_z303_rec_key {

    my $par_id = $param{'id'};
    chomp($par_id);

    $sql = <<EOT;
select count(distinct Z308_ID)
from ${usr_library}.z308
where trim(substr(Z308_REC_KEY,3)) = ?
EOT

    $sth = $dbh->prepare($sql);
    $sth->execute($par_id);
    my ($rows) = $sth->fetchrow_array();

    if ( $rows != 1 ) {
        $api_response->{'diag'} = "patron id not found";

        die "unique z308_id not found, count=$rows";
    }

    $sql = <<EOT;
select distinct Z308_ID
from ${usr_library}.z308
where trim(substr(Z308_REC_KEY,3)) = ?
EOT

    $sth = $dbh->prepare($sql);
    $sth->execute($par_id);

    ($z303_rec_key) = $sth->fetchrow_array();

} ## end sub check_z303_id

sub print_xml_response {
    my $api_response_xml = XMLout(
        $api_response,

        #NoAttr => 1,
        KeyAttr => [],

        #RootName => undef,
        RootName => $param{'op'},
        XMLDecl  => '<?xml version = "1.0" encoding = "UTF-8"?>',

        #XMLDecl => 1,
    );
    print STDOUT "$api_response_xml\n";
}

sub check_digest {

    my $text       = shift;
    my $key_string = read_file( $ENV{"alephe_tab"} . "/$key_file" );
    my $rsa_pub    = Crypt::OpenSSL::RSA->new_public_key($key_string);
    my $signature  = decode_base64( $q->param('DIGEST') );

    $api_response->{'diag'} = "signature error"
        unless $rsa_pub->verify( $text, $signature );

    die $api_response->{'diag'} if defined $api_response->{'diag'};

}

sub check_time_diff {

    my $time_dif = abs( $param{'time'} - time );

    $api_response->{'diag'} = "max allowed TIME difference exceeded"
        if $time_dif > $max_time_diff;

    die $api_response->{'diag'} if defined $api_response->{'diag'};

}

sub get_and_check_params {

    foreach ( $q->param ) {

        $param{'op'}     = $q->param($_) if /^op$/i;
        $param{'time'}   = $q->param($_) if /^time$/i;
        $param{'id'}     = $q->param($_) if /^id$/i;
        $param{'amount'} = $q->param($_) if /^amount$/i;
        $param{'tr'}     = $q->param($_) if /^tr$/i;
        $param{'digest'} = $q->param($_) if /^digest$/i;
        $param{'adm'}    = $q->param($_) if /^adm$/i;

    }

    #
    # OP:  list, open, close
    # mandatory parameters: OP, TIME, ID
    # AMOUNT (open, close): numeric, max 99999999 (999.999,99)
    # TR (close): "ALL" / Z31_REC_KEY(27)
    #

    $api_response->{'diag'} = "missing OP" unless defined $param{'op'};
    die $api_response->{'diag'} if defined $api_response->{'diag'};

    $api_response->{'diag'} = "missing TIME" unless defined $param{'time'};
    $api_response->{'diag'} = "missing ID"   unless defined $param{'id'};
    $api_response->{'diag'} = "missing ADM"  unless defined $param{'adm'};
    $api_response->{'diag'} = "unknown OP"
        unless $param{'op'} =~ /^(list|open|close)$/i;
    die $api_response->{'diag'} if defined $api_response->{'diag'};

    if ( $param{'op'} =~ /open|close/ ) {
        $api_response->{'diag'} = "missing AMOUNT"
            unless defined $param{'amount'};
    }

    if ( $param{'op'} =~ /close/ ) {
        $api_response->{'diag'} = "missing TR" unless defined $param{'tr'};
    }

    $api_response->{'diag'} = "missing DIGEST"
        unless ( defined $param{'digest'} || $ignore_digest );

    my $sig_text = join( '|', $param{'op'}, $param{'time'}, $param{'id'},
        $param{'adm'} );
    $sig_text = join( '|', $sig_text, $param{'amount'} )
        if ( defined $param{'amount'} );
    $sig_text = join( '|', $sig_text, $param{'tr'} )
        if ( defined $param{'tr'} );

    $param{'adm'} = uc( $param{'adm'} );
    $param{'op'}  = lc( $param{'op'} );
    $param{'id'}  = uc( $param{'id'} );
    $param{'tr'}  = uc( $param{'tr'} ) if ( defined $param{'tr'} );

    $api_response->{'diag'} = "wrong adm"
        unless $param{'adm'} =~ /^[A-Z]{3}5[0-9]$/;
    $api_response->{'diag'} = "wrong id" unless $param{'id'} =~ /^[A-Z0-9]+$/;
    $api_response->{'diag'} = "wrong time"
        unless $param{'time'} =~ /^[0-9]{10}$/;

    if ( defined $param{'amount'} ) {
        $api_response->{'diag'} = "wrong AMOUNT"
            unless $param{'amount'} =~ /^0*[0-9]{1,8}$/;
    }

    $param{'amount'} += 0;

    die $api_response->{'diag'} if defined $api_response->{'diag'};

    check_digest($sig_text) unless $ignore_digest;

}

sub check_libs {

    $sql = <<EOT;
       select * FROM ${adm_library}.z31 where 1 = 0
EOT

    my $print_error = $dbh->{PrintError};
    $dbh->{PrintError} = 0;

    eval {

        $sth = $dbh->prepare($sql);
        $sth->execute();
    } or $api_response->{'diag'} = "ADM base not found\n";

    die $api_response->{'diag'} if defined $api_response->{'diag'};

    $sql = <<EOT;
       select * FROM ${usr_library}.z308 where 1 = 0
EOT

    eval {

        $sth = $dbh->prepare($sql);
        $sth->execute();
    } or die "z308 table not found in usr_library ${usr_library}\n";

    $dbh->{PrintError} = $print_error;

}
