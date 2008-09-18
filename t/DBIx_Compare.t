#! /usr/bin/perl -w

use strict;

use FindBin;
use Test::More tests=>19;
use Test::Group;
use Test::Differences;

use DBI;

# 1
BEGIN {
	use_ok('DBIx::Compare');
	$SIG{__WARN__} = \&trap_warn;
}

my $user_name = 'test';
my $user_pass = '';

my $dsn1 = "DBI:mysql:test:localhost";
my $dsn2 = "DBI:mysql:test2:localhost";

my ($dbh1,$dbh2,$oDB_Content,$oDB_Content2,$oDB_Content3,$oDB_Content4,$oDB_Content5,$oDB_Content6,$sql_file1,$sql_file2);

eval {
	$dbh1 = DBI->connect($dsn1, $user_name, $user_pass);
	$dbh2 = DBI->connect($dsn2, $user_name, $user_pass);
};

if ($dbh1 && $dbh2){
	create_test_db($dbh1);
	create_test_db($dbh2);
} else {
	begin_skipping_tests "Could not create the test databases";
}

#2
test 'object init' => sub {
	ok($oDB_Content = db_comparison->new($dbh1,$dbh2),'init');
	isa_ok($oDB_Content,'db_comparison','DBIx::Compare object');
	my ($dbh1b,$dbh2b) = $oDB_Content->get_dbh;
	isa_ok($dbh1b,'DBI::db','dbh1 after set');
	isa_ok($dbh2b,'DBI::db','dbh2 after set');
};

#3
test 'db names' => sub {
	ok(my @aNames = $oDB_Content->get_db_names,'get_db_names');
	eq_or_diff \@aNames,['test:localhost','test2:localhost'],'database names';
};

#4
test 'table lists' => sub {
	my (@aTables,$aTables1);
	ok(@aTables = $oDB_Content->get_tables,'get_tables 1 & 2');
	eq_or_diff \@aTables,[['filter','fluorochrome','laser','protocol_type'],['filter','fluorochrome','laser','protocol_type']],'table lists';
	ok($aTables1 = $oDB_Content->get_tables,'get_tables 1');
	eq_or_diff $aTables1,$aTables[0],'tables vs tables1';
};

#5
test 'primary keys' => sub {
	my (@aKeys,$keys);
	ok($keys = $oDB_Content->get_primary_keys('filter',$dbh1),'get_primary_keys');
	cmp_ok($keys,'eq','filter_id','primary key string');
	ok(@aKeys = $oDB_Content->get_primary_keys('filter',$dbh1),'get_primary_keys');
	eq_or_diff \@aKeys,['filter_id'],'primary key list';
};

#6
test 'row counts' => sub {
	cmp_ok($oDB_Content->row_count('protocol_type',$dbh1),'==',4,'row_count');
	cmp_ok($oDB_Content->row_count('filter',$dbh1),'==',3,'row_count');
	cmp_ok($oDB_Content->row_count('laser',$dbh1),'==',3,'row_count');
	cmp_ok($oDB_Content->row_count('fluorochrome',$dbh1),'==',3,'row_count');
};

#7
test 'the comparisons' => sub {
	my ($hDiffs,$hDiffs1);
	cmp_ok($oDB_Content->compare_table_lists,'==',1,'compare_table_lists');
	cmp_ok($oDB_Content->compare_field_lists,'==',1,'compare_field_lists');
	cmp_ok($oDB_Content->compare_row_counts,'==',1,'compare_row_counts');
	
	ok($oDB_Content->compare,'compare in void context');	# just re-does the above
	ok($hDiffs1 = $oDB_Content->compare,'compare in scalar context');
	eq_or_diff $hDiffs1,{},'differences hashref';
	
	ok($hDiffs = $oDB_Content->get_differences,'get_differences');
	eq_or_diff $hDiffs,{},'differences hashref';

	cmp_ok($oDB_Content->deep_compare,'==',1,'deep_compare');
	eq_or_diff $hDiffs1,{},'differences hashref';
};

### now make the two databases different ###

add_differences($dbh1) if ($dbh1);

#8
test 'object re-init' => sub {
	ok($oDB_Content2 = db_comparison->new($dbh1,$dbh2),'init');
	isa_ok($oDB_Content2,'db_comparison','DBIx::Compare object');
	my ($dbh1b,$dbh2b) = $oDB_Content2->get_dbh;
	isa_ok($dbh1b,'DBI::db','dbh1 after set');
	isa_ok($dbh2b,'DBI::db','dbh2 after set');
};

###--------------------------------------###

#9
test 'no primary key in table extra' => sub {
	my (@aKeys,$keys);
	$keys = $oDB_Content2->get_primary_keys('extra',$dbh1);
	is($keys,undef,'primary key string');
	@aKeys = $oDB_Content2->get_primary_keys('extra',$dbh1);
	cmp_ok(@aKeys,'==',0,'primary key list');
};

#10
test 're-examine databases' => sub {
	my @aTables;
	
	# table lists
	ok(@aTables = $oDB_Content2->get_tables,'get_tables 1 & 2');
	eq_or_diff \@aTables,[['extra','filter','fluorochrome','laser','protocol_type'],['filter','fluorochrome','laser','protocol_type']],'table lists';
	
	# extra row in filter
	cmp_ok($oDB_Content2->row_count('filter',$dbh1),'==',4,'row_count');
};

#11
test 're-do the individual comparisons' => sub {
	my $hDiffs2;
	is($oDB_Content2->compare_table_lists,undef,'compare_table_lists');
	is($oDB_Content2->compare_table_fields,undef,'compare_table_fields');
	is($oDB_Content2->compare_row_counts,undef,'compare_row_counts');
	
	ok($hDiffs2 = $oDB_Content2->get_differences,'get_differences');
	eq_or_diff $hDiffs2,{ 
			'Fields unique to test2:localhost.fluorochrome' => ['cf260'],
			'Row count' => ['filter'],
			'Tables unique to test:localhost' => ['extra']
		},'differences';
};	

### re-init for another round of comparison ###
#12
test 'object re-init' => sub {
	ok($oDB_Content3 = db_comparison->new($dbh1,$dbh2),'init');
	isa_ok($oDB_Content3,'db_comparison','DBIx::Compare object');
	my ($dbh1b,$dbh2b) = $oDB_Content3->get_dbh;
	isa_ok($dbh1b,'DBI::db','dbh1 after set');
	isa_ok($dbh2b,'DBI::db','dbh2 after set');
};

###--------------------------------------###
	
#13
test 're-do the comparison using compare in scalar context' => sub {
	my $hDiffs3;
	ok($hDiffs3 = $oDB_Content3->compare,'compare');	# just re-does the above
	eq_or_diff $hDiffs3,{ 
			'Fields unique to test2:localhost.fluorochrome' => ['cf260'],
			'Row count' => ['filter'],
			'Tables unique to test:localhost' => ['extra']
		},'differences';
};

### re-init for another round of comparison ###
#14
test 'object re-init' => sub {
	ok($oDB_Content4 = db_comparison->new($dbh1,$dbh2),'init');
	isa_ok($oDB_Content4,'db_comparison','DBIx::Compare object');
	my ($dbh1b,$dbh2b) = $oDB_Content4->get_dbh;
	isa_ok($dbh1b,'DBI::db','dbh1 after set');
	isa_ok($dbh2b,'DBI::db','dbh2 after set');
};

###--------------------------------------###

#15
test 're-do deep_compare' => sub {
	my $hDiffs4;
	is($oDB_Content4->deep_compare,undef,'deep_compare');
	ok($hDiffs4 = $oDB_Content4->get_differences,'get_differences');
	eq_or_diff $hDiffs4,{ 
			'Discrepancy in table laser' => [2],
			'Fields unique to test2:localhost.fluorochrome' => ['cf260'],
			'Row count' => ['filter'],
			'Tables unique to test:localhost' => ['extra']
		},'differences';
};

### re-init for another round of comparison, the other way round ###
#16
test 'object re-init' => sub {
	ok($oDB_Content5 = db_comparison->new($dbh2,$dbh1),'init');
	isa_ok($oDB_Content5,'db_comparison','DBIx::Compare object');
	my ($dbh2b,$dbh1b) = $oDB_Content5->get_dbh;
	isa_ok($dbh1b,'DBI::db','dbh1 after set');
	isa_ok($dbh2b,'DBI::db','dbh2 after set');
};

###--------------------------------------###

#17
test 're-do the comparison using compare in scalar context' => sub {
	my $hDiffs5;
	ok($hDiffs5 = $oDB_Content5->compare,'compare');	# just re-does the above
	eq_or_diff $hDiffs5,{ 
			'Fields unique to test2:localhost.fluorochrome' => ['cf260'],
			'Row count' => ['filter'],
			'Tables unique to test:localhost' => ['extra']
		},'differences';
};

### re-init for another round of comparison, the other way round ###
#18
test 'object re-init' => sub {
	ok($oDB_Content6 = db_comparison->new($dbh2,$dbh1),'init');
	isa_ok($oDB_Content6,'db_comparison','DBIx::Compare object');
	my ($dbh2b,$dbh1b) = $oDB_Content6->get_dbh;
	isa_ok($dbh1b,'DBI::db','dbh1 after set');
	isa_ok($dbh2b,'DBI::db','dbh2 after set');
};

###--------------------------------------###

#19
test 're-do deep_compare' => sub {
	my $hDiffs6;
	is($oDB_Content6->deep_compare,undef,'deep_compare');
	ok($hDiffs6 = $oDB_Content6->get_differences,'get_differences');
	eq_or_diff $hDiffs6,{ 
			'Discrepancy in table laser' => [2],
			'Fields unique to test2:localhost.fluorochrome' => ['cf260'],
			'Row count' => ['filter'],
			'Tables unique to test:localhost' => ['extra']
		},'differences';
};

# tests finished - disconnect from test
$dbh1->disconnect if ($dbh1);
$dbh2->disconnect if ($dbh2);


end_skipping_tests;





sub create_test_db {
	my $dbh = shift;
	drop_tables($dbh);
	my %hTables = return_tables();
	while (my ($table,$create) = each %hTables){
		$dbh->do($create);
	}
	insert_data($dbh);
	return 1;
}
sub drop_tables {
	my $dbh = shift;
	my (@aTables,$value);
	my $sth = $dbh->prepare('show tables');
	$sth->execute(); 
	$sth->bind_columns(undef, \$value);
	while($sth->fetch()) {
		push @aTables, $value;
	}
	$sth->finish(); 
	for my $table (@aTables){
		$dbh->do("drop table $table"); 
	}
}
sub insert_data {
	my $dbh = shift;
	$dbh->do("insert into filter values('1','522',NULL),('3','570',NULL),('8','670',NULL)");
	$dbh->do("insert into laser values('0','Red','633'),('2','Green','543'),('3','Blue','488')");
	$dbh->do("insert into fluorochrome values('11','Cyanine 5','649','670','0','8',NULL,250000,649,0.25),('3','Cyanine 3','550','570','2','3',NULL,150000,550,0.15),('13','Alexa 488','490','519','3','1',NULL,62000,492,0.30)");
	$dbh->do("insert into protocol_type values(1,'Other','Other types of protocol'),(2,'Hybridisation','CGH Microarray hybridisation protocol'),(3,'Labelling','DNA labelling reaction'),(4,'Plate manipulation','Transfer of samples from one plate to another, or joining/splitting of plates')");
}
sub return_tables {
	return (
		"filter",
		"CREATE TABLE filter (
			filter_id tinyint(2) unsigned NOT NULL,
			nm_peak int(3) unsigned NOT NULL,
			nm_width int(3) unsigned DEFAULT NULL,
			PRIMARY KEY (filter_id)
		) ENGINE=MyISAM",
		"laser",
		"CREATE TABLE laser (
			laser_id tinyint(1) unsigned NOT NULL,
			colour_name varchar(20) NOT NULL,
			nm_wavelength int(3) unsigned NOT NULL,
			PRIMARY KEY (laser_id)
		) ENGINE=MyISAM",
		"fluorochrome",
		"CREATE TABLE fluorochrome (
			fluorochrome_id tinyint(2) unsigned NOT NULL,
			name varchar(30) NOT NULL,
			excitation_nm int(3) unsigned NOT NULL,
			emission_nm int(3) unsigned NOT NULL,
			laser_id tinyint(1) unsigned NOT NULL,
			filter_id tinyint(2) unsigned NOT NULL,
			manufacturer varchar(30) DEFAULT NULL,
			extinction_coefficient int(7) unsigned NOT NULL,
			lambda_max int(3) unsigned NOT NULL,
			cf260 double(3,2) unsigned NOT NULL,
			PRIMARY KEY (fluorochrome_id)
		) ENGINE=MyISAM",
		"protocol_type",
		"CREATE TABLE protocol_type (
			protocol_type_id int(6) unsigned NOT NULL,
			type_name varchar(100) NOT NULL,
			description text,
			PRIMARY KEY (protocol_type_id)
		) ENGINE=MyISAM"
	);
}
sub add_differences {
	my $dbh = shift;
	$dbh->do(
		"CREATE TABLE extra (
			extra_id int(1) unsigned not null, 
			KEY extra_id (extra_id) 
		) ENGINE=MyISAM"
	);
	$dbh->do("insert into extra values(1),(2),(3),(4),(5)");
	$dbh->do("insert into filter values('2','545',NULL)");
	$dbh->do("update laser set colour_name = 'Greeny' where laser_id = 2");
	$dbh->do("alter table fluorochrome drop column cf260");
	
}

sub trap_warn {
	my $signal = shift;
	if ($signal =~ /Use of uninitialized value in join or string at .*DBIx-Compare-ContentChecksum-mysql-1\.0\/blib\/lib\/DBIx\/Compare\.pm line 121\./){
		return 1;
	} else {
		return 0;
	}
}

