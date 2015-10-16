#!/usr/bin/perl

use strict;
use warnings;

$| = 1;

# mysqldump -uroot -p mysql help_keyword > test.sql
my $sql_file = shift @ARGV;
die ".sql should be passed" unless $sql_file;

my $size = 1024;

my $buf; my $_is_started = 0;
open(my $fh, '<', $sql_file) or die "Can't open $sql_file: $!\n";
while (sysread($fh, my $bytes, $size)) {
    $buf .= $bytes;

    if (not $_is_started and $buf =~ /^INSERT INTO \`(.*?)\` VALUES /m) {
        $buf =~ s/^(.*?)INSERT INTO \`(.*?)\` VALUES //s;
        $_is_started = 1;
    }
    next unless $_is_started;

    # I know it will be broken in some certain case but I do not have that case
    while ($buf =~ s/\((.*?)\)((\,\()|(\;[\r\n]+))/$2/s) { ## ends with '),(' or ');\n'
        my ($pair, $splitter) = ($1, $2);
        print "$pair\n"; # just print out

        $_is_started = 0 if $splitter =~ ';'; # LINE END
        if (not $_is_started and $buf =~ /^INSERT INTO \`(.*?)\` VALUES /m) {
            $buf =~ s/^(.*?)INSERT INTO \`(.*?)\` VALUES //s;
            $_is_started = 1;
        }
        last unless $_is_started;
        $buf =~ s/^\,//;
    }
}
close($fh);

1;
__END__