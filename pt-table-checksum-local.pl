#!/usr/bin/env perl

# This program is part of Percona Toolkit: http://www.percona.com/software/
# See "COPYRIGHT, LICENSE, AND WARRANTY" at the end of this file for legal
# notices and disclaimers.

use strict;
use warnings FATAL => 'all';

# This tool is "fat-packed": most of its dependent modules are embedded
# in this file.  Setting %INC to this file for each module makes Perl aware
# of this so it will not try to load the module from @INC.  See the tool's
# documentation for a full list of dependencies.
BEGIN {
   $INC{$_} = __FILE__ for map { (my $pkg = "$_.pm") =~ s!::!/!g; $pkg } (qw(
      Percona::Toolkit
      HTTP::Micro
      VersionCheck
      DSNParser
      OptionParser
      Lmo::Utils
      Lmo::Meta
      Lmo::Object
      Lmo::Types
      Lmo
      Cxn
      Percona::XtraDB::Cluster
      Quoter
      VersionParser
      TableParser
      TableNibbler
      MasterSlave
      RowChecksum
      NibbleIterator
      OobNibbleIterator
      Daemon
      SchemaIterator
      Retry
      Transformers
      Progress
      ReplicaLagWaiter
      MySQLConfig
      MySQLStatusWaiter
      WeightedAvgRate
      IndexLength
      Runtime
   ));
}

# ###########################################################################
# Percona::Toolkit package
# This package is a copy without comments from the original.  The original
# with comments and its test file can be found in the GitHub repository at,
#   lib/Percona/Toolkit.pm
#   t/lib/Percona/Toolkit.t
# See https://github.com/percona/percona-toolkit for more information.
# ###########################################################################
{
package Percona::Toolkit;

our $VERSION = '3.5.1';

use strict;
use warnings FATAL => 'all';
use English qw(-no_match_vars);
use constant PTDEBUG => $ENV{PTDEBUG} || 0;

use Carp qw(carp cluck);
use Data::Dumper qw();

require Exporter;
our @ISA         = qw(Exporter);
our @EXPORT_OK   = qw(
   have_required_args
   Dumper
   _d
);

sub have_required_args {
   my ($args, @required_args) = @_;
   my $have_required_args = 1;
   foreach my $arg ( @required_args ) {
      if ( !defined $args->{$arg} ) {
         $have_required_args = 0;
         carp "Argument $arg is not defined";
      }
   }
   cluck unless $have_required_args;  # print backtrace
   return $have_required_args;
}

sub Dumper {
   local $Data::Dumper::Indent    = 1;
   local $Data::Dumper::Sortkeys  = 1;
   local $Data::Dumper::Quotekeys = 0;
   Data::Dumper::Dumper(@_);
}

sub _d {
   my ($package, undef, $line) = caller 0;
   @_ = map { (my $temp = $_) =~ s/\n/\n# /g; $temp; }
        map { defined $_ ? $_ : 'undef' }
        @_;
   print STDERR "# $package:$line $PID ", join(' ', @_), "\n";
}

1;
}
# ###########################################################################
# End Percona::Toolkit package
# ###########################################################################

# ###########################################################################
# HTTP::Micro package
# This package is a copy without comments from the original.  The original
# with comments and its test file can be found in the GitHub repository at,
#   lib/HTTP/Micro.pm
#   t/lib/HTTP/Micro.t
# See https://github.com/percona/percona-toolkit for more information.
# ###########################################################################
{
package HTTP::Micro;

our $VERSION = '0.01';

use strict;
use warnings FATAL => 'all';
use English qw(-no_match_vars);
use Carp ();

my @attributes;
BEGIN {
    @attributes = qw(agent timeout);
    no strict 'refs';
    for my $accessor ( @attributes ) {
        *{$accessor} = sub {
            @_ > 1 ? $_[0]->{$accessor} = $_[1] : $_[0]->{$accessor};
        };
    }
}

sub new {
    my($class, %args) = @_;
    (my $agent = $class) =~ s{::}{-}g;
    my $self = {
        agent        => $agent . "/" . ($class->VERSION || 0),
        timeout      => 60,
    };
    for my $key ( @attributes ) {
        $self->{$key} = $args{$key} if exists $args{$key}
    }
    return bless $self, $class;
}

my %DefaultPort = (
    http => 80,
    https => 443,
);

sub request {
    my ($self, $method, $url, $args) = @_;
    @_ == 3 || (@_ == 4 && ref $args eq 'HASH')
      or Carp::croak(q/Usage: $http->request(METHOD, URL, [HASHREF])/);
    $args ||= {}; # we keep some state in this during _request

    my $response;
    for ( 0 .. 1 ) {
        $response = eval { $self->_request($method, $url, $args) };
        last unless $@ && $method eq 'GET'
            && $@ =~ m{^(?:Socket closed|Unexpected end)};
    }

    if (my $e = "$@") {
        $response = {
            success => q{},
            status  => 599,
            reason  => 'Internal Exception',
            content => $e,
            headers => {
                'content-type'   => 'text/plain',
                'content-length' => length $e,
            }
        };
    }
    return $response;
}

sub _request {
    my ($self, $method, $url, $args) = @_;

    my ($scheme, $host, $port, $path_query) = $self->_split_url($url);

    my $request = {
        method    => $method,
        scheme    => $scheme,
        host_port => ($port == $DefaultPort{$scheme} ? $host : "$host:$port"),
        uri       => $path_query,
        headers   => {},
    };

    my $handle  = HTTP::Micro::Handle->new(timeout => $self->{timeout});

    $handle->connect($scheme, $host, $port);

    $self->_prepare_headers_and_cb($request, $args);
    $handle->write_request_header(@{$request}{qw/method uri headers/});
    $handle->write_content_body($request) if $request->{content};

    my $response;
    do { $response = $handle->read_response_header }
        until (substr($response->{status},0,1) ne '1');

    if (!($method eq 'HEAD' || $response->{status} =~ /^[23]04/)) {
        $response->{content} = '';
        $handle->read_content_body(sub { $_[1]->{content} .= $_[0] }, $response);
    }

    $handle->close;
    $response->{success} = substr($response->{status},0,1) eq '2';
    return $response;
}

sub _prepare_headers_and_cb {
    my ($self, $request, $args) = @_;

    for ($args->{headers}) {
        next unless defined;
        while (my ($k, $v) = each %$_) {
            $request->{headers}{lc $k} = $v;
        }
    }
    $request->{headers}{'host'}         = $request->{host_port};
    $request->{headers}{'connection'}   = "close";
    $request->{headers}{'user-agent'} ||= $self->{agent};

    if (defined $args->{content}) {
        $request->{headers}{'content-type'} ||= "application/octet-stream";
        utf8::downgrade($args->{content}, 1)
            or Carp::croak(q/Wide character in request message body/);
        $request->{headers}{'content-length'} = length $args->{content};
        $request->{content} = $args->{content};
    }
    return;
}

sub _split_url {
    my $url = pop;

    my ($scheme, $authority, $path_query) = $url =~ m<\A([^:/?#]+)://([^/?#]*)([^#]*)>
      or Carp::croak(qq/Cannot parse URL: '$url'/);

    $scheme     = lc $scheme;
    $path_query = "/$path_query" unless $path_query =~ m<\A/>;

    my $host = (length($authority)) ? lc $authority : 'localhost';
       $host =~ s/\A[^@]*@//;   # userinfo
    my $port = do {
       $host =~ s/:([0-9]*)\z// && length $1
         ? $1
         : $DefaultPort{$scheme}
    };

    return ($scheme, $host, $port, $path_query);
}

} # HTTP::Micro

{
   package HTTP::Micro::Handle;

   use strict;
   use warnings FATAL => 'all';
   use English qw(-no_match_vars);

   use Carp       qw(croak);
   use Errno      qw(EINTR EPIPE);
   use IO::Socket qw(SOCK_STREAM);

   sub BUFSIZE () { 32768 }

   my $Printable = sub {
       local $_ = shift;
       s/\r/\\r/g;
       s/\n/\\n/g;
       s/\t/\\t/g;
       s/([^\x20-\x7E])/sprintf('\\x%.2X', ord($1))/ge;
       $_;
   };

   sub new {
       my ($class, %args) = @_;
       return bless {
           rbuf          => '',
           timeout       => 60,
           max_line_size => 16384,
           %args
       }, $class;
   }

   my $ssl_verify_args = {
       check_cn         => "when_only",
       wildcards_in_alt => "anywhere",
       wildcards_in_cn  => "anywhere"
   };

   sub connect {
       @_ == 4 || croak(q/Usage: $handle->connect(scheme, host, port)/);
       my ($self, $scheme, $host, $port) = @_;

       if ( $scheme eq 'https' ) {
           eval "require IO::Socket::SSL"
               unless exists $INC{'IO/Socket/SSL.pm'};
           croak(qq/IO::Socket::SSL must be installed for https support\n/)
               unless $INC{'IO/Socket/SSL.pm'};
       }
       elsif ( $scheme ne 'http' ) {
         croak(qq/Unsupported URL scheme '$scheme'\n/);
       }

       $self->{fh} = IO::Socket::INET->new(
           PeerHost  => $host,
           PeerPort  => $port,
           Proto     => 'tcp',
           Type      => SOCK_STREAM,
           Timeout   => $self->{timeout}
       ) or croak(qq/Could not connect to '$host:$port': $@/);

       binmode($self->{fh})
         or croak(qq/Could not binmode() socket: '$!'/);

       if ( $scheme eq 'https') {
           IO::Socket::SSL->start_SSL($self->{fh});
           ref($self->{fh}) eq 'IO::Socket::SSL'
               or die(qq/SSL connection failed for $host\n/);
           if ( $self->{fh}->can("verify_hostname") ) {
               $self->{fh}->verify_hostname( $host, $ssl_verify_args )
                  or die(qq/SSL certificate not valid for $host\n/);
           }
           else {
            my $fh = $self->{fh};
            _verify_hostname_of_cert($host, _peer_certificate($fh), $ssl_verify_args)
                  or die(qq/SSL certificate not valid for $host\n/);
            }
       }
         
       $self->{host} = $host;
       $self->{port} = $port;

       return $self;
   }

   sub close {
       @_ == 1 || croak(q/Usage: $handle->close()/);
       my ($self) = @_;
       CORE::close($self->{fh})
         or croak(qq/Could not close socket: '$!'/);
   }

   sub write {
       @_ == 2 || croak(q/Usage: $handle->write(buf)/);
       my ($self, $buf) = @_;

       my $len = length $buf;
       my $off = 0;

       local $SIG{PIPE} = 'IGNORE';

       while () {
           $self->can_write
             or croak(q/Timed out while waiting for socket to become ready for writing/);
           my $r = syswrite($self->{fh}, $buf, $len, $off);
           if (defined $r) {
               $len -= $r;
               $off += $r;
               last unless $len > 0;
           }
           elsif ($! == EPIPE) {
               croak(qq/Socket closed by remote server: $!/);
           }
           elsif ($! != EINTR) {
               croak(qq/Could not write to socket: '$!'/);
           }
       }
       return $off;
   }

   sub read {
       @_ == 2 || @_ == 3 || croak(q/Usage: $handle->read(len)/);
       my ($self, $len) = @_;

       my $buf  = '';
       my $got = length $self->{rbuf};

       if ($got) {
           my $take = ($got < $len) ? $got : $len;
           $buf  = substr($self->{rbuf}, 0, $take, '');
           $len -= $take;
       }

       while ($len > 0) {
           $self->can_read
             or croak(q/Timed out while waiting for socket to become ready for reading/);
           my $r = sysread($self->{fh}, $buf, $len, length $buf);
           if (defined $r) {
               last unless $r;
               $len -= $r;
           }
           elsif ($! != EINTR) {
               croak(qq/Could not read from socket: '$!'/);
           }
       }
       if ($len) {
           croak(q/Unexpected end of stream/);
       }
       return $buf;
   }

   sub readline {
       @_ == 1 || croak(q/Usage: $handle->readline()/);
       my ($self) = @_;

       while () {
           if ($self->{rbuf} =~ s/\A ([^\x0D\x0A]* \x0D?\x0A)//x) {
               return $1;
           }
           $self->can_read
             or croak(q/Timed out while waiting for socket to become ready for reading/);
           my $r = sysread($self->{fh}, $self->{rbuf}, BUFSIZE, length $self->{rbuf});
           if (defined $r) {
               last unless $r;
           }
           elsif ($! != EINTR) {
               croak(qq/Could not read from socket: '$!'/);
           }
       }
       croak(q/Unexpected end of stream while looking for line/);
   }

   sub read_header_lines {
       @_ == 1 || @_ == 2 || croak(q/Usage: $handle->read_header_lines([headers])/);
       my ($self, $headers) = @_;
       $headers ||= {};
       my $lines   = 0;
       my $val;

       while () {
            my $line = $self->readline;

            if ($line =~ /\A ([^\x00-\x1F\x7F:]+) : [\x09\x20]* ([^\x0D\x0A]*)/x) {
                my ($field_name) = lc $1;
                $val = \($headers->{$field_name} = $2);
            }
            elsif ($line =~ /\A [\x09\x20]+ ([^\x0D\x0A]*)/x) {
                $val
                  or croak(q/Unexpected header continuation line/);
                next unless length $1;
                $$val .= ' ' if length $$val;
                $$val .= $1;
            }
            elsif ($line =~ /\A \x0D?\x0A \z/x) {
               last;
            }
            else {
               croak(q/Malformed header line: / . $Printable->($line));
            }
       }
       return $headers;
   }

   sub write_header_lines {
       (@_ == 2 && ref $_[1] eq 'HASH') || croak(q/Usage: $handle->write_header_lines(headers)/);
       my($self, $headers) = @_;

       my $buf = '';
       while (my ($k, $v) = each %$headers) {
           my $field_name = lc $k;
            $field_name =~ /\A [\x21\x23-\x27\x2A\x2B\x2D\x2E\x30-\x39\x41-\x5A\x5E-\x7A\x7C\x7E]+ \z/x
               or croak(q/Invalid HTTP header field name: / . $Printable->($field_name));
            $field_name =~ s/\b(\w)/\u$1/g;
            $buf .= "$field_name: $v\x0D\x0A";
       }
       $buf .= "\x0D\x0A";
       return $self->write($buf);
   }

   sub read_content_body {
       @_ == 3 || @_ == 4 || croak(q/Usage: $handle->read_content_body(callback, response, [read_length])/);
       my ($self, $cb, $response, $len) = @_;
       $len ||= $response->{headers}{'content-length'};

       croak("No content-length in the returned response, and this "
           . "UA doesn't implement chunking") unless defined $len;

       while ($len > 0) {
           my $read = ($len > BUFSIZE) ? BUFSIZE : $len;
           $cb->($self->read($read), $response);
           $len -= $read;
       }

       return;
   }

   sub write_content_body {
       @_ == 2 || croak(q/Usage: $handle->write_content_body(request)/);
       my ($self, $request) = @_;
       my ($len, $content_length) = (0, $request->{headers}{'content-length'});

       $len += $self->write($request->{content});

       $len == $content_length
         or croak(qq/Content-Length missmatch (got: $len expected: $content_length)/);

       return $len;
   }

   sub read_response_header {
       @_ == 1 || croak(q/Usage: $handle->read_response_header()/);
       my ($self) = @_;

       my $line = $self->readline;

       $line =~ /\A (HTTP\/(0*\d+\.0*\d+)) [\x09\x20]+ ([0-9]{3}) [\x09\x20]+ ([^\x0D\x0A]*) \x0D?\x0A/x
         or croak(q/Malformed Status-Line: / . $Printable->($line));

       my ($protocol, $version, $status, $reason) = ($1, $2, $3, $4);

       return {
           status   => $status,
           reason   => $reason,
           headers  => $self->read_header_lines,
           protocol => $protocol,
       };
   }

   sub write_request_header {
       @_ == 4 || croak(q/Usage: $handle->write_request_header(method, request_uri, headers)/);
       my ($self, $method, $request_uri, $headers) = @_;

       return $self->write("$method $request_uri HTTP/1.1\x0D\x0A")
            + $self->write_header_lines($headers);
   }

   sub _do_timeout {
       my ($self, $type, $timeout) = @_;
       $timeout = $self->{timeout}
           unless defined $timeout && $timeout >= 0;

       my $fd = fileno $self->{fh};
       defined $fd && $fd >= 0
         or croak(q/select(2): 'Bad file descriptor'/);

       my $initial = time;
       my $pending = $timeout;
       my $nfound;

       vec(my $fdset = '', $fd, 1) = 1;

       while () {
           $nfound = ($type eq 'read')
               ? select($fdset, undef, undef, $pending)
               : select(undef, $fdset, undef, $pending) ;
           if ($nfound == -1) {
               $! == EINTR
                 or croak(qq/select(2): '$!'/);
               redo if !$timeout || ($pending = $timeout - (time - $initial)) > 0;
               $nfound = 0;
           }
           last;
       }
       $! = 0;
       return $nfound;
   }

   sub can_read {
       @_ == 1 || @_ == 2 || croak(q/Usage: $handle->can_read([timeout])/);
       my $self = shift;
       return $self->_do_timeout('read', @_)
   }

   sub can_write {
       @_ == 1 || @_ == 2 || croak(q/Usage: $handle->can_write([timeout])/);
       my $self = shift;
       return $self->_do_timeout('write', @_)
   }
}  # HTTP::Micro::Handle

my $prog = <<'EOP';
BEGIN {
   if ( defined &IO::Socket::SSL::CAN_IPV6 ) {
      *CAN_IPV6 = \*IO::Socket::SSL::CAN_IPV6;
   }
   else {
      constant->import( CAN_IPV6 => '' );
   }
   my %const = (
      NID_CommonName => 13,
      GEN_DNS => 2,
      GEN_IPADD => 7,
   );
   while ( my ($name,$value) = each %const ) {
      no strict 'refs';
      *{$name} = UNIVERSAL::can( 'Net::SSLeay', $name ) || sub { $value };
   }
}
{
   use Carp qw(croak);
   my %dispatcher = (
      issuer =>  sub { Net::SSLeay::X509_NAME_oneline( Net::SSLeay::X509_get_issuer_name( shift )) },
      subject => sub { Net::SSLeay::X509_NAME_oneline( Net::SSLeay::X509_get_subject_name( shift )) },
   );
   if ( $Net::SSLeay::VERSION >= 1.30 ) {
      $dispatcher{commonName} = sub {
         my $cn = Net::SSLeay::X509_NAME_get_text_by_NID(
            Net::SSLeay::X509_get_subject_name( shift ), NID_CommonName);
         $cn =~s{\0$}{}; # work around Bug in Net::SSLeay <1.33
         $cn;
      }
   } else {
      $dispatcher{commonName} = sub {
         croak "you need at least Net::SSLeay version 1.30 for getting commonName"
      }
   }

   if ( $Net::SSLeay::VERSION >= 1.33 ) {
      $dispatcher{subjectAltNames} = sub { Net::SSLeay::X509_get_subjectAltNames( shift ) };
   } else {
      $dispatcher{subjectAltNames} = sub {
         return;
      };
   }

   $dispatcher{authority} = $dispatcher{issuer};
   $dispatcher{owner}     = $dispatcher{subject};
   $dispatcher{cn}        = $dispatcher{commonName};

   sub _peer_certificate {
      my ($self, $field) = @_;
      my $ssl = $self->_get_ssl_object or return;

      my $cert = ${*$self}{_SSL_certificate}
         ||= Net::SSLeay::get_peer_certificate($ssl)
         or return $self->error("Could not retrieve peer certificate");

      if ($field) {
         my $sub = $dispatcher{$field} or croak
            "invalid argument for peer_certificate, valid are: ".join( " ",keys %dispatcher ).
            "\nMaybe you need to upgrade your Net::SSLeay";
         return $sub->($cert);
      } else {
         return $cert
      }
   }


   my %scheme = (
      ldap => {
         wildcards_in_cn    => 0,
         wildcards_in_alt => 'leftmost',
         check_cn         => 'always',
      },
      http => {
         wildcards_in_cn    => 'anywhere',
         wildcards_in_alt => 'anywhere',
         check_cn         => 'when_only',
      },
      smtp => {
         wildcards_in_cn    => 0,
         wildcards_in_alt => 0,
         check_cn         => 'always'
      },
      none => {}, # do not check
   );

   $scheme{www}  = $scheme{http}; # alias
   $scheme{xmpp} = $scheme{http}; # rfc 3920
   $scheme{pop3} = $scheme{ldap}; # rfc 2595
   $scheme{imap} = $scheme{ldap}; # rfc 2595
   $scheme{acap} = $scheme{ldap}; # rfc 2595
   $scheme{nntp} = $scheme{ldap}; # rfc 4642
   $scheme{ftp}  = $scheme{http}; # rfc 4217


   sub _verify_hostname_of_cert {
      my $identity = shift;
      my $cert = shift;
      my $scheme = shift || 'none';
      if ( ! ref($scheme) ) {
         $scheme = $scheme{$scheme} or croak "scheme $scheme not defined";
      }

      return 1 if ! %$scheme; # 'none'

      my $commonName = $dispatcher{cn}->($cert);
      my @altNames   = $dispatcher{subjectAltNames}->($cert);

      if ( my $sub = $scheme->{callback} ) {
         return $sub->($identity,$commonName,@altNames);
      }


      my $ipn;
      if ( CAN_IPV6 and $identity =~m{:} ) {
         $ipn = IO::Socket::SSL::inet_pton(IO::Socket::SSL::AF_INET6,$identity)
            or croak "'$identity' is not IPv6, but neither IPv4 nor hostname";
      } elsif ( $identity =~m{^\d+\.\d+\.\d+\.\d+$} ) {
         $ipn = IO::Socket::SSL::inet_aton( $identity ) or croak "'$identity' is not IPv4, but neither IPv6 nor hostname";
      } else {
         if ( $identity =~m{[^a-zA-Z0-9_.\-]} ) {
            $identity =~m{\0} and croak("name '$identity' has \\0 byte");
            $identity = IO::Socket::SSL::idn_to_ascii($identity) or
               croak "Warning: Given name '$identity' could not be converted to IDNA!";
         }
      }

      my $check_name = sub {
         my ($name,$identity,$wtyp) = @_;
         $wtyp ||= '';
         my $pattern;
         if ( $wtyp eq 'anywhere' and $name =~m{^([a-zA-Z0-9_\-]*)\*(.+)} ) {
            $pattern = qr{^\Q$1\E[a-zA-Z0-9_\-]*\Q$2\E$}i;
         } elsif ( $wtyp eq 'leftmost' and $name =~m{^\*(\..+)$} ) {
            $pattern = qr{^[a-zA-Z0-9_\-]*\Q$1\E$}i;
         } else {
            $pattern = qr{^\Q$name\E$}i;
         }
         return $identity =~ $pattern;
      };

      my $alt_dnsNames = 0;
      while (@altNames) {
         my ($type, $name) = splice (@altNames, 0, 2);
         if ( $ipn and $type == GEN_IPADD ) {
            return 1 if $ipn eq $name;

         } elsif ( ! $ipn and $type == GEN_DNS ) {
            $name =~s/\s+$//; $name =~s/^\s+//;
            $alt_dnsNames++;
            $check_name->($name,$identity,$scheme->{wildcards_in_alt})
               and return 1;
         }
      }

      if ( ! $ipn and (
         $scheme->{check_cn} eq 'always' or
         $scheme->{check_cn} eq 'when_only' and !$alt_dnsNames)) {
         $check_name->($commonName,$identity,$scheme->{wildcards_in_cn})
            and return 1;
      }

      return 0; # no match
   }
}
EOP

eval { require IO::Socket::SSL };
if ( $INC{"IO/Socket/SSL.pm"} ) {
   eval $prog;
   die $@ if $@;
}

1;
# ###########################################################################
# End HTTP::Micro package
# ###########################################################################

# ###########################################################################
# VersionCheck package
# This package is a copy without comments from the original.  The original
# with comments and its test file can be found in the GitHub repository at,
#   lib/VersionCheck.pm
#   t/lib/VersionCheck.t
# See https://github.com/percona/percona-toolkit for more information.
# ###########################################################################
{
package VersionCheck;


use strict;
use warnings FATAL => 'all';
use English qw(-no_match_vars);

use constant PTDEBUG => $ENV{PTDEBUG} || 0;

use Data::Dumper;
local $Data::Dumper::Indent    = 1;
local $Data::Dumper::Sortkeys  = 1;
local $Data::Dumper::Quotekeys = 0;

use Digest::MD5 qw(md5_hex);
use Sys::Hostname qw(hostname);
use File::Basename qw();
use File::Spec;
use FindBin qw();

eval {
   require Percona::Toolkit;
   require HTTP::Micro;
};

my $home    = $ENV{HOME} || $ENV{HOMEPATH} || $ENV{USERPROFILE} || '.';
my @vc_dirs = (
   '/etc/percona',
   '/etc/percona-toolkit',
   '/tmp',
   "$home",
);

{
   my $file    = 'percona-version-check';

   sub version_check_file {
      foreach my $dir ( @vc_dirs ) {
         if ( -d $dir && -w $dir ) {
            PTDEBUG && _d('Version check file', $file, 'in', $dir);
            return $dir . '/' . $file;
         }
      }
      PTDEBUG && _d('Version check file', $file, 'in', $ENV{PWD});
      return $file;  # in the CWD
   } 
}

sub version_check_time_limit {
   return 60 * 60 * 24;  # one day
}


sub version_check {
   my (%args) = @_;

   my $instances = $args{instances} || [];
   my $instances_to_check;

   PTDEBUG && _d('FindBin::Bin:', $FindBin::Bin);
   if ( !$args{force} ) {
      if ( $FindBin::Bin
           && (-d "$FindBin::Bin/../.bzr"    || 
               -d "$FindBin::Bin/../../.bzr" ||
               -d "$FindBin::Bin/../.git"    || 
               -d "$FindBin::Bin/../../.git" 
              ) 
         ) {
         PTDEBUG && _d("$FindBin::Bin/../.bzr disables --version-check");
         return;
      }
   }

   eval {
      foreach my $instance ( @$instances ) {
         my ($name, $id) = get_instance_id($instance);
         $instance->{name} = $name;
         $instance->{id}   = $id;
      }

      push @$instances, { name => 'system', id => 0 };

      $instances_to_check = get_instances_to_check(
         instances => $instances,
         vc_file   => $args{vc_file},  # testing
         now       => $args{now},      # testing
      );
      PTDEBUG && _d(scalar @$instances_to_check, 'instances to check');
      return unless @$instances_to_check;

      my $protocol = 'https';  
      eval { require IO::Socket::SSL; };
      if ( $EVAL_ERROR ) {
         PTDEBUG && _d($EVAL_ERROR);
         PTDEBUG && _d("SSL not available, won't run version_check");
         return;
      }
      PTDEBUG && _d('Using', $protocol);
      my $url = $args{url}                       # testing
                || $ENV{PERCONA_VERSION_CHECK_URL}  # testing
                || "$protocol://v.percona.com";
      PTDEBUG && _d('API URL:', $url);

      my $advice = pingback(
         instances => $instances_to_check,
         protocol  => $protocol,
         url       => $url,
      );
      if ( $advice ) {
         PTDEBUG && _d('Advice:', Dumper($advice));
         if ( scalar @$advice > 1) {
            print "\n# " . scalar @$advice . " software updates are "
               . "available:\n";
         }
         else {
            print "\n# A software update is available:\n";
         }
         print join("\n", map { "#   * $_" } @$advice), "\n\n";
      }
   };
   if ( $EVAL_ERROR ) {
      PTDEBUG && _d('Version check failed:', $EVAL_ERROR);
   }

   if ( @$instances_to_check ) {
      eval {
         update_check_times(
            instances => $instances_to_check,
            vc_file   => $args{vc_file},  # testing
            now       => $args{now},      # testing
         );
      };
      if ( $EVAL_ERROR ) {
         PTDEBUG && _d('Error updating version check file:', $EVAL_ERROR);
      }
   }

   if ( $ENV{PTDEBUG_VERSION_CHECK} ) {
      warn "Exiting because the PTDEBUG_VERSION_CHECK "
         . "environment variable is defined.\n";
      exit 255;
   }

   return;
}

sub get_instances_to_check {
   my (%args) = @_;

   my $instances = $args{instances};
   my $now       = $args{now}     || int(time);
   my $vc_file   = $args{vc_file} || version_check_file();

   if ( !-f $vc_file ) {
      PTDEBUG && _d('Version check file', $vc_file, 'does not exist;',
         'version checking all instances');
      return $instances;
   }

   open my $fh, '<', $vc_file or die "Cannot open $vc_file: $OS_ERROR";
   chomp(my $file_contents = do { local $/ = undef; <$fh> });
   PTDEBUG && _d('Version check file', $vc_file, 'contents:', $file_contents);
   close $fh;
   my %last_check_time_for = $file_contents =~ /^([^,]+),(.+)$/mg;

   my $check_time_limit = version_check_time_limit();
   my @instances_to_check;
   foreach my $instance ( @$instances ) {
      my $last_check_time = $last_check_time_for{ $instance->{id} };
      PTDEBUG && _d('Instance', $instance->{id}, 'last checked',
         $last_check_time, 'now', $now, 'diff', $now - ($last_check_time || 0),
         'hours until next check',
         sprintf '%.2f',
            ($check_time_limit - ($now - ($last_check_time || 0))) / 3600);
      if ( !defined $last_check_time
           || ($now - $last_check_time) >= $check_time_limit ) {
         PTDEBUG && _d('Time to check', Dumper($instance));
         push @instances_to_check, $instance;
      }
   }

   return \@instances_to_check;
}

sub update_check_times {
   my (%args) = @_;

   my $instances = $args{instances};
   my $now       = $args{now}     || int(time);
   my $vc_file   = $args{vc_file} || version_check_file();
   PTDEBUG && _d('Updating last check time:', $now);

   my %all_instances = map {
      $_->{id} => { name => $_->{name}, ts => $now }
   } @$instances;

   if ( -f $vc_file ) {
      open my $fh, '<', $vc_file or die "Cannot read $vc_file: $OS_ERROR";
      my $contents = do { local $/ = undef; <$fh> };
      close $fh;

      foreach my $line ( split("\n", ($contents || '')) ) {
         my ($id, $ts) = split(',', $line);
         if ( !exists $all_instances{$id} ) {
            $all_instances{$id} = { ts => $ts };  # original ts, not updated
         }
      }
   }

   open my $fh, '>', $vc_file or die "Cannot write to $vc_file: $OS_ERROR";
   foreach my $id ( sort keys %all_instances ) {
      PTDEBUG && _d('Updated:', $id, Dumper($all_instances{$id}));
      print { $fh } $id . ',' . $all_instances{$id}->{ts} . "\n";
   }
   close $fh;

   return;
}

sub get_instance_id {
   my ($instance) = @_;

   my $dbh = $instance->{dbh};
   my $dsn = $instance->{dsn};

   my $sql = q{SELECT CONCAT(@@hostname, @@port)};
   PTDEBUG && _d($sql);
   my ($name) = eval { $dbh->selectrow_array($sql) };
   if ( $EVAL_ERROR ) {
      PTDEBUG && _d($EVAL_ERROR);
      $sql = q{SELECT @@hostname};
      PTDEBUG && _d($sql);
      ($name) = eval { $dbh->selectrow_array($sql) };
      if ( $EVAL_ERROR ) {
         PTDEBUG && _d($EVAL_ERROR);
         $name = ($dsn->{h} || 'localhost') . ($dsn->{P} || 3306);
      }
      else {
         $sql = q{SHOW VARIABLES LIKE 'port'};
         PTDEBUG && _d($sql);
         my (undef, $port) = eval { $dbh->selectrow_array($sql) };
         PTDEBUG && _d('port:', $port);
         $name .= $port || '';
      }
   }
   my $id = md5_hex($name);

   PTDEBUG && _d('MySQL instance:', $id, $name, Dumper($dsn));

   return $name, $id;
}


sub get_uuid {
    my $uuid_file = '/.percona-toolkit.uuid';
    foreach my $dir (@vc_dirs) {
        my $filename = $dir.$uuid_file;
        my $uuid=_read_uuid($filename);
        return $uuid if $uuid;
    }

    my $filename = $ENV{"HOME"} . $uuid_file;
    my $uuid = _generate_uuid();

    my $fh;
    eval {
        open($fh, '>', $filename);
    };
    if (!$EVAL_ERROR) {
        print $fh $uuid;
        close $fh;
    }

    return $uuid;
}   

sub _generate_uuid {
    return sprintf+($}="%04x")."$}-$}-$}-$}-".$}x3,map rand 65537,0..7;
}

sub _read_uuid {
    my $filename = shift;
    my $fh;

    eval {
        open($fh, '<:encoding(UTF-8)', $filename);
    };
    return if ($EVAL_ERROR);

    my $uuid;
    eval { $uuid = <$fh>; };
    return if ($EVAL_ERROR);

    chomp $uuid;
    return $uuid;
}


sub pingback {
   my (%args) = @_;
   my @required_args = qw(url instances);
   foreach my $arg ( @required_args ) {
      die "I need a $arg arugment" unless $args{$arg};
   }
   my $url       = $args{url};
   my $instances = $args{instances};

   my $ua = $args{ua} || HTTP::Micro->new( timeout => 3 );

   my $response = $ua->request('GET', $url);
   PTDEBUG && _d('Server response:', Dumper($response));
   die "No response from GET $url"
      if !$response;
   die("GET on $url returned HTTP status $response->{status}; expected 200\n",
       ($response->{content} || '')) if $response->{status} != 200;
   die("GET on $url did not return any programs to check")
      if !$response->{content};

   my $items = parse_server_response(
      response => $response->{content}
   );
   die "Failed to parse server requested programs: $response->{content}"
      if !scalar keys %$items;
      
   my $versions = get_versions(
      items     => $items,
      instances => $instances,
   );
   die "Failed to get any program versions; should have at least gotten Perl"
      if !scalar keys %$versions;

   my $client_content = encode_client_response(
      items      => $items,
      versions   => $versions,
      general_id => get_uuid(),
   );

   my $tool_name = $ENV{XTRABACKUP_VERSION} ? "Percona XtraBackup" : File::Basename::basename($0);
   my $client_response = {
      headers => { "X-Percona-Toolkit-Tool" => $tool_name },
      content => $client_content,
   };
   PTDEBUG && _d('Client response:', Dumper($client_response));

   $response = $ua->request('POST', $url, $client_response);
   PTDEBUG && _d('Server suggestions:', Dumper($response));
   die "No response from POST $url $client_response"
      if !$response;
   die "POST $url returned HTTP status $response->{status}; expected 200"
      if $response->{status} != 200;

   return unless $response->{content};

   $items = parse_server_response(
      response   => $response->{content},
      split_vars => 0,
   );
   die "Failed to parse server suggestions: $response->{content}"
      if !scalar keys %$items;
   my @suggestions = map { $_->{vars} }
                     sort { $a->{item} cmp $b->{item} }
                     values %$items;

   return \@suggestions;
}

sub encode_client_response {
   my (%args) = @_;
   my @required_args = qw(items versions general_id);
   foreach my $arg ( @required_args ) {
      die "I need a $arg arugment" unless $args{$arg};
   }
   my ($items, $versions, $general_id) = @args{@required_args};

   my @lines;
   foreach my $item ( sort keys %$items ) {
      next unless exists $versions->{$item};
      if ( ref($versions->{$item}) eq 'HASH' ) {
         my $mysql_versions = $versions->{$item};
         for my $id ( sort keys %$mysql_versions ) {
            push @lines, join(';', $id, $item, $mysql_versions->{$id});
         }
      }
      else {
         push @lines, join(';', $general_id, $item, $versions->{$item});
      }
   }

   my $client_response = join("\n", @lines) . "\n";
   return $client_response;
}

sub parse_server_response {
   my (%args) = @_;
   my @required_args = qw(response);
   foreach my $arg ( @required_args ) {
      die "I need a $arg arugment" unless $args{$arg};
   }
   my ($response) = @args{@required_args};

   my %items = map {
      my ($item, $type, $vars) = split(";", $_);
      if ( !defined $args{split_vars} || $args{split_vars} ) {
         $vars = [ split(",", ($vars || '')) ];
      }
      $item => {
         item => $item,
         type => $type,
         vars => $vars,
      };
   } split("\n", $response);

   PTDEBUG && _d('Items:', Dumper(\%items));

   return \%items;
}

my %sub_for_type = (
   os_version          => \&get_os_version,
   perl_version        => \&get_perl_version,
   perl_module_version => \&get_perl_module_version,
   mysql_variable      => \&get_mysql_variable,
   xtrabackup          => \&get_xtrabackup_version,
);

sub valid_item {
   my ($item) = @_;
   return unless $item;
   if ( !exists $sub_for_type{ $item->{type} } ) {
      PTDEBUG && _d('Invalid type:', $item->{type});
      return 0;
   }
   return 1;
}

sub get_versions {
   my (%args) = @_;
   my @required_args = qw(items);
   foreach my $arg ( @required_args ) {
      die "I need a $arg arugment" unless $args{$arg};
   }
   my ($items) = @args{@required_args};

   my %versions;
   foreach my $item ( values %$items ) {
      next unless valid_item($item);
      eval {
         my $version = $sub_for_type{ $item->{type} }->(
            item      => $item,
            instances => $args{instances},
         );
         if ( $version ) {
            chomp $version unless ref($version);
            $versions{$item->{item}} = $version;
         }
      };
      if ( $EVAL_ERROR ) {
         PTDEBUG && _d('Error getting version for', Dumper($item), $EVAL_ERROR);
      }
   }

   return \%versions;
}


sub get_os_version {
   if ( $OSNAME eq 'MSWin32' ) {
      require Win32;
      return Win32::GetOSDisplayName();
   }

  chomp(my $platform = `uname -s`);
  PTDEBUG && _d('platform:', $platform);
  return $OSNAME unless $platform;

   chomp(my $lsb_release
            = `which lsb_release 2>/dev/null | awk '{print \$1}'` || '');
   PTDEBUG && _d('lsb_release:', $lsb_release);

   my $release = "";

   if ( $platform eq 'Linux' ) {
      if ( -f "/etc/fedora-release" ) {
         $release = `cat /etc/fedora-release`;
      }
      elsif ( -f "/etc/redhat-release" ) {
         $release = `cat /etc/redhat-release`;
      }
      elsif ( -f "/etc/system-release" ) {
         $release = `cat /etc/system-release`;
      }
      elsif ( $lsb_release ) {
         $release = `$lsb_release -ds`;
      }
      elsif ( -f "/etc/lsb-release" ) {
         $release = `grep DISTRIB_DESCRIPTION /etc/lsb-release`;
         $release =~ s/^\w+="([^"]+)".+/$1/;
      }
      elsif ( -f "/etc/debian_version" ) {
         chomp(my $rel = `cat /etc/debian_version`);
         $release = "Debian $rel";
         if ( -f "/etc/apt/sources.list" ) {
             chomp(my $code_name = `awk '/^deb/ {print \$3}' /etc/apt/sources.list | awk -F/ '{print \$1}'| awk 'BEGIN {FS="|"} {print \$1}' | sort | uniq -c | sort -rn | head -n1 | awk '{print \$2}'`);
             $release .= " ($code_name)" if $code_name;
         }
      }
      elsif ( -f "/etc/os-release" ) { # openSUSE
         chomp($release = `grep PRETTY_NAME /etc/os-release`);
         $release =~ s/^PRETTY_NAME="(.+)"$/$1/;
      }
      elsif ( `ls /etc/*release 2>/dev/null` ) {
         if ( `grep DISTRIB_DESCRIPTION /etc/*release 2>/dev/null` ) {
            $release = `grep DISTRIB_DESCRIPTION /etc/*release | head -n1`;
         }
         else {
            $release = `cat /etc/*release | head -n1`;
         }
      }
   }
   elsif ( $platform =~ m/(?:BSD|^Darwin)$/ ) {
      my $rel = `uname -r`;
      $release = "$platform $rel";
   }
   elsif ( $platform eq "SunOS" ) {
      my $rel = `head -n1 /etc/release` || `uname -r`;
      $release = "$platform $rel";
   }

   if ( !$release ) {
      PTDEBUG && _d('Failed to get the release, using platform');
      $release = $platform;
   }
   chomp($release);

   $release =~ s/^"|"$//g;

   PTDEBUG && _d('OS version =', $release);
   return $release;
}

sub get_perl_version {
   my (%args) = @_;
   my $item = $args{item};
   return unless $item;

   my $version = sprintf '%vd', $PERL_VERSION;
   PTDEBUG && _d('Perl version', $version);
   return $version;
}

sub get_xtrabackup_version {
    return $ENV{XTRABACKUP_VERSION};
}

sub get_perl_module_version {
   my (%args) = @_;
   my $item = $args{item};
   return unless $item;

   my $var     = '$' . $item->{item} . '::VERSION';
   my $version = eval "use $item->{item}; $var;";
   PTDEBUG && _d('Perl version for', $var, '=', $version);
   return $version;
}

sub get_mysql_variable {
   return get_from_mysql(
      show => 'VARIABLES',
      @_,
   );
}

sub get_from_mysql {
   my (%args) = @_;
   my $show      = $args{show};
   my $item      = $args{item};
   my $instances = $args{instances};
   return unless $show && $item;

   if ( !$instances || !@$instances ) {
      PTDEBUG && _d('Cannot check', $item,
         'because there are no MySQL instances');
      return;
   }

   if ($item->{item} eq 'MySQL' && $item->{type} eq 'mysql_variable') {
      @{$item->{vars}} = grep { $_ eq 'version' || $_ eq 'version_comment' } @{$item->{vars}};
   }
 

   my @versions;
   my %version_for;
   foreach my $instance ( @$instances ) {
      next unless $instance->{id};  # special system instance has id=0
      my $dbh = $instance->{dbh};
      local $dbh->{FetchHashKeyName} = 'NAME_lc';
      my $sql = qq/SHOW $show/;
      PTDEBUG && _d($sql);
      my $rows = $dbh->selectall_hashref($sql, 'variable_name');

      my @versions;
      foreach my $var ( @{$item->{vars}} ) {
         $var = lc($var);
         my $version = $rows->{$var}->{value};
         PTDEBUG && _d('MySQL version for', $item->{item}, '=', $version,
            'on', $instance->{name});
         push @versions, $version;
      }
      $version_for{ $instance->{id} } = join(' ', @versions);
   }

   return \%version_for;
}

sub _d {
   my ($package, undef, $line) = caller 0;
   @_ = map { (my $temp = $_) =~ s/\n/\n# /g; $temp; }
        map { defined $_ ? $_ : 'undef' }
        @_;
   print STDERR "# $package:$line $PID ", join(' ', @_), "\n";
}

1;
}
# ###########################################################################
# End VersionCheck package
# ###########################################################################

# ###########################################################################
# DSNParser package
# This package is a copy without comments from the original.  The original
# with comments and its test file can be found in the GitHub repository at,
#   lib/DSNParser.pm
#   t/lib/DSNParser.t
# See https://github.com/percona/percona-toolkit for more information.
# ###########################################################################
{
package DSNParser;

use strict;
use warnings FATAL => 'all';
use English qw(-no_match_vars);
use constant PTDEBUG => $ENV{PTDEBUG} || 0;

use Data::Dumper;
$Data::Dumper::Indent    = 0;
$Data::Dumper::Quotekeys = 0;

my $dsn_sep = qr/(?<!\\),/;

eval {
   require DBI;
};
my $have_dbi = $EVAL_ERROR ? 0 : 1;

sub new {
   my ( $class, %args ) = @_;
   foreach my $arg ( qw(opts) ) {
      die "I need a $arg argument" unless $args{$arg};
   }
   my $self = {
      opts => {}  # h, P, u, etc.  Should come from DSN OPTIONS section in POD.
   };
   foreach my $opt ( @{$args{opts}} ) {
      if ( !$opt->{key} || !$opt->{desc} ) {
         die "Invalid DSN option: ", Dumper($opt);
      }
      PTDEBUG && _d('DSN option:',
         join(', ',
            map { "$_=" . (defined $opt->{$_} ? ($opt->{$_} || '') : 'undef') }
               keys %$opt
         )
      );
      $self->{opts}->{$opt->{key}} = {
         dsn  => $opt->{dsn},
         desc => $opt->{desc},
         copy => $opt->{copy} || 0,
      };
   }
   return bless $self, $class;
}

sub prop {
   my ( $self, $prop, $value ) = @_;
   if ( @_ > 2 ) {
      PTDEBUG && _d('Setting', $prop, 'property');
      $self->{$prop} = $value;
   }
   return $self->{$prop};
}

sub parse {
   my ( $self, $dsn, $prev, $defaults ) = @_;
   if ( !$dsn ) {
      PTDEBUG && _d('No DSN to parse');
      return;
   }
   PTDEBUG && _d('Parsing', $dsn);
   $prev     ||= {};
   $defaults ||= {};
   my %given_props;
   my %final_props;
   my $opts = $self->{opts};

   foreach my $dsn_part ( split($dsn_sep, $dsn) ) {
      $dsn_part =~ s/\\,/,/g;
      if ( my ($prop_key, $prop_val) = $dsn_part =~  m/^(.)=(.*)$/ ) {
         $given_props{$prop_key} = $prop_val;
      }
      else {
         PTDEBUG && _d('Interpreting', $dsn_part, 'as h=', $dsn_part);
         $given_props{h} = $dsn_part;
      }
   }

   foreach my $key ( keys %$opts ) {
      PTDEBUG && _d('Finding value for', $key);
      $final_props{$key} = $given_props{$key};
      if ( !defined $final_props{$key}  
           && defined $prev->{$key} && $opts->{$key}->{copy} )
      {
         $final_props{$key} = $prev->{$key};
         PTDEBUG && _d('Copying value for', $key, 'from previous DSN');
      }
      if ( !defined $final_props{$key} ) {
         $final_props{$key} = $defaults->{$key};
         PTDEBUG && _d('Copying value for', $key, 'from defaults');
      }
   }

   foreach my $key ( keys %given_props ) {
      die "Unknown DSN option '$key' in '$dsn'.  For more details, "
            . "please use the --help option, or try 'perldoc $PROGRAM_NAME' "
            . "for complete documentation."
         unless exists $opts->{$key};
   }
   if ( (my $required = $self->prop('required')) ) {
      foreach my $key ( keys %$required ) {
         die "Missing required DSN option '$key' in '$dsn'.  For more details, "
               . "please use the --help option, or try 'perldoc $PROGRAM_NAME' "
               . "for complete documentation."
            unless $final_props{$key};
      }
   }

   return \%final_props;
}

sub parse_options {
   my ( $self, $o ) = @_;
   die 'I need an OptionParser object' unless ref $o eq 'OptionParser';
   my $dsn_string
      = join(',',
          map  { "$_=".$o->get($_); }
          grep { $o->has($_) && $o->get($_) }
          keys %{$self->{opts}}
        );
   PTDEBUG && _d('DSN string made from options:', $dsn_string);
   return $self->parse($dsn_string);
}

sub as_string {
   my ( $self, $dsn, $props ) = @_;
   return $dsn unless ref $dsn;
   my @keys = $props ? @$props : sort keys %$dsn;
   return join(',',
      map  { "$_=" . ($_ eq 'p' ? '...' : $dsn->{$_}) }
      grep {
         exists $self->{opts}->{$_}
         && exists $dsn->{$_}
         && defined $dsn->{$_}
      } @keys);
}

sub usage {
   my ( $self ) = @_;
   my $usage
      = "DSN syntax is key=value[,key=value...]  Allowable DSN keys:\n\n"
      . "  KEY  COPY  MEANING\n"
      . "  ===  ====  =============================================\n";
   my %opts = %{$self->{opts}};
   foreach my $key ( sort keys %opts ) {
      $usage .= "  $key    "
             .  ($opts{$key}->{copy} ? 'yes   ' : 'no    ')
             .  ($opts{$key}->{desc} || '[No description]')
             . "\n";
   }
   $usage .= "\n  If the DSN is a bareword, the word is treated as the 'h' key.\n";
   return $usage;
}

sub get_cxn_params {
   my ( $self, $info ) = @_;
   my $dsn;
   my %opts = %{$self->{opts}};
   my $driver = $self->prop('dbidriver') || '';
   if ( $driver eq 'Pg' ) {
      $dsn = 'DBI:Pg:dbname=' . ( $info->{D} || '' ) . ';'
         . join(';', map  { "$opts{$_}->{dsn}=$info->{$_}" }
                     grep { defined $info->{$_} }
                     qw(h P));
   }
   else {
      $dsn = 'DBI:mysql:' . ( $info->{D} || '' ) . ';'
         . join(';', map  { "$opts{$_}->{dsn}=$info->{$_}" }
                     grep { defined $info->{$_} }
                     qw(F h P S A))
         . ';mysql_read_default_group=client'
         . ($info->{L} ? ';mysql_local_infile=1' : '');
   }
   PTDEBUG && _d($dsn);
   return ($dsn, $info->{u}, $info->{p});
}

sub fill_in_dsn {
   my ( $self, $dbh, $dsn ) = @_;
   my $vars = $dbh->selectall_hashref('SHOW VARIABLES', 'Variable_name');
   my ($user, $db) = $dbh->selectrow_array('SELECT USER(), DATABASE()');
   $user =~ s/@.*//;
   $dsn->{h} ||= $vars->{hostname}->{Value};
   $dsn->{S} ||= $vars->{'socket'}->{Value};
   $dsn->{P} ||= $vars->{port}->{Value};
   $dsn->{u} ||= $user;
   $dsn->{D} ||= $db;
}

sub get_dbh {
   my ( $self, $cxn_string, $user, $pass, $opts ) = @_;
   $opts ||= {};
   my $defaults = {
      AutoCommit         => 0,
      RaiseError         => 1,
      PrintError         => 0,
      ShowErrorStatement => 1,
      mysql_enable_utf8 => ($cxn_string =~ m/charset=utf8/i ? 1 : 0),
   };
   @{$defaults}{ keys %$opts } = values %$opts;
   if (delete $defaults->{L}) { # L for LOAD DATA LOCAL INFILE, our own extension
      $defaults->{mysql_local_infile} = 1;
   }

   if ( $opts->{mysql_use_result} ) {
      $defaults->{mysql_use_result} = 1;
   }

   if ( !$have_dbi ) {
      die "Cannot connect to MySQL because the Perl DBI module is not "
         . "installed or not found.  Run 'perl -MDBI' to see the directories "
         . "that Perl searches for DBI.  If DBI is not installed, try:\n"
         . "  Debian/Ubuntu  apt-get install libdbi-perl\n"
         . "  RHEL/CentOS    yum install perl-DBI\n"
         . "  OpenSolaris    pkg install pkg:/SUNWpmdbi\n";

   }

   my $dbh;
   my $tries = 2;
   while ( !$dbh && $tries-- ) {
      PTDEBUG && _d($cxn_string, ' ', $user, ' ', $pass, 
         join(', ', map { "$_=>$defaults->{$_}" } keys %$defaults ));

      $dbh = eval { DBI->connect($cxn_string, $user, $pass, $defaults) };

      if ( !$dbh && $EVAL_ERROR ) {
         if ( $EVAL_ERROR =~ m/locate DBD\/mysql/i ) {
            die "Cannot connect to MySQL because the Perl DBD::mysql module is "
               . "not installed or not found.  Run 'perl -MDBD::mysql' to see "
               . "the directories that Perl searches for DBD::mysql.  If "
               . "DBD::mysql is not installed, try:\n"
               . "  Debian/Ubuntu  apt-get install libdbd-mysql-perl\n"
               . "  RHEL/CentOS    yum install perl-DBD-MySQL\n"
               . "  OpenSolaris    pgk install pkg:/SUNWapu13dbd-mysql\n";
         }
         elsif ( $EVAL_ERROR =~ m/not a compiled character set|character set utf8/ ) {
            PTDEBUG && _d('Going to try again without utf8 support');
            delete $defaults->{mysql_enable_utf8};
         }
         if ( !$tries ) {
            die $EVAL_ERROR;
         }
      }
   }

   if ( $cxn_string =~ m/mysql/i ) {
      my $sql;

      if ( my ($charset) = $cxn_string =~ m/charset=([\w]+)/ ) {
         $sql = qq{/*!40101 SET NAMES "$charset"*/};
         PTDEBUG && _d($dbh, $sql);
         eval { $dbh->do($sql) };
         if ( $EVAL_ERROR ) {
            die "Error setting NAMES to $charset: $EVAL_ERROR";
         }
         PTDEBUG && _d('Enabling charset for STDOUT');
         if ( $charset eq 'utf8' ) {
            binmode(STDOUT, ':utf8')
               or die "Can't binmode(STDOUT, ':utf8'): $OS_ERROR";
         }
         else {
            binmode(STDOUT) or die "Can't binmode(STDOUT): $OS_ERROR";
         }
      }

      if ( my $vars = $self->prop('set-vars') ) {
         $self->set_vars($dbh, $vars);
      }

      $sql = 'SELECT @@SQL_MODE';
      PTDEBUG && _d($dbh, $sql);
      my ($sql_mode) = eval { $dbh->selectrow_array($sql) };
      if ( $EVAL_ERROR ) {
         die "Error getting the current SQL_MODE: $EVAL_ERROR";
      }

      $sql = 'SET @@SQL_QUOTE_SHOW_CREATE = 1'
            . '/*!40101, @@SQL_MODE=\'NO_AUTO_VALUE_ON_ZERO'
            . ($sql_mode ? ",$sql_mode" : '')
            . '\'*/';
      PTDEBUG && _d($dbh, $sql);
      eval { $dbh->do($sql) };
      if ( $EVAL_ERROR ) {
         die "Error setting SQL_QUOTE_SHOW_CREATE, SQL_MODE"
           . ($sql_mode ? " and $sql_mode" : '')
           . ": $EVAL_ERROR";
      }
   }
   my ($mysql_version) = eval { $dbh->selectrow_array('SELECT VERSION()') };
   if ($EVAL_ERROR) {
       die "Cannot get MySQL version: $EVAL_ERROR";
   }

   my (undef, $character_set_server) = eval { $dbh->selectrow_array("SHOW VARIABLES LIKE 'character_set_server'") };
   if ($EVAL_ERROR) {
       die "Cannot get MySQL var character_set_server: $EVAL_ERROR";
   }

   if ($mysql_version =~ m/^(\d+)\.(\d)\.(\d+).*/) {
       if ($1 >= 8 && $character_set_server =~ m/^utf8/) {
           $dbh->{mysql_enable_utf8} = 1;
           my $msg = "MySQL version $mysql_version >= 8 and character_set_server = $character_set_server\n".
                     "Setting: SET NAMES $character_set_server";
           PTDEBUG && _d($msg);
           eval { $dbh->do("SET NAMES 'utf8mb4'") };
           if ($EVAL_ERROR) {
               die "Cannot SET NAMES $character_set_server: $EVAL_ERROR";
           }
       }
   }

   PTDEBUG && _d('DBH info: ',
      $dbh,
      Dumper($dbh->selectrow_hashref(
         'SELECT DATABASE(), CONNECTION_ID(), VERSION()/*!50038 , @@hostname*/')),
      'Connection info:',      $dbh->{mysql_hostinfo},
      'Character set info:',   Dumper($dbh->selectall_arrayref(
                     "SHOW VARIABLES LIKE 'character_set%'", { Slice => {}})),
      '$DBD::mysql::VERSION:', $DBD::mysql::VERSION,
      '$DBI::VERSION:',        $DBI::VERSION,
   );

   return $dbh;
}

sub get_hostname {
   my ( $self, $dbh ) = @_;
   if ( my ($host) = ($dbh->{mysql_hostinfo} || '') =~ m/^(\w+) via/ ) {
      return $host;
   }
   my ( $hostname, $one ) = $dbh->selectrow_array(
      'SELECT /*!50038 @@hostname, */ 1');
   return $hostname;
}

sub disconnect {
   my ( $self, $dbh ) = @_;
   PTDEBUG && $self->print_active_handles($dbh);
   $dbh->disconnect;
}

sub print_active_handles {
   my ( $self, $thing, $level ) = @_;
   $level ||= 0;
   printf("# Active %sh: %s %s %s\n", ($thing->{Type} || 'undef'), "\t" x $level,
      $thing, (($thing->{Type} || '') eq 'st' ? $thing->{Statement} || '' : ''))
      or die "Cannot print: $OS_ERROR";
   foreach my $handle ( grep {defined} @{ $thing->{ChildHandles} } ) {
      $self->print_active_handles( $handle, $level + 1 );
   }
}

sub copy {
   my ( $self, $dsn_1, $dsn_2, %args ) = @_;
   die 'I need a dsn_1 argument' unless $dsn_1;
   die 'I need a dsn_2 argument' unless $dsn_2;
   my %new_dsn = map {
      my $key = $_;
      my $val;
      if ( $args{overwrite} ) {
         $val = defined $dsn_1->{$key} ? $dsn_1->{$key} : $dsn_2->{$key};
      }
      else {
         $val = defined $dsn_2->{$key} ? $dsn_2->{$key} : $dsn_1->{$key};
      }
      $key => $val;
   } keys %{$self->{opts}};
   return \%new_dsn;
}

sub set_vars {
   my ($self, $dbh, $vars) = @_;

   return unless $vars;

   foreach my $var ( sort keys %$vars ) {
      my $val = $vars->{$var}->{val};

      (my $quoted_var = $var) =~ s/_/\\_/;
      my ($var_exists, $current_val);
      eval {
         ($var_exists, $current_val) = $dbh->selectrow_array(
            "SHOW VARIABLES LIKE '$quoted_var'");
      };
      my $e = $EVAL_ERROR;
      if ( $e ) {
         PTDEBUG && _d($e);
      }

      if ( $vars->{$var}->{default} && !$var_exists ) {
         PTDEBUG && _d('Not setting default var', $var,
            'because it does not exist');
         next;
      }

      if ( $current_val && $current_val eq $val ) {
         PTDEBUG && _d('Not setting var', $var, 'because its value',
            'is already', $val);
         next;
      }

      my $sql = "SET SESSION $var=$val";
      PTDEBUG && _d($dbh, $sql);
      eval { $dbh->do($sql) };
      if ( my $set_error = $EVAL_ERROR ) {
         chomp($set_error);
         $set_error =~ s/ at \S+ line \d+//;
         my $msg = "Error setting $var: $set_error";
         if ( $current_val ) {
            $msg .= "  The current value for $var is $current_val.  "
                  . "If the variable is read only (not dynamic), specify "
                  . "--set-vars $var=$current_val to avoid this warning, "
                  . "else manually set the variable and restart MySQL.";
         }
         warn $msg . "\n\n";
      }
   }

   return; 
}

sub _d {
   my ($package, undef, $line) = caller 0;
   @_ = map { (my $temp = $_) =~ s/\n/\n# /g; $temp; }
        map { defined $_ ? $_ : 'undef' }
        @_;
   print STDERR "# $package:$line $PID ", join(' ', @_), "\n";
}

1;
}
# ###########################################################################
# End DSNParser package
# ###########################################################################

# ###########################################################################
# OptionParser package
# This package is a copy without comments from the original.  The original
# with comments and its test file can be found in the GitHub repository at,
#   lib/OptionParser.pm
#   t/lib/OptionParser.t
# See https://github.com/percona/percona-toolkit for more information.
# ###########################################################################
{
package OptionParser;

use strict;
use warnings FATAL => 'all';
use English qw(-no_match_vars);
use constant PTDEBUG => $ENV{PTDEBUG} || 0;

use List::Util qw(max);
use Getopt::Long;
use Data::Dumper;

my $POD_link_re = '[LC]<"?([^">]+)"?>';

sub new {
   my ( $class, %args ) = @_;
   my @required_args = qw();
   foreach my $arg ( @required_args ) {
      die "I need a $arg argument" unless $args{$arg};
   }

   my ($program_name) = $PROGRAM_NAME =~ m/([.A-Za-z-]+)$/;
   $program_name ||= $PROGRAM_NAME;
   my $home = $ENV{HOME} || $ENV{HOMEPATH} || $ENV{USERPROFILE} || '.';

   my %attributes = (
      'type'       => 1,
      'short form' => 1,
      'group'      => 1,
      'default'    => 1,
      'cumulative' => 1,
      'negatable'  => 1,
      'repeatable' => 1,  # means it can be specified more than once
   );

   my $self = {
      head1             => 'OPTIONS',        # These args are used internally
      skip_rules        => 0,                # to instantiate another Option-
      item              => '--(.*)',         # Parser obj that parses the
      attributes        => \%attributes,     # DSN OPTIONS section.  Tools
      parse_attributes  => \&_parse_attribs, # don't tinker with these args.

      %args,

      strict            => 1,  # disabled by a special rule
      program_name      => $program_name,
      opts              => {},
      got_opts          => 0,
      short_opts        => {},
      defaults          => {},
      groups            => {},
      allowed_groups    => {},
      errors            => [],
      rules             => [],  # desc of rules for --help
      mutex             => [],  # rule: opts are mutually exclusive
      atleast1          => [],  # rule: at least one opt is required
      disables          => {},  # rule: opt disables other opts 
      defaults_to       => {},  # rule: opt defaults to value of other opt
      DSNParser         => undef,
      default_files     => [
         "/etc/percona-toolkit/percona-toolkit.conf",
         "/etc/percona-toolkit/$program_name.conf",
         "$home/.percona-toolkit.conf",
         "$home/.$program_name.conf",
      ],
      types             => {
         string => 's', # standard Getopt type
         int    => 'i', # standard Getopt type
         float  => 'f', # standard Getopt type
         Hash   => 'H', # hash, formed from a comma-separated list
         hash   => 'h', # hash as above, but only if a value is given
         Array  => 'A', # array, similar to Hash
         array  => 'a', # array, similar to hash
         DSN    => 'd', # DSN
         size   => 'z', # size with kMG suffix (powers of 2^10)
         time   => 'm', # time, with an optional suffix of s/h/m/d
      },
   };

   return bless $self, $class;
}

sub get_specs {
   my ( $self, $file ) = @_;
   $file ||= $self->{file} || __FILE__;
   my @specs = $self->_pod_to_specs($file);
   $self->_parse_specs(@specs);

   open my $fh, "<", $file or die "Cannot open $file: $OS_ERROR";
   my $contents = do { local $/ = undef; <$fh> };
   close $fh;
   if ( $contents =~ m/^=head1 DSN OPTIONS/m ) {
      PTDEBUG && _d('Parsing DSN OPTIONS');
      my $dsn_attribs = {
         dsn  => 1,
         copy => 1,
      };
      my $parse_dsn_attribs = sub {
         my ( $self, $option, $attribs ) = @_;
         map {
            my $val = $attribs->{$_};
            if ( $val ) {
               $val    = $val eq 'yes' ? 1
                       : $val eq 'no'  ? 0
                       :                 $val;
               $attribs->{$_} = $val;
            }
         } keys %$attribs;
         return {
            key => $option,
            %$attribs,
         };
      };
      my $dsn_o = new OptionParser(
         description       => 'DSN OPTIONS',
         head1             => 'DSN OPTIONS',
         dsn               => 0,         # XXX don't infinitely recurse!
         item              => '\* (.)',  # key opts are a single character
         skip_rules        => 1,         # no rules before opts
         attributes        => $dsn_attribs,
         parse_attributes  => $parse_dsn_attribs,
      );
      my @dsn_opts = map {
         my $opts = {
            key  => $_->{spec}->{key},
            dsn  => $_->{spec}->{dsn},
            copy => $_->{spec}->{copy},
            desc => $_->{desc},
         };
         $opts;
      } $dsn_o->_pod_to_specs($file);
      $self->{DSNParser} = DSNParser->new(opts => \@dsn_opts);
   }

   if ( $contents =~ m/^=head1 VERSION\n\n^(.+)$/m ) {
      $self->{version} = $1;
      PTDEBUG && _d($self->{version});
   }

   return;
}

sub DSNParser {
   my ( $self ) = @_;
   return $self->{DSNParser};
};

sub get_defaults_files {
   my ( $self ) = @_;
   return @{$self->{default_files}};
}

sub _pod_to_specs {
   my ( $self, $file ) = @_;
   $file ||= $self->{file} || __FILE__;
   open my $fh, '<', $file or die "Cannot open $file: $OS_ERROR";

   my @specs = ();
   my @rules = ();
   my $para;

   local $INPUT_RECORD_SEPARATOR = '';
   while ( $para = <$fh> ) {
      next unless $para =~ m/^=head1 $self->{head1}/;
      last;
   }

   while ( $para = <$fh> ) {
      last if $para =~ m/^=over/;
      next if $self->{skip_rules};
      chomp $para;
      $para =~ s/\s+/ /g;
      $para =~ s/$POD_link_re/$1/go;
      PTDEBUG && _d('Option rule:', $para);
      push @rules, $para;
   }

   die "POD has no $self->{head1} section" unless $para;

   do {
      if ( my ($option) = $para =~ m/^=item $self->{item}/ ) {
         chomp $para;
         PTDEBUG && _d($para);
         my %attribs;

         $para = <$fh>; # read next paragraph, possibly attributes

         if ( $para =~ m/: / ) { # attributes
            $para =~ s/\s+\Z//g;
            %attribs = map {
                  my ( $attrib, $val) = split(/: /, $_);
                  die "Unrecognized attribute for --$option: $attrib"
                     unless $self->{attributes}->{$attrib};
                  ($attrib, $val);
               } split(/; /, $para);
            if ( $attribs{'short form'} ) {
               $attribs{'short form'} =~ s/-//;
            }
            $para = <$fh>; # read next paragraph, probably short help desc
         }
         else {
            PTDEBUG && _d('Option has no attributes');
         }

         $para =~ s/\s+\Z//g;
         $para =~ s/\s+/ /g;
         $para =~ s/$POD_link_re/$1/go;

         $para =~ s/\.(?:\n.*| [A-Z].*|\Z)//s;
         PTDEBUG && _d('Short help:', $para);

         die "No description after option spec $option" if $para =~ m/^=item/;

         if ( my ($base_option) =  $option =~ m/^\[no\](.*)/ ) {
            $option = $base_option;
            $attribs{'negatable'} = 1;
         }

         push @specs, {
            spec  => $self->{parse_attributes}->($self, $option, \%attribs), 
            desc  => $para
               . (defined $attribs{default} ? " (default $attribs{default})" : ''),
            group => ($attribs{'group'} ? $attribs{'group'} : 'default'),
            attributes => \%attribs
         };
      }
      while ( $para = <$fh> ) {
         last unless $para;
         if ( $para =~ m/^=head1/ ) {
            $para = undef; # Can't 'last' out of a do {} block.
            last;
         }
         last if $para =~ m/^=item /;
      }
   } while ( $para );

   die "No valid specs in $self->{head1}" unless @specs;

   close $fh;
   return @specs, @rules;
}

sub _parse_specs {
   my ( $self, @specs ) = @_;
   my %disables; # special rule that requires deferred checking

   foreach my $opt ( @specs ) {
      if ( ref $opt ) { # It's an option spec, not a rule.
         PTDEBUG && _d('Parsing opt spec:',
            map { ($_, '=>', $opt->{$_}) } keys %$opt);

         my ( $long, $short ) = $opt->{spec} =~ m/^([\w-]+)(?:\|([^!+=]*))?/;
         if ( !$long ) {
            die "Cannot parse long option from spec $opt->{spec}";
         }
         $opt->{long} = $long;

         die "Duplicate long option --$long" if exists $self->{opts}->{$long};
         $self->{opts}->{$long} = $opt;

         if ( length $long == 1 ) {
            PTDEBUG && _d('Long opt', $long, 'looks like short opt');
            $self->{short_opts}->{$long} = $long;
         }

         if ( $short ) {
            die "Duplicate short option -$short"
               if exists $self->{short_opts}->{$short};
            $self->{short_opts}->{$short} = $long;
            $opt->{short} = $short;
         }
         else {
            $opt->{short} = undef;
         }

         $opt->{is_negatable}  = $opt->{spec} =~ m/!/        ? 1 : 0;
         $opt->{is_cumulative} = $opt->{spec} =~ m/\+/       ? 1 : 0;
         $opt->{is_repeatable} = $opt->{attributes}->{repeatable} ? 1 : 0;
         $opt->{is_required}   = $opt->{desc} =~ m/required/ ? 1 : 0;

         $opt->{group} ||= 'default';
         $self->{groups}->{ $opt->{group} }->{$long} = 1;

         $opt->{value} = undef;
         $opt->{got}   = 0;

         my ( $type ) = $opt->{spec} =~ m/=(.)/;
         $opt->{type} = $type;
         PTDEBUG && _d($long, 'type:', $type);


         $opt->{spec} =~ s/=./=s/ if ( $type && $type =~ m/[HhAadzm]/ );

         if ( (my ($def) = $opt->{desc} =~ m/default\b(?: ([^)]+))?/) ) {
            $self->{defaults}->{$long} = defined $def ? $def : 1;
            PTDEBUG && _d($long, 'default:', $def);
         }

         if ( $long eq 'config' ) {
            $self->{defaults}->{$long} = join(',', $self->get_defaults_files());
         }

         if ( (my ($dis) = $opt->{desc} =~ m/(disables .*)/) ) {
            $disables{$long} = $dis;
            PTDEBUG && _d('Deferring check of disables rule for', $opt, $dis);
         }

         $self->{opts}->{$long} = $opt;
      }
      else { # It's an option rule, not a spec.
         PTDEBUG && _d('Parsing rule:', $opt); 
         push @{$self->{rules}}, $opt;
         my @participants = $self->_get_participants($opt);
         my $rule_ok = 0;

         if ( $opt =~ m/mutually exclusive|one and only one/ ) {
            $rule_ok = 1;
            push @{$self->{mutex}}, \@participants;
            PTDEBUG && _d(@participants, 'are mutually exclusive');
         }
         if ( $opt =~ m/at least one|one and only one/ ) {
            $rule_ok = 1;
            push @{$self->{atleast1}}, \@participants;
            PTDEBUG && _d(@participants, 'require at least one');
         }
         if ( $opt =~ m/default to/ ) {
            $rule_ok = 1;
            $self->{defaults_to}->{$participants[0]} = $participants[1];
            PTDEBUG && _d($participants[0], 'defaults to', $participants[1]);
         }
         if ( $opt =~ m/restricted to option groups/ ) {
            $rule_ok = 1;
            my ($groups) = $opt =~ m/groups ([\w\s\,]+)/;
            my @groups = split(',', $groups);
            %{$self->{allowed_groups}->{$participants[0]}} = map {
               s/\s+//;
               $_ => 1;
            } @groups;
         }
         if( $opt =~ m/accepts additional command-line arguments/ ) {
            $rule_ok = 1;
            $self->{strict} = 0;
            PTDEBUG && _d("Strict mode disabled by rule");
         }

         die "Unrecognized option rule: $opt" unless $rule_ok;
      }
   }

   foreach my $long ( keys %disables ) {
      my @participants = $self->_get_participants($disables{$long});
      $self->{disables}->{$long} = \@participants;
      PTDEBUG && _d('Option', $long, 'disables', @participants);
   }

   return; 
}

sub _get_participants {
   my ( $self, $str ) = @_;
   my @participants;
   foreach my $long ( $str =~ m/--(?:\[no\])?([\w-]+)/g ) {
      die "Option --$long does not exist while processing rule $str"
         unless exists $self->{opts}->{$long};
      push @participants, $long;
   }
   PTDEBUG && _d('Participants for', $str, ':', @participants);
   return @participants;
}

sub opts {
   my ( $self ) = @_;
   my %opts = %{$self->{opts}};
   return %opts;
}

sub short_opts {
   my ( $self ) = @_;
   my %short_opts = %{$self->{short_opts}};
   return %short_opts;
}

sub set_defaults {
   my ( $self, %defaults ) = @_;
   $self->{defaults} = {};
   foreach my $long ( keys %defaults ) {
      die "Cannot set default for nonexistent option $long"
         unless exists $self->{opts}->{$long};
      $self->{defaults}->{$long} = $defaults{$long};
      PTDEBUG && _d('Default val for', $long, ':', $defaults{$long});
   }
   return;
}

sub get_defaults {
   my ( $self ) = @_;
   return $self->{defaults};
}

sub get_groups {
   my ( $self ) = @_;
   return $self->{groups};
}

sub _set_option {
   my ( $self, $opt, $val ) = @_;
   my $long = exists $self->{opts}->{$opt}       ? $opt
            : exists $self->{short_opts}->{$opt} ? $self->{short_opts}->{$opt}
            : die "Getopt::Long gave a nonexistent option: $opt";
   $opt = $self->{opts}->{$long};
   if ( $opt->{is_cumulative} ) {
      $opt->{value}++;
   }
   elsif ( ($opt->{type} || '') eq 's' && $val =~ m/^--?(.+)/ ) {
      my $next_opt = $1;
      if (    exists $self->{opts}->{$next_opt}
           || exists $self->{short_opts}->{$next_opt} ) {
         $self->save_error("--$long requires a string value");
         return;
      }
      else {
         if ($opt->{is_repeatable}) {
            push @{$opt->{value}} , $val;
         }
         else {
            $opt->{value} = $val;
         }
      }
   }
   else {
      if ($opt->{is_repeatable}) {
         push @{$opt->{value}} , $val;
      }
      else {
         $opt->{value} = $val;
      }
   }
   $opt->{got} = 1;
   PTDEBUG && _d('Got option', $long, '=', $val);
}

sub get_opts {
   my ( $self ) = @_; 

   foreach my $long ( keys %{$self->{opts}} ) {
      $self->{opts}->{$long}->{got} = 0;
      $self->{opts}->{$long}->{value}
         = exists $self->{defaults}->{$long}       ? $self->{defaults}->{$long}
         : $self->{opts}->{$long}->{is_cumulative} ? 0
         : undef;
   }
   $self->{got_opts} = 0;

   $self->{errors} = [];

   if ( @ARGV && $ARGV[0] =~/^--config=/ ) {
      $ARGV[0] = substr($ARGV[0],9);
      $ARGV[0] =~ s/^'(.*)'$/$1/;
      $ARGV[0] =~ s/^"(.*)"$/$1/;
      $self->_set_option('config', shift @ARGV);
   }
   if ( @ARGV && $ARGV[0] eq "--config" ) {
      shift @ARGV;
      $self->_set_option('config', shift @ARGV);
   }
   if ( $self->has('config') ) {
      my @extra_args;
      foreach my $filename ( split(',', $self->get('config')) ) {
         eval {
            push @extra_args, $self->_read_config_file($filename);
         };
         if ( $EVAL_ERROR ) {
            if ( $self->got('config') ) {
               die $EVAL_ERROR;
            }
            elsif ( PTDEBUG ) {
               _d($EVAL_ERROR);
            }
         }
      }
      unshift @ARGV, @extra_args;
   }

   Getopt::Long::Configure('no_ignore_case', 'bundling');
   GetOptions(
      map    { $_->{spec} => sub { $self->_set_option(@_); } }
      grep   { $_->{long} ne 'config' } # --config is handled specially above.
      values %{$self->{opts}}
   ) or $self->save_error('Error parsing options');

   if ( exists $self->{opts}->{version} && $self->{opts}->{version}->{got} ) {
      if ( $self->{version} ) {
         print $self->{version}, "\n";
         exit 0;
      }
      else {
         print "Error parsing version.  See the VERSION section of the tool's documentation.\n";
         exit 1;
      }
   }

   if ( @ARGV && $self->{strict} ) {
      $self->save_error("Unrecognized command-line options @ARGV");
   }

   foreach my $mutex ( @{$self->{mutex}} ) {
      my @set = grep { $self->{opts}->{$_}->{got} } @$mutex;
      if ( @set > 1 ) {
         my $err = join(', ', map { "--$self->{opts}->{$_}->{long}" }
                      @{$mutex}[ 0 .. scalar(@$mutex) - 2] )
                 . ' and --'.$self->{opts}->{$mutex->[-1]}->{long}
                 . ' are mutually exclusive.';
         $self->save_error($err);
      }
   }

   foreach my $required ( @{$self->{atleast1}} ) {
      my @set = grep { $self->{opts}->{$_}->{got} } @$required;
      if ( @set == 0 ) {
         my $err = join(', ', map { "--$self->{opts}->{$_}->{long}" }
                      @{$required}[ 0 .. scalar(@$required) - 2] )
                 .' or --'.$self->{opts}->{$required->[-1]}->{long};
         $self->save_error("Specify at least one of $err");
      }
   }

   $self->_check_opts( keys %{$self->{opts}} );
   $self->{got_opts} = 1;
   return;
}

sub _check_opts {
   my ( $self, @long ) = @_;
   my $long_last = scalar @long;
   while ( @long ) {
      foreach my $i ( 0..$#long ) {
         my $long = $long[$i];
         next unless $long;
         my $opt  = $self->{opts}->{$long};
         if ( $opt->{got} ) {
            if ( exists $self->{disables}->{$long} ) {
               my @disable_opts = @{$self->{disables}->{$long}};
               map { $self->{opts}->{$_}->{value} = undef; } @disable_opts;
               PTDEBUG && _d('Unset options', @disable_opts,
                  'because', $long,'disables them');
            }

            if ( exists $self->{allowed_groups}->{$long} ) {

               my @restricted_groups = grep {
                  !exists $self->{allowed_groups}->{$long}->{$_}
               } keys %{$self->{groups}};

               my @restricted_opts;
               foreach my $restricted_group ( @restricted_groups ) {
                  RESTRICTED_OPT:
                  foreach my $restricted_opt (
                     keys %{$self->{groups}->{$restricted_group}} )
                  {
                     next RESTRICTED_OPT if $restricted_opt eq $long;
                     push @restricted_opts, $restricted_opt
                        if $self->{opts}->{$restricted_opt}->{got};
                  }
               }

               if ( @restricted_opts ) {
                  my $err;
                  if ( @restricted_opts == 1 ) {
                     $err = "--$restricted_opts[0]";
                  }
                  else {
                     $err = join(', ',
                               map { "--$self->{opts}->{$_}->{long}" }
                               grep { $_ } 
                               @restricted_opts[0..scalar(@restricted_opts) - 2]
                            )
                          . ' or --'.$self->{opts}->{$restricted_opts[-1]}->{long};
                  }
                  $self->save_error("--$long is not allowed with $err");
               }
            }

         }
         elsif ( $opt->{is_required} ) { 
            $self->save_error("Required option --$long must be specified");
         }

         $self->_validate_type($opt);
         if ( $opt->{parsed} ) {
            delete $long[$i];
         }
         else {
            PTDEBUG && _d('Temporarily failed to parse', $long);
         }
      }

      die "Failed to parse options, possibly due to circular dependencies"
         if @long == $long_last;
      $long_last = @long;
   }

   return;
}

sub _validate_type {
   my ( $self, $opt ) = @_;
   return unless $opt;

   if ( !$opt->{type} ) {
      $opt->{parsed} = 1;
      return;
   }

   my $val = $opt->{value};

   if ( $val && $opt->{type} eq 'm' ) {  # type time
      PTDEBUG && _d('Parsing option', $opt->{long}, 'as a time value');
      my ( $prefix, $num, $suffix ) = $val =~ m/([+-]?)(\d+)([a-z])?$/;
      if ( !$suffix ) {
         my ( $s ) = $opt->{desc} =~ m/\(suffix (.)\)/;
         $suffix = $s || 's';
         PTDEBUG && _d('No suffix given; using', $suffix, 'for',
            $opt->{long}, '(value:', $val, ')');
      }
      if ( $suffix =~ m/[smhd]/ ) {
         $val = $suffix eq 's' ? $num            # Seconds
              : $suffix eq 'm' ? $num * 60       # Minutes
              : $suffix eq 'h' ? $num * 3600     # Hours
              :                  $num * 86400;   # Days
         $opt->{value} = ($prefix || '') . $val;
         PTDEBUG && _d('Setting option', $opt->{long}, 'to', $val);
      }
      else {
         $self->save_error("Invalid time suffix for --$opt->{long}");
      }
   }
   elsif ( $val && $opt->{type} eq 'd' ) {  # type DSN
      PTDEBUG && _d('Parsing option', $opt->{long}, 'as a DSN');
      my $prev = {};
      my $from_key = $self->{defaults_to}->{ $opt->{long} };
      if ( $from_key ) {
         PTDEBUG && _d($opt->{long}, 'DSN copies from', $from_key, 'DSN');
         if ( $self->{opts}->{$from_key}->{parsed} ) {
            $prev = $self->{opts}->{$from_key}->{value};
         }
         else {
            PTDEBUG && _d('Cannot parse', $opt->{long}, 'until',
               $from_key, 'parsed');
            return;
         }
      }
      my $defaults = $self->{DSNParser}->parse_options($self);
      if (!$opt->{attributes}->{repeatable}) {
          $opt->{value} = $self->{DSNParser}->parse($val, $prev, $defaults);
      } else {
          my $values = [];
          for my $dsn_string (@$val) {
              push @$values, $self->{DSNParser}->parse($dsn_string, $prev, $defaults);
          }
          $opt->{value} = $values;
      }
   }
   elsif ( $val && $opt->{type} eq 'z' ) {  # type size
      PTDEBUG && _d('Parsing option', $opt->{long}, 'as a size value');
      $self->_parse_size($opt, $val);
   }
   elsif ( $opt->{type} eq 'H' || (defined $val && $opt->{type} eq 'h') ) {
      $opt->{value} = { map { $_ => 1 } split(/(?<!\\),\s*/, ($val || '')) };
   }
   elsif ( $opt->{type} eq 'A' || (defined $val && $opt->{type} eq 'a') ) {
      $opt->{value} = [ split(/(?<!\\),\s*/, ($val || '')) ];
   }
   else {
      PTDEBUG && _d('Nothing to validate for option',
         $opt->{long}, 'type', $opt->{type}, 'value', $val);
   }

   $opt->{parsed} = 1;
   return;
}

sub get {
   my ( $self, $opt ) = @_;
   my $long = (length $opt == 1 ? $self->{short_opts}->{$opt} : $opt);
   die "Option $opt does not exist"
      unless $long && exists $self->{opts}->{$long};
   return $self->{opts}->{$long}->{value};
}

sub got {
   my ( $self, $opt ) = @_;
   my $long = (length $opt == 1 ? $self->{short_opts}->{$opt} : $opt);
   die "Option $opt does not exist"
      unless $long && exists $self->{opts}->{$long};
   return $self->{opts}->{$long}->{got};
}

sub has {
   my ( $self, $opt ) = @_;
   my $long = (length $opt == 1 ? $self->{short_opts}->{$opt} : $opt);
   return defined $long ? exists $self->{opts}->{$long} : 0;
}

sub set {
   my ( $self, $opt, $val ) = @_;
   my $long = (length $opt == 1 ? $self->{short_opts}->{$opt} : $opt);
   die "Option $opt does not exist"
      unless $long && exists $self->{opts}->{$long};
   $self->{opts}->{$long}->{value} = $val;
   return;
}

sub save_error {
   my ( $self, $error ) = @_;
   push @{$self->{errors}}, $error;
   return;
}

sub errors {
   my ( $self ) = @_;
   return $self->{errors};
}

sub usage {
   my ( $self ) = @_;
   warn "No usage string is set" unless $self->{usage}; # XXX
   return "Usage: " . ($self->{usage} || '') . "\n";
}

sub descr {
   my ( $self ) = @_;
   warn "No description string is set" unless $self->{description}; # XXX
   my $descr  = ($self->{description} || $self->{program_name} || '')
              . "  For more details, please use the --help option, "
              . "or try 'perldoc $PROGRAM_NAME' "
              . "for complete documentation.";
   $descr = join("\n", $descr =~ m/(.{0,80})(?:\s+|$)/g)
      unless $ENV{DONT_BREAK_LINES};
   $descr =~ s/ +$//mg;
   return $descr;
}

sub usage_or_errors {
   my ( $self, $file, $return ) = @_;
   $file ||= $self->{file} || __FILE__;

   if ( !$self->{description} || !$self->{usage} ) {
      PTDEBUG && _d("Getting description and usage from SYNOPSIS in", $file);
      my %synop = $self->_parse_synopsis($file);
      $self->{description} ||= $synop{description};
      $self->{usage}       ||= $synop{usage};
      PTDEBUG && _d("Description:", $self->{description},
         "\nUsage:", $self->{usage});
   }

   if ( $self->{opts}->{help}->{got} ) {
      print $self->print_usage() or die "Cannot print usage: $OS_ERROR";
      exit 0 unless $return;
   }
   elsif ( scalar @{$self->{errors}} ) {
      print $self->print_errors() or die "Cannot print errors: $OS_ERROR";
      exit 1 unless $return;
   }

   return;
}

sub print_errors {
   my ( $self ) = @_;
   my $usage = $self->usage() . "\n";
   if ( (my @errors = @{$self->{errors}}) ) {
      $usage .= join("\n  * ", 'Errors in command-line arguments:', @errors)
              . "\n";
   }
   return $usage . "\n" . $self->descr();
}

sub print_usage {
   my ( $self ) = @_;
   die "Run get_opts() before print_usage()" unless $self->{got_opts};
   my @opts = values %{$self->{opts}};

   my $maxl = max(
      map {
         length($_->{long})               # option long name
         + ($_->{is_negatable} ? 4 : 0)   # "[no]" if opt is negatable
         + ($_->{type} ? 2 : 0)           # "=x" where x is the opt type
      }
      @opts);

   my $maxs = max(0,
      map {
         length($_)
         + ($self->{opts}->{$_}->{is_negatable} ? 4 : 0)
         + ($self->{opts}->{$_}->{type} ? 2 : 0)
      }
      values %{$self->{short_opts}});

   my $lcol = max($maxl, ($maxs + 3));
   my $rcol = 80 - $lcol - 6;
   my $rpad = ' ' x ( 80 - $rcol );

   $maxs = max($lcol - 3, $maxs);

   my $usage = $self->descr() . "\n" . $self->usage();

   my @groups = reverse sort grep { $_ ne 'default'; } keys %{$self->{groups}};
   push @groups, 'default';

   foreach my $group ( reverse @groups ) {
      $usage .= "\n".($group eq 'default' ? 'Options' : $group).":\n\n";
      foreach my $opt (
         sort { $a->{long} cmp $b->{long} }
         grep { $_->{group} eq $group }
         @opts )
      {
         my $long  = $opt->{is_negatable} ? "[no]$opt->{long}" : $opt->{long};
         my $short = $opt->{short};
         my $desc  = $opt->{desc};

         $long .= $opt->{type} ? "=$opt->{type}" : "";

         if ( $opt->{type} && $opt->{type} eq 'm' ) {
            my ($s) = $desc =~ m/\(suffix (.)\)/;
            $s    ||= 's';
            $desc =~ s/\s+\(suffix .\)//;
            $desc .= ".  Optional suffix s=seconds, m=minutes, h=hours, "
                   . "d=days; if no suffix, $s is used.";
         }
         $desc = join("\n$rpad", grep { $_ } $desc =~ m/(.{0,$rcol}(?!\W))(?:\s+|(?<=\W)|$)/g);
         $desc =~ s/ +$//mg;
         if ( $short ) {
            $usage .= sprintf("  --%-${maxs}s -%s  %s\n", $long, $short, $desc);
         }
         else {
            $usage .= sprintf("  --%-${lcol}s  %s\n", $long, $desc);
         }
      }
   }

   $usage .= "\nOption types: s=string, i=integer, f=float, h/H/a/A=comma-separated list, d=DSN, z=size, m=time\n";

   if ( (my @rules = @{$self->{rules}}) ) {
      $usage .= "\nRules:\n\n";
      $usage .= join("\n", map { "  $_" } @rules) . "\n";
   }
   if ( $self->{DSNParser} ) {
      $usage .= "\n" . $self->{DSNParser}->usage();
   }
   $usage .= "\nOptions and values after processing arguments:\n\n";
   foreach my $opt ( sort { $a->{long} cmp $b->{long} } @opts ) {
      my $val   = $opt->{value};
      my $type  = $opt->{type} || '';
      my $bool  = $opt->{spec} =~ m/^[\w-]+(?:\|[\w-])?!?$/;
      $val      = $bool              ? ( $val ? 'TRUE' : 'FALSE' )
                : !defined $val      ? '(No value)'
                : $type eq 'd'       ? $self->{DSNParser}->as_string($val)
                : $type =~ m/H|h/    ? join(',', sort keys %$val)
                : $type =~ m/A|a/    ? join(',', @$val)
                :                    $val;
      $usage .= sprintf("  --%-${lcol}s  %s\n", $opt->{long}, $val);
   }
   return $usage;
}

sub prompt_noecho {
   shift @_ if ref $_[0] eq __PACKAGE__;
   my ( $prompt ) = @_;
   local $OUTPUT_AUTOFLUSH = 1;
   print STDERR $prompt
      or die "Cannot print: $OS_ERROR";
   my $response;
   eval {
      require Term::ReadKey;
      Term::ReadKey::ReadMode('noecho');
      chomp($response = <STDIN>);
      Term::ReadKey::ReadMode('normal');
      print "\n"
         or die "Cannot print: $OS_ERROR";
   };
   if ( $EVAL_ERROR ) {
      die "Cannot read response; is Term::ReadKey installed? $EVAL_ERROR";
   }
   return $response;
}

sub _read_config_file {
   my ( $self, $filename ) = @_;
   open my $fh, "<", $filename or die "Cannot open $filename: $OS_ERROR\n";
   my @args;
   my $prefix = '--';
   my $parse  = 1;

   LINE:
   while ( my $line = <$fh> ) {
      chomp $line;
      next LINE if $line =~ m/^\s*(?:\#|\;|$)/;
      $line =~ s/\s+#.*$//g;
      $line =~ s/^\s+|\s+$//g;
      if ( $line eq '--' ) {
         $prefix = '';
         $parse  = 0;
         next LINE;
      }

      if (  $parse
            && !$self->has('version-check')
            && $line =~ /version-check/
      ) {
         next LINE;
      }

      if ( $parse
         && (my($opt, $arg) = $line =~ m/^\s*([^=\s]+?)(?:\s*=\s*(.*?)\s*)?$/)
      ) {
         push @args, grep { defined $_ } ("$prefix$opt", $arg);
      }
      elsif ( $line =~ m/./ ) {
         push @args, $line;
      }
      else {
         die "Syntax error in file $filename at line $INPUT_LINE_NUMBER";
      }
   }
   close $fh;
   return @args;
}

sub read_para_after {
   my ( $self, $file, $regex ) = @_;
   open my $fh, "<", $file or die "Can't open $file: $OS_ERROR";
   local $INPUT_RECORD_SEPARATOR = '';
   my $para;
   while ( $para = <$fh> ) {
      next unless $para =~ m/^=pod$/m;
      last;
   }
   while ( $para = <$fh> ) {
      next unless $para =~ m/$regex/;
      last;
   }
   $para = <$fh>;
   chomp($para);
   close $fh or die "Can't close $file: $OS_ERROR";
   return $para;
}

sub clone {
   my ( $self ) = @_;

   my %clone = map {
      my $hashref  = $self->{$_};
      my $val_copy = {};
      foreach my $key ( keys %$hashref ) {
         my $ref = ref $hashref->{$key};
         $val_copy->{$key} = !$ref           ? $hashref->{$key}
                           : $ref eq 'HASH'  ? { %{$hashref->{$key}} }
                           : $ref eq 'ARRAY' ? [ @{$hashref->{$key}} ]
                           : $hashref->{$key};
      }
      $_ => $val_copy;
   } qw(opts short_opts defaults);

   foreach my $scalar ( qw(got_opts) ) {
      $clone{$scalar} = $self->{$scalar};
   }

   return bless \%clone;     
}

sub _parse_size {
   my ( $self, $opt, $val ) = @_;

   if ( lc($val || '') eq 'null' ) {
      PTDEBUG && _d('NULL size for', $opt->{long});
      $opt->{value} = 'null';
      return;
   }

   my %factor_for = (k => 1_024, M => 1_048_576, G => 1_073_741_824);
   my ($pre, $num, $factor) = $val =~ m/^([+-])?(\d+)([kMG])?$/;
   if ( defined $num ) {
      if ( $factor ) {
         $num *= $factor_for{$factor};
         PTDEBUG && _d('Setting option', $opt->{y},
            'to num', $num, '* factor', $factor);
      }
      $opt->{value} = ($pre || '') . $num;
   }
   else {
      $self->save_error("Invalid size for --$opt->{long}: $val");
   }
   return;
}

sub _parse_attribs {
   my ( $self, $option, $attribs ) = @_;
   my $types = $self->{types};
   return $option
      . ($attribs->{'short form'} ? '|' . $attribs->{'short form'}   : '' )
      . ($attribs->{'negatable'}  ? '!'                              : '' )
      . ($attribs->{'cumulative'} ? '+'                              : '' )
      . ($attribs->{'type'}       ? '=' . $types->{$attribs->{type}} : '' );
}

sub _parse_synopsis {
   my ( $self, $file ) = @_;
   $file ||= $self->{file} || __FILE__;
   PTDEBUG && _d("Parsing SYNOPSIS in", $file);

   local $INPUT_RECORD_SEPARATOR = '';  # read paragraphs
   open my $fh, "<", $file or die "Cannot open $file: $OS_ERROR";
   my $para;
   1 while defined($para = <$fh>) && $para !~ m/^=head1 SYNOPSIS/;
   die "$file does not contain a SYNOPSIS section" unless $para;
   my @synop;
   for ( 1..2 ) {  # 1 for the usage, 2 for the description
      my $para = <$fh>;
      push @synop, $para;
   }
   close $fh;
   PTDEBUG && _d("Raw SYNOPSIS text:", @synop);
   my ($usage, $desc) = @synop;
   die "The SYNOPSIS section in $file is not formatted properly"
      unless $usage && $desc;

   $usage =~ s/^\s*Usage:\s+(.+)/$1/;
   chomp $usage;

   $desc =~ s/\n/ /g;
   $desc =~ s/\s{2,}/ /g;
   $desc =~ s/\. ([A-Z][a-z])/.  $1/g;
   $desc =~ s/\s+$//;

   return (
      description => $desc,
      usage       => $usage,
   );
};

sub set_vars {
   my ($self, $file) = @_;
   $file ||= $self->{file} || __FILE__;

   my %user_vars;
   my $user_vars = $self->has('set-vars') ? $self->get('set-vars') : undef;
   if ( $user_vars ) {
      foreach my $var_val ( @$user_vars ) {
         my ($var, $val) = $var_val =~ m/([^\s=]+)=(\S+)/;
         die "Invalid --set-vars value: $var_val\n" unless $var && defined $val;
         $user_vars{$var} = {
            val     => $val,
            default => 0,
         };
      }
   }

   my %default_vars;
   my $default_vars = $self->read_para_after($file, qr/MAGIC_set_vars/);
   if ( $default_vars ) {
      %default_vars = map {
         my $var_val = $_;
         my ($var, $val) = $var_val =~ m/([^\s=]+)=(\S+)/;
         die "Invalid --set-vars value: $var_val\n" unless $var && defined $val;
         $var => {
            val     => $val,
            default => 1,
         };
      } split("\n", $default_vars);
   }

   my %vars = (
      %default_vars, # first the tool's defaults
      %user_vars,    # then the user's which overwrite the defaults
   );
   PTDEBUG && _d('--set-vars:', Dumper(\%vars));
   return \%vars;
}

sub _d {
   my ($package, undef, $line) = caller 0;
   @_ = map { (my $temp = $_) =~ s/\n/\n# /g; $temp; }
        map { defined $_ ? $_ : 'undef' }
        @_;
   print STDERR "# $package:$line $PID ", join(' ', @_), "\n";
}

if ( PTDEBUG ) {
   print STDERR '# ', $^X, ' ', $], "\n";
   if ( my $uname = `uname -a` ) {
      $uname =~ s/\s+/ /g;
      print STDERR "# $uname\n";
   }
   print STDERR '# Arguments: ',
      join(' ', map { my $a = "_[$_]_"; $a =~ s/\n/\n# /g; $a; } @ARGV), "\n";
}

1;
}
# ###########################################################################
# End OptionParser package
# ###########################################################################

# ###########################################################################
# Lmo::Utils package
# This package is a copy without comments from the original.  The original
# with comments and its test file can be found in the GitHub repository at,
#   lib/Lmo/Utils.pm
#   t/lib/Lmo/Utils.t
# See https://github.com/percona/percona-toolkit for more information.
# ###########################################################################
{
package Lmo::Utils;

use strict;
use warnings qw( FATAL all );
require Exporter;
our (@ISA, @EXPORT, @EXPORT_OK);

BEGIN {
   @ISA = qw(Exporter);
   @EXPORT = @EXPORT_OK = qw(
      _install_coderef
      _unimport_coderefs
      _glob_for
      _stash_for
   );
}

{
   no strict 'refs';
   sub _glob_for {
      return \*{shift()}
   }

   sub _stash_for {
      return \%{ shift() . "::" };
   }
}

sub _install_coderef {
   my ($to, $code) = @_;

   return *{ _glob_for $to } = $code;
}

sub _unimport_coderefs {
   my ($target, @names) = @_;
   return unless @names;
   my $stash = _stash_for($target);
   foreach my $name (@names) {
      if ($stash->{$name} and defined(&{$stash->{$name}})) {
         delete $stash->{$name};
      }
   }
}

1;
}
# ###########################################################################
# End Lmo::Utils package
# ###########################################################################

# ###########################################################################
# Lmo::Meta package
# This package is a copy without comments from the original.  The original
# with comments and its test file can be found in the GitHub repository at,
#   lib/Lmo/Meta.pm
#   t/lib/Lmo/Meta.t
# See https://github.com/percona/percona-toolkit for more information.
# ###########################################################################
{
package Lmo::Meta;
use strict;
use warnings qw( FATAL all );

my %metadata_for;

sub new {
   my $class = shift;
   return bless { @_ }, $class
}

sub metadata_for {
   my $self    = shift;
   my ($class) = @_;

   return $metadata_for{$class} ||= {};
}

sub class { shift->{class} }

sub attributes {
   my $self = shift;
   return keys %{$self->metadata_for($self->class)}
}

sub attributes_for_new {
   my $self = shift;
   my @attributes;

   my $class_metadata = $self->metadata_for($self->class);
   while ( my ($attr, $meta) = each %$class_metadata ) {
      if ( exists $meta->{init_arg} ) {
         push @attributes, $meta->{init_arg}
               if defined $meta->{init_arg};
      }
      else {
         push @attributes, $attr;
      }
   }
   return @attributes;
}

1;
}
# ###########################################################################
# End Lmo::Meta package
# ###########################################################################

# ###########################################################################
# Lmo::Object package
# This package is a copy without comments from the original.  The original
# with comments and its test file can be found in the GitHub repository at,
#   lib/Lmo/Object.pm
#   t/lib/Lmo/Object.t
# See https://github.com/percona/percona-toolkit for more information.
# ###########################################################################
{
package Lmo::Object;

use strict;
use warnings qw( FATAL all );

use Carp ();
use Scalar::Util qw(blessed);

use Lmo::Meta;
use Lmo::Utils qw(_glob_for);

sub new {
   my $class = shift;
   my $args  = $class->BUILDARGS(@_);

   my $class_metadata = Lmo::Meta->metadata_for($class);

   my @args_to_delete;
   while ( my ($attr, $meta) = each %$class_metadata ) {
      next unless exists $meta->{init_arg};
      my $init_arg = $meta->{init_arg};

      if ( defined $init_arg ) {
         $args->{$attr} = delete $args->{$init_arg};
      }
      else {
         push @args_to_delete, $attr;
      }
   }

   delete $args->{$_} for @args_to_delete;

   for my $attribute ( keys %$args ) {
      if ( my $coerce = $class_metadata->{$attribute}{coerce} ) {
         $args->{$attribute} = $coerce->($args->{$attribute});
      }
      if ( my $isa_check = $class_metadata->{$attribute}{isa} ) {
         my ($check_name, $check_sub) = @$isa_check;
         $check_sub->($args->{$attribute});
      }
   }

   while ( my ($attribute, $meta) = each %$class_metadata ) {
      next unless $meta->{required};
      Carp::confess("Attribute ($attribute) is required for $class")
         if ! exists $args->{$attribute}
   }

   my $self = bless $args, $class;

   my @build_subs;
   my $linearized_isa = mro::get_linear_isa($class);

   for my $isa_class ( @$linearized_isa ) {
      unshift @build_subs, *{ _glob_for "${isa_class}::BUILD" }{CODE};
   }
   my @args = %$args;
   for my $sub (grep { defined($_) && exists &$_ } @build_subs) {
      $sub->( $self, @args);
   }
   return $self;
}

sub BUILDARGS {
   shift; # No need for the classname
   if ( @_ == 1 && ref($_[0]) ) {
      Carp::confess("Single parameters to new() must be a HASH ref, not $_[0]")
         unless ref($_[0]) eq ref({});
      return {%{$_[0]}} # We want a new reference, always
   }
   else {
      return { @_ };
   }
}

sub meta {
   my $class = shift;
   $class    = Scalar::Util::blessed($class) || $class;
   return Lmo::Meta->new(class => $class);
}

1;
}
# ###########################################################################
# End Lmo::Object package
# ###########################################################################

# ###########################################################################
# Lmo::Types package
# This package is a copy without comments from the original.  The original
# with comments and its test file can be found in the GitHub repository at,
#   lib/Lmo/Types.pm
#   t/lib/Lmo/Types.t
# See https://github.com/percona/percona-toolkit for more information.
# ###########################################################################
{
package Lmo::Types;

use strict;
use warnings qw( FATAL all );

use Carp ();
use Scalar::Util qw(looks_like_number blessed);


our %TYPES = (
   Bool   => sub { !$_[0] || (defined $_[0] && looks_like_number($_[0]) && $_[0] == 1) },
   Num    => sub { defined $_[0] && looks_like_number($_[0]) },
   Int    => sub { defined $_[0] && looks_like_number($_[0]) && $_[0] == int($_[0]) },
   Str    => sub { defined $_[0] },
   Object => sub { defined $_[0] && blessed($_[0]) },
   FileHandle => sub { local $@; require IO::Handle; fileno($_[0]) && $_[0]->opened },

   map {
      my $type = /R/ ? $_ : uc $_;
      $_ . "Ref" => sub { ref $_[0] eq $type }
   } qw(Array Code Hash Regexp Glob Scalar)
);

sub check_type_constaints {
   my ($attribute, $type_check, $check_name, $val) = @_;
   ( ref($type_check) eq 'CODE'
      ? $type_check->($val)
      : (ref $val eq $type_check
         || ($val && $val eq $type_check)
         || (exists $TYPES{$type_check} && $TYPES{$type_check}->($val)))
   )
   || Carp::confess(
        qq<Attribute ($attribute) does not pass the type constraint because: >
      . qq<Validation failed for '$check_name' with value >
      . (defined $val ? Lmo::Dumper($val) : 'undef') )
}

sub _nested_constraints {
   my ($attribute, $aggregate_type, $type) = @_;

   my $inner_types;
   if ( $type =~ /\A(ArrayRef|Maybe)\[(.*)\]\z/ ) {
      $inner_types = _nested_constraints($1, $2);
   }
   else {
      $inner_types = $TYPES{$type};
   }

   if ( $aggregate_type eq 'ArrayRef' ) {
      return sub {
         my ($val) = @_;
         return unless ref($val) eq ref([]);

         if ($inner_types) {
            for my $value ( @{$val} ) {
               return unless $inner_types->($value)
            }
         }
         else {
            for my $value ( @{$val} ) {
               return unless $value && ($value eq $type
                        || (Scalar::Util::blessed($value) && $value->isa($type)));
            }
         }
         return 1;
      };
   }
   elsif ( $aggregate_type eq 'Maybe' ) {
      return sub {
         my ($value) = @_;
         return 1 if ! defined($value);
         if ($inner_types) {
            return unless $inner_types->($value)
         }
         else {
            return unless $value eq $type
                        || (Scalar::Util::blessed($value) && $value->isa($type));
         }
         return 1;
      }
   }
   else {
      Carp::confess("Nested aggregate types are only implemented for ArrayRefs and Maybe");
   }
}

1;
}
# ###########################################################################
# End Lmo::Types package
# ###########################################################################

# ###########################################################################
# Lmo package
# This package is a copy without comments from the original.  The original
# with comments and its test file can be found in the GitHub repository at,
#   lib/Lmo.pm
#   t/lib/Lmo.t
# See https://github.com/percona/percona-toolkit for more information.
# ###########################################################################
{
BEGIN {
$INC{"Lmo.pm"} = __FILE__;
package Lmo;
our $VERSION = '0.30_Percona'; # Forked from 0.30 of Mo.


use strict;
use warnings qw( FATAL all );

use Carp ();
use Scalar::Util qw(looks_like_number blessed);

use Lmo::Meta;
use Lmo::Object;
use Lmo::Types;

use Lmo::Utils;

my %export_for;
sub import {
   warnings->import(qw(FATAL all));
   strict->import();

   my $caller     = scalar caller(); # Caller's package
   my %exports = (
      extends  => \&extends,
      has      => \&has,
      with     => \&with,
      override => \&override,
      confess  => \&Carp::confess,
   );

   $export_for{$caller} = \%exports;

   for my $keyword ( keys %exports ) {
      _install_coderef "${caller}::$keyword" => $exports{$keyword};
   }

   if ( !@{ *{ _glob_for "${caller}::ISA" }{ARRAY} || [] } ) {
      @_ = "Lmo::Object";
      goto *{ _glob_for "${caller}::extends" }{CODE};
   }
}

sub extends {
   my $caller = scalar caller();
   for my $class ( @_ ) {
      _load_module($class);
   }
   _set_package_isa($caller, @_);
   _set_inherited_metadata($caller);
}

sub _load_module {
   my ($class) = @_;
   
   (my $file = $class) =~ s{::|'}{/}g;
   $file .= '.pm';
   { local $@; eval { require "$file" } } # or warn $@;
   return;
}

sub with {
   my $package = scalar caller();
   require Role::Tiny;
   for my $role ( @_ ) {
      _load_module($role);
      _role_attribute_metadata($package, $role);
   }
   Role::Tiny->apply_roles_to_package($package, @_);
}

sub _role_attribute_metadata {
   my ($package, $role) = @_;

   my $package_meta = Lmo::Meta->metadata_for($package);
   my $role_meta    = Lmo::Meta->metadata_for($role);

   %$package_meta = (%$role_meta, %$package_meta);
}

sub has {
   my $names  = shift;
   my $caller = scalar caller();

   my $class_metadata = Lmo::Meta->metadata_for($caller);
   
   for my $attribute ( ref $names ? @$names : $names ) {
      my %args   = @_;
      my $method = ($args{is} || '') eq 'ro'
         ? sub {
            Carp::confess("Cannot assign a value to a read-only accessor at reader ${caller}::${attribute}")
               if $#_;
            return $_[0]{$attribute};
         }
         : sub {
            return $#_
                  ? $_[0]{$attribute} = $_[1]
                  : $_[0]{$attribute};
         };

      $class_metadata->{$attribute} = ();

      if ( my $type_check = $args{isa} ) {
         my $check_name = $type_check;
         
         if ( my ($aggregate_type, $inner_type) = $type_check =~ /\A(ArrayRef|Maybe)\[(.*)\]\z/ ) {
            $type_check = Lmo::Types::_nested_constraints($attribute, $aggregate_type, $inner_type);
         }
         
         my $check_sub = sub {
            my ($new_val) = @_;
            Lmo::Types::check_type_constaints($attribute, $type_check, $check_name, $new_val);
         };
         
         $class_metadata->{$attribute}{isa} = [$check_name, $check_sub];
         my $orig_method = $method;
         $method = sub {
            $check_sub->($_[1]) if $#_;
            goto &$orig_method;
         };
      }

      if ( my $builder = $args{builder} ) {
         my $original_method = $method;
         $method = sub {
               $#_
                  ? goto &$original_method
                  : ! exists $_[0]{$attribute}
                     ? $_[0]{$attribute} = $_[0]->$builder
                     : goto &$original_method
         };
      }

      if ( my $code = $args{default} ) {
         Carp::confess("${caller}::${attribute}'s default is $code, but should be a coderef")
               unless ref($code) eq 'CODE';
         my $original_method = $method;
         $method = sub {
               $#_
                  ? goto &$original_method
                  : ! exists $_[0]{$attribute}
                     ? $_[0]{$attribute} = $_[0]->$code
                     : goto &$original_method
         };
      }

      if ( my $role = $args{does} ) {
         my $original_method = $method;
         $method = sub {
            if ( $#_ ) {
               Carp::confess(qq<Attribute ($attribute) doesn't consume a '$role' role">)
                  unless Scalar::Util::blessed($_[1]) && eval { $_[1]->does($role) }
            }
            goto &$original_method
         };
      }

      if ( my $coercion = $args{coerce} ) {
         $class_metadata->{$attribute}{coerce} = $coercion;
         my $original_method = $method;
         $method = sub {
            if ( $#_ ) {
               return $original_method->($_[0], $coercion->($_[1]))
            }
            goto &$original_method;
         }
      }

      _install_coderef "${caller}::$attribute" => $method;

      if ( $args{required} ) {
         $class_metadata->{$attribute}{required} = 1;
      }

      if ($args{clearer}) {
         _install_coderef "${caller}::$args{clearer}"
            => sub { delete shift->{$attribute} }
      }

      if ($args{predicate}) {
         _install_coderef "${caller}::$args{predicate}"
            => sub { exists shift->{$attribute} }
      }

      if ($args{handles}) {
         _has_handles($caller, $attribute, \%args);
      }

      if (exists $args{init_arg}) {
         $class_metadata->{$attribute}{init_arg} = $args{init_arg};
      }
   }
}

sub _has_handles {
   my ($caller, $attribute, $args) = @_;
   my $handles = $args->{handles};

   my $ref = ref $handles;
   my $kv;
   if ( $ref eq ref [] ) {
         $kv = { map { $_,$_ } @{$handles} };
   }
   elsif ( $ref eq ref {} ) {
         $kv = $handles;
   }
   elsif ( $ref eq ref qr// ) {
         Carp::confess("Cannot delegate methods based on a Regexp without a type constraint (isa)")
            unless $args->{isa};
         my $target_class = $args->{isa};
         $kv = {
            map   { $_, $_     }
            grep  { $_ =~ $handles }
            grep  { !exists $Lmo::Object::{$_} && $target_class->can($_) }
            grep  { !$export_for{$target_class}->{$_} }
            keys %{ _stash_for $target_class }
         };
   }
   else {
         Carp::confess("handles for $ref not yet implemented");
   }

   while ( my ($method, $target) = each %{$kv} ) {
         my $name = _glob_for "${caller}::$method";
         Carp::confess("You cannot overwrite a locally defined method ($method) with a delegation")
            if defined &$name;

         my ($target, @curried_args) = ref($target) ? @$target : $target;
         *$name = sub {
            my $self        = shift;
            my $delegate_to = $self->$attribute();
            my $error = "Cannot delegate $method to $target because the value of $attribute";
            Carp::confess("$error is not defined") unless $delegate_to;
            Carp::confess("$error is not an object (got '$delegate_to')")
               unless Scalar::Util::blessed($delegate_to) || (!ref($delegate_to) && $delegate_to->can($target));
            return $delegate_to->$target(@curried_args, @_);
         }
   }
}

sub _set_package_isa {
   my ($package, @new_isa) = @_;
   my $package_isa  = \*{ _glob_for "${package}::ISA" };
   @{*$package_isa} = @new_isa;
}

sub _set_inherited_metadata {
   my $class = shift;
   my $class_metadata = Lmo::Meta->metadata_for($class);
   my $linearized_isa = mro::get_linear_isa($class);
   my %new_metadata;

   for my $isa_class (reverse @$linearized_isa) {
      my $isa_metadata = Lmo::Meta->metadata_for($isa_class);
      %new_metadata = (
         %new_metadata,
         %$isa_metadata,
      );
   }
   %$class_metadata = %new_metadata;
}

sub unimport {
   my $caller = scalar caller();
   my $target = caller;
  _unimport_coderefs($target, keys %{$export_for{$caller}});
}

sub Dumper {
   require Data::Dumper;
   local $Data::Dumper::Indent    = 0;
   local $Data::Dumper::Sortkeys  = 0;
   local $Data::Dumper::Quotekeys = 0;
   local $Data::Dumper::Terse     = 1;

   Data::Dumper::Dumper(@_)
}

BEGIN {
   if ($] >= 5.010) {
      { local $@; require mro; }
   }
   else {
      local $@;
      eval {
         require MRO::Compat;
      } or do {
         *mro::get_linear_isa = *mro::get_linear_isa_dfs = sub {
            no strict 'refs';

            my $classname = shift;

            my @lin = ($classname);
            my %stored;
            foreach my $parent (@{"$classname\::ISA"}) {
               my $plin = mro::get_linear_isa_dfs($parent);
               foreach (@$plin) {
                     next if exists $stored{$_};
                     push(@lin, $_);
                     $stored{$_} = 1;
               }
            }
            return \@lin;
         };
      }
   }
}

sub override {
   my ($methods, $code) = @_;
   my $caller          = scalar caller;

   for my $method ( ref($methods) ? @$methods : $methods ) {
      my $full_method     = "${caller}::${method}";
      *{_glob_for $full_method} = $code;
   }
}

}
1;
}
# ###########################################################################
# End Lmo package
# ###########################################################################

# ###########################################################################
# Cxn package
# This package is a copy without comments from the original.  The original
# with comments and its test file can be found in the GitHub repository at,
#   lib/Cxn.pm
#   t/lib/Cxn.t
# See https://github.com/percona/percona-toolkit for more information.
# ###########################################################################
{
package Cxn;

use strict;
use warnings FATAL => 'all';
use English qw(-no_match_vars);
use Scalar::Util qw(blessed);
use constant {
   PTDEBUG => $ENV{PTDEBUG} || 0,
   PERCONA_TOOLKIT_TEST_USE_DSN_NAMES => $ENV{PERCONA_TOOLKIT_TEST_USE_DSN_NAMES} || 0,
};

sub new {
   my ( $class, %args ) = @_;
   my @required_args = qw(DSNParser OptionParser);
   foreach my $arg ( @required_args ) {
      die "I need a $arg argument" unless $args{$arg};
   };
   my ($dp, $o) = @args{@required_args};

   my $dsn_defaults = $dp->parse_options($o);
   my $prev_dsn     = $args{prev_dsn};
   my $dsn          = $args{dsn};
   if ( !$dsn ) {
      $args{dsn_string} ||= 'h=' . ($dsn_defaults->{h} || 'localhost');

      $dsn = $dp->parse(
         $args{dsn_string}, $prev_dsn, $dsn_defaults);
   }
   elsif ( $prev_dsn ) {
      $dsn = $dp->copy($prev_dsn, $dsn);
   }

   my $dsn_name = $dp->as_string($dsn, [qw(h P S)])
               || $dp->as_string($dsn, [qw(F)])
               || '';

   my $self = {
      dsn             => $dsn,
      dbh             => $args{dbh},
      dsn_name        => $dsn_name,
      hostname        => '',
      set             => $args{set},
      NAME_lc         => defined($args{NAME_lc}) ? $args{NAME_lc} : 1,
      dbh_set         => 0,
      ask_pass        => $o->get('ask-pass'),
      DSNParser       => $dp,
      is_cluster_node => undef,
      parent          => $args{parent},
   };

   return bless $self, $class;
}

sub connect {
   my ( $self, %opts ) = @_;
   my $dsn = $opts{dsn} || $self->{dsn};
   my $dp  = $self->{DSNParser};

   my $dbh = $self->{dbh};
   if ( !$dbh || !$dbh->ping() ) {
      if ( $self->{ask_pass} && !$self->{asked_for_pass} && !defined $dsn->{p} ) {
         $dsn->{p} = OptionParser::prompt_noecho("Enter MySQL password: ");
         $self->{asked_for_pass} = 1;
      }
      $dbh = $dp->get_dbh(
         $dp->get_cxn_params($dsn),
         {
            AutoCommit => 1,
            %opts,
         },
      );
   }

   $dbh = $self->set_dbh($dbh);
   if ( $opts{dsn} ) {
      $self->{dsn}      = $dsn;
      $self->{dsn_name} = $dp->as_string($dsn, [qw(h P S)])
                       || $dp->as_string($dsn, [qw(F)])
                       || '';

   }
   PTDEBUG && _d($dbh, 'Connected dbh to', $self->{hostname},$self->{dsn_name});
   return $dbh;
}

sub set_dbh {
   my ($self, $dbh) = @_;

   if ( $self->{dbh} && $self->{dbh} == $dbh && $self->{dbh_set} ) {
      PTDEBUG && _d($dbh, 'Already set dbh');
      return $dbh;
   }

   PTDEBUG && _d($dbh, 'Setting dbh');

   $dbh->{FetchHashKeyName} = 'NAME_lc' if $self->{NAME_lc};

   my $sql = 'SELECT @@server_id /*!50038 , @@hostname*/';
   PTDEBUG && _d($dbh, $sql);
   my ($server_id, $hostname) = $dbh->selectrow_array($sql);
   PTDEBUG && _d($dbh, 'hostname:', $hostname, $server_id);
   if ( $hostname ) {
      $self->{hostname} = $hostname;
   }

   if ( $self->{parent} ) {
      PTDEBUG && _d($dbh, 'Setting InactiveDestroy=1 in parent');
      $dbh->{InactiveDestroy} = 1;
   }

   if ( my $set = $self->{set}) {
      $set->($dbh);
   }

   $self->{dbh}     = $dbh;
   $self->{dbh_set} = 1;
   return $dbh;
}

sub lost_connection {
   my ($self, $e) = @_;
   return 0 unless $e;
   return $e =~ m/MySQL server has gone away/
       || $e =~ m/Lost connection to MySQL server/
       || $e =~ m/Server shutdown in progress/;
}

sub dbh {
   my ($self) = @_;
   return $self->{dbh};
}

sub dsn {
   my ($self) = @_;
   return $self->{dsn};
}

sub name {
   my ($self) = @_;
   return $self->{dsn_name} if PERCONA_TOOLKIT_TEST_USE_DSN_NAMES;
   return $self->{hostname} || $self->{dsn_name} || 'unknown host';
}

sub description {
   my ($self) = @_;
   return sprintf("%s -> %s:%s", $self->name(), $self->{dsn}->{h} || 'localhost' , $self->{dsn}->{P} || 'socket');
}

sub get_id {
   my ($self, $cxn) = @_;

   $cxn ||= $self;

   my $unique_id;
   if ($cxn->is_cluster_node()) {  # for cluster we concatenate various variables to maximize id 'uniqueness' across versions
      my $sql  = q{SHOW STATUS LIKE 'wsrep\_local\_index'};
      my (undef, $wsrep_local_index) = $cxn->dbh->selectrow_array($sql);
      PTDEBUG && _d("Got cluster wsrep_local_index: ",$wsrep_local_index);
      $unique_id = $wsrep_local_index."|"; 
      foreach my $val ('server\_id', 'wsrep\_sst\_receive\_address', 'wsrep\_node\_name', 'wsrep\_node\_address') {
         my $sql = "SHOW VARIABLES LIKE '$val'";
         PTDEBUG && _d($cxn->name, $sql);
         my (undef, $val) = $cxn->dbh->selectrow_array($sql);
         $unique_id .= "|$val";
      }
   } else {
      my $sql  = 'SELECT @@SERVER_ID';
      PTDEBUG && _d($sql);
      $unique_id = $cxn->dbh->selectrow_array($sql);
   }
   PTDEBUG && _d("Generated unique id for cluster:", $unique_id);
   return $unique_id;
}


sub is_cluster_node {
   my ($self, $cxn) = @_;

   $cxn ||= $self;

   my $sql = "SHOW VARIABLES LIKE 'wsrep\_on'";

   my $dbh;
   if ($cxn->isa('DBI::db')) {
      $dbh = $cxn;
      PTDEBUG && _d($sql); #don't invoke name() if it's not a Cxn!
   }
   else {
      $dbh = $cxn->dbh();      
      PTDEBUG && _d($cxn->name, $sql);
   }

   my $row = $dbh->selectrow_arrayref($sql);
   return $row && $row->[1] && ($row->[1] eq 'ON' || $row->[1] eq '1') ? 1 : 0;

}

sub remove_duplicate_cxns {
   my ($self, %args) = @_;
   my @cxns     = @{$args{cxns}};
   my $seen_ids = $args{seen_ids} || {};
   PTDEBUG && _d("Removing duplicates from ", join(" ", map { $_->name } @cxns));
   my @trimmed_cxns;

   for my $cxn ( @cxns ) {

      my $id = $cxn->get_id();
      PTDEBUG && _d('Server ID for ', $cxn->name, ': ', $id);

      if ( ! $seen_ids->{$id}++ ) {
         push @trimmed_cxns, $cxn
      }
      else {
         PTDEBUG && _d("Removing ", $cxn->name,
                       ", ID ", $id, ", because we've already seen it");
      }
   }

   return \@trimmed_cxns;
}

sub DESTROY {
   my ($self) = @_;

   PTDEBUG && _d('Destroying cxn');

   if ( $self->{parent} ) {
      PTDEBUG && _d($self->{dbh}, 'Not disconnecting dbh in parent');
   }
   elsif ( $self->{dbh}
           && blessed($self->{dbh})
           && $self->{dbh}->can("disconnect") )
   {
      PTDEBUG && _d($self->{dbh}, 'Disconnecting dbh on', $self->{hostname},
         $self->{dsn_name});
      $self->{dbh}->disconnect();
   }

   return;
}

sub _d {
   my ($package, undef, $line) = caller 0;
   @_ = map { (my $temp = $_) =~ s/\n/\n# /g; $temp; }
        map { defined $_ ? $_ : 'undef' }
        @_;
   print STDERR "# $package:$line $PID ", join(' ', @_), "\n";
}

1;
}
# ###########################################################################
# End Cxn package
# ###########################################################################

# ###########################################################################
# Percona::XtraDB::Cluster package
# This package is a copy without comments from the original.  The original
# with comments and its test file can be found in the GitHub repository at,
#   lib/Percona/XtraDB/Cluster.pm
#   t/lib/Percona/XtraDB/Cluster.t
# See https://github.com/percona/percona-toolkit for more information.
# ###########################################################################
{
package Percona::XtraDB::Cluster;

use strict;
use warnings FATAL => 'all';
use English qw(-no_match_vars);
use constant PTDEBUG => $ENV{PTDEBUG} || 0;

use Lmo;
use Data::Dumper;

{ local $EVAL_ERROR; eval { require Cxn } };

sub get_cluster_name {
   my ($self, $cxn) = @_;
   my $sql = "SHOW VARIABLES LIKE 'wsrep\_cluster\_name'";
   PTDEBUG && _d($cxn->name, $sql);
   my (undef, $cluster_name) = $cxn->dbh->selectrow_array($sql);
   return $cluster_name;
}

sub is_cluster_node {
   my ($self, $cxn) = @_;

   my $sql = "SHOW VARIABLES LIKE 'wsrep\_on'";
   PTDEBUG && _d($cxn->name, $sql);
   my $row = $cxn->dbh->selectrow_arrayref($sql);
   PTDEBUG && _d(Dumper($row));
   return unless $row && $row->[1] && ($row->[1] eq 'ON' || $row->[1] eq '1');

   my $cluster_name = $self->get_cluster_name($cxn);
   return $cluster_name;
}

sub same_node {
   my ($self, $cxn1, $cxn2) = @_;

   foreach my $val ('wsrep\_sst\_receive\_address', 'wsrep\_node\_name', 'wsrep\_node\_address') {
      my $sql = "SHOW VARIABLES LIKE '$val'";
      PTDEBUG && _d($cxn1->name, $cxn2->name, $sql);
      my (undef, $val1) = $cxn1->dbh->selectrow_array($sql);
      my (undef, $val2) = $cxn2->dbh->selectrow_array($sql);

      return unless ($val1 || '') eq ($val2 || '');
   }

   return 1;
}

sub find_cluster_nodes {
   my ($self, %args) = @_;

   my $dbh = $args{dbh};
   my $dsn = $args{dsn};
   my $dp  = $args{DSNParser};
   my $make_cxn = $args{make_cxn};

   
   my $sql = q{SHOW STATUS LIKE 'wsrep\_incoming\_addresses'};
   PTDEBUG && _d($sql);
   my (undef, $addresses) = $dbh->selectrow_array($sql);
   PTDEBUG && _d("Cluster nodes found: ", $addresses);
   return unless $addresses;

   my @addresses = grep { !/\Aunspecified\z/i }
                   split /,\s*/, $addresses;

   my @nodes;
   foreach my $address ( @addresses ) {
      my ($host, $port) = split /:/, $address;
      my $spec = "h=$host"
               . ($port ? ",P=$port" : "");
      my $node_dsn = $dp->parse($spec, $dsn);
      my $node_dbh = eval { $dp->get_dbh(
            $dp->get_cxn_params($node_dsn), { AutoCommit => 1 }) };
      if ( $EVAL_ERROR ) {
         print STDERR "Cannot connect to ", $dp->as_string($node_dsn),
                      ", discovered through $sql: $EVAL_ERROR\n";
         if ( !$port && $dsn->{P} != 3306 ) {
            $address .= ":3306";
            redo;
         }
         next;
      }
      PTDEBUG && _d('Connected to', $dp->as_string($node_dsn));
      $node_dbh->disconnect();

      push @nodes, $make_cxn->(dsn => $node_dsn);
   }

   return \@nodes;
}

sub remove_duplicate_cxns {
   my ($self, %args) = @_;
   my @cxns     = @{$args{cxns}};
   my $seen_ids = $args{seen_ids} || {};
   PTDEBUG && _d("Removing duplicates nodes from ", join(" ", map { $_->name } @cxns));
   my @trimmed_cxns;

   for my $cxn ( @cxns ) {
      my $id = $cxn->get_id();
      PTDEBUG && _d('Server ID for ', $cxn->name, ': ', $id);

      if ( ! $seen_ids->{$id}++ ) {
         push @trimmed_cxns, $cxn
      }
      else {
         PTDEBUG && _d("Removing ", $cxn->name,
                       ", ID ", $id, ", because we've already seen it");
      }
   }
   return \@trimmed_cxns;
}

sub same_cluster {
   my ($self, $cxn1, $cxn2) = @_;

   return 0 if !$self->is_cluster_node($cxn1) || !$self->is_cluster_node($cxn2);

   my $cluster1 = $self->get_cluster_name($cxn1);
   my $cluster2 = $self->get_cluster_name($cxn2);

   return ($cluster1 || '') eq ($cluster2 || '');
}

sub autodetect_nodes {
   my ($self, %args) = @_;
   my $ms       = $args{MasterSlave};
   my $dp       = $args{DSNParser};
   my $make_cxn = $args{make_cxn};
   my $nodes    = $args{nodes};
   my $seen_ids = $args{seen_ids};

   my $new_nodes = [];

   return $new_nodes unless @$nodes;
   
   for my $node ( @$nodes ) {
      my $nodes_found = $self->find_cluster_nodes(
         dbh       => $node->dbh(),
         dsn       => $node->dsn(),
         make_cxn  => $make_cxn,
         DSNParser => $dp,
      );
      push @$new_nodes, @$nodes_found;
   }

   $new_nodes = $self->remove_duplicate_cxns(
      cxns     => $new_nodes,
      seen_ids => $seen_ids
   );

   my $new_slaves = [];
   foreach my $node (@$new_nodes) {
      my $node_slaves = $ms->get_slaves(
         dbh      => $node->dbh(),
         dsn      => $node->dsn(),
         make_cxn => $make_cxn,
      );
      push @$new_slaves, @$node_slaves;
   }

   $new_slaves = $self->remove_duplicate_cxns(
      cxns     => $new_slaves,
      seen_ids => $seen_ids
   );

   my @new_slave_nodes = grep { $self->is_cluster_node($_) } @$new_slaves;
   
   my $slaves_of_slaves = $self->autodetect_nodes(
         %args,
         nodes => \@new_slave_nodes,
   );
   
   my @autodetected_nodes = ( @$new_nodes, @$new_slaves, @$slaves_of_slaves );
   return \@autodetected_nodes;
}

sub _d {
   my ($package, undef, $line) = caller 0;
   @_ = map { (my $temp = $_) =~ s/\n/\n# /g; $temp; }
        map { defined $_ ? $_ : 'undef' }
        @_;
   print STDERR "# $package:$line $PID ", join(' ', @_), "\n";
}

1;
}
# ###########################################################################
# End Percona::XtraDB::Cluster package
# ###########################################################################

# ###########################################################################
# Quoter package
# This package is a copy without comments from the original.  The original
# with comments and its test file can be found in the GitHub repository at,
#   lib/Quoter.pm
#   t/lib/Quoter.t
# See https://github.com/percona/percona-toolkit for more information.
# ###########################################################################
{
package Quoter;

use strict;
use warnings FATAL => 'all';
use English qw(-no_match_vars);
use constant PTDEBUG => $ENV{PTDEBUG} || 0;

use Data::Dumper;
$Data::Dumper::Indent    = 1;
$Data::Dumper::Sortkeys  = 1;
$Data::Dumper::Quotekeys = 0;

sub new {
   my ( $class, %args ) = @_;
   return bless {}, $class;
}

sub quote {
   my ( $self, @vals ) = @_;
   foreach my $val ( @vals ) {
      $val =~ s/`/``/g;
   }
   return join('.', map { '`' . $_ . '`' } @vals);
}

sub quote_val {
   my ( $self, $val, %args ) = @_;

   return 'NULL' unless defined $val;          # undef = NULL
   return "''" if $val eq '';                  # blank string = ''
   return $val if $val =~ m/^0x[0-9a-fA-F]+$/  # quote hex data
                  && !$args{is_char};          # unless is_char is true

   return $val if $args{is_float};

   $val =~ s/(['\\])/\\$1/g;
   return "'$val'";
}

sub split_unquote {
   my ( $self, $db_tbl, $default_db ) = @_;
   my ( $db, $tbl ) = split(/[.]/, $db_tbl);
   if ( !$tbl ) {
      $tbl = $db;
      $db  = $default_db;
   }
   for ($db, $tbl) {
      next unless $_;
      s/\A`//;
      s/`\z//;
      s/``/`/g;
   }
   
   return ($db, $tbl);
}

sub literal_like {
   my ( $self, $like ) = @_;
   return unless $like;
   $like =~ s/([%_])/\\$1/g;
   return "'$like'";
}

sub join_quote {
   my ( $self, $default_db, $db_tbl ) = @_;
   return unless $db_tbl;
   my ($db, $tbl) = split(/[.]/, $db_tbl);
   if ( !$tbl ) {
      $tbl = $db;
      $db  = $default_db;
   }
   $db  = "`$db`"  if $db  && $db  !~ m/^`/;
   $tbl = "`$tbl`" if $tbl && $tbl !~ m/^`/;
   return $db ? "$db.$tbl" : $tbl;
}

sub serialize_list {
   my ( $self, @args ) = @_;
   PTDEBUG && _d('Serializing', Dumper(\@args));
   return unless @args;

   my @parts;
   foreach my $arg  ( @args ) {
      if ( defined $arg ) {
         $arg =~ s/,/\\,/g;      # escape commas
         $arg =~ s/\\N/\\\\N/g;  # escape literal \N
         push @parts, $arg;
      }
      else {
         push @parts, '\N';
      }
   }

   my $string = join(',', @parts);
   PTDEBUG && _d('Serialized: <', $string, '>');
   return $string;
}

sub deserialize_list {
   my ( $self, $string ) = @_;
   PTDEBUG && _d('Deserializing <', $string, '>');
   die "Cannot deserialize an undefined string" unless defined $string;

   my @parts;
   foreach my $arg ( split(/(?<!\\),/, $string) ) {
      if ( $arg eq '\N' ) {
         $arg = undef;
      }
      else {
         $arg =~ s/\\,/,/g;
         $arg =~ s/\\\\N/\\N/g;
      }
      push @parts, $arg;
   }

   if ( !@parts ) {
      my $n_empty_strings = $string =~ tr/,//;
      $n_empty_strings++;
      PTDEBUG && _d($n_empty_strings, 'empty strings');
      map { push @parts, '' } 1..$n_empty_strings;
   }
   elsif ( $string =~ m/(?<!\\),$/ ) {
      PTDEBUG && _d('Last value is an empty string');
      push @parts, '';
   }

   PTDEBUG && _d('Deserialized', Dumper(\@parts));
   return @parts;
}

sub _d {
   my ($package, undef, $line) = caller 0;
   @_ = map { (my $temp = $_) =~ s/\n/\n# /g; $temp; }
        map { defined $_ ? $_ : 'undef' }
        @_;
   print STDERR "# $package:$line $PID ", join(' ', @_), "\n";
}

1;
}
# ###########################################################################
# End Quoter package
# ###########################################################################

# ###########################################################################
# VersionParser package
# This package is a copy without comments from the original.  The original
# with comments and its test file can be found in the GitHub repository at,
#   lib/VersionParser.pm
#   t/lib/VersionParser.t
# See https://github.com/percona/percona-toolkit for more information.
# ###########################################################################
{
package VersionParser;

use Lmo;
use Scalar::Util qw(blessed);
use English qw(-no_match_vars);
use constant PTDEBUG => $ENV{PTDEBUG} || 0;

use overload (
   '""'     => "version",
   '<=>'    => "cmp",
   'cmp'    => "cmp",
   fallback => 1,
);

use Carp ();

has major => (
    is       => 'ro',
    isa      => 'Int',
    required => 1,
);

has [qw( minor revision )] => (
    is  => 'ro',
    isa => 'Num',
);

has flavor => (
    is      => 'ro',
    isa     => 'Str',
    default => sub { 'Unknown' },
);

has innodb_version => (
    is      => 'ro',
    isa     => 'Str',
    default => sub { 'NO' },
);

sub series {
   my $self = shift;
   return $self->_join_version($self->major, $self->minor);
}

sub version {
   my $self = shift;
   return $self->_join_version($self->major, $self->minor, $self->revision);
}

sub is_in {
   my ($self, $target) = @_;

   return $self eq $target;
}

sub _join_version {
    my ($self, @parts) = @_;

    return join ".", map { my $c = $_; $c =~ s/^0\./0/; $c } grep defined, @parts;
}
sub _split_version {
   my ($self, $str) = @_;
   my @version_parts = map { s/^0(?=\d)/0./; $_ } $str =~ m/(\d+)/g;
   return @version_parts[0..2];
}

sub normalized_version {
   my ( $self ) = @_;
   my $result = sprintf('%d%02d%02d', map { $_ || 0 } $self->major,
                                                      $self->minor,
                                                      $self->revision);
   PTDEBUG && _d($self->version, 'normalizes to', $result);
   return $result;
}

sub comment {
   my ( $self, $cmd ) = @_;
   my $v = $self->normalized_version();

   return "/*!$v $cmd */"
}

my @methods = qw(major minor revision);
sub cmp {
   my ($left, $right) = @_;
   my $right_obj = (blessed($right) && $right->isa(ref($left)))
                   ? $right
                   : ref($left)->new($right);

   my $retval = 0;
   for my $m ( @methods ) {
      last unless defined($left->$m) && defined($right_obj->$m);
      $retval = $left->$m <=> $right_obj->$m;
      last if $retval;
   }
   return $retval;
}

sub BUILDARGS {
   my $self = shift;

   if ( @_ == 1 ) {
      my %args;
      if ( blessed($_[0]) && $_[0]->can("selectrow_hashref") ) {
         PTDEBUG && _d("VersionParser got a dbh, trying to get the version");
         my $dbh = $_[0];
         local $dbh->{FetchHashKeyName} = 'NAME_lc';
         my $query = eval {
            $dbh->selectall_arrayref(q/SHOW VARIABLES LIKE 'version%'/, { Slice => {} })
         };
         if ( $query ) {
            $query = { map { $_->{variable_name} => $_->{value} } @$query };
            @args{@methods} = $self->_split_version($query->{version});
            $args{flavor} = delete $query->{version_comment}
                  if $query->{version_comment};
         }
         elsif ( eval { ($query) = $dbh->selectrow_array(q/SELECT VERSION()/) } ) {
            @args{@methods} = $self->_split_version($query);
         }
         else {
            Carp::confess("Couldn't get the version from the dbh while "
                        . "creating a VersionParser object: $@");
         }
         $args{innodb_version} = eval { $self->_innodb_version($dbh) };
      }
      elsif ( !ref($_[0]) ) {
         @args{@methods} = $self->_split_version($_[0]);
      }

      for my $method (@methods) {
         delete $args{$method} unless defined $args{$method};
      }
      @_ = %args if %args;
   }

   return $self->SUPER::BUILDARGS(@_);
}

sub _innodb_version {
   my ( $self, $dbh ) = @_;
   return unless $dbh;
   my $innodb_version = "NO";

   my ($innodb) =
      grep { $_->{engine} =~ m/InnoDB/i }
      map  {
         my %hash;
         @hash{ map { lc $_ } keys %$_ } = values %$_;
         \%hash;
      }
      @{ $dbh->selectall_arrayref("SHOW ENGINES", {Slice=>{}}) };
   if ( $innodb ) {
      PTDEBUG && _d("InnoDB support:", $innodb->{support});
      if ( $innodb->{support} =~ m/YES|DEFAULT/i ) {
         my $vars = $dbh->selectrow_hashref(
            "SHOW VARIABLES LIKE 'innodb_version'");
         $innodb_version = !$vars ? "BUILTIN"
                         :          ($vars->{Value} || $vars->{value});
      }
      else {
         $innodb_version = $innodb->{support};  # probably DISABLED or NO
      }
   }

   PTDEBUG && _d("InnoDB version:", $innodb_version);
   return $innodb_version;
}

sub _d {
   my ($package, undef, $line) = caller 0;
   @_ = map { (my $temp = $_) =~ s/\n/\n# /g; $temp; }
        map { defined $_ ? $_ : 'undef' }
        @_;
   print STDERR "# $package:$line $PID ", join(' ', @_), "\n";
}

no Lmo;
1;
}
# ###########################################################################
# End VersionParser package
# ###########################################################################

# ###########################################################################
# TableParser package
# This package is a copy without comments from the original.  The original
# with comments and its test file can be found in the GitHub repository at,
#   lib/TableParser.pm
#   t/lib/TableParser.t
# See https://github.com/percona/percona-toolkit for more information.
# ###########################################################################
{
package TableParser;

use strict;
use warnings FATAL => 'all';
use English qw(-no_match_vars);
use constant PTDEBUG => $ENV{PTDEBUG} || 0;

use Data::Dumper;
$Data::Dumper::Indent    = 1;
$Data::Dumper::Sortkeys  = 1;
$Data::Dumper::Quotekeys = 0;

local $EVAL_ERROR;
eval {
   require Quoter;
};

sub new {
   my ( $class, %args ) = @_;
   my $self = { %args };
   $self->{Quoter} ||= Quoter->new();
   return bless $self, $class;
}

sub Quoter { shift->{Quoter} }

sub get_create_table {
   my ( $self, $dbh, $db, $tbl ) = @_;
   die "I need a dbh parameter" unless $dbh;
   die "I need a db parameter"  unless $db;
   die "I need a tbl parameter" unless $tbl;
   my $q = $self->{Quoter};

   my $new_sql_mode
      = q{/*!40101 SET @OLD_SQL_MODE := @@SQL_MODE, }
      . q{@@SQL_MODE := '', }
      . q{@OLD_QUOTE := @@SQL_QUOTE_SHOW_CREATE, }
      . q{@@SQL_QUOTE_SHOW_CREATE := 1 */};

   my $old_sql_mode
      = q{/*!40101 SET @@SQL_MODE := @OLD_SQL_MODE, }
      . q{@@SQL_QUOTE_SHOW_CREATE := @OLD_QUOTE */};

   PTDEBUG && _d($new_sql_mode);
   eval { $dbh->do($new_sql_mode); };
   PTDEBUG && $EVAL_ERROR && _d($EVAL_ERROR);

   my $use_sql = 'USE ' . $q->quote($db);
   PTDEBUG && _d($dbh, $use_sql);
   $dbh->do($use_sql);

   my $show_sql = "SHOW CREATE TABLE " . $q->quote($db, $tbl);
   PTDEBUG && _d($show_sql);
   my $href;
   eval { $href = $dbh->selectrow_hashref($show_sql); };
   if ( my $e = $EVAL_ERROR ) {
      PTDEBUG && _d($old_sql_mode);
      $dbh->do($old_sql_mode);

      die $e;
   }

   PTDEBUG && _d($old_sql_mode);
   $dbh->do($old_sql_mode);

   my ($key) = grep { m/create (?:table|view)/i } keys %$href;
   if ( !$key ) {
      die "Error: no 'Create Table' or 'Create View' in result set from "
         . "$show_sql: " . Dumper($href);
   }

   return $href->{$key};
}

sub parse {
   my ( $self, $ddl, $opts ) = @_;
   return unless $ddl;

   if ( $ddl =~ m/CREATE (?:TEMPORARY )?TABLE "/ ) {
      $ddl = $self->ansi_to_legacy($ddl);
   }
   elsif ( $ddl !~ m/CREATE (?:TEMPORARY )?TABLE `/ ) {
      die "TableParser doesn't handle CREATE TABLE without quoting.";
   }

   my ($name)     = $ddl =~ m/CREATE (?:TEMPORARY )?TABLE\s+(`.+?`)/;
   (undef, $name) = $self->{Quoter}->split_unquote($name) if $name;

   $ddl =~ s/(`[^`\n]+`)/\L$1/gm;

   my $engine = $self->get_engine($ddl);

   my @defs = $ddl =~ m/(?:(?<=,\n)|(?<=\(\n))(\s+`(?:.|\n)+?`.+?),?\n/g;
   my @cols   = map { $_ =~ m/`([^`]+)`/ } @defs;
   PTDEBUG && _d('Table cols:', join(', ', map { "`$_`" } @cols));

   my %def_for;
   @def_for{@cols} = @defs;

   my (@nums, @null, @non_generated);
   my (%type_for, %is_nullable, %is_numeric, %is_autoinc, %is_generated);
   foreach my $col ( @cols ) {
      my $def = $def_for{$col};

      $def =~ s/``//g;

      my ( $type ) = $def =~ m/`[^`]+`\s([a-z]+)/;
      die "Can't determine column type for $def" unless $type;
      $type_for{$col} = $type;
      if ( $type =~ m/(?:(?:tiny|big|medium|small)?int|float|double|decimal|year)/ ) {
         push @nums, $col;
         $is_numeric{$col} = 1;
      }
      if ( $def !~ m/NOT NULL/ ) {
         push @null, $col;
         $is_nullable{$col} = 1;
      }
      if ( remove_quoted_text($def) =~ m/\WGENERATED\W/i ) {
          $is_generated{$col} = 1;
      } else {
          push @non_generated, $col;
      }
      $is_autoinc{$col} = $def =~ m/AUTO_INCREMENT/i ? 1 : 0;
   }

   my ($keys, $clustered_key) = $self->get_keys($ddl, $opts, \%is_nullable);

   my ($charset) = $ddl =~ m/DEFAULT CHARSET=(\w+)/;

   return {
      name               => $name,
      cols               => \@cols,
      col_posn           => { map { $cols[$_] => $_ } 0..$#cols },
      is_col             => { map { $_ => 1 } @non_generated },
      null_cols          => \@null,
      is_nullable        => \%is_nullable,
      non_generated_cols => \@non_generated,
      is_autoinc         => \%is_autoinc,
      is_generated       => \%is_generated,
      clustered_key      => $clustered_key,
      keys               => $keys,
      defs               => \%def_for,
      numeric_cols       => \@nums,
      is_numeric         => \%is_numeric,
      engine             => $engine,
      type_for           => \%type_for,
      charset            => $charset,
   };
}

sub remove_quoted_text {
   my ($string) = @_;
   $string =~ s/\\['"]//g;
   $string =~ s/`[^`]*?`//g; 
   $string =~ s/"[^"]*?"//g; 
   $string =~ s/'[^']*?'//g; 
   return $string;
}

sub sort_indexes {
   my ( $self, $tbl ) = @_;

   my @indexes
      = sort {
         (($a ne 'PRIMARY') <=> ($b ne 'PRIMARY'))
         || ( !$tbl->{keys}->{$a}->{is_unique} <=> !$tbl->{keys}->{$b}->{is_unique} )
         || ( $tbl->{keys}->{$a}->{is_nullable} <=> $tbl->{keys}->{$b}->{is_nullable} )
         || ( scalar(@{$tbl->{keys}->{$a}->{cols}}) <=> scalar(@{$tbl->{keys}->{$b}->{cols}}) )
      }
      grep {
         $tbl->{keys}->{$_}->{type} eq 'BTREE'
      }
      sort keys %{$tbl->{keys}};

   PTDEBUG && _d('Indexes sorted best-first:', join(', ', @indexes));
   return @indexes;
}

sub find_best_index {
   my ( $self, $tbl, $index ) = @_;
   my $best;
   if ( $index ) {
      ($best) = grep { uc $_ eq uc $index } keys %{$tbl->{keys}};
   }
   if ( !$best ) {
      if ( $index ) {
         die "Index '$index' does not exist in table";
      }
      else {
         ($best) = $self->sort_indexes($tbl);
      }
   }
   PTDEBUG && _d('Best index found is', $best);
   return $best;
}

sub find_possible_keys {
   my ( $self, $dbh, $database, $table, $quoter, $where ) = @_;
   return () unless $where;
   my $sql = 'EXPLAIN SELECT * FROM ' . $quoter->quote($database, $table)
      . ' WHERE ' . $where;
   PTDEBUG && _d($sql);
   my $expl = $dbh->selectrow_hashref($sql);
   $expl = { map { lc($_) => $expl->{$_} } keys %$expl };
   if ( $expl->{possible_keys} ) {
      PTDEBUG && _d('possible_keys =', $expl->{possible_keys});
      my @candidates = split(',', $expl->{possible_keys});
      my %possible   = map { $_ => 1 } @candidates;
      if ( $expl->{key} ) {
         PTDEBUG && _d('MySQL chose', $expl->{key});
         unshift @candidates, grep { $possible{$_} } split(',', $expl->{key});
         PTDEBUG && _d('Before deduping:', join(', ', @candidates));
         my %seen;
         @candidates = grep { !$seen{$_}++ } @candidates;
      }
      PTDEBUG && _d('Final list:', join(', ', @candidates));
      return @candidates;
   }
   else {
      PTDEBUG && _d('No keys in possible_keys');
      return ();
   }
}

sub check_table {
   my ( $self, %args ) = @_;
   my @required_args = qw(dbh db tbl);
   foreach my $arg ( @required_args ) {
      die "I need a $arg argument" unless $args{$arg};
   }
   my ($dbh, $db, $tbl) = @args{@required_args};
   my $q      = $self->{Quoter} || 'Quoter';
   my $db_tbl = $q->quote($db, $tbl);
   PTDEBUG && _d('Checking', $db_tbl);

   $self->{check_table_error} = undef;

   my $sql = "SHOW TABLES FROM " . $q->quote($db)
           . ' LIKE ' . $q->literal_like($tbl);
   PTDEBUG && _d($sql);
   my $row;
   eval {
      $row = $dbh->selectrow_arrayref($sql);
   };
   if ( my $e = $EVAL_ERROR ) {
      PTDEBUG && _d($e);
      $self->{check_table_error} = $e;
      return 0;
   }
   if ( !$row->[0] || $row->[0] ne $tbl ) {
      PTDEBUG && _d('Table does not exist');
      return 0;
   }

   PTDEBUG && _d('Table', $db, $tbl, 'exists');
   return 1;

}

sub get_engine {
   my ( $self, $ddl, $opts ) = @_;
   my ( $engine ) = $ddl =~ m/\).*?(?:ENGINE|TYPE)=(\w+)/;
   PTDEBUG && _d('Storage engine:', $engine);
   return $engine || undef;
}

sub get_keys {
   my ( $self, $ddl, $opts, $is_nullable ) = @_;
   my $engine        = $self->get_engine($ddl);
   my $keys          = {};
   my $clustered_key = undef;

   KEY:
   foreach my $key ( $ddl =~ m/^  ((?:[A-Z]+ )?KEY \(?`[\s\S]*?`\),?)$/gm ) {

      next KEY if $key =~ m/FOREIGN/;

      my $key_ddl = $key;
      PTDEBUG && _d('Parsed key:', $key_ddl);

      if ( !$engine || $engine !~ m/MEMORY|HEAP/ ) {
         $key =~ s/USING HASH/USING BTREE/;
      }

      my ( $type, $cols ) = $key =~ m/(?:USING (\w+))? \(([\s\S]+?)\)/;
      my ( $special ) = $key =~ m/(FULLTEXT|SPATIAL)/;
      $type = $type || $special || 'BTREE';
      my ($name) = $key =~ m/(PRIMARY|`[^`]*`)/;
      my $unique = $key =~ m/PRIMARY|UNIQUE/ ? 1 : 0;
      my @cols;
      my @col_prefixes;
      foreach my $col_def ( $cols =~ m/`[^`]+`(?:\(\d+\))?/g ) {
         my ($name, $prefix) = $col_def =~ m/`([^`]+)`(?:\((\d+)\))?/;
         push @cols, $name;
         push @col_prefixes, $prefix;
      }
      $name =~ s/`//g;

      PTDEBUG && _d( $name, 'key cols:', join(', ', map { "`$_`" } @cols));

      $keys->{$name} = {
         name         => $name,
         type         => $type,
         colnames     => $cols,
         cols         => \@cols,
         col_prefixes => \@col_prefixes,
         is_unique    => $unique,
         is_nullable  => scalar(grep { $is_nullable->{$_} } @cols),
         is_col       => { map { $_ => 1 } @cols },
         ddl          => $key_ddl,
      };

      if ( ($engine || '') =~ m/InnoDB/i && !$clustered_key ) {
         my $this_key = $keys->{$name};
         if ( $this_key->{name} eq 'PRIMARY' ) {
            $clustered_key = 'PRIMARY';
         }
         elsif ( $this_key->{is_unique} && !$this_key->{is_nullable} ) {
            $clustered_key = $this_key->{name};
         }
         PTDEBUG && $clustered_key && _d('This key is the clustered key');
      }
   }

   return $keys, $clustered_key;
}

sub get_fks {
   my ( $self, $ddl, $opts ) = @_;
   my $q   = $self->{Quoter};
   my $fks = {};

   foreach my $fk (
      $ddl =~ m/CONSTRAINT .* FOREIGN KEY .* REFERENCES [^\)]*\)/mg )
   {
      my ( $name ) = $fk =~ m/CONSTRAINT `(.*?)`/;
      my ( $cols ) = $fk =~ m/FOREIGN KEY \(([^\)]+)\)/;
      my ( $parent, $parent_cols ) = $fk =~ m/REFERENCES (\S+) \(([^\)]+)\)/;

      my ($db, $tbl) = $q->split_unquote($parent, $opts->{database});
      my %parent_tbl = (tbl => $tbl);
      $parent_tbl{db} = $db if $db;

      if ( $parent !~ m/\./ && $opts->{database} ) {
         $parent = $q->quote($opts->{database}) . ".$parent";
      }

      $fks->{$name} = {
         name           => $name,
         colnames       => $cols,
         cols           => [ map { s/[ `]+//g; $_; } split(',', $cols) ],
         parent_tbl     => \%parent_tbl,
         parent_tblname => $parent,
         parent_cols    => [ map { s/[ `]+//g; $_; } split(',', $parent_cols) ],
         parent_colnames=> $parent_cols,
         ddl            => $fk,
      };
   }

   return $fks;
}

sub remove_auto_increment {
   my ( $self, $ddl ) = @_;
   $ddl =~ s/(^\).*?) AUTO_INCREMENT=\d+\b/$1/m;
   return $ddl;
}

sub get_table_status {
   my ( $self, $dbh, $db, $like ) = @_;
   my $q = $self->{Quoter};
   my $sql = "SHOW TABLE STATUS FROM " . $q->quote($db);
   my @params;
   if ( $like ) {
      $sql .= ' LIKE ?';
      push @params, $like;
   }
   PTDEBUG && _d($sql, @params);
   my $sth = $dbh->prepare($sql);
   eval { $sth->execute(@params); };
   if ($EVAL_ERROR) {
      PTDEBUG && _d($EVAL_ERROR);
      return;
   }
   my @tables = @{$sth->fetchall_arrayref({})};
   @tables = map {
      my %tbl; # Make a copy with lowercased keys
      @tbl{ map { lc $_ } keys %$_ } = values %$_;
      $tbl{engine} ||= $tbl{type} || $tbl{comment};
      delete $tbl{type};
      \%tbl;
   } @tables;
   return @tables;
}

my $ansi_quote_re = qr/" [^"]* (?: "" [^"]* )* (?<=.) "/ismx;
sub ansi_to_legacy {
   my ($self, $ddl) = @_;
   $ddl =~ s/($ansi_quote_re)/ansi_quote_replace($1)/ge;
   return $ddl;
}

sub ansi_quote_replace {
   my ($val) = @_;
   $val =~ s/^"|"$//g;
   $val =~ s/`/``/g;
   $val =~ s/""/"/g;
   return "`$val`";
}

sub _d {
   my ($package, undef, $line) = caller 0;
   @_ = map { (my $temp = $_) =~ s/\n/\n# /g; $temp; }
        map { defined $_ ? $_ : 'undef' }
        @_;
   print STDERR "# $package:$line $PID ", join(' ', @_), "\n";
}

1;
}
# ###########################################################################
# End TableParser package
# ###########################################################################

# ###########################################################################
# TableNibbler package
# This package is a copy without comments from the original.  The original
# with comments and its test file can be found in the GitHub repository at,
#   lib/TableNibbler.pm
#   t/lib/TableNibbler.t
# See https://github.com/percona/percona-toolkit for more information.
# ###########################################################################
{
package TableNibbler;

use strict;
use warnings FATAL => 'all';
use English qw(-no_match_vars);
use constant PTDEBUG => $ENV{PTDEBUG} || 0;

sub new {
   my ( $class, %args ) = @_;
   my @required_args = qw(TableParser Quoter);
   foreach my $arg ( @required_args ) {
      die "I need a $arg argument" unless $args{$arg};
   }
   my $self = { %args };
   return bless $self, $class;
}

sub generate_asc_stmt {
   my ( $self, %args ) = @_;
   my @required_args = qw(tbl_struct index);
   foreach my $arg ( @required_args ) {
      die "I need a $arg argument" unless defined $args{$arg};
   }
   my ($tbl_struct, $index) = @args{@required_args};
   my @cols = $args{cols} ? @{$args{cols}} : @{$tbl_struct->{cols}};
   my $q    = $self->{Quoter};

   die "Index '$index' does not exist in table"
      unless exists $tbl_struct->{keys}->{$index};
   PTDEBUG && _d('Will ascend index', $index);  

   my @asc_cols = @{$tbl_struct->{keys}->{$index}->{cols}};
   if ( $args{asc_first} ) {
      PTDEBUG && _d('Ascending only first column');
      @asc_cols = $asc_cols[0];
   }
   elsif ( my $n = $args{n_index_cols} ) {
      $n = scalar @asc_cols if $n > @asc_cols;
      PTDEBUG && _d('Ascending only first', $n, 'columns');
      @asc_cols = @asc_cols[0..($n-1)];
   }
   PTDEBUG && _d('Will ascend columns', join(', ', @asc_cols));

   my @asc_slice;
   my %col_posn = do { my $i = 0; map { $_ => $i++ } @cols };
   foreach my $col ( @asc_cols ) {
      if ( !exists $col_posn{$col} ) {
         push @cols, $col;
         $col_posn{$col} = $#cols;
      }
      push @asc_slice, $col_posn{$col};
   }
   PTDEBUG && _d('Will ascend, in ordinal position:', join(', ', @asc_slice));

   my $asc_stmt = {
      cols  => \@cols,
      index => $index,
      where => '',
      slice => [],
      scols => [],
   };

   if ( @asc_slice ) {
      my $cmp_where;
      foreach my $cmp ( qw(< <= >= >) ) {
         $cmp_where = $self->generate_cmp_where(
            type        => $cmp,
            slice       => \@asc_slice,
            cols        => \@cols,
            quoter      => $q,
            is_nullable => $tbl_struct->{is_nullable},
            type_for    => $tbl_struct->{type_for},
         );
         $asc_stmt->{boundaries}->{$cmp} = $cmp_where->{where};
      }
      my $cmp = $args{asc_only} ? '>' : '>=';
      $asc_stmt->{where} = $asc_stmt->{boundaries}->{$cmp};
      $asc_stmt->{slice} = $cmp_where->{slice};
      $asc_stmt->{scols} = $cmp_where->{scols};
   }

   return $asc_stmt;
}

sub generate_cmp_where {
   my ( $self, %args ) = @_;
   foreach my $arg ( qw(type slice cols is_nullable) ) {
      die "I need a $arg arg" unless defined $args{$arg};
   }
   my @slice       = @{$args{slice}};
   my @cols        = @{$args{cols}};
   my $is_nullable = $args{is_nullable};
   my $type_for    = $args{type_for};
   my $type        = $args{type};
   my $q           = $self->{Quoter};

   (my $cmp = $type) =~ s/=//;

   my @r_slice;    # Resulting slice columns, by ordinal
   my @r_scols;    # Ditto, by name

   my @clauses;
   foreach my $i ( 0 .. $#slice ) {
      my @clause;

      foreach my $j ( 0 .. $i - 1 ) {
         my $ord = $slice[$j];
         my $col = $cols[$ord];
         my $quo = $q->quote($col);
         my $val = ($col && ($type_for->{$col} || '')) eq 'enum' ? "CAST(? AS UNSIGNED)" : "?";
         if ( $is_nullable->{$col} ) {
            push @clause, "(($val IS NULL AND $quo IS NULL) OR ($quo = $val))";
            push @r_slice, $ord, $ord;
            push @r_scols, $col, $col;
         }
         else {
            push @clause, "$quo = $val";
            push @r_slice, $ord;
            push @r_scols, $col;
         }
      }

      my $ord = $slice[$i];
      my $col = $cols[$ord];
      my $quo = $q->quote($col);
      my $end = $i == $#slice; # Last clause of the whole group.
      my $val = ($col && ($type_for->{$col} || '')) eq 'enum' ? "CAST(? AS UNSIGNED)" : "?";
      if ( $is_nullable->{$col} ) {
         if ( $type =~ m/=/ && $end ) {
            push @clause, "($val IS NULL OR $quo $type $val)";
         }
         elsif ( $type =~ m/>/ ) {
            push @clause, "($val IS NULL AND $quo IS NOT NULL) OR ($quo $cmp $val)";
         }
         else { # If $type =~ m/</ ) {
            push @clauses, "(($val IS NOT NULL AND $quo IS NULL) OR ($quo $cmp $val))";
         }
         push @r_slice, $ord, $ord;
         push @r_scols, $col, $col;
      }
      else {
         push @r_slice, $ord;
         push @r_scols, $col;
         push @clause, ($type =~ m/=/ && $end ? "$quo $type $val" : "$quo $cmp $val");
      }

      push @clauses, '(' . join(' AND ', @clause) . ')' if @clause;
   }
   my $result = '(' . join(' OR ', @clauses) . ')';
   my $where = {
      slice => \@r_slice,
      scols => \@r_scols,
      where => $result,
   };
   return $where;
}

sub generate_del_stmt {
   my ( $self, %args ) = @_;

   my $tbl  = $args{tbl_struct};
   my @cols = $args{cols} ? @{$args{cols}} : ();
   my $tp   = $self->{TableParser};
   my $q    = $self->{Quoter};

   my @del_cols;
   my @del_slice;

   my $index = $tp->find_best_index($tbl, $args{index});
   die "Cannot find an ascendable index in table" unless $index;

   if ( $index && $tbl->{keys}->{$index}->{is_unique}) {
      @del_cols = @{$tbl->{keys}->{$index}->{cols}};
   }
   else {
      @del_cols = @{$tbl->{cols}};
   }
   PTDEBUG && _d('Columns needed for DELETE:', join(', ', @del_cols));

   my %col_posn = do { my $i = 0; map { $_ => $i++ } @cols };
   foreach my $col ( @del_cols ) {
      if ( !exists $col_posn{$col} ) {
         push @cols, $col;
         $col_posn{$col} = $#cols;
      }
      push @del_slice, $col_posn{$col};
   }
   PTDEBUG && _d('Ordinals needed for DELETE:', join(', ', @del_slice));

   my $del_stmt = {
      cols  => \@cols,
      index => $index,
      where => '',
      slice => [],
      scols => [],
   };

   my @clauses;
   foreach my $i ( 0 .. $#del_slice ) {
      my $ord = $del_slice[$i];
      my $col = $cols[$ord];
      my $quo = $q->quote($col);
      if ( $tbl->{is_nullable}->{$col} ) {
         push @clauses, "((? IS NULL AND $quo IS NULL) OR ($quo = ?))";
         push @{$del_stmt->{slice}}, $ord, $ord;
         push @{$del_stmt->{scols}}, $col, $col;
      }
      else {
         push @clauses, "$quo = ?";
         push @{$del_stmt->{slice}}, $ord;
         push @{$del_stmt->{scols}}, $col;
      }
   }

   $del_stmt->{where} = '(' . join(' AND ', @clauses) . ')';

   return $del_stmt;
}

sub generate_ins_stmt {
   my ( $self, %args ) = @_;
   foreach my $arg ( qw(ins_tbl sel_cols) ) {
      die "I need a $arg argument" unless $args{$arg};
   }
   my $ins_tbl  = $args{ins_tbl};
   my @sel_cols = @{$args{sel_cols}};

   die "You didn't specify any SELECT columns" unless @sel_cols;

   my @ins_cols;
   my @ins_slice;
   for my $i ( 0..$#sel_cols ) {
      next unless $ins_tbl->{is_col}->{$sel_cols[$i]};
      push @ins_cols, $sel_cols[$i];
      push @ins_slice, $i;
   }

   return {
      cols  => \@ins_cols,
      slice => \@ins_slice,
   };
}

sub _d {
   my ($package, undef, $line) = caller 0;
   @_ = map { (my $temp = $_) =~ s/\n/\n# /g; $temp; }
        map { defined $_ ? $_ : 'undef' }
        @_;
   print STDERR "# $package:$line $PID ", join(' ', @_), "\n";
}

1;
}
# ###########################################################################
# End TableNibbler package
# ###########################################################################

# ###########################################################################
# MasterSlave package
# This package is a copy without comments from the original.  The original
# with comments and its test file can be found in the GitHub repository at,
#   lib/MasterSlave.pm
#   t/lib/MasterSlave.t
# See https://github.com/percona/percona-toolkit for more information.
# ###########################################################################
{
package MasterSlave;

use strict;
use warnings FATAL => 'all';
use English qw(-no_match_vars);
use constant PTDEBUG => $ENV{PTDEBUG} || 0;

sub check_recursion_method {
   my ($methods) = @_;
   if ( @$methods != 1 ) {
      if ( grep({ !m/processlist|hosts/i } @$methods)
            && $methods->[0] !~ /^dsn=/i )
      {
         die  "Invalid combination of recursion methods: "
            . join(", ", map { defined($_) ? $_ : 'undef' } @$methods) . ". "
            . "Only hosts and processlist may be combined.\n"
      }
   }
   else {
      my ($method) = @$methods;
      die "Invalid recursion method: " . ( $method || 'undef' )
         unless $method && $method =~ m/^(?:processlist$|hosts$|none$|cluster$|dsn=)/i;
   }
}

sub new {
   my ( $class, %args ) = @_;
   my @required_args = qw(OptionParser DSNParser Quoter);
   foreach my $arg ( @required_args ) {
      die "I need a $arg argument" unless $args{$arg};
   }
   my $self = {
      %args,
      replication_thread => {},
   };
   return bless $self, $class;
}

sub get_slaves {
   my ($self, %args) = @_;
   my @required_args = qw(make_cxn);
   foreach my $arg ( @required_args ) {
      die "I need a $arg argument" unless $args{$arg};
   }
   my ($make_cxn) = @args{@required_args};

   my $slaves  = [];
   my $dp      = $self->{DSNParser};
   my $methods = $self->_resolve_recursion_methods($args{dsn});

   return $slaves unless @$methods;

   if ( grep { m/processlist|hosts/i } @$methods ) {
      my @required_args = qw(dbh dsn);
      foreach my $arg ( @required_args ) {
         die "I need a $arg argument" unless $args{$arg};
      }
      my ($dbh, $dsn) = @args{@required_args};
      my $o = $self->{OptionParser};

      $self->recurse_to_slaves(
         {  dbh            => $dbh,
            dsn            => $dsn,
            slave_user     => $o->got('slave-user') ? $o->get('slave-user') : '',
            slave_password => $o->got('slave-password') ? $o->get('slave-password') : '',
            callback  => sub {
               my ( $dsn, $dbh, $level, $parent ) = @_;
               return unless $level;
               PTDEBUG && _d('Found slave:', $dp->as_string($dsn));
               my $slave_dsn = $dsn;
               if ($o->got('slave-user')) {
                  $slave_dsn->{u} = $o->get('slave-user');
                  PTDEBUG && _d("Using slave user ".$o->get('slave-user')." on ".$slave_dsn->{h}.":".$slave_dsn->{P});
               }
               if ($o->got('slave-password')) {
                  $slave_dsn->{p} = $o->get('slave-password');
                  PTDEBUG && _d("Slave password set");
               }
               push @$slaves, $make_cxn->(dsn => $slave_dsn, dbh => $dbh, parent => $parent);
               return;
            },
         }
      );
   } elsif ( $methods->[0] =~ m/^dsn=/i ) {
      (my $dsn_table_dsn = join ",", @$methods) =~ s/^dsn=//i;
      $slaves = $self->get_cxn_from_dsn_table(
         %args,
         dsn_table_dsn => $dsn_table_dsn,
      );
   }
   elsif ( $methods->[0] =~ m/none/i ) {
      PTDEBUG && _d('Not getting to slaves');
   }
   else {
      die "Unexpected recursion methods: @$methods";
   }

   return $slaves;
}

sub _resolve_recursion_methods {
   my ($self, $dsn) = @_;
   my $o = $self->{OptionParser};
   if ( $o->got('recursion-method') ) {
      return $o->get('recursion-method');
   }
   elsif ( $dsn && ($dsn->{P} || 3306) != 3306 ) {
      PTDEBUG && _d('Port number is non-standard; using only hosts method');
      return [qw(hosts)];
   }
   else {
      return $o->get('recursion-method');
   }
}

sub recurse_to_slaves {
   my ( $self, $args, $level ) = @_;
   $level ||= 0;
   my $dp = $self->{DSNParser};
   my $recurse = $args->{recurse} || $self->{OptionParser}->get('recurse');
   my $dsn = $args->{dsn};
   my $slave_user = $args->{slave_user} || '';
   my $slave_password = $args->{slave_password} || '';

   my $methods = $self->_resolve_recursion_methods($dsn);
   PTDEBUG && _d('Recursion methods:', @$methods);
   if ( lc($methods->[0]) eq 'none' ) {
      PTDEBUG && _d('Not recursing to slaves');
      return;
   }

   my $slave_dsn = $dsn;
   if ($slave_user) {
      $slave_dsn->{u} = $slave_user;
      PTDEBUG && _d("Using slave user $slave_user on ".$slave_dsn->{h}.":".$slave_dsn->{P});
   }
   if ($slave_password) {
      $slave_dsn->{p} = $slave_password;
      PTDEBUG && _d("Slave password set");
   }

   my $dbh;
   eval {
      $dbh = $args->{dbh} || $dp->get_dbh(
         $dp->get_cxn_params($slave_dsn), { AutoCommit => 1 });
      PTDEBUG && _d('Connected to', $dp->as_string($slave_dsn));
   };
   if ( $EVAL_ERROR ) {
      print STDERR "Cannot connect to ", $dp->as_string($slave_dsn), "\n"
         or die "Cannot print: $OS_ERROR";
      return;
   }

   my $sql  = 'SELECT @@SERVER_ID';
   PTDEBUG && _d($sql);
   my ($id) = $dbh->selectrow_array($sql);
   PTDEBUG && _d('Working on server ID', $id);
   my $master_thinks_i_am = $dsn->{server_id};
   if ( !defined $id
       || ( defined $master_thinks_i_am && $master_thinks_i_am != $id )
       || $args->{server_ids_seen}->{$id}++
   ) {
      PTDEBUG && _d('Server ID seen, or not what master said');
      if ( $args->{skip_callback} ) {
         $args->{skip_callback}->($dsn, $dbh, $level, $args->{parent});
      }
      return;
   }

   $args->{callback}->($dsn, $dbh, $level, $args->{parent});

   if ( !defined $recurse || $level < $recurse ) {

      my @slaves =
         grep { !$_->{master_id} || $_->{master_id} == $id } # Only my slaves.
         $self->find_slave_hosts($dp, $dbh, $dsn, $methods);

      foreach my $slave ( @slaves ) {
         PTDEBUG && _d('Recursing from',
            $dp->as_string($dsn), 'to', $dp->as_string($slave));
         $self->recurse_to_slaves(
            { %$args, dsn => $slave, dbh => undef, parent => $dsn, slave_user => $slave_user, $slave_password => $slave_password }, $level + 1 );
      }
   }
}

sub find_slave_hosts {
   my ( $self, $dsn_parser, $dbh, $dsn, $methods ) = @_;

   PTDEBUG && _d('Looking for slaves on', $dsn_parser->as_string($dsn),
      'using methods', @$methods);

   my @slaves;
   METHOD:
   foreach my $method ( @$methods ) {
      my $find_slaves = "_find_slaves_by_$method";
      PTDEBUG && _d('Finding slaves with', $find_slaves);
      @slaves = $self->$find_slaves($dsn_parser, $dbh, $dsn);
      last METHOD if @slaves;
   }

   PTDEBUG && _d('Found', scalar(@slaves), 'slaves');
   return @slaves;
}

sub _find_slaves_by_processlist {
   my ( $self, $dsn_parser, $dbh, $dsn ) = @_;
   my @connected_slaves = $self->get_connected_slaves($dbh);
   my @slaves = $self->_process_slaves_list($dsn_parser, $dsn, \@connected_slaves);
   return @slaves;
}

sub _process_slaves_list {
   my ($self, $dsn_parser, $dsn, $connected_slaves) = @_;
   my @slaves = map  {
      my $slave        = $dsn_parser->parse("h=$_", $dsn);
      $slave->{source} = 'processlist';
      $slave;
   }
   grep { $_ }
   map  {
      my ( $host ) = $_->{host} =~ m/^(.*):\d+$/;
      if ( $host eq 'localhost' ) {
         $host = '127.0.0.1'; # Replication never uses sockets.
      }
      if ($host =~ m/::/) {
          $host = '['.$host.']';
      }
      $host;
   } @$connected_slaves;

   return @slaves;
}

sub _find_slaves_by_hosts {
   my ( $self, $dsn_parser, $dbh, $dsn ) = @_;

   my @slaves;
   my $sql = 'SHOW SLAVE HOSTS';
   PTDEBUG && _d($dbh, $sql);
   @slaves = @{$dbh->selectall_arrayref($sql, { Slice => {} })};

   if ( @slaves ) {
      PTDEBUG && _d('Found some SHOW SLAVE HOSTS info');
      @slaves = map {
         my %hash;
         @hash{ map { lc $_ } keys %$_ } = values %$_;
         my $spec = "h=$hash{host},P=$hash{port}"
            . ( $hash{user} ? ",u=$hash{user}" : '')
            . ( $hash{password} ? ",p=$hash{password}" : '');
         my $dsn           = $dsn_parser->parse($spec, $dsn);
         $dsn->{server_id} = $hash{server_id};
         $dsn->{master_id} = $hash{master_id};
         $dsn->{source}    = 'hosts';
         $dsn;
      } @slaves;
   }

   return @slaves;
}

sub get_connected_slaves {
   my ( $self, $dbh ) = @_;

   my $show = "SHOW GRANTS FOR ";
   my $user = 'CURRENT_USER()';
   my $sql = $show . $user;
   PTDEBUG && _d($dbh, $sql);

   my $proc;
   eval {
      $proc = grep {
         m/ALL PRIVILEGES.*?\*\.\*|PROCESS/
      } @{$dbh->selectcol_arrayref($sql)};
   };
   if ( $EVAL_ERROR ) {

      if ( $EVAL_ERROR =~ m/no such grant defined for user/ ) {
         PTDEBUG && _d('Retrying SHOW GRANTS without host; error:',
            $EVAL_ERROR);
         ($user) = split('@', $user);
         $sql    = $show . $user;
         PTDEBUG && _d($sql);
         eval {
            $proc = grep {
               m/ALL PRIVILEGES.*?\*\.\*|PROCESS/
            } @{$dbh->selectcol_arrayref($sql)};
         };
      }

      die "Failed to $sql: $EVAL_ERROR" if $EVAL_ERROR;
   }
   if ( !$proc ) {
      die "You do not have the PROCESS privilege";
   }

   $sql = 'SHOW FULL PROCESSLIST';
   PTDEBUG && _d($dbh, $sql);
   grep { $_->{command} =~ m/Binlog Dump/i }
   map  { # Lowercase the column names
      my %hash;
      @hash{ map { lc $_ } keys %$_ } = values %$_;
      \%hash;
   }
   @{$dbh->selectall_arrayref($sql, { Slice => {} })};
}

sub is_master_of {
   my ( $self, $master, $slave ) = @_;
   my $master_status = $self->get_master_status($master)
      or die "The server specified as a master is not a master";
   my $slave_status  = $self->get_slave_status($slave)
      or die "The server specified as a slave is not a slave";
   my @connected     = $self->get_connected_slaves($master)
      or die "The server specified as a master has no connected slaves";
   my (undef, $port) = $master->selectrow_array("SHOW VARIABLES LIKE 'port'");

   if ( $port != $slave_status->{master_port} ) {
      die "The slave is connected to $slave_status->{master_port} "
         . "but the master's port is $port";
   }

   if ( !grep { $slave_status->{master_user} eq $_->{user} } @connected ) {
      die "I don't see any slave I/O thread connected with user "
         . $slave_status->{master_user};
   }

   if ( ($slave_status->{slave_io_state} || '')
      eq 'Waiting for master to send event' )
   {
      my ( $master_log_name, $master_log_num )
         = $master_status->{file} =~ m/^(.*?)\.0*([1-9][0-9]*)$/;
      my ( $slave_log_name, $slave_log_num )
         = $slave_status->{master_log_file} =~ m/^(.*?)\.0*([1-9][0-9]*)$/;
      if ( $master_log_name ne $slave_log_name
         || abs($master_log_num - $slave_log_num) > 1 )
      {
         die "The slave thinks it is reading from "
            . "$slave_status->{master_log_file},  but the "
            . "master is writing to $master_status->{file}";
      }
   }
   return 1;
}

sub get_master_dsn {
   my ( $self, $dbh, $dsn, $dsn_parser ) = @_;
   my $master = $self->get_slave_status($dbh) or return undef;
   my $spec   = "h=$master->{master_host},P=$master->{master_port}";
   return       $dsn_parser->parse($spec, $dsn);
}

sub get_slave_status {
   my ( $self, $dbh ) = @_;

   if ( !$self->{not_a_slave}->{$dbh} ) {
      my $sth = $self->{sths}->{$dbh}->{SLAVE_STATUS}
            ||= $dbh->prepare('SHOW SLAVE STATUS');
      PTDEBUG && _d($dbh, 'SHOW SLAVE STATUS');
      $sth->execute();
      my ($sss_rows) = $sth->fetchall_arrayref({}); # Show Slave Status rows

      my $ss;
      if ( $sss_rows && @$sss_rows ) {
          if (scalar @$sss_rows > 1) {
              if (!$self->{channel}) {
                  die 'This server returned more than one row for SHOW SLAVE STATUS but "channel" was not specified on the command line';
              }
              my $slave_use_channels;
              for my $row (@$sss_rows) {
                  $row = { map { lc($_) => $row->{$_} } keys %$row }; # lowercase the keys
                  if ($row->{channel_name}) {
                      $slave_use_channels = 1;
                  }
                  if ($row->{channel_name} eq $self->{channel}) {
                      $ss = $row;
                      last;
                  }
              }
              if (!$ss && $slave_use_channels) {
                 die 'This server is using replication channels but "channel" was not specified on the command line';
              }
          } else {
              if ($sss_rows->[0]->{channel_name} && $sss_rows->[0]->{channel_name} ne $self->{channel}) {
                  die 'This server is using replication channels but "channel" was not specified on the command line';
              } else {
                  $ss = $sss_rows->[0];
              }
          }

          if ( $ss && %$ss ) {
             $ss = { map { lc($_) => $ss->{$_} } keys %$ss }; # lowercase the keys
             return $ss;
          }
          if (!$ss && $self->{channel}) {
              die "Specified channel name is invalid";
          }
      }

      PTDEBUG && _d('This server returns nothing for SHOW SLAVE STATUS');
      $self->{not_a_slave}->{$dbh}++;
  }
}

sub get_master_status {
   my ( $self, $dbh ) = @_;

   if ( $self->{not_a_master}->{$dbh} ) {
      PTDEBUG && _d('Server on dbh', $dbh, 'is not a master');
      return;
   }

   my $sth = $self->{sths}->{$dbh}->{MASTER_STATUS}
         ||= $dbh->prepare('SHOW MASTER STATUS');
   PTDEBUG && _d($dbh, 'SHOW MASTER STATUS');
   $sth->execute();
   my ($ms) = @{$sth->fetchall_arrayref({})};
   PTDEBUG && _d(
      $ms ? map { "$_=" . (defined $ms->{$_} ? $ms->{$_} : '') } keys %$ms
          : '');

   if ( !$ms || scalar keys %$ms < 2 ) {
      PTDEBUG && _d('Server on dbh', $dbh, 'does not seem to be a master');
      $self->{not_a_master}->{$dbh}++;
   }

  return { map { lc($_) => $ms->{$_} } keys %$ms }; # lowercase the keys
}

sub wait_for_master {
   my ( $self, %args ) = @_;
   my @required_args = qw(master_status slave_dbh);
   foreach my $arg ( @required_args ) {
      die "I need a $arg argument" unless $args{$arg};
   }
   my ($master_status, $slave_dbh) = @args{@required_args};
   my $timeout       = $args{timeout} || 60;

   my $result;
   my $waited;
   if ( $master_status ) {
      my $slave_status;
      eval {
          $slave_status = $self->get_slave_status($slave_dbh);
      };
      if ($EVAL_ERROR) {
          return {
              result => undef,
              waited => 0,
              error  =>'Wait for master: this is a multi-master slave but "channel" was not specified on the command line',
          };
      }
      my $server_version = VersionParser->new($slave_dbh);
      my $channel_sql = $server_version > '5.6' && $self->{channel} ? ", '$self->{channel}'" : '';
      my $sql = "SELECT MASTER_POS_WAIT('$master_status->{file}', $master_status->{position}, $timeout $channel_sql)";
      PTDEBUG && _d($slave_dbh, $sql);
      my $start = time;
      ($result) = $slave_dbh->selectrow_array($sql);

      $waited = time - $start;

      PTDEBUG && _d('Result of waiting:', $result);
      PTDEBUG && _d("Waited", $waited, "seconds");
   }
   else {
      PTDEBUG && _d('Not waiting: this server is not a master');
   }

   return {
      result => $result,
      waited => $waited,
   };
}

sub stop_slave {
   my ( $self, $dbh ) = @_;
   my $sth = $self->{sths}->{$dbh}->{STOP_SLAVE}
         ||= $dbh->prepare('STOP SLAVE');
   PTDEBUG && _d($dbh, $sth->{Statement});
   $sth->execute();
}

sub start_slave {
   my ( $self, $dbh, $pos ) = @_;
   if ( $pos ) {
      my $sql = "START SLAVE UNTIL MASTER_LOG_FILE='$pos->{file}', "
              . "MASTER_LOG_POS=$pos->{position}";
      PTDEBUG && _d($dbh, $sql);
      $dbh->do($sql);
   }
   else {
      my $sth = $self->{sths}->{$dbh}->{START_SLAVE}
            ||= $dbh->prepare('START SLAVE');
      PTDEBUG && _d($dbh, $sth->{Statement});
      $sth->execute();
   }
}

sub catchup_to_master {
   my ( $self, $slave, $master, $timeout ) = @_;
   $self->stop_slave($master);
   $self->stop_slave($slave);
   my $slave_status  = $self->get_slave_status($slave);
   my $slave_pos     = $self->repl_posn($slave_status);
   my $master_status = $self->get_master_status($master);
   my $master_pos    = $self->repl_posn($master_status);
   PTDEBUG && _d('Master position:', $self->pos_to_string($master_pos),
      'Slave position:', $self->pos_to_string($slave_pos));

   my $result;
   if ( $self->pos_cmp($slave_pos, $master_pos) < 0 ) {
      PTDEBUG && _d('Waiting for slave to catch up to master');
      $self->start_slave($slave, $master_pos);

      $result = $self->wait_for_master(
            master_status => $master_status,
            slave_dbh     => $slave,
            timeout       => $timeout,
            master_status => $master_status
      );
      if ($result->{error}) {
          die $result->{error};
      }
      if ( !defined $result->{result} ) {
         $slave_status = $self->get_slave_status($slave);
         if ( !$self->slave_is_running($slave_status) ) {
            PTDEBUG && _d('Master position:',
               $self->pos_to_string($master_pos),
               'Slave position:', $self->pos_to_string($slave_pos));
            $slave_pos = $self->repl_posn($slave_status);
            if ( $self->pos_cmp($slave_pos, $master_pos) != 0 ) {
               die "MASTER_POS_WAIT() returned NULL but slave has not "
                  . "caught up to master";
            }
            PTDEBUG && _d('Slave is caught up to master and stopped');
         }
         else {
            die "Slave has not caught up to master and it is still running";
         }
      }
   }
   else {
      PTDEBUG && _d("Slave is already caught up to master");
   }

   return $result;
}

sub catchup_to_same_pos {
   my ( $self, $s1_dbh, $s2_dbh ) = @_;
   $self->stop_slave($s1_dbh);
   $self->stop_slave($s2_dbh);
   my $s1_status = $self->get_slave_status($s1_dbh);
   my $s2_status = $self->get_slave_status($s2_dbh);
   my $s1_pos    = $self->repl_posn($s1_status);
   my $s2_pos    = $self->repl_posn($s2_status);
   if ( $self->pos_cmp($s1_pos, $s2_pos) < 0 ) {
      $self->start_slave($s1_dbh, $s2_pos);
   }
   elsif ( $self->pos_cmp($s2_pos, $s1_pos) < 0 ) {
      $self->start_slave($s2_dbh, $s1_pos);
   }

   $s1_status = $self->get_slave_status($s1_dbh);
   $s2_status = $self->get_slave_status($s2_dbh);
   $s1_pos    = $self->repl_posn($s1_status);
   $s2_pos    = $self->repl_posn($s2_status);

   if ( $self->slave_is_running($s1_status)
     || $self->slave_is_running($s2_status)
     || $self->pos_cmp($s1_pos, $s2_pos) != 0)
   {
      die "The servers aren't both stopped at the same position";
   }

}

sub slave_is_running {
   my ( $self, $slave_status ) = @_;
   return ($slave_status->{slave_sql_running} || 'No') eq 'Yes';
}

sub has_slave_updates {
   my ( $self, $dbh ) = @_;
   my $sql = q{SHOW VARIABLES LIKE 'log_slave_updates'};
   PTDEBUG && _d($dbh, $sql);
   my ($name, $value) = $dbh->selectrow_array($sql);
   return $value && $value =~ m/^(1|ON)$/;
}

sub repl_posn {
   my ( $self, $status ) = @_;
   if ( exists $status->{file} && exists $status->{position} ) {
      return {
         file     => $status->{file},
         position => $status->{position},
      };
   }
   else {
      return {
         file     => $status->{relay_master_log_file},
         position => $status->{exec_master_log_pos},
      };
   }
}

sub get_slave_lag {
   my ( $self, $dbh ) = @_;
   my $stat = $self->get_slave_status($dbh);
   return unless $stat;  # server is not a slave
   return $stat->{seconds_behind_master};
}

sub pos_cmp {
   my ( $self, $a, $b ) = @_;
   return $self->pos_to_string($a) cmp $self->pos_to_string($b);
}

sub short_host {
   my ( $self, $dsn ) = @_;
   my ($host, $port);
   if ( $dsn->{master_host} ) {
      $host = $dsn->{master_host};
      $port = $dsn->{master_port};
   }
   else {
      $host = $dsn->{h};
      $port = $dsn->{P};
   }
   return ($host || '[default]') . ( ($port || 3306) == 3306 ? '' : ":$port" );
}

sub is_replication_thread {
   my ( $self, $query, %args ) = @_;
   return unless $query;

   my $type = lc($args{type} || 'all');
   die "Invalid type: $type"
      unless $type =~ m/^binlog_dump|slave_io|slave_sql|all$/i;

   my $match = 0;
   if ( $type =~ m/binlog_dump|all/i ) {
      $match = 1
         if ($query->{Command} || $query->{command} || '') eq "Binlog Dump";
   }
   if ( !$match ) {
      if ( ($query->{User} || $query->{user} || '') eq "system user" ) {
         PTDEBUG && _d("Slave replication thread");
         if ( $type ne 'all' ) {
            my $state = $query->{State} || $query->{state} || '';

            if ( $state =~ m/^init|end$/ ) {
               PTDEBUG && _d("Special state:", $state);
               $match = 1;
            }
            else {
               my ($slave_sql) = $state =~ m/
                  ^(Waiting\sfor\sthe\snext\sevent
                   |Reading\sevent\sfrom\sthe\srelay\slog
                   |Has\sread\sall\srelay\slog;\swaiting
                   |Making\stemp\sfile
                   |Waiting\sfor\sslave\smutex\son\sexit)/xi;

               $match = $type eq 'slave_sql' &&  $slave_sql ? 1
                      : $type eq 'slave_io'  && !$slave_sql ? 1
                      :                                       0;
            }
         }
         else {
            $match = 1;
         }
      }
      else {
         PTDEBUG && _d('Not system user');
      }

      if ( !defined $args{check_known_ids} || $args{check_known_ids} ) {
         my $id = $query->{Id} || $query->{id};
         if ( $match ) {
            $self->{replication_thread}->{$id} = 1;
         }
         else {
            if ( $self->{replication_thread}->{$id} ) {
               PTDEBUG && _d("Thread ID is a known replication thread ID");
               $match = 1;
            }
         }
      }
   }

   PTDEBUG && _d('Matches', $type, 'replication thread:',
      ($match ? 'yes' : 'no'), '; match:', $match);

   return $match;
}


sub get_replication_filters {
   my ( $self, %args ) = @_;
   my @required_args = qw(dbh);
   foreach my $arg ( @required_args ) {
      die "I need a $arg argument" unless $args{$arg};
   }
   my ($dbh) = @args{@required_args};

   my %filters = ();

   my $status = $self->get_master_status($dbh);
   if ( $status ) {
      map { $filters{$_} = $status->{$_} }
      grep { defined $status->{$_} && $status->{$_} ne '' }
      qw(
         binlog_do_db
         binlog_ignore_db
      );
   }

   $status = $self->get_slave_status($dbh);
   if ( $status ) {
      map { $filters{$_} = $status->{$_} }
      grep { defined $status->{$_} && $status->{$_} ne '' }
      qw(
         replicate_do_db
         replicate_ignore_db
         replicate_do_table
         replicate_ignore_table
         replicate_wild_do_table
         replicate_wild_ignore_table
      );

      my $sql = "SHOW VARIABLES LIKE 'slave_skip_errors'";
      PTDEBUG && _d($dbh, $sql);
      my $row = $dbh->selectrow_arrayref($sql);
      $filters{slave_skip_errors} = $row->[1] if $row->[1] && $row->[1] ne 'OFF';
   }

   return \%filters;
}


sub pos_to_string {
   my ( $self, $pos ) = @_;
   my $fmt  = '%s/%020d';
   return sprintf($fmt, @{$pos}{qw(file position)});
}

sub reset_known_replication_threads {
   my ( $self ) = @_;
   $self->{replication_thread} = {};
   return;
}

sub get_cxn_from_dsn_table {
   my ($self, %args) = @_;
   my @required_args = qw(dsn_table_dsn make_cxn);
   foreach my $arg ( @required_args ) {
      die "I need a $arg argument" unless $args{$arg};
   }
   my ($dsn_table_dsn, $make_cxn) = @args{@required_args};
   PTDEBUG && _d('DSN table DSN:', $dsn_table_dsn);

   my $dp = $self->{DSNParser};
   my $q  = $self->{Quoter};

   my $dsn = $dp->parse($dsn_table_dsn);
   my $dsn_table;
   if ( $dsn->{D} && $dsn->{t} ) {
      $dsn_table = $q->quote($dsn->{D}, $dsn->{t});
   }
   elsif ( $dsn->{t} && $dsn->{t} =~ m/\./ ) {
      $dsn_table = $q->quote($q->split_unquote($dsn->{t}));
   }
   else {
      die "DSN table DSN does not specify a database (D) "
        . "or a database-qualified table (t)";
   }

   my $dsn_tbl_cxn = $make_cxn->(dsn => $dsn);
   my $dbh         = $dsn_tbl_cxn->connect();
   my $sql         = "SELECT dsn FROM $dsn_table ORDER BY id";
   PTDEBUG && _d($sql);
   my $dsn_strings = $dbh->selectcol_arrayref($sql);
   my @cxn;
   if ( $dsn_strings ) {
      foreach my $dsn_string ( @$dsn_strings ) {
         PTDEBUG && _d('DSN from DSN table:', $dsn_string);
         push @cxn, $make_cxn->(dsn_string => $dsn_string);
      }
   }
   return \@cxn;
}

sub _d {
   my ($package, undef, $line) = caller 0;
   @_ = map { (my $temp = $_) =~ s/\n/\n# /g; $temp; }
        map { defined $_ ? $_ : 'undef' }
        @_;
   print STDERR "# $package:$line $PID ", join(' ', @_), "\n";
}

1;
}
# ###########################################################################
# End MasterSlave package
# ###########################################################################

# ###########################################################################
# RowChecksum package
# This package is a copy without comments from the original.  The original
# with comments and its test file can be found in the GitHub repository at,
#   lib/RowChecksum.pm
#   t/lib/RowChecksum.t
# See https://github.com/percona/percona-toolkit for more information.
# ###########################################################################
{
package RowChecksum;

use strict;
use warnings FATAL => 'all';
use English qw(-no_match_vars);
use constant PTDEBUG => $ENV{PTDEBUG} || 0;

use List::Util qw(max);
use Data::Dumper;
$Data::Dumper::Indent    = 1;
$Data::Dumper::Sortkeys  = 1;
$Data::Dumper::Quotekeys = 0;

sub new {
   my ( $class, %args ) = @_;
   foreach my $arg ( qw(OptionParser Quoter) ) {
      die "I need a $arg argument" unless defined $args{$arg};
   }
   my $self = { %args };
   return bless $self, $class;
}

sub make_row_checksum {
   my ( $self, %args ) = @_;
   my @required_args = qw(tbl);
   foreach my $arg( @required_args ) {
      die "I need a $arg argument" unless $args{$arg};
   }
   my ($tbl) = @args{@required_args};

   my $o          = $self->{OptionParser};
   my $q          = $self->{Quoter};
   my $tbl_struct = $tbl->{tbl_struct};
   my $func       = $args{func} || uc($o->get('function'));
   my $cols       = $self->get_checksum_columns(%args);

   die "all columns are excluded by --columns or --ignore-columns"
      unless @{$cols->{select}};
      
   my $query;
   if ( !$args{no_cols} ) {
      $query = join(', ',
                  map { 
                     my $col = $_;
                     if ( $col =~ m/UNIX_TIMESTAMP/ ) {
                        my ($real_col) = /^UNIX_TIMESTAMP\((.+?)\)/;
                        $col .= " AS $real_col";
                     }
                     elsif ( $col =~ m/TRIM/ ) {
                        my ($real_col) = m/TRIM\(([^\)]+)\)/;
                        $col .= " AS $real_col";
                     }
                     $col;
                  } @{$cols->{select}})
             . ', ';
   }

   if ( uc $func ne 'FNV_64' && uc $func ne 'FNV1A_64' ) {
      my $sep = $o->get('separator') || '#';
      $sep    =~ s/'//g;
      $sep  ||= '#';

      my @converted_cols;
      for my $col(@{$cols->{select}}) {
          my $colname = $col;
          $colname =~ s/`//g;
          my $type = $tbl_struct->{type_for}->{$colname} || '';
          if ($type =~ m/^(CHAR|VARCHAR|BINARY|VARBINARY|BLOB|TEXT|ENUM|SET|JSON)$/i) {
              push @converted_cols, "convert($col using utf8mb4)";
          } else {
              push @converted_cols, "$col";
          }
      }

      my @nulls = grep { $cols->{allowed}->{$_} } @{$tbl_struct->{null_cols}};
      if ( @nulls ) {
         my $bitmap = "CONCAT("
            . join(', ', map { 'ISNULL(' . $q->quote($_) . ')' } @nulls)
            . ")";
         push @converted_cols, $bitmap;
      }

      $query .= scalar @converted_cols > 1
              ? "$func(CONCAT_WS('$sep', " . join(', ', @converted_cols) . '))'
              : "$func($converted_cols[0])";
   }
   else {
      my $fnv_func = uc $func;
      $query .= "$fnv_func(" . join(', ', @{$cols->{select}}) . ')';
   }

   PTDEBUG && _d('Row checksum:', $query);
   return $query;
}

sub make_chunk_checksum {
   my ( $self, %args ) = @_;
   my @required_args = qw(tbl);
   foreach my $arg( @required_args ) {
      die "I need a $arg argument" unless $args{$arg};
   }
   if ( !$args{dbh} && !($args{func} && $args{crc_width} && $args{crc_type}) ) {
      die "I need a dbh argument"
   }
   my ($tbl) = @args{@required_args};
   my $o     = $self->{OptionParser};
   my $q     = $self->{Quoter};

   my %crc_args = $self->get_crc_args(%args);
   PTDEBUG && _d("Checksum strat:", Dumper(\%crc_args));

   my $row_checksum = $self->make_row_checksum(
      %args,
      %crc_args,
      no_cols => 1
   );
   my $crc;
   if ( $crc_args{crc_type} =~ m/int$/ ) {
      $crc = "COALESCE(LOWER(CONV(BIT_XOR(CAST($row_checksum AS UNSIGNED)), "
           . "10, 16)), 0)";
   }
   else {
      my $slices = $self->_make_xor_slices(
         row_checksum => $row_checksum,
         %crc_args,
      );
      $crc = "COALESCE(LOWER(CONCAT($slices)), 0)";
   }

   my $select = "COUNT(*) AS cnt, $crc AS crc";
   PTDEBUG && _d('Chunk checksum:', $select);
   return $select;
}

sub get_checksum_columns {
   my ($self, %args) = @_;
   my @required_args = qw(tbl);
   foreach my $arg( @required_args ) {
      die "I need a $arg argument" unless $args{$arg};
   }
   my ($tbl) = @args{@required_args};
   my $o     = $self->{OptionParser};
   my $q     = $self->{Quoter};

   my $trim            = $o->get('trim');
   my $float_precision = $o->get('float-precision');

   my $tbl_struct = $tbl->{tbl_struct};
   my $ignore_col = $o->get('ignore-columns') || {};
   my $all_cols   = $o->get('columns') || $tbl_struct->{cols};
   my %cols       = map { lc($_) => 1 } grep { !$ignore_col->{$_} } @$all_cols;
   my %seen;
   my @cols =
      map {
         my $type   = $tbl_struct->{type_for}->{$_};
         my $result = $q->quote($_);
         if ( $type eq 'timestamp' ) {
            $result = "UNIX_TIMESTAMP($result)";
         }
         elsif ( $float_precision && $type =~ m/float|double/ ) {
            $result = "ROUND($result, $float_precision)";
         }
         elsif ( $trim && $type =~ m/varchar/ ) {
            $result = "TRIM($result)";
         }
         elsif ( $type =~ m/blob|text|binary/ ) {
            $result = "CRC32($result)";
         }
         $result;
      }
      grep {
         $cols{$_} && !$seen{$_}++
      }
      @{$tbl_struct->{cols}};

   return {
      select  => \@cols,
      allowed => \%cols,
   };
}

sub get_crc_args {
   my ($self, %args) = @_;
   my $func      = $args{func}     || $self->_get_hash_func(%args);
   my $crc_width = $args{crc_width}|| $self->_get_crc_width(%args, func=>$func);
   my $crc_type  = $args{crc_type} || $self->_get_crc_type(%args, func=>$func);
   my $opt_slice; 
   if ( $args{dbh} && $crc_type !~ m/int$/ ) {
      $opt_slice = $self->_optimize_xor(%args, func=>$func);
   }

   return (
      func      => $func,
      crc_width => $crc_width,
      crc_type  => $crc_type,
      opt_slice => $opt_slice,
   );
}

sub _get_hash_func {
   my ( $self, %args ) = @_;
   my @required_args = qw(dbh);
   foreach my $arg( @required_args ) {
      die "I need a $arg argument" unless $args{$arg};
   }
   my ($dbh) = @args{@required_args};
   my $o     = $self->{OptionParser};
   my @funcs = qw(CRC32 FNV1A_64 FNV_64 MURMUR_HASH MD5 SHA1);

   if ( my $func = $o->get('function') ) {
      unshift @funcs, $func;
   }

   my $error;
   foreach my $func ( @funcs ) {
      eval {
         my $sql = "SELECT $func('test-string')";
         PTDEBUG && _d($sql);
         $args{dbh}->do($sql);
      };
      if ( $EVAL_ERROR && $EVAL_ERROR =~ m/failed: (.*?) at \S+ line/ ) {
         $error .= qq{$func cannot be used because "$1"\n};
         PTDEBUG && _d($func, 'cannot be used because', $1);
         next;
      }
      PTDEBUG && _d('Chosen hash func:', $func);
      return $func;
   }
   die($error || 'No hash functions (CRC32, MD5, etc.) are available');
}

sub _get_crc_width {
   my ( $self, %args ) = @_;
   my @required_args = qw(dbh func);
   foreach my $arg( @required_args ) {
      die "I need a $arg argument" unless $args{$arg};
   }
   my ($dbh, $func) = @args{@required_args};

   my $crc_width = 16;
   if ( uc $func ne 'FNV_64' && uc $func ne 'FNV1A_64' ) {
      eval {
         my ($val) = $dbh->selectrow_array("SELECT $func('a')");
         $crc_width = max(16, length($val));
      };
   }
   return $crc_width;
}

sub _get_crc_type {
   my ( $self, %args ) = @_;
   my @required_args = qw(dbh func);
   foreach my $arg( @required_args ) {
      die "I need a $arg argument" unless $args{$arg};
   }
   my ($dbh, $func) = @args{@required_args};

   my $type   = '';
   my $length = 0;
   my $sql    = "SELECT $func('a')";
   my $sth    = $dbh->prepare($sql);
   eval {
      $sth->execute();
      $type   = $sth->{mysql_type_name}->[0];
      $length = $sth->{mysql_length}->[0];
      PTDEBUG && _d($sql, $type, $length);
      if ( $type eq 'integer' && $length < 11 ) {
         $type = 'int';
      }
      elsif ( $type eq 'bigint' && $length < 20 ) {
         $type = 'int';
      }
   };
   $sth->finish;
   PTDEBUG && _d('crc_type:', $type, 'length:', $length);
   return $type;
}

sub _optimize_xor {
   my ( $self, %args ) = @_;
   my @required_args = qw(dbh func);
   foreach my $arg( @required_args ) {
      die "I need a $arg argument" unless $args{$arg};
   }
   my ($dbh, $func) = @args{@required_args};

   die "$func never needs BIT_XOR optimization"
      if $func =~ m/^(?:FNV1A_64|FNV_64|CRC32)$/i;

   my $opt_slice = 0;
   my $unsliced  = uc $dbh->selectall_arrayref("SELECT $func('a')")->[0]->[0];
   my $sliced    = '';
   my $start     = 1;
   my $crc_width = length($unsliced) < 16 ? 16 : length($unsliced);

   do { # Try different positions till sliced result equals non-sliced.
      PTDEBUG && _d('Trying slice', $opt_slice);
      $dbh->do(q{SET @crc := '', @cnt := 0});
      my $slices = $self->_make_xor_slices(
         row_checksum => "\@crc := $func('a')",
         crc_width    => $crc_width,
         opt_slice    => $opt_slice,
      );

      my $sql = "SELECT CONCAT($slices) AS TEST FROM (SELECT NULL) AS x";
      $sliced = ($dbh->selectrow_array($sql))[0];
      if ( $sliced ne $unsliced ) {
         PTDEBUG && _d('Slice', $opt_slice, 'does not work');
         $start += 16;
         ++$opt_slice;
      }
   } while ( $start < $crc_width && $sliced ne $unsliced );

   if ( $sliced eq $unsliced ) {
      PTDEBUG && _d('Slice', $opt_slice, 'works');
      return $opt_slice;
   }
   else {
      PTDEBUG && _d('No slice works');
      return undef;
   }
}

sub _make_xor_slices {
   my ( $self, %args ) = @_;
   my @required_args = qw(row_checksum crc_width);
   foreach my $arg( @required_args ) {
      die "I need a $arg argument" unless $args{$arg};
   }
   my ($row_checksum, $crc_width) = @args{@required_args};
   my ($opt_slice) = $args{opt_slice};

   my @slices;
   for ( my $start = 1; $start <= $crc_width; $start += 16 ) {
      my $len = $crc_width - $start + 1;
      if ( $len > 16 ) {
         $len = 16;
      }
      push @slices,
         "LPAD(CONV(BIT_XOR("
         . "CAST(CONV(SUBSTRING(\@crc, $start, $len), 16, 10) AS UNSIGNED))"
         . ", 10, 16), $len, '0')";
   }

   if ( defined $opt_slice && $opt_slice < @slices ) {
      $slices[$opt_slice] =~ s/\@crc/\@crc := $row_checksum/;
   }
   else {
      map { s/\@crc/$row_checksum/ } @slices;
   }

   return join(', ', @slices);
}

sub find_replication_differences {
   my ($self, %args) = @_;
   my @required_args = qw(dbh repl_table);
   foreach my $arg( @required_args ) {
      die "I need a $arg argument" unless $args{$arg};
   }
   my ($dbh, $repl_table) = @args{@required_args};

    
   my $tries = $self->{'OptionParser'}->get('replicate-check-retries') || 1; 
   my $diffs;
   while ($tries--) {
      my $sql
         = "SELECT CONCAT(db, '.', tbl) AS `table`, "
         . "chunk, chunk_index, lower_boundary, upper_boundary, "
         . "COALESCE(this_cnt-master_cnt, 0) AS cnt_diff, "
         . "COALESCE("
         .   "this_crc <> master_crc OR ISNULL(master_crc) <> ISNULL(this_crc), 0"
         . ") AS crc_diff, this_cnt, master_cnt, this_crc, master_crc "
         . "FROM $repl_table "
         . "WHERE (master_cnt <> this_cnt OR master_crc <> this_crc "
         .        "OR ISNULL(master_crc) <> ISNULL(this_crc)) "
         . ($args{where} ? " AND ($args{where})" : "");
      PTDEBUG && _d($sql);
      $diffs = $dbh->selectall_arrayref($sql, { Slice => {} });
      if (!@$diffs || !$tries) { # if no differences are found OR we are out of tries left...
         last;                   # get out now
      }
      sleep 1;            
   }
   return $diffs;
}

sub _d {
   my ($package, undef, $line) = caller 0;
   @_ = map { (my $temp = $_) =~ s/\n/\n# /g; $temp; }
        map { defined $_ ? $_ : 'undef' }
        @_;
   print STDERR "# $package:$line $PID ", join(' ', @_), "\n";
}

1;
}
# ###########################################################################
# End RowChecksum package
# ###########################################################################

# ###########################################################################
# NibbleIterator package
# This package is a copy without comments from the original.  The original
# with comments and its test file can be found in the GitHub repository at,
#   lib/NibbleIterator.pm
#   t/lib/NibbleIterator.t
# See https://github.com/percona/percona-toolkit for more information.
# ###########################################################################
{
package NibbleIterator;

use strict;
use warnings FATAL => 'all';
use English qw(-no_match_vars);
use constant PTDEBUG => $ENV{PTDEBUG} || 0;

use Data::Dumper;
$Data::Dumper::Indent    = 1;
$Data::Dumper::Sortkeys  = 1;
$Data::Dumper::Quotekeys = 0;

sub new {
   my ( $class, %args ) = @_;
   my @required_args = qw(Cxn tbl chunk_size OptionParser Quoter TableNibbler TableParser);
   foreach my $arg ( @required_args ) {
      die "I need a $arg argument" unless $args{$arg};
   }
   my ($cxn, $tbl, $chunk_size, $o, $q) = @args{@required_args};

   my $nibble_params = can_nibble(%args);

   my %comments = (
      bite   => "bite table",
      nibble => "nibble table",
   );
   if ( $args{comments} ) {
      map  { $comments{$_} = $args{comments}->{$_} }
      grep { defined $args{comments}->{$_}         }
      keys %{$args{comments}};
   }

   my $where      = $o->has('where') ? $o->get('where') : '';
   my $tbl_struct = $tbl->{tbl_struct};
   my $ignore_col = $o->has('ignore-columns')
                  ? ($o->get('ignore-columns') || {})
                  : {};
   my $all_cols   = $o->has('columns')
                  ? ($o->get('columns') || $tbl_struct->{cols})
                  : $tbl_struct->{cols};
   my @cols       = grep { !$ignore_col->{$_} } @$all_cols;
   my $self;
   if ( $nibble_params->{one_nibble} ) {
      my $params = _one_nibble(\%args, \@cols, $where, $tbl, \%comments);
      $self = {
         %args,
         one_nibble         => 1,
         limit              => 0,
         nibble_sql         => $params->{nibble_sql},
         explain_nibble_sql => $params->{explain_nibble_sql},
      };
   } else {
      my $params = _nibble_params($nibble_params, $tbl, \%args, \@cols, $chunk_size, $where, \%comments, $q);
      $self = {
         %args,
         index                => $params->{index},
         limit                => $params->{limit},
         first_lb_sql         => $params->{first_lb_sql},
         last_ub_sql          => $params->{last_ub_sql},
         ub_sql               => $params->{ub_sql},
         nibble_sql           => $params->{nibble_sql},
         explain_first_lb_sql => $params->{explain_first_lb_sql},
         explain_ub_sql       => $params->{explain_ub_sql},
         explain_nibble_sql   => $params->{explain_nibble_sql},
         resume_lb_sql        => $params->{resume_lb_sql},
         sql                  => $params->{sql},
      };
   }

   $self->{row_est}    = $nibble_params->{row_est},
   $self->{nibbleno}   = 0;
   $self->{have_rows}  = 0;
   $self->{rowno}      = 0;
   $self->{oktonibble} = 1;
   $self->{pause_file} = $nibble_params->{pause_file};
   $self->{sleep}      = $args{sleep} || 60;

   $self->{nibble_params} = $nibble_params;
   $self->{tbl}           = $tbl;
   $self->{args}          = \%args;
   $self->{cols}          = \@cols;
   $self->{chunk_size}    = $chunk_size;
   $self->{where}         = $where;
   $self->{comments}      = \%comments;

   return bless $self, $class;
}

sub switch_to_nibble {
    my $self = shift;
    my $params = _nibble_params($self->{nibble_params}, $self->{tbl}, $self->{args}, $self->{cols}, 
                                $self->{chunk_size}, $self->{where}, $self->{comments}, $self->{Quoter});

    $self->{one_nibble}           = 0;
    $self->{index}                = $params->{index};
    $self->{limit}                = $params->{limit};
    $self->{first_lb_sql}         = $params->{first_lb_sql};
    $self->{last_ub_sql}          = $params->{last_ub_sql};
    $self->{ub_sql}               = $params->{ub_sql};
    $self->{nibble_sql}           = $params->{nibble_sql};
    $self->{explain_first_lb_sql} = $params->{explain_first_lb_sql};
    $self->{explain_ub_sql}       = $params->{explain_ub_sql};
    $self->{explain_nibble_sql}   = $params->{explain_nibble_sql};
    $self->{resume_lb_sql}        = $params->{resume_lb_sql};
    $self->{sql}                  = $params->{sql};
    $self->_get_bounds();
    $self->_prepare_sths();
}

sub _one_nibble {
    my ($args, $cols, $where, $tbl, $comments) = @_;
    my $q        = new Quoter();

      my $nibble_sql
         = ($args->{dml} ? "$args->{dml} " : "SELECT ")
         . ($args->{select} ? $args->{select}
         : join(', ', map{ $tbl->{tbl_struct}->{type_for}->{$_} eq 'enum' ?
                                   "CAST(".$q->quote($_)." AS UNSIGNED)" : $q->quote($_) } @$cols))
         . " FROM $tbl->{name}"
         . ($where ? " WHERE $where" : '')
         . ($args->{lock_in_share_mode} ? " LOCK IN SHARE MODE" : "")
         . " /*$comments->{bite}*/";
      PTDEBUG && _d('One nibble statement:', $nibble_sql);

      my $explain_nibble_sql
         = "EXPLAIN SELECT "
         . ($args->{select} ? $args->{select}
                          : join(', ', map{ $tbl->{tbl_struct}->{type_for}->{$_} eq 'enum' 
                          ? "CAST(".$q->quote($_)." AS UNSIGNED)" : $q->quote($_) } @$cols))
         . " FROM $tbl->{name}"
         . ($where ? " WHERE $where" : '')
         . ($args->{lock_in_share_mode} ? " LOCK IN SHARE MODE" : "")
         . " /*explain $comments->{bite}*/";
      PTDEBUG && _d('Explain one nibble statement:', $explain_nibble_sql);

      return {
         one_nibble         => 1,
         limit              => 0,
         nibble_sql         => $nibble_sql,
         explain_nibble_sql => $explain_nibble_sql,
      };
}

sub _nibble_params {
      my ($nibble_params, $tbl, $args, $cols, $chunk_size, $where, $comments, $q) = @_;
      my $index      = $nibble_params->{index}; # brevity
      my $index_cols = $tbl->{tbl_struct}->{keys}->{$index}->{cols};

      my $asc = $args->{TableNibbler}->generate_asc_stmt(
         %$args,
         tbl_struct   => $tbl->{tbl_struct},
         index        => $index,
         n_index_cols => $args->{n_chunk_index_cols},
         cols         => $cols,
         asc_only     => 1,
      );
      PTDEBUG && _d('Ascend params:', Dumper($asc));

      my $force_concat_enums;


      my $from     = "$tbl->{name} FORCE INDEX(`$index`)";
      my $order_by = join(', ', map {$q->quote($_)} @{$index_cols});
      my $order_by_dec = join(' DESC,', map {$q->quote($_)} @{$index_cols});

      my $first_lb_sql
         = "SELECT /*!40001 SQL_NO_CACHE */ "
         . join(', ', map { $tbl->{tbl_struct}->{type_for}->{$_} eq 'enum' ? "CAST(".$q->quote($_)." AS UNSIGNED)" : $q->quote($_)} @{$asc->{scols}})
         . " FROM $from"
         . ($where ? " WHERE $where" : '')
         . " ORDER BY $order_by"
         . " LIMIT 1"
         . " /*first lower boundary*/";
      PTDEBUG && _d('First lower boundary statement:', $first_lb_sql);

      my $resume_lb_sql;
      if ( $args->{resume} ) {
         $resume_lb_sql
            = "SELECT /*!40001 SQL_NO_CACHE */ "
            . join(', ', map { $tbl->{tbl_struct}->{type_for}->{$_} eq 'enum' ? "CAST(".$q->quote($_)." AS UNSIGNED)" : $q->quote($_)} @{$asc->{scols}})
            . " FROM $from"
            . " WHERE " . $asc->{boundaries}->{'>'}
            . ($where ? " AND ($where)" : '')
            . " ORDER BY $order_by"
            . " LIMIT 1"
            . " /*resume lower boundary*/";
         PTDEBUG && _d('Resume lower boundary statement:', $resume_lb_sql);
      }

      my $last_ub_sql
         = "SELECT /*!40001 SQL_NO_CACHE */ "
         . join(', ', map { $tbl->{tbl_struct}->{type_for}->{$_} eq 'enum' ? "CAST(".$q->quote($_)." AS UNSIGNED)" : $q->quote($_)} @{$asc->{scols}})
         . " FROM $from"
         . ($where ? " WHERE $where" : '')
         . " ORDER BY "
         . $order_by_dec . ' DESC'
         . " LIMIT 1"
         . " /*last upper boundary*/";
      PTDEBUG && _d('Last upper boundary statement:', $last_ub_sql);

      my $ub_sql
         = "SELECT /*!40001 SQL_NO_CACHE */ "
         . join(', ', map { $tbl->{tbl_struct}->{type_for}->{$_} eq 'enum' ? "CAST(".$q->quote($_)." AS UNSIGNED)" : $q->quote($_)} @{$asc->{scols}})
         . " FROM $from"
         . " WHERE " . $asc->{boundaries}->{'>='}
                     . ($where ? " AND ($where)" : '')
         . " ORDER BY $order_by"
         . " LIMIT ?, 2"
         . " /*next chunk boundary*/";
      PTDEBUG && _d('Upper boundary statement:', $ub_sql);

      my $nibble_sql
         = ($args->{dml} ? "$args->{dml} " : "SELECT ")
         . ($args->{select} ? $args->{select}
                          : join(', ', map { $tbl->{tbl_struct}->{type_for}->{$_} eq 'enum' ? "CAST(".$q->quote($_)." AS UNSIGNED)" : $q->quote($_)} @{$asc->{cols}}))
         . " FROM $from"
         . " WHERE " . $asc->{boundaries}->{'>='}  # lower boundary
         . " AND "   . $asc->{boundaries}->{'<='}  # upper boundary
         . ($where ? " AND ($where)" : '')
         . ($args->{order_by} ? " ORDER BY $order_by" : "")
         . ($args->{lock_in_share_mode} ? " LOCK IN SHARE MODE" : "")
         . " /*$comments->{nibble}*/";
      PTDEBUG && _d('Nibble statement:', $nibble_sql);

      my $explain_nibble_sql 
         = "EXPLAIN SELECT "
         . ($args->{select} ? $args->{select}
                          : join(', ', map { $q->quote($_) } @{$asc->{cols}}))
         . " FROM $from"
         . " WHERE " . $asc->{boundaries}->{'>='}  # lower boundary
         . " AND "   . $asc->{boundaries}->{'<='}  # upper boundary
         . ($where ? " AND ($where)" : '')
         . ($args->{order_by} ? " ORDER BY $order_by" : "")
         . ($args->{lock_in_share_mode} ? " LOCK IN SHARE MODE" : "")
         . " /*explain $comments->{nibble}*/";
      PTDEBUG && _d('Explain nibble statement:', $explain_nibble_sql);

      my $limit = $chunk_size - 1;
      PTDEBUG && _d('Initial chunk size (LIMIT):', $limit);

      my $params = {
         one_nibble           => 0,
         index                => $index,
         limit                => $limit,
         first_lb_sql         => $first_lb_sql,
         last_ub_sql          => $last_ub_sql,
         ub_sql               => $ub_sql,
         nibble_sql           => $nibble_sql,
         explain_first_lb_sql => "EXPLAIN $first_lb_sql",
         explain_ub_sql       => "EXPLAIN $ub_sql",
         explain_nibble_sql   => $explain_nibble_sql,
         resume_lb_sql        => $resume_lb_sql,
         sql                  => {
            columns    => $asc->{scols},
            from       => $from,
            where      => $where,
            boundaries => $asc->{boundaries},
            order_by   => $order_by,
         },
      };
      return $params;
}

sub next {
   my ($self) = @_;

   if ( !$self->{oktonibble} ) {
      PTDEBUG && _d('Not ok to nibble');
      return;
   }

   my %callback_args = (
      Cxn            => $self->{Cxn},
      tbl            => $self->{tbl},
      NibbleIterator => $self,
   );

   if ($self->{nibbleno} == 0) {
      $self->_prepare_sths();
      $self->_get_bounds();
      if ( my $callback = $self->{callbacks}->{init} ) {
         $self->{oktonibble} = $callback->(%callback_args);
         PTDEBUG && _d('init callback returned', $self->{oktonibble});
         if ( !$self->{oktonibble} ) {
            $self->{no_more_boundaries} = 1;
            return;
         }
      }
      if ( !$self->{one_nibble} && !$self->{first_lower} ) {
         PTDEBUG && _d('No first lower boundary, table must be empty');
         $self->{no_more_boundaries} = 1;
         return;
      }
   }

   NIBBLE:
   while ( $self->{have_rows} || $self->_next_boundaries() ) {
      if ($self->{pause_file}) {
         while(-f $self->{pause_file}) {
            print "Sleeping $self->{sleep} seconds because $self->{pause_file} exists\n";
            my $dbh = $self->{Cxn}->dbh();
            if ( !$dbh || !$dbh->ping() ) {
               eval { $dbh = $self->{Cxn}->connect() }; # connect or die trying
               if ( $EVAL_ERROR ) {
                  chomp $EVAL_ERROR;
                  die "Lost connection to " . $self->{Cxn}->name() . " while waiting for "
                  . "replica lag ($EVAL_ERROR)\n";
               }
            }
            $dbh->do("SELECT 'nibble iterator keepalive'");
            sleep($self->{sleep});
         }
      }
  
      if ( !$self->{have_rows} ) {
         $self->{nibbleno}++;
         PTDEBUG && _d('Nibble:', $self->{nibble_sth}->{Statement}, 'params:',
            join(', ', (@{$self->{lower} || []}, @{$self->{upper} || []})));
         if ( my $callback = $self->{callbacks}->{exec_nibble} ) {
            $self->{have_rows} = $callback->(%callback_args);
         }
         else {
            $self->{nibble_sth}->execute(@{$self->{lower}}, @{$self->{upper}});
            $self->{have_rows} = $self->{nibble_sth}->rows();
         }
         PTDEBUG && _d($self->{have_rows}, 'rows in nibble', $self->{nibbleno});
      }

      if ( $self->{have_rows} ) {
         my $row = $self->{nibble_sth}->fetchrow_arrayref();
         if ( $row ) {
            $self->{rowno}++;
            PTDEBUG && _d('Row', $self->{rowno}, 'in nibble',$self->{nibbleno});
            return [ @$row ];
         }
      }

      PTDEBUG && _d('No rows in nibble or nibble skipped');
      if ( my $callback = $self->{callbacks}->{after_nibble} ) {
         $callback->(%callback_args);
      }
      $self->{rowno}     = 0;
      $self->{have_rows} = 0;
      
   }

   PTDEBUG && _d('Done nibbling');
   if ( my $callback = $self->{callbacks}->{done} ) {
      $callback->(%callback_args);
   }

   return;
}

sub nibble_number {
   my ($self) = @_;
   return $self->{nibbleno};
}

sub set_nibble_number {
   my ($self, $n) = @_;
   die "I need a number" unless $n;
   $self->{nibbleno} = $n;
   PTDEBUG && _d('Set new nibble number:', $n);
   return;
}

sub nibble_index {
   my ($self) = @_;
   return $self->{index};
}

sub statements {
   my ($self) = @_;
   return {
      explain_first_lower_boundary => $self->{explain_first_lb_sth},
      nibble                       => $self->{nibble_sth},
      explain_nibble               => $self->{explain_nibble_sth},
      upper_boundary               => $self->{ub_sth},
      explain_upper_boundary       => $self->{explain_ub_sth},
   }
}

sub boundaries {
   my ($self) = @_;
   return {
      first_lower => $self->{first_lower},
      lower       => $self->{lower},
      upper       => $self->{upper},
      next_lower  => $self->{next_lower},
      last_upper  => $self->{last_upper},
   };
}

sub set_boundary {
   my ($self, $boundary, $values) = @_;
   die "I need a boundary parameter"
      unless $boundary;
   die "Invalid boundary: $boundary"
      unless $boundary =~ m/^(?:lower|upper|next_lower|last_upper)$/;
   die "I need a values arrayref parameter"
      unless $values && ref $values eq 'ARRAY';
   $self->{$boundary} = $values;
   PTDEBUG && _d('Set new', $boundary, 'boundary:', Dumper($values));
   return;
}

sub one_nibble {
   my ($self) = @_;
   return $self->{one_nibble};
}

sub limit {
   my ($self) = @_;
   return $self->{limit};
}

sub set_chunk_size {
   my ($self, $limit) = @_;
   return if $self->{one_nibble};
   die "Chunk size must be > 0" unless $limit;
   $self->{limit} = $limit - 1;
   PTDEBUG && _d('Set new chunk size (LIMIT):', $limit);
   return;
}

sub sql {
   my ($self) = @_;
   return $self->{sql};
}

sub more_boundaries {
   my ($self) = @_;
   return !$self->{no_more_boundaries};
}

sub row_estimate {
   my ($self) = @_;
   return $self->{row_est};
}

sub can_nibble {
   my (%args) = @_;
   my @required_args = qw(Cxn tbl chunk_size OptionParser TableParser);
   foreach my $arg ( @required_args ) {
      die "I need a $arg argument" unless $args{$arg};
   }
   my ($cxn, $tbl, $chunk_size, $o) = @args{@required_args};

   my $where = $o->has('where') ? $o->get('where') : '';

   my ($row_est, $mysql_index) = get_row_estimate(
      Cxn   => $cxn,
      tbl   => $tbl,
      where => $where,
   );

   if ( !$where ) {
      $mysql_index = undef;
   }

   my $chunk_size_limit = $o->get('chunk-size-limit') || 1;
   my $one_nibble = !defined $args{one_nibble} || $args{one_nibble}
                  ? $row_est <= $chunk_size * $chunk_size_limit
                  : 0;
   PTDEBUG && _d('One nibble:', $one_nibble ? 'yes' : 'no');

   if ( $args{resume}
        && !defined $args{resume}->{lower_boundary}
        && !defined $args{resume}->{upper_boundary} ) {
      PTDEBUG && _d('Resuming from one nibble table');
      $one_nibble = 1;
   }

   my $index = _find_best_index(%args, mysql_index => $mysql_index);
   if ( !$index && !$one_nibble ) {
      die "There is no good index and the table is oversized.";
   }

   my $pause_file = ($o->has('pause-file') && $o->get('pause-file')) || undef;
   
   return {
      row_est     => $row_est,      # nibble about this many rows
      index       => $index,        # using this index
      one_nibble  => $one_nibble,   # if the table fits in one nibble/chunk
      pause_file  => $pause_file,
   };
}

sub _find_best_index {
   my (%args) = @_;
   my @required_args = qw(Cxn tbl TableParser);
   my ($cxn, $tbl, $tp) = @args{@required_args};
   my $tbl_struct = $tbl->{tbl_struct};
   my $indexes    = $tbl_struct->{keys};

   my $best_index;
   my $want_index = $args{chunk_index};
   if ( $want_index ) {
      PTDEBUG && _d('User wants to use index', $want_index);
      if ( !exists $indexes->{$want_index} ) {
         PTDEBUG && _d('Cannot use user index because it does not exist');
         $want_index = undef;
      } else {
         $best_index = $want_index;
      }
   }

   if ( !$best_index && !$want_index && $args{mysql_index} ) {
      PTDEBUG && _d('MySQL wants to use index', $args{mysql_index});
      $want_index = $args{mysql_index};
   }


   my @possible_indexes;
   if ( !$best_index && $want_index ) {
      if ( $indexes->{$want_index}->{is_unique} ) {
         PTDEBUG && _d('Will use wanted index');
         $best_index = $want_index;
      }
      else {
         PTDEBUG && _d('Wanted index is a possible index');
         push @possible_indexes, $want_index;
      }
   }
   
   if (!$best_index) {
      PTDEBUG && _d('Auto-selecting best index');
      foreach my $index ( $tp->sort_indexes($tbl_struct) ) {
         if ( $index eq 'PRIMARY' || $indexes->{$index}->{is_unique} ) {
            $best_index = $index;
            last;
         }
         else {
            push @possible_indexes, $index;
         }
      }
   }

   if ( !$best_index && @possible_indexes ) {
      PTDEBUG && _d('No PRIMARY or unique indexes;',
         'will use index with highest cardinality');
      foreach my $index ( @possible_indexes ) {
         $indexes->{$index}->{cardinality} = _get_index_cardinality(
            %args,
            index => $index,
         );
      }
      @possible_indexes = sort {
         my $cmp
            = $indexes->{$b}->{cardinality} <=> $indexes->{$a}->{cardinality};
         if ( $cmp == 0 ) {
            $cmp = scalar @{$indexes->{$b}->{cols}}
               <=> scalar @{$indexes->{$a}->{cols}};
         }
         $cmp;
      } @possible_indexes;
      $best_index = $possible_indexes[0];
   }

   PTDEBUG && _d('Best index:', $best_index);
   return $best_index;
}

sub _get_index_cardinality {
   my (%args) = @_;
   my @required_args = qw(Cxn tbl index);
   my ($cxn, $tbl, $index) = @args{@required_args};

   my $sql = "SHOW INDEXES FROM $tbl->{name} "
           . "WHERE Key_name = '$index'";
   PTDEBUG && _d($sql);
   my $cardinality = 1;
   my $dbh         = $cxn->dbh();
   my $key_name    = $dbh && ($dbh->{FetchHashKeyName} || '') eq 'NAME_lc'
                   ? 'key_name'
                   : 'Key_name';
   my $rows = $dbh->selectall_hashref($sql, $key_name);
   foreach my $row ( values %$rows ) {
      $cardinality *= $row->{cardinality} if $row->{cardinality};
   }
   PTDEBUG && _d('Index', $index, 'cardinality:', $cardinality);
   return $cardinality;
}

sub get_row_estimate {
   my (%args) = @_;
   my @required_args = qw(Cxn tbl);
   foreach my $arg ( @required_args ) {
      die "I need a $arg argument" unless $args{$arg};
   }
   my ($cxn, $tbl) = @args{@required_args};

   my $sql = "EXPLAIN SELECT * FROM $tbl->{name} "
           . "WHERE " . ($args{where} || '1=1');
   PTDEBUG && _d($sql);
   my $expl = $cxn->dbh()->selectrow_hashref($sql);
   PTDEBUG && _d(Dumper($expl));
   my $mysql_index = $expl->{key} || '';
   if ( $mysql_index ne 'PRIMARY' ) {
      $mysql_index = lc($mysql_index);
   }
   return ($expl->{rows} || 0), $mysql_index;
}

sub _prepare_sths {
   my ($self) = @_;
   PTDEBUG && _d('Preparing statement handles');

   my $dbh = $self->{Cxn}->dbh();

   $self->{nibble_sth}         = $dbh->prepare($self->{nibble_sql});
   $self->{explain_nibble_sth} = $dbh->prepare($self->{explain_nibble_sql});

   if ( !$self->{one_nibble} ) {
      $self->{explain_first_lb_sth} = $dbh->prepare($self->{explain_first_lb_sql});
      $self->{ub_sth}               = $dbh->prepare($self->{ub_sql});
      $self->{explain_ub_sth}       = $dbh->prepare($self->{explain_ub_sql});
   }

   return;
}

sub _get_bounds { 
   my ($self) = @_;

   if ( $self->{one_nibble} ) {
      if ( $self->{resume} ) {
         $self->{no_more_boundaries} = 1;
      }
      return;
   }

   my $dbh = $self->{Cxn}->dbh();

   $self->{first_lower} = $dbh->selectrow_arrayref($self->{first_lb_sql});
   PTDEBUG && _d('First lower boundary:', Dumper($self->{first_lower}));  

   if ( my $nibble = $self->{resume} ) {
      if (    defined $nibble->{lower_boundary}
           && defined $nibble->{upper_boundary} ) {
         my $sth = $dbh->prepare($self->{resume_lb_sql});
         my @ub = $self->{Quoter}->deserialize_list($nibble->{upper_boundary});
         PTDEBUG && _d($sth->{Statement}, 'params:', @ub);
         $sth->execute(@ub);
         $self->{next_lower} = $sth->fetchrow_arrayref();
         $sth->finish();
      }
   }
   else {
      $self->{next_lower}  = $self->{first_lower};   
   }
   PTDEBUG && _d('Next lower boundary:', Dumper($self->{next_lower}));  

   if ( !$self->{next_lower} ) {
      PTDEBUG && _d('At end of table, or no more boundaries to resume');
      $self->{no_more_boundaries} = 1;

      $self->{last_upper} = $dbh->selectrow_arrayref($self->{last_ub_sql});
      PTDEBUG && _d('Last upper boundary:', Dumper($self->{last_upper}));
   }

   return;
}

sub _next_boundaries {
   my ($self) = @_;

   if ( $self->{no_more_boundaries} ) {
      PTDEBUG && _d('No more boundaries');
      return; # stop nibbling
   }

   if ( $self->{one_nibble} ) {
      $self->{lower} = $self->{upper} = [];
      $self->{no_more_boundaries} = 1;  # for next call
      return 1; # continue nibbling
   }



   if ( $self->identical_boundaries($self->{lower}, $self->{next_lower}) ) {
      PTDEBUG && _d('Infinite loop detected');
      my $tbl     = $self->{tbl};
      my $index   = $tbl->{tbl_struct}->{keys}->{$self->{index}};
      my $n_cols  = scalar @{$index->{cols}};
      my $chunkno = $self->{nibbleno};

      die "Possible infinite loop detected!  "
         . "The lower boundary for chunk $chunkno is "
         . "<" . join(', ', @{$self->{lower}}) . "> and the lower "
         . "boundary for chunk " . ($chunkno + 1) . " is also "
         . "<" . join(', ', @{$self->{next_lower}}) . ">.  "
         . "This usually happens when using a non-unique single "
         . "column index.  The current chunk index for table "
         . "$tbl->{db}.$tbl->{tbl} is $self->{index} which is"
         . ($index->{is_unique} ? '' : ' not') . " unique and covers "
         . ($n_cols > 1 ? "$n_cols columns" : "1 column") . ".\n";
   }
   $self->{lower} = $self->{next_lower};

   if ( my $callback = $self->{callbacks}->{next_boundaries} ) {
      my $oktonibble = $callback->(
         Cxn            => $self->{Cxn},
         tbl            => $self->{tbl},
         NibbleIterator => $self,
      );
      PTDEBUG && _d('next_boundaries callback returned', $oktonibble);
      if ( !$oktonibble ) {
         $self->{no_more_boundaries} = 1;
         return; # stop nibbling
      }
   }


   PTDEBUG && _d($self->{ub_sth}->{Statement}, 'params:',
      join(', ', @{$self->{lower}}), $self->{limit});
   $self->{ub_sth}->execute(@{$self->{lower}}, $self->{limit});
   my $boundary = $self->{ub_sth}->fetchall_arrayref();
   PTDEBUG && _d('Next boundary:', Dumper($boundary));
   if ( $boundary && @$boundary ) {
      $self->{upper} = $boundary->[0];

      if ( $boundary->[1] ) {
         $self->{next_lower} = $boundary->[1];
      }
      else {
         PTDEBUG && _d('End of table boundary:', Dumper($boundary->[0]));
         $self->{no_more_boundaries} = 1;  # for next call

         $self->{last_upper} = $boundary->[0];
      }
   }
   else {
      my $dbh = $self->{Cxn}->dbh();
      $self->{upper} = $dbh->selectrow_arrayref($self->{last_ub_sql});
      PTDEBUG && _d('Last upper boundary:', Dumper($self->{upper}));
      $self->{no_more_boundaries} = 1;  # for next call
      
      $self->{last_upper} = $self->{upper};
   }
   $self->{ub_sth}->finish();

   return 1; # continue nibbling
}

sub identical_boundaries {
   my ($self, $b1, $b2) = @_;

   return 0 if ($b1 && !$b2) || (!$b1 && $b2);

   return 1 if !$b1 && !$b2;

   die "Boundaries have different numbers of values"
      if scalar @$b1 != scalar @$b2;  # shouldn't happen
   my $n_vals = scalar @$b1;
   for my $i ( 0..($n_vals-1) ) {
      return 0 if ($b1->[$i] || '') ne ($b2->[$i] || ''); # diff
   }
   return 1;
}

sub DESTROY {
   my ( $self ) = @_;
   foreach my $key ( keys %$self ) {
      if ( $key =~ m/_sth$/ ) {
         PTDEBUG && _d('Finish', $key);
         $self->{$key}->finish();
      }
   }
   return;
}

sub _d {
   my ($package, undef, $line) = caller 0;
   @_ = map { (my $temp = $_) =~ s/\n/\n# /g; $temp; }
        map { defined $_ ? $_ : 'undef' }
        @_;
   print STDERR "# $package:$line $PID ", join(' ', @_), "\n";
}

1;
}
# ###########################################################################
# End NibbleIterator package
# ###########################################################################

# ###########################################################################
# OobNibbleIterator package
# This package is a copy without comments from the original.  The original
# with comments and its test file can be found in the GitHub repository at,
#   lib/OobNibbleIterator.pm
#   t/lib/OobNibbleIterator.t
# See https://github.com/percona/percona-toolkit for more information.
# ###########################################################################
{
package OobNibbleIterator;
use base 'NibbleIterator';

use strict;
use warnings FATAL => 'all';
use English qw(-no_match_vars);
use constant PTDEBUG => $ENV{PTDEBUG} || 0;

use Data::Dumper;
$Data::Dumper::Indent    = 1;
$Data::Dumper::Sortkeys  = 1;
$Data::Dumper::Quotekeys = 0;

sub new {
   my ( $class, %args ) = @_;
   my @required_args = qw();
   foreach my $arg ( @required_args ) {
      die "I need a $arg argument" unless $args{$arg};
   }

   my $self = $class->SUPER::new(%args);

   my $q     = $self->{Quoter};
   my $o     = $self->{OptionParser};
   my $where = $o->has('where') ? $o->get('where') : undef;

   if ( !$self->one_nibble() ) {
      my $head_sql
         = ($args{past_dml} || "SELECT ")
         . ($args{past_select}
            || join(', ', map { $q->quote($_) } @{$self->{sql}->{columns}}))
         . " FROM "  . $self->{sql}->{from};

      my $tail_sql
         = ($where ? " AND ($where)" : '')
         . " ORDER BY " . $self->{sql}->{order_by};

      my $past_lower_sql
         = $head_sql
         . " WHERE " . $self->{sql}->{boundaries}->{'<'}
         . $tail_sql
         . " /*past lower chunk*/";
      PTDEBUG && _d('Past lower statement:', $past_lower_sql);

      my $explain_past_lower_sql
         = "EXPLAIN SELECT "
         . ($args{past_select}
            || join(', ', map { $q->quote($_) } @{$self->{sql}->{columns}}))
         . " FROM "  . $self->{sql}->{from}
         . " WHERE " . $self->{sql}->{boundaries}->{'<'}
         . $tail_sql
         . " /*explain past lower chunk*/";
      PTDEBUG && _d('Past lower statement:', $explain_past_lower_sql);

      my $past_upper_sql
         = $head_sql
         . " WHERE " . $self->{sql}->{boundaries}->{'>'}
         . $tail_sql
         . " /*past upper chunk*/";
      PTDEBUG && _d('Past upper statement:', $past_upper_sql);
      
      my $explain_past_upper_sql
         = "EXPLAIN SELECT "
         . ($args{past_select}
            || join(', ', map { $q->quote($_) } @{$self->{sql}->{columns}}))
         . " FROM "  . $self->{sql}->{from}
         . " WHERE " . $self->{sql}->{boundaries}->{'>'}
         . $tail_sql
         . " /*explain past upper chunk*/";
      PTDEBUG && _d('Past upper statement:', $explain_past_upper_sql);

      $self->{past_lower_sql}         = $past_lower_sql;
      $self->{past_upper_sql}         = $past_upper_sql;
      $self->{explain_past_lower_sql} = $explain_past_lower_sql;
      $self->{explain_past_upper_sql} = $explain_past_upper_sql;

      $self->{past_nibbles} = [qw(lower upper)];
      if ( my $nibble = $args{resume} ) {
         if (    !defined $nibble->{lower_boundary}
              || !defined $nibble->{upper_boundary} ) {
            $self->{past_nibbles} = !defined $nibble->{lower_boundary}
                                  ? ['upper']
                                  : [];
         }
      }
      PTDEBUG && _d('Nibble past', @{$self->{past_nibbles}});

   } # not one nibble

   return bless $self, $class;
}

sub more_boundaries {
   my ($self) = @_;
   return $self->SUPER::more_boundaries() if $self->{one_nibble};
   return scalar @{$self->{past_nibbles}} ? 1 : 0;
}

sub statements {
   my ($self) = @_;

   my $sths = $self->SUPER::statements();

   $sths->{past_lower_boundary} = $self->{past_lower_sth};
   $sths->{past_upper_boundary} = $self->{past_upper_sth};

   return $sths;
}

sub _prepare_sths {
   my ($self) = @_;
   PTDEBUG && _d('Preparing out-of-bound statement handles');

   if ( !$self->{one_nibble} ) {
      my $dbh = $self->{Cxn}->dbh();
      $self->{past_lower_sth}         = $dbh->prepare($self->{past_lower_sql});
      $self->{past_upper_sth}         = $dbh->prepare($self->{past_upper_sql});
      $self->{explain_past_lower_sth} = $dbh->prepare($self->{explain_past_lower_sql});
      $self->{explain_past_upper_sth} = $dbh->prepare($self->{explain_past_upper_sql});
   }

   return $self->SUPER::_prepare_sths();
}

sub _next_boundaries {
   my ($self) = @_;

   return $self->SUPER::_next_boundaries() unless $self->{no_more_boundaries};

   if ( my $past = shift @{$self->{past_nibbles}} ) {
      if ( $past eq 'lower' ) {
         PTDEBUG && _d('Nibbling values below lower boundary');
         $self->{nibble_sth}         = $self->{past_lower_sth};
         $self->{explain_nibble_sth} = $self->{explain_past_lower_sth};
         $self->{lower}              = [];
         $self->{upper}              = $self->boundaries()->{first_lower};
         $self->{next_lower}         = undef;
      }
      elsif ( $past eq 'upper' ) {
         PTDEBUG && _d('Nibbling values above upper boundary');
         $self->{nibble_sth}         = $self->{past_upper_sth};
         $self->{explain_nibble_sth} = $self->{explain_past_upper_sth};
         $self->{lower}              = $self->boundaries()->{last_upper};
         $self->{upper}              = [];
         $self->{next_lower}         = undef;
      }
      else {
         die "Invalid past nibble: $past";
      }
      return 1; # continue nibbling
   }

   PTDEBUG && _d('Done nibbling past boundaries');
   return; # stop nibbling
}

sub DESTROY {
   my ( $self ) = @_;
   foreach my $key ( keys %$self ) {
      if ( $key =~ m/_sth$/ ) {
         PTDEBUG && _d('Finish', $key);
         $self->{$key}->finish();
      }
   }
   return;
}

sub _d {
   my ($package, undef, $line) = caller 0;
   @_ = map { (my $temp = $_) =~ s/\n/\n# /g; $temp; }
        map { defined $_ ? $_ : 'undef' }
        @_;
   print STDERR "# $package:$line $PID ", join(' ', @_), "\n";
}

1;
}
# ###########################################################################
# End OobNibbleIterator package
# ###########################################################################

# ###########################################################################
# Daemon package
# This package is a copy without comments from the original.  The original
# with comments and its test file can be found in the GitHub repository at,
#   lib/Daemon.pm
#   t/lib/Daemon.t
# See https://github.com/percona/percona-toolkit for more information.
# ###########################################################################
{
package Daemon;

use strict;
use warnings FATAL => 'all';
use English qw(-no_match_vars);

use constant PTDEBUG => $ENV{PTDEBUG} || 0;

use POSIX qw(setsid);
use Fcntl qw(:DEFAULT);

sub new {
   my ($class, %args) = @_;
   my $self = {
      log_file       => $args{log_file},
      pid_file       => $args{pid_file},
      daemonize      => $args{daemonize},
      force_log_file => $args{force_log_file},
      parent_exit    => $args{parent_exit},
      pid_file_owner => 0,
   };
   return bless $self, $class;
}

sub run {
   my ($self) = @_;

   my $daemonize      = $self->{daemonize};
   my $pid_file       = $self->{pid_file};
   my $log_file       = $self->{log_file};
   my $force_log_file = $self->{force_log_file};
   my $parent_exit    = $self->{parent_exit};

   PTDEBUG && _d('Starting daemon');

   if ( $pid_file ) {
      eval {
         $self->_make_pid_file(
            pid      => $PID,  # parent's pid
            pid_file => $pid_file,
         );
      };
      die "$EVAL_ERROR\n" if $EVAL_ERROR;
      if ( !$daemonize ) {
         $self->{pid_file_owner} = $PID;  # parent's pid
      }
   }

   if ( $daemonize ) {
      defined (my $child_pid = fork()) or die "Cannot fork: $OS_ERROR";
      if ( $child_pid ) {
         PTDEBUG && _d('Forked child', $child_pid);
         $parent_exit->($child_pid) if $parent_exit;
         exit 0;
      }
 
      POSIX::setsid() or die "Cannot start a new session: $OS_ERROR";
      chdir '/'       or die "Cannot chdir to /: $OS_ERROR";

      if ( $pid_file ) {
         $self->_update_pid_file(
            pid      => $PID,  # child's pid
            pid_file => $pid_file,
         );
         $self->{pid_file_owner} = $PID;
      }
   }

   if ( $daemonize || $force_log_file ) {
      PTDEBUG && _d('Redirecting STDIN to /dev/null');
      close STDIN;
      open  STDIN, '/dev/null'
         or die "Cannot reopen STDIN to /dev/null: $OS_ERROR";
      if ( $log_file ) {
         PTDEBUG && _d('Redirecting STDOUT and STDERR to', $log_file);
         close STDOUT;
         open  STDOUT, '>>', $log_file
            or die "Cannot open log file $log_file: $OS_ERROR";

         close STDERR;
         open  STDERR, ">&STDOUT"
            or die "Cannot dupe STDERR to STDOUT: $OS_ERROR"; 
      }
      else {
         if ( -t STDOUT ) {
            PTDEBUG && _d('No log file and STDOUT is a terminal;',
               'redirecting to /dev/null');
            close STDOUT;
            open  STDOUT, '>', '/dev/null'
               or die "Cannot reopen STDOUT to /dev/null: $OS_ERROR";
         }
         if ( -t STDERR ) {
            PTDEBUG && _d('No log file and STDERR is a terminal;',
               'redirecting to /dev/null');
            close STDERR;
            open  STDERR, '>', '/dev/null'
               or die "Cannot reopen STDERR to /dev/null: $OS_ERROR";
         }
      }

      $OUTPUT_AUTOFLUSH = 1;
   }

   PTDEBUG && _d('Daemon running');
   return;
}

sub _make_pid_file {
   my ($self, %args) = @_;
   my @required_args = qw(pid pid_file);
   foreach my $arg ( @required_args ) {
      die "I need a $arg argument" unless $args{$arg};
   };
   my $pid      = $args{pid};
   my $pid_file = $args{pid_file};

   eval {
      sysopen(PID_FH, $pid_file, O_RDWR|O_CREAT|O_EXCL) or die $OS_ERROR;
      print PID_FH $PID, "\n";
      close PID_FH; 
   };
   if ( my $e = $EVAL_ERROR ) {
      if ( $e =~ m/file exists/i ) {
         my $old_pid = $self->_check_pid_file(
            pid_file => $pid_file,
            pid      => $PID,
         );
         if ( $old_pid ) {
            warn "Overwriting PID file $pid_file because PID $old_pid "
               . "is not running.\n";
         }
         $self->_update_pid_file(
            pid      => $PID,
            pid_file => $pid_file
         );
      }
      else {
         die "Error creating PID file $pid_file: $e\n";
      }
   }

   return;
}

sub _check_pid_file {
   my ($self, %args) = @_;
   my @required_args = qw(pid_file pid);
   foreach my $arg ( @required_args ) {
      die "I need a $arg argument" unless $args{$arg};
   };
   my $pid_file = $args{pid_file};
   my $pid      = $args{pid};

   PTDEBUG && _d('Checking if PID in', $pid_file, 'is running');

   if ( ! -f $pid_file ) {
      PTDEBUG && _d('PID file', $pid_file, 'does not exist');
      return;
   }

   open my $fh, '<', $pid_file
      or die "Error opening $pid_file: $OS_ERROR";
   my $existing_pid = do { local $/; <$fh> };
   chomp($existing_pid) if $existing_pid;
   close $fh
      or die "Error closing $pid_file: $OS_ERROR";

   if ( $existing_pid ) {
      if ( $existing_pid == $pid ) {
         warn "The current PID $pid already holds the PID file $pid_file\n";
         return;
      }
      else {
         PTDEBUG && _d('Checking if PID', $existing_pid, 'is running');
         my $pid_is_alive = kill 0, $existing_pid;
         if ( $pid_is_alive ) {
            die "PID file $pid_file exists and PID $existing_pid is running\n";
         }
      }
   }
   else {
      die "PID file $pid_file exists but it is empty.  Remove the file "
         . "if the process is no longer running.\n";
   }

   return $existing_pid;
}

sub _update_pid_file {
   my ($self, %args) = @_;
   my @required_args = qw(pid pid_file);
   foreach my $arg ( @required_args ) {
      die "I need a $arg argument" unless $args{$arg};
   };
   my $pid      = $args{pid};
   my $pid_file = $args{pid_file};

   open my $fh, '>', $pid_file
      or die "Cannot open $pid_file: $OS_ERROR";
   print { $fh } $pid, "\n"
      or die "Cannot print to $pid_file: $OS_ERROR";
   close $fh
      or warn "Cannot close $pid_file: $OS_ERROR";

   return;
}

sub remove_pid_file {
   my ($self, $pid_file) = @_;
   $pid_file ||= $self->{pid_file};
   if ( $pid_file && -f $pid_file ) {
      unlink $self->{pid_file}
         or warn "Cannot remove PID file $pid_file: $OS_ERROR";
      PTDEBUG && _d('Removed PID file');
   }
   else {
      PTDEBUG && _d('No PID to remove');
   }
   return;
}

sub DESTROY {
   my ($self) = @_;

   if ( $self->{pid_file_owner} == $PID ) {
      $self->remove_pid_file();
   }

   return;
}

sub _d {
   my ($package, undef, $line) = caller 0;
   @_ = map { (my $temp = $_) =~ s/\n/\n# /g; $temp; }
        map { defined $_ ? $_ : 'undef' }
        @_;
   print STDERR "# $package:$line $PID ", join(' ', @_), "\n";
}

1;
}
# ###########################################################################
# End Daemon package
# ###########################################################################

# ###########################################################################
# SchemaIterator package
# This package is a copy without comments from the original.  The original
# with comments and its test file can be found in the GitHub repository at,
#   lib/SchemaIterator.pm
#   t/lib/SchemaIterator.t
# See https://github.com/percona/percona-toolkit for more information.
# ###########################################################################
{
package SchemaIterator;

use strict;
use warnings FATAL => 'all';
use English qw(-no_match_vars);
use constant PTDEBUG => $ENV{PTDEBUG} || 0;

use Data::Dumper;
$Data::Dumper::Indent    = 1;
$Data::Dumper::Sortkeys  = 1;
$Data::Dumper::Quotekeys = 0;

my $open_comment = qr{/\*!\d{5} };
my $tbl_name     = qr{
   CREATE\s+
   (?:TEMPORARY\s+)?
   TABLE\s+
   (?:IF NOT EXISTS\s+)?
   ([^\(]+)
}x;


sub new {
   my ( $class, %args ) = @_;
   my @required_args = qw(OptionParser TableParser Quoter);
   foreach my $arg ( @required_args ) {
      die "I need a $arg argument" unless $args{$arg};
   }

   my ($file_itr, $dbh) = @args{qw(file_itr dbh)};
   die "I need either a dbh or file_itr argument"
      if (!$dbh && !$file_itr) || ($dbh && $file_itr);

   my %resume;
   if ( my $table = $args{resume} ) {
      PTDEBUG && _d('Will resume from or after', $table);
      my ($db, $tbl) = $args{Quoter}->split_unquote($table);
      die "Resume table must be database-qualified: $table"
         unless $db && $tbl;
      $resume{db}  = $db;
      $resume{tbl} = $tbl;
   }

   my $self = {
      %args,
      resume  => \%resume,
      filters => _make_filters(%args),
   };

   return bless $self, $class;
}

sub _make_filters {
   my ( %args ) = @_;
   my @required_args = qw(OptionParser Quoter);
   foreach my $arg ( @required_args ) {
      die "I need a $arg argument" unless $args{$arg};
   }
   my ($o, $q) = @args{@required_args};

   my %filters;


   my @simple_filters = qw(
      databases         tables         engines
      ignore-databases  ignore-tables  ignore-engines);
   FILTER:
   foreach my $filter ( @simple_filters ) {
      if ( $o->has($filter) ) {
         my $objs = $o->get($filter);
         next FILTER unless $objs && scalar keys %$objs;
         my $is_table = $filter =~ m/table/ ? 1 : 0;
         foreach my $obj ( keys %$objs ) {
            die "Undefined value for --$filter" unless $obj;
            $obj = lc $obj;
            if ( $is_table ) {
               my ($db, $tbl) = $q->split_unquote($obj);
               $db ||= '*';
               PTDEBUG && _d('Filter', $filter, 'value:', $db, $tbl);
               $filters{$filter}->{$db}->{$tbl} = 1;
            }
            else { # database
               PTDEBUG && _d('Filter', $filter, 'value:', $obj);
               $filters{$filter}->{$obj} = 1;
            }
         }
      }
   }

   my @regex_filters = qw(
      databases-regex         tables-regex
      ignore-databases-regex  ignore-tables-regex);
   REGEX_FILTER:
   foreach my $filter ( @regex_filters ) {
      if ( $o->has($filter) ) {
         my $pat = $o->get($filter);
         next REGEX_FILTER unless $pat;
         $filters{$filter} = qr/$pat/;
         PTDEBUG && _d('Filter', $filter, 'value:', $filters{$filter});
      }
   }

   PTDEBUG && _d('Schema object filters:', Dumper(\%filters));
   return \%filters;
}

sub next {
   my ( $self ) = @_;

   if ( !$self->{initialized} ) {
      $self->{initialized} = 1;
      if ( $self->{resume}->{tbl} ) {
         if ( !$self->table_is_allowed(@{$self->{resume}}{qw(db tbl)}) ) {
            PTDEBUG && _d('Will resume after',
               join('.', @{$self->{resume}}{qw(db tbl)}));
            $self->{resume}->{after}->{tbl} = 1;
         }
         if ( !$self->database_is_allowed($self->{resume}->{db}) ) {
            PTDEBUG && _d('Will resume after', $self->{resume}->{db});
            $self->{resume}->{after}->{db}  = 1;
         }
      }
   }

   my $schema_obj;
   if ( $self->{file_itr} ) {
      $schema_obj= $self->_iterate_files();
   }
   else { # dbh
      $schema_obj= $self->_iterate_dbh();
   }

   if ( $schema_obj ) {
      if ( my $schema = $self->{Schema} ) {
         $schema->add_schema_object($schema_obj);
      }
      PTDEBUG && _d('Next schema object:',
         $schema_obj->{db}, $schema_obj->{tbl});
   }

   return $schema_obj;
}

sub _iterate_files {
   my ( $self ) = @_;

   if ( !$self->{fh} ) {
      my ($fh, $file) = $self->{file_itr}->();
      if ( !$fh ) {
         PTDEBUG && _d('No more files to iterate');
         return;
      }
      $self->{fh}   = $fh;
      $self->{file} = $file;
   }
   my $fh = $self->{fh};
   PTDEBUG && _d('Getting next schema object from', $self->{file});

   local $INPUT_RECORD_SEPARATOR = '';
   CHUNK:
   while (defined(my $chunk = <$fh>)) {
      if ($chunk =~ m/Database: (\S+)/) {
         my $db = $1; # XXX
         $db =~ s/^`//;  # strip leading `
         $db =~ s/`$//;  # and trailing `
         if ( $self->database_is_allowed($db)
              && $self->_resume_from_database($db) ) {
            $self->{db} = $db;
         }
      }
      elsif ($self->{db} && $chunk =~ m/CREATE TABLE/) {
         if ($chunk =~ m/DROP VIEW IF EXISTS/) {
            PTDEBUG && _d('Table is a VIEW, skipping');
            next CHUNK;
         }

         my ($tbl) = $chunk =~ m/$tbl_name/;
         $tbl      =~ s/^\s*`//;
         $tbl      =~ s/`\s*$//;
         if ( $self->_resume_from_table($tbl)
              && $self->table_is_allowed($self->{db}, $tbl) ) {
            my ($ddl) = $chunk =~ m/^(?:$open_comment)?(CREATE TABLE.+?;)$/ms;
            if ( !$ddl ) {
               warn "Failed to parse CREATE TABLE from\n" . $chunk;
               next CHUNK;
            }
            $ddl =~ s/ \*\/;\Z/;/;  # remove end of version comment
            my $tbl_struct = $self->{TableParser}->parse($ddl);
            if ( $self->engine_is_allowed($tbl_struct->{engine}) ) {
               return {
                  db         => $self->{db},
                  tbl        => $tbl,
                  name       => $self->{Quoter}->quote($self->{db}, $tbl),
                  ddl        => $ddl,
                  tbl_struct => $tbl_struct,
               };
            }
         }
      }
   }  # CHUNK

   PTDEBUG && _d('No more schema objects in', $self->{file});
   close $self->{fh};
   $self->{fh} = undef;

   return $self->_iterate_files();
}

sub _iterate_dbh {
   my ( $self ) = @_;
   my $q   = $self->{Quoter};
   my $tp  = $self->{TableParser};
   my $dbh = $self->{dbh};
   PTDEBUG && _d('Getting next schema object from dbh', $dbh);

   if ( !defined $self->{dbs} ) {
      my $sql = 'SHOW DATABASES';
      PTDEBUG && _d($sql);
      my @dbs = grep {
                  $self->_resume_from_database($_)
                  &&
                  $self->database_is_allowed($_)
                } @{$dbh->selectcol_arrayref($sql)};
      PTDEBUG && _d('Found', scalar @dbs, 'databases');
      $self->{dbs} = \@dbs;
   }

   DATABASE:
   while ( $self->{db} || defined(my $db = shift @{$self->{dbs}}) ) {
      if ( !$self->{db} ) {
         PTDEBUG && _d('Next database:', $db);
         $self->{db} = $db;
      }

      if ( !$self->{tbls} ) {
         my $sql = 'SHOW /*!50002 FULL*/ TABLES FROM ' . $q->quote($self->{db});
         PTDEBUG && _d($sql);
         my @tbls = map {
            $_->[0];  # (tbl, type)
         }
         grep {
            my ($tbl, $type) = @$_;
            (!$type || ($type ne 'VIEW'))
            && $self->_resume_from_table($tbl)
            && $self->table_is_allowed($self->{db}, $tbl);
         }

         eval { @{$dbh->selectall_arrayref($sql)}; };
         if ($EVAL_ERROR) {
             warn "Skipping $self->{db}...";
             $self->{db} = undef;
             next;
         }

         PTDEBUG && _d('Found', scalar @tbls, 'tables in database',$self->{db});
         $self->{tbls} = \@tbls;
      }

      TABLE:
      while ( my $tbl = shift @{$self->{tbls}} ) {
         my $ddl = eval { $tp->get_create_table($dbh, $self->{db}, $tbl) };
         if ( my $e = $EVAL_ERROR ) {
            my $table_name = "$self->{db}.$tbl";
            if ( $e =~ /\QTable '$table_name' doesn't exist/ ) {
               PTDEBUG && _d("$table_name no longer exists");
            }
            else {
               warn "Skipping $table_name because SHOW CREATE TABLE failed: $e";
            }
            next TABLE;
         }

         my $tbl_struct = $tp->parse($ddl);
         if ( $self->engine_is_allowed($tbl_struct->{engine}) ) {
            return {
               db         => $self->{db},
               tbl        => $tbl,
               name       => $q->quote($self->{db}, $tbl),
               ddl        => $ddl,
               tbl_struct => $tbl_struct,
            };
         }
      }

      PTDEBUG && _d('No more tables in database', $self->{db});
      $self->{db}   = undef;
      $self->{tbls} = undef;
   } # DATABASE

   PTDEBUG && _d('No more databases');
   return;
}

sub database_is_allowed {
   my ( $self, $db ) = @_;
   die "I need a db argument" unless $db;

   $db = lc $db;

   my $filter = $self->{filters};

   if ( $db =~ m/^(information_schema|performance_schema|lost\+found|percona_schema)$/ ) {
      PTDEBUG && _d('Database', $db, 'is a system database, ignoring');
      return 0;
   }

   if ( $self->{filters}->{'ignore-databases'}->{$db} ) {
      PTDEBUG && _d('Database', $db, 'is in --ignore-databases list');
      return 0;
   }

   if ( $filter->{'ignore-databases-regex'}
        && $db =~ $filter->{'ignore-databases-regex'} ) {
      PTDEBUG && _d('Database', $db, 'matches --ignore-databases-regex');
      return 0;
   }

   if ( $filter->{'databases'}
        && !$filter->{'databases'}->{$db} ) {
      PTDEBUG && _d('Database', $db, 'is not in --databases list, ignoring');
      return 0;
   }

   if ( $filter->{'databases-regex'}
        && $db !~ $filter->{'databases-regex'} ) {
      PTDEBUG && _d('Database', $db, 'does not match --databases-regex, ignoring');
      return 0;
   }

   return 1;
}

sub table_is_allowed {
   my ( $self, $db, $tbl ) = @_;
   die "I need a db argument"  unless $db;
   die "I need a tbl argument" unless $tbl;

   $db  = lc $db;
   $tbl = lc $tbl;

   my $filter = $self->{filters};

   return 0 if $db eq 'mysql' && $tbl =~ m/^(?:
       general_log
      |gtid_executed
      |innodb_index_stats
      |innodb_table_stats
      |slave_master_info
      |slave_relay_log_info
      |slave_worker_info
      |slow_log
   )$/x;

   if ( $filter->{'ignore-tables'}->{'*'}->{$tbl}
         || $filter->{'ignore-tables'}->{$db}->{$tbl}) {
      PTDEBUG && _d('Table', $tbl, 'is in --ignore-tables list');
      return 0;
   }

   if ( $filter->{'ignore-tables-regex'}
        && $tbl =~ $filter->{'ignore-tables-regex'} ) {
      PTDEBUG && _d('Table', $tbl, 'matches --ignore-tables-regex');
      return 0;
   }

   if ( $filter->{'tables'}
        && (!$filter->{'tables'}->{'*'}->{$tbl} && !$filter->{'tables'}->{$db}->{$tbl}) ) {
      PTDEBUG && _d('Table', $tbl, 'is not in --tables list, ignoring');
      return 0;
   }

   if ( $filter->{'tables-regex'}
        && $tbl !~ $filter->{'tables-regex'} ) {
      PTDEBUG && _d('Table', $tbl, 'does not match --tables-regex, ignoring');
      return 0;
   }

   if ( $filter->{'tables'}
        && $filter->{'tables'}->{$tbl}
        && $filter->{'tables'}->{$tbl} ne '*'
        && $filter->{'tables'}->{$tbl} ne $db ) {
      PTDEBUG && _d('Table', $tbl, 'is only allowed in database',
         $filter->{'tables'}->{$tbl});
      return 0;
   }

   return 1;
}

sub engine_is_allowed {
   my ( $self, $engine ) = @_;

   if ( !$engine ) {
      PTDEBUG && _d('No engine specified; allowing the table');
      return 1;
   }

   $engine = lc $engine;

   my $filter = $self->{filters};

   if ( $filter->{'ignore-engines'}->{$engine} ) {
      PTDEBUG && _d('Engine', $engine, 'is in --ignore-engines list');
      return 0;
   }

   if ( $filter->{'engines'}
        && !$filter->{'engines'}->{$engine} ) {
      PTDEBUG && _d('Engine', $engine, 'is not in --engines list, ignoring');
      return 0;
   }

   return 1;
}

sub _resume_from_database {
   my ($self, $db) = @_;

   return 1 unless $self->{resume}->{db};
   if ( $db eq $self->{resume}->{db} ) {
      if ( !$self->{resume}->{after}->{db} ) {
         PTDEBUG && _d('Resuming from db', $db);
         delete $self->{resume}->{db};
         return 1;
      }
      else {
         PTDEBUG && _d('Resuming after db', $db);
         delete $self->{resume}->{db};
         delete $self->{resume}->{tbl};
      }
   }

   return 0;
}

sub _resume_from_table {
   my ($self, $tbl) = @_;

   return 1 unless $self->{resume}->{tbl};

   if ( $tbl eq $self->{resume}->{tbl} ) {
      if ( !$self->{resume}->{after}->{tbl} ) {
         PTDEBUG && _d('Resuming from table', $tbl);
         delete $self->{resume}->{tbl};
         return 1;
      }
      else {
         PTDEBUG && _d('Resuming after table', $tbl);
         delete $self->{resume}->{tbl};
      }
   }

   return 0;
}

sub _d {
   my ($package, undef, $line) = caller 0;
   @_ = map { (my $temp = $_) =~ s/\n/\n# /g; $temp; }
        map { defined $_ ? $_ : 'undef' }
        @_;
   print STDERR "# $package:$line $PID ", join(' ', @_), "\n";
}

1;
}
# ###########################################################################
# End SchemaIterator package
# ###########################################################################

# ###########################################################################
# Retry package
# This package is a copy without comments from the original.  The original
# with comments and its test file can be found in the GitHub repository at,
#   lib/Retry.pm
#   t/lib/Retry.t
# See https://github.com/percona/percona-toolkit for more information.
# ###########################################################################
{
package Retry;

use strict;
use warnings FATAL => 'all';
use English qw(-no_match_vars);
use constant PTDEBUG => $ENV{PTDEBUG} || 0;

use Time::HiRes qw(sleep);

sub new {
   my ( $class, %args ) = @_;
   my $self = {
      %args,
   };
   return bless $self, $class;
}

sub retry {
   my ( $self, %args ) = @_;
   my @required_args = qw(try fail final_fail);
   foreach my $arg ( @required_args ) {
      die "I need a $arg argument" unless $args{$arg};
   };
   my ($try, $fail, $final_fail) = @args{@required_args};
   my $wait  = $args{wait}  || sub { sleep 1; };
   my $tries = $args{tries} || 3;

   my $last_error;
   my $tryno = 0;
   TRY:
   while ( ++$tryno <= $tries ) {
      PTDEBUG && _d("Try", $tryno, "of", $tries);
      my $result;
      eval {
         $result = $try->(tryno=>$tryno);
      };
      if ( $EVAL_ERROR ) {
         PTDEBUG && _d("Try code failed:", $EVAL_ERROR);
         $last_error = $EVAL_ERROR;

         if ( $tryno < $tries ) {   # more retries
            my $retry = $fail->(tryno=>$tryno, error=>$last_error);
            last TRY unless $retry;
            PTDEBUG && _d("Calling wait code");
            $wait->(tryno=>$tryno);
         }
      }
      else {
         PTDEBUG && _d("Try code succeeded");
         return $result;
      }
   }

   PTDEBUG && _d('Try code did not succeed');
   return $final_fail->(error=>$last_error);
}

sub _d {
   my ($package, undef, $line) = caller 0;
   @_ = map { (my $temp = $_) =~ s/\n/\n# /g; $temp; }
        map { defined $_ ? $_ : 'undef' }
        @_;
   print STDERR "# $package:$line $PID ", join(' ', @_), "\n";
}

1;
}
# ###########################################################################
# End Retry package
# ###########################################################################

# ###########################################################################
# Transformers package
# This package is a copy without comments from the original.  The original
# with comments and its test file can be found in the GitHub repository at,
#   lib/Transformers.pm
#   t/lib/Transformers.t
# See https://github.com/percona/percona-toolkit for more information.
# ###########################################################################
{
package Transformers;

use strict;
use warnings FATAL => 'all';
use English qw(-no_match_vars);
use constant PTDEBUG => $ENV{PTDEBUG} || 0;

use Time::Local qw(timegm timelocal);
use Digest::MD5 qw(md5_hex);
use B qw();

BEGIN {
   require Exporter;
   our @ISA         = qw(Exporter);
   our %EXPORT_TAGS = ();
   our @EXPORT      = ();
   our @EXPORT_OK   = qw(
      micro_t
      percentage_of
      secs_to_time
      time_to_secs
      shorten
      ts
      parse_timestamp
      unix_timestamp
      any_unix_timestamp
      make_checksum
      crc32
      encode_json
   );
}

our $mysql_ts  = qr/(\d\d)(\d\d)(\d\d) +(\d+):(\d+):(\d+)(\.\d+)?/;
our $proper_ts = qr/(\d\d\d\d)-(\d\d)-(\d\d)[T ](\d\d):(\d\d):(\d\d)(\.\d+)?/;
our $n_ts      = qr/(\d{1,5})([shmd]?)/; # Limit \d{1,5} because \d{6} looks

sub micro_t {
   my ( $t, %args ) = @_;
   my $p_ms = defined $args{p_ms} ? $args{p_ms} : 0;  # precision for ms vals
   my $p_s  = defined $args{p_s}  ? $args{p_s}  : 0;  # precision for s vals
   my $f;

   $t = 0 if $t < 0;

   $t = sprintf('%.17f', $t) if $t =~ /e/;

   $t =~ s/\.(\d{1,6})\d*/\.$1/;

   if ($t > 0 && $t <= 0.000999) {
      $f = ($t * 1000000) . 'us';
   }
   elsif ($t >= 0.001000 && $t <= 0.999999) {
      $f = sprintf("%.${p_ms}f", $t * 1000);
      $f = ($f * 1) . 'ms'; # * 1 to remove insignificant zeros
   }
   elsif ($t >= 1) {
      $f = sprintf("%.${p_s}f", $t);
      $f = ($f * 1) . 's'; # * 1 to remove insignificant zeros
   }
   else {
      $f = 0;  # $t should = 0 at this point
   }

   return $f;
}

sub percentage_of {
   my ( $is, $of, %args ) = @_;
   my $p   = $args{p} || 0; # float precision
   my $fmt = $p ? "%.${p}f" : "%d";
   return sprintf $fmt, ($is * 100) / ($of ||= 1);
}

sub secs_to_time {
   my ( $secs, $fmt ) = @_;
   $secs ||= 0;
   return '00:00' unless $secs;

   $fmt ||= $secs >= 86_400 ? 'd'
          : $secs >= 3_600  ? 'h'
          :                   'm';

   return
      $fmt eq 'd' ? sprintf(
         "%d+%02d:%02d:%02d",
         int($secs / 86_400),
         int(($secs % 86_400) / 3_600),
         int(($secs % 3_600) / 60),
         $secs % 60)
      : $fmt eq 'h' ? sprintf(
         "%02d:%02d:%02d",
         int(($secs % 86_400) / 3_600),
         int(($secs % 3_600) / 60),
         $secs % 60)
      : sprintf(
         "%02d:%02d",
         int(($secs % 3_600) / 60),
         $secs % 60);
}

sub time_to_secs {
   my ( $val, $default_suffix ) = @_;
   die "I need a val argument" unless defined $val;
   my $t = 0;
   my ( $prefix, $num, $suffix ) = $val =~ m/([+-]?)(\d+)([a-z])?$/;
   $suffix = $suffix || $default_suffix || 's';
   if ( $suffix =~ m/[smhd]/ ) {
      $t = $suffix eq 's' ? $num * 1        # Seconds
         : $suffix eq 'm' ? $num * 60       # Minutes
         : $suffix eq 'h' ? $num * 3600     # Hours
         :                  $num * 86400;   # Days

      $t *= -1 if $prefix && $prefix eq '-';
   }
   else {
      die "Invalid suffix for $val: $suffix";
   }
   return $t;
}

sub shorten {
   my ( $num, %args ) = @_;
   my $p = defined $args{p} ? $args{p} : 2;     # float precision
   my $d = defined $args{d} ? $args{d} : 1_024; # divisor
   my $n = 0;
   my @units = ('', qw(k M G T P E Z Y));
   while ( $num >= $d && $n < @units - 1 ) {
      $num /= $d;
      ++$n;
   }
   return sprintf(
      $num =~ m/\./ || $n
         ? '%1$.'.$p.'f%2$s'
         : '%1$d',
      $num, $units[$n]);
}

sub ts {
   my ( $time, $gmt ) = @_;
   my ( $sec, $min, $hour, $mday, $mon, $year )
      = $gmt ? gmtime($time) : localtime($time);
   $mon  += 1;
   $year += 1900;
   my $val = sprintf("%d-%02d-%02dT%02d:%02d:%02d",
      $year, $mon, $mday, $hour, $min, $sec);
   if ( my ($us) = $time =~ m/(\.\d+)$/ ) {
      $us = sprintf("%.6f", $us);
      $us =~ s/^0\././;
      $val .= $us;
   }
   return $val;
}

sub parse_timestamp {
   my ( $val ) = @_;
   if ( my($y, $m, $d, $h, $i, $s, $f)
         = $val =~ m/^$mysql_ts$/ )
   {
      return sprintf "%d-%02d-%02d %02d:%02d:"
                     . (defined $f ? '%09.6f' : '%02d'),
                     $y + 2000, $m, $d, $h, $i, (defined $f ? $s + $f : $s);
   }
   elsif ( $val =~ m/^$proper_ts$/ ) {
      return $val;
   }
   return $val;
}

sub unix_timestamp {
   my ( $val, $gmt ) = @_;
   if ( my($y, $m, $d, $h, $i, $s, $us) = $val =~ m/^$proper_ts$/ ) {
      $val = $gmt
         ? timegm($s, $i, $h, $d, $m - 1, $y)
         : timelocal($s, $i, $h, $d, $m - 1, $y);
      if ( defined $us ) {
         $us = sprintf('%.6f', $us);
         $us =~ s/^0\././;
         $val .= $us;
      }
   }
   return $val;
}

sub any_unix_timestamp {
   my ( $val, $callback ) = @_;

   if ( my ($n, $suffix) = $val =~ m/^$n_ts$/ ) {
      $n = $suffix eq 's' ? $n            # Seconds
         : $suffix eq 'm' ? $n * 60       # Minutes
         : $suffix eq 'h' ? $n * 3600     # Hours
         : $suffix eq 'd' ? $n * 86400    # Days
         :                  $n;           # default: Seconds
      PTDEBUG && _d('ts is now - N[shmd]:', $n);
      return time - $n;
   }
   elsif ( $val =~ m/^\d{9,}/ ) {
      PTDEBUG && _d('ts is already a unix timestamp');
      return $val;
   }
   elsif ( my ($ymd, $hms) = $val =~ m/^(\d{6})(?:\s+(\d+:\d+:\d+))?/ ) {
      PTDEBUG && _d('ts is MySQL slow log timestamp');
      $val .= ' 00:00:00' unless $hms;
      return unix_timestamp(parse_timestamp($val));
   }
   elsif ( ($ymd, $hms) = $val =~ m/^(\d{4}-\d\d-\d\d)(?:[T ](\d+:\d+:\d+))?/) {
      PTDEBUG && _d('ts is properly formatted timestamp');
      $val .= ' 00:00:00' unless $hms;
      return unix_timestamp($val);
   }
   else {
      PTDEBUG && _d('ts is MySQL expression');
      return $callback->($val) if $callback && ref $callback eq 'CODE';
   }

   PTDEBUG && _d('Unknown ts type:', $val);
   return;
}

sub make_checksum {
   my ( $val ) = @_;
   my $checksum = uc substr(md5_hex($val), -16);
   PTDEBUG && _d($checksum, 'checksum for', $val);
   return $checksum;
}

sub crc32 {
   my ( $string ) = @_;
   return unless $string;
   my $poly = 0xEDB88320;
   my $crc  = 0xFFFFFFFF;
   foreach my $char ( split(//, $string) ) {
      my $comp = ($crc ^ ord($char)) & 0xFF;
      for ( 1 .. 8 ) {
         $comp = $comp & 1 ? $poly ^ ($comp >> 1) : $comp >> 1;
      }
      $crc = (($crc >> 8) & 0x00FFFFFF) ^ $comp;
   }
   return $crc ^ 0xFFFFFFFF;
}

my $got_json = eval { require JSON };
sub encode_json {
   return JSON::encode_json(@_) if $got_json;
   my ( $data ) = @_;
   return (object_to_json($data) || '');
}


sub object_to_json {
   my ($obj) = @_;
   my $type  = ref($obj);

   if($type eq 'HASH'){
      return hash_to_json($obj);
   }
   elsif($type eq 'ARRAY'){
      return array_to_json($obj);
   }
   else {
      return value_to_json($obj);
   }
}

sub hash_to_json {
   my ($obj) = @_;
   my @res;
   for my $k ( sort { $a cmp $b } keys %$obj ) {
      push @res, string_to_json( $k )
         .  ":"
         . ( object_to_json( $obj->{$k} ) || value_to_json( $obj->{$k} ) );
   }
   return '{' . ( @res ? join( ",", @res ) : '' )  . '}';
}

sub array_to_json {
   my ($obj) = @_;
   my @res;

   for my $v (@$obj) {
      push @res, object_to_json($v) || value_to_json($v);
   }

   return '[' . ( @res ? join( ",", @res ) : '' ) . ']';
}

sub value_to_json {
   my ($value) = @_;

   return 'null' if(!defined $value);

   my $b_obj = B::svref_2object(\$value);  # for round trip problem
   my $flags = $b_obj->FLAGS;
   return $value # as is 
      if $flags & ( B::SVp_IOK | B::SVp_NOK ) and !( $flags & B::SVp_POK ); # SvTYPE is IV or NV?

   my $type = ref($value);

   if( !$type ) {
      return string_to_json($value);
   }
   else {
      return 'null';
   }

}

my %esc = (
   "\n" => '\n',
   "\r" => '\r',
   "\t" => '\t',
   "\f" => '\f',
   "\b" => '\b',
   "\"" => '\"',
   "\\" => '\\\\',
   "\'" => '\\\'',
);

sub string_to_json {
   my ($arg) = @_;

   $arg =~ s/([\x22\x5c\n\r\t\f\b])/$esc{$1}/g;
   $arg =~ s/\//\\\//g;
   $arg =~ s/([\x00-\x08\x0b\x0e-\x1f])/'\\u00' . unpack('H2', $1)/eg;

   utf8::upgrade($arg);
   utf8::encode($arg);

   return '"' . $arg . '"';
}

sub _d {
   my ($package, undef, $line) = caller 0;
   @_ = map { (my $temp = $_) =~ s/\n/\n# /g; $temp; }
        map { defined $_ ? $_ : 'undef' }
        @_;
   print STDERR "# $package:$line $PID ", join(' ', @_), "\n";
}

1;
}
# ###########################################################################
# End Transformers package
# ###########################################################################

# ###########################################################################
# Progress package
# This package is a copy without comments from the original.  The original
# with comments and its test file can be found in the GitHub repository at,
#   lib/Progress.pm
#   t/lib/Progress.t
# See https://github.com/percona/percona-toolkit for more information.
# ###########################################################################
{
package Progress;

use strict;
use warnings FATAL => 'all';
use English qw(-no_match_vars);
use constant PTDEBUG => $ENV{PTDEBUG} || 0;

sub new {
   my ( $class, %args ) = @_;
   foreach my $arg (qw(jobsize)) {
      die "I need a $arg argument" unless defined $args{$arg};
   }
   if ( (!$args{report} || !$args{interval}) ) {
      if ( $args{spec} && @{$args{spec}} == 2 ) {
         @args{qw(report interval)} = @{$args{spec}};
      }
      else {
         die "I need either report and interval arguments, or a spec";
      }
   }

   my $name  = $args{name} || "Progress";
   $args{start} ||= time();
   my $self;
   $self = {
      last_reported => $args{start},
      fraction      => 0,       # How complete the job is
      callback      => sub {
         my ($fraction, $elapsed, $remaining) = @_;
         printf STDERR "$name: %3d%% %s remain\n",
            $fraction * 100,
            Transformers::secs_to_time($remaining);
      },
      %args,
   };
   return bless $self, $class;
}

sub validate_spec {
   shift @_ if $_[0] eq 'Progress'; # Permit calling as Progress-> or Progress::
   my ( $spec ) = @_;
   if ( @$spec != 2 ) {
      die "spec array requires a two-part argument\n";
   }
   if ( $spec->[0] !~ m/^(?:percentage|time|iterations)$/ ) {
      die "spec array's first element must be one of "
        . "percentage,time,iterations\n";
   }
   if ( $spec->[1] !~ m/^\d+$/ ) {
      die "spec array's second element must be an integer\n";
   }
}

sub set_callback {
   my ( $self, $callback ) = @_;
   $self->{callback} = $callback;
}

sub start {
   my ( $self, $start ) = @_;
   $self->{start} = $self->{last_reported} = $start || time();
   $self->{first_report} = 0;
}

sub update {
   my ( $self, $callback, %args ) = @_;
   my $jobsize   = $self->{jobsize};
   my $now    ||= $args{now} || time;

   $self->{iterations}++; # How many updates have happened;

   if ( !$self->{first_report} && $args{first_report} ) {
      $args{first_report}->();
      $self->{first_report} = 1;
   }

   if ( $self->{report} eq 'time'
         && $self->{interval} > $now - $self->{last_reported}
   ) {
      return;
   }
   elsif ( $self->{report} eq 'iterations'
         && ($self->{iterations} - 1) % $self->{interval} > 0
   ) {
      return;
   }
   $self->{last_reported} = $now;

   my $completed = $callback->();
   $self->{updates}++; # How many times we have run the update callback

   return if $completed > $jobsize;

   my $fraction = $completed > 0 ? $completed / $jobsize : 0;

   if ( $self->{report} eq 'percentage'
         && $self->fraction_modulo($self->{fraction})
            >= $self->fraction_modulo($fraction)
   ) {
      $self->{fraction} = $fraction;
      return;
   }
   $self->{fraction} = $fraction;

   my $elapsed   = $now - $self->{start};
   my $remaining = 0;
   my $eta       = $now;
   if ( $completed > 0 && $completed <= $jobsize && $elapsed > 0 ) {
      my $rate = $completed / $elapsed;
      if ( $rate > 0 ) {
         $remaining = ($jobsize - $completed) / $rate;
         $eta       = $now + int($remaining);
      }
   }
   $self->{callback}->($fraction, $elapsed, $remaining, $eta, $completed);
}

sub fraction_modulo {
   my ( $self, $num ) = @_;
   $num *= 100; # Convert from fraction to percentage
   return sprintf('%d',
      sprintf('%d', $num / $self->{interval}) * $self->{interval});
}

sub _d {
   my ($package, undef, $line) = caller 0;
   @_ = map { (my $temp = $_) =~ s/\n/\n# /g; $temp; }
        map { defined $_ ? $_ : 'undef' }
        @_;
   print STDERR "# $package:$line $PID ", join(' ', @_), "\n";
}

1;
}
# ###########################################################################
# End Progress package
# ###########################################################################

# ###########################################################################
# ReplicaLagWaiter package
# This package is a copy without comments from the original.  The original
# with comments and its test file can be found in the GitHub repository at,
#   lib/ReplicaLagWaiter.pm
#   t/lib/ReplicaLagWaiter.t
# See https://github.com/percona/percona-toolkit for more information.
# ###########################################################################
{
package ReplicaLagWaiter;

use strict;
use warnings FATAL => 'all';
use English qw(-no_match_vars);
use constant PTDEBUG => $ENV{PTDEBUG} || 0;

use Time::HiRes qw(sleep time);
use Data::Dumper;

sub new {
   my ( $class, %args ) = @_;
   my @required_args = qw(oktorun get_lag sleep max_lag slaves);
   foreach my $arg ( @required_args ) {
      die "I need a $arg argument" unless defined $args{$arg};
   }

   my $self = {
      %args,
   };

   return bless $self, $class;
}

sub wait {
   my ( $self, %args ) = @_;
   my @required_args = qw();
   foreach my $arg ( @required_args ) {
      die "I need a $arg argument" unless $args{$arg};
   }
   my $pr = $args{Progress};

   my $oktorun = $self->{oktorun};
   my $get_lag = $self->{get_lag};
   my $sleep   = $self->{sleep};
   my $slaves  = $self->{slaves};
   my $max_lag = $self->{max_lag};

   my $worst;  # most lagging slave
   my $pr_callback;
   my $pr_first_report;

   my $pr_refresh_slave_list = sub {
      my ($self) = @_;
      my ($slaves, $refresher) = ($self->{slaves}, $self->{get_slaves_cb});
      return $slaves if ( not defined $refresher );
      my $before = join ' ', sort map {$_->name()} @$slaves;
      $slaves = $refresher->();
      my $after = join ' ', sort map {$_->name()} @$slaves;
      if ($before ne $after) {
         $self->{slaves} = $slaves;
         printf STDERR "Slave set to watch has changed\n  Was: %s\n  Now: %s\n",
            $before, $after;
      }
      return($self->{slaves});
   };

   $slaves = $pr_refresh_slave_list->($self);

   if ( $pr ) {
      $pr_callback = sub {
         my ($fraction, $elapsed, $remaining, $eta, $completed) = @_;
         my $dsn_name = $worst->{cxn}->name();
         if ( defined $worst->{lag} ) {
            print STDERR "Replica lag is " . ($worst->{lag} || '?')
               . " seconds on $dsn_name.  Waiting.\n";
         }
         else {
            if ($self->{fail_on_stopped_replication}) {
                die 'replication is stopped';
            }
            print STDERR "Replica $dsn_name is stopped.  Waiting.\n";
         }
         return;
      };
      $pr->set_callback($pr_callback);

      $pr_first_report = sub {
         my $dsn_name = $worst->{cxn}->name();
         if ( !defined $worst->{lag} ) {
            if ($self->{fail_on_stopped_replication}) {
                die 'replication is stopped';
            }
            print STDERR "Replica $dsn_name is stopped.  Waiting.\n";
         }
         return;
      };
   }

   my @lagged_slaves = map { {cxn=>$_, lag=>undef} } @$slaves;
   while ( $oktorun->() && @lagged_slaves ) {
      PTDEBUG && _d('Checking slave lag');

      $slaves = $pr_refresh_slave_list->($self);
      my $watched = 0;
      @lagged_slaves = grep {
         my $slave_name = $_->{cxn}->name();
         grep {$slave_name eq $_->name()} @{$slaves // []}
                            } @lagged_slaves;

      for my $i ( 0..$#lagged_slaves ) {
         my $lag;
         eval {
             $lag = $get_lag->($lagged_slaves[$i]->{cxn});
         };
         if ($EVAL_ERROR) {
             die $EVAL_ERROR;
         }
         PTDEBUG && _d($lagged_slaves[$i]->{cxn}->name(),
            'slave lag:', $lag);
         if ( !defined $lag || $lag > $max_lag ) {
            $lagged_slaves[$i]->{lag} = $lag;
         }
         else {
            delete $lagged_slaves[$i];
         }
      }

      @lagged_slaves = grep { defined $_ } @lagged_slaves;
      if ( @lagged_slaves ) {
         @lagged_slaves = reverse sort {
              defined $a->{lag} && defined $b->{lag} ? $a->{lag} <=> $b->{lag}
            : defined $a->{lag}                      ? -1
            :                                           1;
         } @lagged_slaves;
         $worst = $lagged_slaves[0];
         PTDEBUG && _d(scalar @lagged_slaves, 'slaves are lagging, worst:',
            $worst->{lag}, 'on', Dumper($worst->{cxn}->dsn()));

         if ( $pr ) {
            $pr->update(
               sub { return 0; },
               first_report => $pr_first_report,
            );
         }

         PTDEBUG && _d('Calling sleep callback');
         $sleep->($worst->{cxn}, $worst->{lag});
      }
   }

   PTDEBUG && _d('All slaves caught up');
   return;
}

sub _d {
   my ($package, undef, $line) = caller 0;
   @_ = map { (my $temp = $_) =~ s/\n/\n# /g; $temp; }
        map { defined $_ ? $_ : 'undef' }
        @_;
   print STDERR "# $package:$line $PID ", join(' ', @_), "\n";
}

1;
}
# ###########################################################################
# End ReplicaLagWaiter package
# ###########################################################################

# This program is copyright 2010-2011 Percona Ireland Ltd.
# Feedback and improvements are welcome.
#
# THIS PROGRAM IS PROVIDED "AS IS" AND WITHOUT ANY EXPRESS OR IMPLIED
# WARRANTIES, INCLUDING, WITHOUT LIMITATION, THE IMPLIED WARRANTIES OF
# MERCHANTIBILITY AND FITNESS FOR A PARTICULAR PURPOSE.
#
# This program is free software; you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation, version 2; OR the Perl Artistic License.  On UNIX and similar
# systems, you can issue `man perlgpl' or `man perlartistic' to read these
# licenses.
#
# You should have received a copy of the GNU General Public License along with
# this program; if not, write to the Free Software Foundation, Inc., 59 Temple
# Place, Suite 330, Boston, MA  02111-1307  USA.
# ###########################################################################
# MySQLConfig package
# This package is a copy without comments from the original.  The original
# with comments and its test file can be found in the GitHub repository at,
#   lib/MySQLConfig.pm
#   t/lib/MySQLConfig.t
# See https://github.com/percona/percona-toolkit for more information.
# ###########################################################################
{
package MySQLConfig;

use strict;
use warnings FATAL => 'all';
use English qw(-no_match_vars);
use constant PTDEBUG => $ENV{PTDEBUG} || 0;

my %can_be_duplicate = (
   replicate_wild_do_table     => 1,
   replicate_wild_ignore_table => 1,
   replicate_rewrite_db        => 1,
   replicate_ignore_table      => 1,
   replicate_ignore_db         => 1,
   replicate_do_table          => 1,
   replicate_do_db             => 1,
);

sub new {
   my ( $class, %args ) = @_;
   my @requires_one_of = qw(file output result_set dbh);
   my $required_arg    = grep { $args{$_} } @requires_one_of;
   if ( !$required_arg ) {
      die "I need a " . join(', ', @requires_one_of[0..$#requires_one_of-1])
         . " or " . $requires_one_of[-1] . " argument";
   }
   if ( $required_arg > 1 ) {
      die "Specify only one "
         . join(', ', @requires_one_of[0..$#requires_one_of-1])
         . " or " . $requires_one_of[-1] . " argument";
   }
   if ( $args{file} || $args{output} ) {
      die "I need a TextResultSetParser argument"
         unless $args{TextResultSetParser};
   }

   if ( $args{file} ) {
      $args{output} = _slurp_file($args{file});
   }

   my %config_data = _parse_config(%args);

   my $self = {
      %args,
      %config_data,
   };

   return bless $self, $class;
}

sub _parse_config {
   my ( %args ) = @_;

   my %config_data;
   if ( $args{output} ) {
      %config_data = _parse_config_output(%args);
   }
   elsif ( my $rows = $args{result_set} ) {
      $config_data{format} = $args{format} || 'show_variables';
      $config_data{vars}   = { map { @$_ } @$rows };
   }
   elsif ( my $dbh = $args{dbh} ) {
      $config_data{format} = $args{format} || 'show_variables';
      my $sql = "SHOW /*!40103 GLOBAL*/ VARIABLES";
      PTDEBUG && _d($dbh, $sql);
      my $rows = $dbh->selectall_arrayref($sql);
      $config_data{vars} = { map { @$_ } @$rows };
      $config_data{mysql_version} = _get_version($dbh);
   }
   else {
      die "Unknown config source";
   }

   handle_special_vars(\%config_data);
   
   return %config_data;
}

sub handle_special_vars {
   my ($config_data) = @_;
   
   if ( $config_data->{vars}->{wsrep_provider_options} ) {
      my $vars  = $config_data->{vars};
      my $dupes = $config_data->{duplicate_vars};
      for my $wpo ( $vars->{wsrep_provider_options}, @{$dupes->{wsrep_provider_options} || [] } ) {
         my %opts = $wpo =~ /(\S+)\s*=\s*(\S*)(?:;|;?$)/g;
         while ( my ($var, $val) = each %opts ) {
            $val =~ s/;$//;
            if ( exists $vars->{$var} ) {
               push @{$dupes->{$var} ||= []}, $val;
            }
            $vars->{$var} = $val;
         }
      }
      delete $vars->{wsrep_provider_options};
   }

   return;
}

sub _parse_config_output {
   my ( %args ) = @_;
   my @required_args = qw(output TextResultSetParser);
   foreach my $arg ( @required_args ) {
      die "I need a $arg arugment" unless $args{$arg};
   }
   my ($output) = @args{@required_args};
   PTDEBUG && _d("Parsing config output");

   my $format = $args{format} || detect_config_output_format(%args);
   if ( !$format ) {
      die "Cannot auto-detect the MySQL config format";
   }

   my $vars;      # variables hashref
   my $dupes;     # duplicate vars hashref
   my $opt_files; # option files arrayref
   if ( $format eq 'show_variables' ) {
      $vars = parse_show_variables(%args);
   }
   elsif ( $format eq 'mysqld' ) {
      ($vars, $opt_files) = parse_mysqld(%args);
   }
   elsif ( $format eq 'my_print_defaults' ) {
      ($vars, $dupes) = parse_my_print_defaults(%args);
   }
   elsif ( $format eq 'option_file' ) {
      ($vars, $dupes) = parse_option_file(%args);
   }
   else {
      die "Invalid MySQL config format: $format";
   }

   die "Failed to parse MySQL config" unless $vars && keys %$vars;

   if ( $format ne 'show_variables' ) {
      _mimic_show_variables(
         %args,
         format => $format,
         vars   => $vars,
      );
   }
   
   return (
      format         => $format,
      vars           => $vars,
      option_files   => $opt_files,
      duplicate_vars => $dupes,
   );
}

sub detect_config_output_format {
   my ( %args ) = @_;
   my @required_args = qw(output);
   foreach my $arg ( @required_args ) {
      die "I need a $arg arugment" unless $args{$arg};
   }
   my ($output) = @args{@required_args};

   my $format;
   if (    $output =~ m/\|\s+\w+\s+\|\s+.+?\|/
        || $output =~ m/\*+ \d/
        || $output =~ m/Variable_name:\s+\w+/
        || $output =~ m/Variable_name\s+Value$/m )
   {
      PTDEBUG && _d('show variables format');
      $format = 'show_variables';
   }
   elsif (    $output =~ m/Starts the MySQL database server/
           || $output =~ m/Default options are read from /
           || $output =~ m/^help\s+TRUE /m )
   {
      PTDEBUG && _d('mysqld format');
      $format = 'mysqld';
   }
   elsif ( $output =~ m/^--\w+/m ) {
      PTDEBUG && _d('my_print_defaults format');
      $format = 'my_print_defaults';
   }
   elsif ( $output =~ m/^\s*\[[a-zA-Z]+\]\s*$/m ) {
      PTDEBUG && _d('option file format');
      $format = 'option_file',
   }

   return $format;
}

sub parse_show_variables {
   my ( %args ) = @_;
   my @required_args = qw(output TextResultSetParser);
   foreach my $arg ( @required_args ) {
      die "I need a $arg arugment" unless $args{$arg};
   }
   my ($output, $trp) = @args{@required_args};

   my %config = map {
      $_->{Variable_name} => $_->{Value}
   } @{ $trp->parse($output) };

   return \%config;
}

sub parse_mysqld {
   my ( %args ) = @_;
   my @required_args = qw(output);
   foreach my $arg ( @required_args ) {
      die "I need a $arg arugment" unless $args{$arg};
   }
   my ($output) = @args{@required_args};

   my @opt_files;
   if ( $output =~ m/^Default options are read.+\n/mg ) {
      my ($opt_files) = $output =~ m/\G^(.+)\n/m;
      my %seen;
      my @opt_files = grep { !$seen{$_} } split(' ', $opt_files);
      PTDEBUG && _d('Option files:', @opt_files);
   }
   else {
      PTDEBUG && _d("mysqld help output doesn't list option files");
   }

   if ( $output !~ m/^-+ -+$(.+?)(?:\n\n.+)?\z/sm ) {
      PTDEBUG && _d("mysqld help output doesn't list vars and vals");
      return;
   }

   my $varvals = $1;

   my ($config, undef) = _parse_varvals(
      qr/^(\S+)(.*)$/,
      $varvals,
   );

   return $config, \@opt_files;
}

sub parse_my_print_defaults {
   my ( %args ) = @_;
   my @required_args = qw(output);
   foreach my $arg ( @required_args ) {
      die "I need a $arg arugment" unless $args{$arg};
   }
   my ($output) = @args{@required_args};

   my ($config, $dupes) = _parse_varvals(
      qr/^--([^=]+)(?:=(.*))?$/,
      $output,
   );

   return $config, $dupes;
}

sub parse_option_file {
   my ( %args ) = @_;
   my @required_args = qw(output);
   foreach my $arg ( @required_args ) {
      die "I need a $arg arugment" unless $args{$arg};
   }
   my ($output) = @args{@required_args};

   my ($mysqld_section) = $output =~ m/\[mysqld\](.+?)(?:^\s*\[\w+\]|\Z)/xms;
   die "Failed to parse the [mysqld] section" unless $mysqld_section;

   my ($config, $dupes) = _parse_varvals(
      qr/^([^=]+)(?:=(.*))?$/,
      $mysqld_section,
   );

   return $config, $dupes;
}

sub _preprocess_varvals {
   my ($re, $to_parse) = @_;

   my %vars;
   LINE:
   foreach my $line ( split /\n/, $to_parse ) {
      next LINE if $line =~ m/^\s*$/;   # no empty lines
      next LINE if $line =~ /^\s*[#;]/; # no # or ; comment lines

      if ( $line !~ $re ) {
         PTDEBUG && _d("Line <", $line, "> didn't match $re");
         next LINE;
      }

      my ($var, $val) = ($1, $2);
      
      $var =~ tr/-/_/;

      $var =~ s/\s*#.*$//;

      if ( !defined $val ) {
         $val = '';
      }
      
      for my $item ($var, $val) {
         $item =~ s/^\s+//;
         $item =~ s/\s+$//;
      }

      push @{$vars{$var} ||= []}, $val
   }

   return \%vars;
}

sub _parse_varvals {
   my ( $vars ) = _preprocess_varvals(@_);

   my %config;

   my %duplicates;

   while ( my ($var, $vals) = each %$vars ) {
      my $val = _process_val( pop @$vals );
      if ( @$vals && !$can_be_duplicate{$var} ) {
         PTDEBUG && _d("Duplicate var:", $var);
         foreach my $current_val ( map { _process_val($_) } @$vals ) {
            push @{$duplicates{$var} ||= []}, $current_val;
         }
      }

      PTDEBUG && _d("Var:", $var, "val:", $val);

      $config{$var} = $val;
   }

   return \%config, \%duplicates;
}

my $quote_re = qr/
   \A             # Start of value
   (['"])         # Opening quote
   (.*)           # Value
   \1             # Closing quote
   \s*(?:\#.*)?   # End of line comment
   [\n\r]*\z      # End of value
/x;
sub _process_val {
   my ($val) = @_;

   if ( $val =~ $quote_re ) {
      $val = $2;
   }
   else {
      $val =~ s/\s*#.*//;
   }

   if ( my ($num, $factor) = $val =~ m/(\d+)([KMGT])b?$/i ) {
      my %factor_for = (
         k => 1_024,
         m => 1_048_576,
         g => 1_073_741_824,
         t => 1_099_511_627_776,
      );
      $val = $num * $factor_for{lc $factor};
   }
   elsif ( $val =~ m/No default/ ) {
      $val = '';
   }
   return $val;
}

sub _mimic_show_variables {
   my ( %args ) = @_;
   my @required_args = qw(vars format);
   foreach my $arg ( @required_args ) {
      die "I need a $arg arugment" unless $args{$arg};
   }
   my ($vars, $format) = @args{@required_args};
   
   foreach my $var ( keys %$vars ) {
      if ( $vars->{$var} eq '' ) {
         if ( $format eq 'mysqld' ) {
            if ( $var ne 'log_error' && $var =~ m/^(?:log|skip|ignore)/ ) {
               $vars->{$var} = 'OFF';
            }
         }
         else {
            $vars->{$var} = 'ON';
         }
      }
   }

   return;
}

sub _slurp_file {
   my ( $file ) = @_;
   die "I need a file argument" unless $file;
   PTDEBUG && _d("Reading", $file);
   open my $fh, '<', $file or die "Cannot open $file: $OS_ERROR";
   my $contents = do { local $/ = undef; <$fh> };
   close $fh;
   return $contents;
}

sub _get_version {
   my ( $dbh ) = @_;
   return unless $dbh;
   my $version = $dbh->selectrow_arrayref('SELECT VERSION()')->[0];
   $version =~ s/(\d\.\d{1,2}.\d{1,2})/$1/;
   PTDEBUG && _d('MySQL version', $version);
   return $version;
}


sub has {
   my ( $self, $var ) = @_;
   return exists $self->{vars}->{$var};
}

sub value_of {
   my ( $self, $var ) = @_;
   return unless $var;
   return $self->{vars}->{$var};
}

sub variables {
   my ( $self, %args ) = @_;
   return $self->{vars};
}

sub duplicate_variables {
   my ( $self ) = @_;
   return $self->{duplicate_vars};
}

sub option_files {
   my ( $self ) = @_;
   return $self->{option_files};
}

sub mysql_version {
   my ( $self ) = @_;
   return $self->{mysql_version};
}

sub format {
   my ( $self ) = @_;
   return $self->{format};
}

sub is_active {
   my ( $self ) = @_;
   return $self->{dbh} ? 1 : 0;
}

sub has_engine {
    my ($self, $engine) = @_;
    if (!$self->{dbh}) {
        die "invalid dbh in has_engine method";
    }

    my $rows = $self->{dbh}->selectall_arrayref('SHOW ENGINES', {Slice=>{}});
    my $is_enabled;
    for my $row (@$rows) {
        if ($row->{engine} eq 'ROCKSDB') {
            $is_enabled = 1;
            last;
        }
    }
    return $is_enabled;
}

sub _d {
   my ($package, undef, $line) = caller 0;
   @_ = map { (my $temp = $_) =~ s/\n/\n# /g; $temp; }
        map { defined $_ ? $_ : 'undef' }
        @_;
   print STDERR "# $package:$line $PID ", join(' ', @_), "\n";
}

1;
}
# ###########################################################################
# End MySQLConfig package
# ###########################################################################
# ###########################################################################
# MySQLStatusWaiter package
# This package is a copy without comments from the original.  The original
# with comments and its test file can be found in the GitHub repository at,
#   lib/MySQLStatusWaiter.pm
#   t/lib/MySQLStatusWaiter.t
# See https://github.com/percona/percona-toolkit for more information.
# ###########################################################################
{
package MySQLStatusWaiter;

use strict;
use warnings FATAL => 'all';
use POSIX qw( ceil );
use English qw(-no_match_vars);
use constant PTDEBUG => $ENV{PTDEBUG} || 0;

sub new {
   my ( $class, %args ) = @_;
   my @required_args = qw(max_spec get_status sleep oktorun);
   foreach my $arg ( @required_args ) {
      die "I need a $arg argument" unless defined $args{$arg};
   }

   PTDEBUG && _d('Parsing spec for max thresholds');
   my $max_val_for = _parse_spec($args{max_spec});
   if ( $max_val_for ) {
      _check_and_set_vals(
         vars             => $max_val_for,
         get_status       => $args{get_status},
         threshold_factor => 0.2, # +20%
      );
   }

   PTDEBUG && _d('Parsing spec for critical thresholds');
   my $critical_val_for = _parse_spec($args{critical_spec} || []);
   if ( $critical_val_for ) {
      _check_and_set_vals(
         vars             => $critical_val_for,
         get_status       => $args{get_status},
         threshold_factor => 1.0, # double (x2; +100%)
      );
   }

   my $self = {
      get_status       => $args{get_status},
      sleep            => $args{sleep},
      oktorun          => $args{oktorun},
      max_val_for      => $max_val_for,
      critical_val_for => $critical_val_for,
   };

   return bless $self, $class;
}

sub _parse_spec {
   my ($spec) = @_;

   return unless $spec && scalar @$spec;

   my %max_val_for;
   foreach my $var_val ( @$spec ) {
      die "Empty or undefined spec\n" unless $var_val;
      $var_val =~ s/^\s+//;
      $var_val =~ s/\s+$//g;

      my ($var, $val) = split /[:=]/, $var_val;
      die "$var_val does not contain a variable\n" unless $var;
      die "$var is not a variable name\n" unless $var =~ m/^[a-zA-Z_]+$/;

      if ( !$val ) {
         PTDEBUG && _d('Will get intial value for', $var, 'later');
         $max_val_for{$var} = undef;
      }
      else {
         die "The value for $var must be a number\n"
            unless $val =~ m/^[\d\.]+$/;
         $max_val_for{$var} = $val;
      }
   }

   return \%max_val_for; 
}

sub max_values {
   my ($self) = @_;
   return $self->{max_val_for};
}

sub critical_values {
   my ($self) = @_;
   return $self->{critical_val_for};
}

sub wait {
   my ( $self, %args ) = @_;

   return unless $self->{max_val_for};

   my $pr = $args{Progress}; # optional

   my $oktorun    = $self->{oktorun};
   my $get_status = $self->{get_status};
   my $sleep      = $self->{sleep};

   my %vals_too_high = %{$self->{max_val_for}};
   my $pr_callback;
   if ( $pr ) {
      $pr_callback = sub {
         print STDERR "Pausing because "
            . join(', ',
                 map {
                    "$_="
                    . (defined $vals_too_high{$_} ? $vals_too_high{$_}
                                                  : 'unknown')
                 } sort keys %vals_too_high
              )
            . ".\n";
         return;
      };
      $pr->set_callback($pr_callback);
   }

   while ( $oktorun->() ) {
      PTDEBUG && _d('Checking status variables');
      foreach my $var ( sort keys %vals_too_high ) {
         my $val = $get_status->($var);
         PTDEBUG && _d($var, '=', $val);
         if ( $val
              && exists $self->{critical_val_for}->{$var}
              && $val >= $self->{critical_val_for}->{$var} ) {
            die "$var=$val exceeds its critical threshold "
               . "$self->{critical_val_for}->{$var}\n";
         }
         if ( $val && $val >= $self->{max_val_for}->{$var} ) {
            $vals_too_high{$var} = $val;
         }
         else {
            delete $vals_too_high{$var};
         }
      }

      last unless scalar keys %vals_too_high;

      PTDEBUG && _d(scalar keys %vals_too_high, 'values are too high:',
         %vals_too_high);
      if ( $pr ) {
         $pr->update(sub { return 0; });
      }
      PTDEBUG && _d('Calling sleep callback');
      $sleep->();
      %vals_too_high = %{$self->{max_val_for}}; # recheck all vars
   }

   PTDEBUG && _d('All var vals are low enough');
   return;
}

sub _check_and_set_vals {
   my (%args) = @_;
   my @required_args = qw(vars get_status threshold_factor);
   foreach my $arg ( @required_args ) {
      die "I need a $arg argument" unless defined $args{$arg};
   }
   my ($vars, $get_status, $threshold_factor) = @args{@required_args};

   PTDEBUG && _d('Checking and setting values');
   return unless $vars && scalar %$vars;

   foreach my $var ( keys %$vars ) {
      my $init_val = $get_status->($var);
      die "Variable $var does not exist or its value is undefined\n"
         unless defined $init_val;
      my $val;
      if ( defined $vars->{$var} ) {
         $val = $vars->{$var};
      }
      else {
         PTDEBUG && _d('Initial', $var, 'value:', $init_val);
         $val = ($init_val * $threshold_factor) + $init_val;
         $vars->{$var} = int(ceil($val));
      }
      PTDEBUG && _d('Wait if', $var, '>=', $val);
   }
}

sub _d {
   my ($package, undef, $line) = caller 0;
   @_ = map { (my $temp = $_) =~ s/\n/\n# /g; $temp; }
        map { defined $_ ? $_ : 'undef' }
        @_;
   print STDERR "# $package:$line $PID ", join(' ', @_), "\n";
}

1;
}
# ###########################################################################
# End MySQLStatusWaiter package
# ###########################################################################

# ###########################################################################
# WeightedAvgRate package
# This package is a copy without comments from the original.  The original
# with comments and its test file can be found in the GitHub repository at,
#   lib/WeightedAvgRate.pm
#   t/lib/WeightedAvgRate.t
# See https://github.com/percona/percona-toolkit for more information.
# ###########################################################################
{
package WeightedAvgRate;

use strict;
use warnings FATAL => 'all';
use English qw(-no_match_vars);
use constant PTDEBUG => $ENV{PTDEBUG} || 0;

sub new {
   my ( $class, %args ) = @_;
   my @required_args = qw(target_t);
   foreach my $arg ( @required_args ) {
      die "I need a $arg argument" unless defined $args{$arg};
   }

   my $self = {
      %args,
      avg_n  => 0,
      avg_t  => 0,
      weight => $args{weight} || 0.75,
   };

   return bless $self, $class;
}

sub update {
   my ($self, $n, $t) = @_;
   PTDEBUG && _d('Master op time:', $n, 'n /', $t, 's');

   if ( $self->{avg_n} && $self->{avg_t} ) {
      $self->{avg_n}    = ($self->{avg_n} * $self->{weight}) + $n;
      $self->{avg_t}    = ($self->{avg_t} * $self->{weight}) + $t;
      $self->{avg_rate} = $self->{avg_n}  / $self->{avg_t};
      PTDEBUG && _d('Weighted avg rate:', $self->{avg_rate}, 'n/s');
   }
   else {
      $self->{avg_n}    = $n;
      $self->{avg_t}    = $t;
      $self->{avg_rate} = $self->{avg_n}  / $self->{avg_t};
      PTDEBUG && _d('Initial avg rate:', $self->{avg_rate}, 'n/s');
   }

   my $new_n = int($self->{avg_rate} * $self->{target_t});
   PTDEBUG && _d('Adjust n to', $new_n);
   return $new_n;
}

sub _d {
   my ($package, undef, $line) = caller 0;
   @_ = map { (my $temp = $_) =~ s/\n/\n# /g; $temp; }
        map { defined $_ ? $_ : 'undef' }
        @_;
   print STDERR "# $package:$line $PID ", join(' ', @_), "\n";
}

1;
}
# ###########################################################################
# End WeightedAvgRate package
# ###########################################################################

# ###########################################################################
# IndexLength package
# This package is a copy without comments from the original.  The original
# with comments and its test file can be found in the GitHub repository at,
#   lib/IndexLength.pm
#   t/lib/IndexLength.t
# See https://github.com/percona/percona-toolkit for more information.
# ###########################################################################
{

package IndexLength;

use strict;
use warnings FATAL => 'all';
use English qw(-no_match_vars);
use constant PTDEBUG => $ENV{PTDEBUG} || 0;

use Data::Dumper;
use Carp;
$Data::Dumper::Indent    = 1;
$Data::Dumper::Sortkeys  = 1;
$Data::Dumper::Quotekeys = 0;

sub new {
   my ( $class, %args ) = @_;
   my @required_args = qw(Quoter);
   foreach my $arg ( @required_args ) {
      die "I need a $arg argument" unless $args{$arg};
   }

   my $self = {
       Quoter => $args{Quoter},
   };

   return bless $self, $class;
}

sub index_length {
   my ($self, %args) = @_;
   my @required_args = qw(Cxn tbl index);
   foreach my $arg ( @required_args ) {
      die "I need a $arg argument" unless $args{$arg};
   }
   my ($cxn) = @args{@required_args};

   die "The tbl argument does not have a tbl_struct"
      unless exists $args{tbl}->{tbl_struct};
   die "Index $args{index} does not exist in table $args{tbl}->{name}"
      unless $args{tbl}->{tbl_struct}->{keys}->{$args{index}};

   my $index_struct = $args{tbl}->{tbl_struct}->{keys}->{$args{index}};
   my $index_cols   = $index_struct->{cols};
   my $n_index_cols = $args{n_index_cols};
   if ( !$n_index_cols || $n_index_cols > @$index_cols ) {
      $n_index_cols = scalar @$index_cols;
   }

   my $vals = $self->_get_first_values(
      %args,
      n_index_cols => $n_index_cols,
   );

   my $sql = $self->_make_range_query(
      %args,
      n_index_cols => $n_index_cols,
      vals         => $vals,
   );
   my $sth = $cxn->dbh()->prepare($sql);
   PTDEBUG && _d($sth->{Statement}, 'params:', @$vals);
   $sth->execute(@$vals);
   my $row = $sth->fetchrow_hashref();
   $sth->finish();
   PTDEBUG && _d('Range scan:', Dumper($row));
   return $row->{key_len}, $row->{key};
}

sub _get_first_values {
   my ($self, %args) = @_;
   my @required_args = qw(Cxn tbl index n_index_cols);
   foreach my $arg ( @required_args ) {
      die "I need a $arg argument" unless $args{$arg};
   }
   my ($cxn, $tbl, $index, $n_index_cols) = @args{@required_args};

   my $q = $self->{Quoter};

   my $index_struct  = $tbl->{tbl_struct}->{keys}->{$index};
   my $index_cols    = $index_struct->{cols};
   my $index_columns;
   eval {
   $index_columns = join (', ',
      map { $q->quote($_) } @{$index_cols}[0..($n_index_cols - 1)]);
  };
  if ($EVAL_ERROR) {
      confess "$EVAL_ERROR";
  }



   my @where;
   foreach my $col ( @{$index_cols}[0..($n_index_cols - 1)] ) {
      push @where, $q->quote($col) . " IS NOT NULL"
   }

   my $sql = "SELECT /*!40001 SQL_NO_CACHE */ $index_columns "
           . "FROM $tbl->{name} FORCE INDEX (" . $q->quote($index) . ") "
           . "WHERE " . join(' AND ', @where)
           . " ORDER BY $index_columns "
           . "LIMIT 1 /*key_len*/";  # only need 1 row
   PTDEBUG && _d($sql);
   my $vals = $cxn->dbh()->selectrow_arrayref($sql);
   return $vals;
}

sub _make_range_query {
   my ($self, %args) = @_;
   my @required_args = qw(tbl index n_index_cols vals);
   foreach my $arg ( @required_args ) {
      die "I need a $arg argument" unless $args{$arg};
   }
   my ($tbl, $index, $n_index_cols, $vals) = @args{@required_args};

   my $q = $self->{Quoter};

   my $index_struct = $tbl->{tbl_struct}->{keys}->{$index};
   my $index_cols   = $index_struct->{cols};

   my @where;
   if ( $n_index_cols > 1 ) {
      foreach my $n ( 0..($n_index_cols - 2) ) {
         my $col = $index_cols->[$n];
         my $val = $tbl->{tbl_struct}->{type_for}->{$col} eq 'enum' ? "CAST(? AS UNSIGNED)" : "?";
         push @where, $q->quote($col) . " = " . $val;
      }
   }

   my $col = $index_cols->[$n_index_cols - 1];
   my $val = $vals->[-1];  # should only be as many vals as cols
   my $condition = $tbl->{tbl_struct}->{type_for}->{$col} eq 'enum' ? "CAST(? AS UNSIGNED)" : "?";
   push @where, $q->quote($col) . " >= " . $condition;

   my $sql = "EXPLAIN SELECT /*!40001 SQL_NO_CACHE */ * "
           . "FROM $tbl->{name} FORCE INDEX (" . $q->quote($index) . ") "
           . "WHERE " . join(' AND ', @where)
           . " /*key_len*/";
   return $sql;
}

sub _d {
   my ($package, undef, $line) = caller 0;
   @_ = map { (my $temp = $_) =~ s/\n/\n# /g; $temp; }
        map { defined $_ ? $_ : 'undef' }
        @_;
   print STDERR "# $package:$line $PID ", join(' ', @_), "\n";
}

1;
}
# ###########################################################################
# End IndexLength package
# ###########################################################################

# ###########################################################################
# Runtime package
# This package is a copy without comments from the original.  The original
# with comments and its test file can be found in the GitHub repository at,
#   lib/Runtime.pm
#   t/lib/Runtime.t
# See https://github.com/percona/percona-toolkit for more information.
# ###########################################################################
{
package Runtime;

use strict;
use warnings FATAL => 'all';
use English qw(-no_match_vars);
use constant PTDEBUG => $ENV{PTDEBUG} || 0;

sub new {
   my ( $class, %args ) = @_;
   my @required_args = qw(now);
   foreach my $arg ( @required_args ) {
      die "I need a $arg argument" unless exists $args{$arg};
   }

   my $run_time = $args{run_time};
   if ( defined $run_time ) {
      die "run_time must be > 0" if $run_time <= 0;
   }

   my $now = $args{now};
   die "now must be a callback" unless ref $now eq 'CODE';

   my $self = {
      run_time   => $run_time,
      now        => $now,
      start_time => undef,
      end_time   => undef,
      time_left  => undef,
      stop       => 0,
   };

   return bless $self, $class;
}

sub time_left {
   my ( $self, %args ) = @_;

   if ( $self->{stop} ) {
      PTDEBUG && _d("No time left because stop was called");
      return 0;
   }

   my $now = $self->{now}->(%args);
   PTDEBUG && _d("Current time:", $now);

   if ( !defined $self->{start_time} ) {
      $self->{start_time} = $now;
   }

   return unless defined $now;

   my $run_time = $self->{run_time};
   return unless defined $run_time;

   if ( !$self->{end_time} ) {
      $self->{end_time} = $now + $run_time;
      PTDEBUG && _d("End time:", $self->{end_time});
   }

   $self->{time_left} = $self->{end_time} - $now;
   PTDEBUG && _d("Time left:", $self->{time_left});
   return $self->{time_left};
}

sub have_time {
   my ( $self, %args ) = @_;
   my $time_left = $self->time_left(%args);
   return 1 if !defined $time_left;  # run forever
   return $time_left <= 0 ? 0 : 1;   # <=0s means run time has elapsed
}

sub time_elapsed {
   my ( $self, %args ) = @_;

   my $start_time = $self->{start_time};
   return 0 unless $start_time;

   my $now = $self->{now}->(%args);
   PTDEBUG && _d("Current time:", $now);

   my $time_elapsed = $now - $start_time;
   PTDEBUG && _d("Time elapsed:", $time_elapsed);
   if ( $time_elapsed < 0 ) {
      warn "Current time $now is earlier than start time $start_time";
   }
   return $time_elapsed;
}

sub reset {
   my ( $self ) = @_;
   $self->{start_time} = undef;
   $self->{end_time}   = undef;
   $self->{time_left}  = undef;
   $self->{stop}       = 0;
   PTDEBUG && _d("Reset run time");
   return;
}

sub stop {
   my ( $self ) = @_;
   $self->{stop} = 1;
   return;
}

sub start {
   my ( $self ) = @_;
   $self->{stop} = 0;
   return;
}

sub _d {
   my ($package, undef, $line) = caller 0;
   @_ = map { (my $temp = $_) =~ s/\n/\n# /g; $temp; }
        map { defined $_ ? $_ : 'undef' }
        @_;
   print STDERR "# $package:$line $PID ", join(' ', @_), "\n";
}

1;
}
# ###########################################################################
# End Runtime package
# ###########################################################################

# ###########################################################################
# This is a combination of modules and programs in one -- a runnable module.
# http://www.perl.com/pub/a/2006/07/13/lightning-articles.html?page=last
# Or, look it up in the Camel book on pages 642 and 643 in the 3rd edition.
#
# Check at the end of this package for the call to main() which actually runs
# the program.
# ###########################################################################
package pt_table_checksum;

use strict;
use warnings FATAL => 'all';
use English qw(-no_match_vars);

use Percona::Toolkit;
use constant PTDEBUG => $ENV{PTDEBUG} || 0;

use POSIX qw(signal_h);
use List::Util qw(max);
use Time::HiRes qw(sleep time);
use Data::Dumper;
use Carp;
$Data::Dumper::Indent    = 1;
$Data::Dumper::Sortkeys  = 1;
$Data::Dumper::Quotekeys = 0;

use sigtrap 'handler', \&sig_int, 'normal-signals';

my $oktorun      = 1;
my $print_header = 1;
my $exit_status  = 0;
my $original_qrt_plugin_master_status = undef;

# "exit codes 1 - 2, 126 - 165, and 255 [1] have special meanings,
# and should therefore be avoided for user-specified exit parameters"
# http://www.tldp.org/LDP/abs/html/exitcodes.html
our %PTC_EXIT_STATUS = (
   # General flags:
   ERROR               => 1,
   ALREADY_RUNNING     => 2,
   CAUGHT_SIGNAL       => 4,
   NO_SLAVES_FOUND     => 8,
   # Tool-specific flags:
   TABLE_DIFF          => 16,
   SKIP_CHUNK          => 32,
   SKIP_TABLE          => 64,
   REPLICATION_STOPPED => 128,
);

# The following two hashes are used in exec_nibble().
# They're static, so they do not need to be reset in main().
# See also https://bugs.launchpad.net/percona-toolkit/+bug/919499

# Completely ignore these error codes.
my %ignore_code = (
   # Error: 1592 SQLSTATE: HY000  (ER_BINLOG_UNSAFE_STATEMENT)
   # Message: Statement may not be safe to log in statement format.
   # Ignore this warning because we have purposely set statement-based
   # replication.
   1592 => 1,
   1300 => 1,
);

# Warn once per-table for these error codes if the error message
# matches the pattern.
my %warn_code = (
   # Error: 1265 SQLSTATE: 01000 (WARN_DATA_TRUNCATED)
   # Message: Data truncated for column '%s' at row %ld
   1265 => {
      # any pattern
      # use MySQL's message for this warning
   },
   1406 => {
      # any pattern
      # use MySQL's message for this warning
   },
);

sub main {
   # Reset global vars else tests will fail in strange ways.
   local @ARGV   = @_;
   $oktorun      = 1;
   $print_header = 1;
   $exit_status  = 0;


   # ########################################################################
   # Get configuration information.
   # ########################################################################
   my $o = new OptionParser();
   $o->get_specs();
   $o->get_opts();

   my $dp = $o->DSNParser();
   $dp->prop('set-vars', $o->set_vars());

   # Add the --replicate table to --ignore-tables.
   my %ignore_tables = (
      %{$o->get('ignore-tables')},
      $o->get('replicate') => 1,
   );
   $o->set('ignore-tables', \%ignore_tables);

   $o->set('chunk-time', 0) if $o->got('chunk-size');

   foreach my $opt ( qw(max-load critical-load) ) {
      next unless $o->has($opt);
      my $spec = $o->get($opt);
      eval {
         MySQLStatusWaiter::_parse_spec($o->get($opt));
      };
      if ( $EVAL_ERROR ) {
         chomp $EVAL_ERROR;
         $o->save_error("Invalid --$opt: $EVAL_ERROR");
      }
   }

   # https://bugs.launchpad.net/percona-toolkit/+bug/1010232
   my $n_chunk_index_cols = $o->get('chunk-index-columns');
   if ( defined $n_chunk_index_cols
        && (!$n_chunk_index_cols
            || $n_chunk_index_cols =~ m/\D/
            || $n_chunk_index_cols < 1) ) {
      $o->save_error('Invalid number of --chunk-index columns: '
         . $n_chunk_index_cols);
   }

   if ( !$o->get('help') ) {
      if ( @ARGV > 1 ) {
         $o->save_error("More than one host specified; only one allowed");
      }

      if ( ($o->get('replicate') || '') !~ m/[\w`]\.[\w`]/ ) {
         $o->save_error('The --replicate table must be database-qualified');
      }

      if ( my $limit = $o->get('chunk-size-limit') ) {
         if ( $limit < 0 || ($limit > 0 && $limit < 1) ) {
            $o->save_error('--chunk-size-limit must be >= 1 or 0 to disable');
         }
      }

      if ( $o->get('progress') ) {
         eval { Progress->validate_spec($o->get('progress')) };
         if ( $EVAL_ERROR ) {
            chomp $EVAL_ERROR;
            $o->save_error("--progress $EVAL_ERROR");
         }
      }
   }

   my $autodiscover_cluster;
   my $recursion_method = [];
   foreach my $method ( @{$o->get('recursion-method')} ) {
      if ( $method eq 'cluster' ) {
         $autodiscover_cluster = 1;
      }
      else {
         push @$recursion_method, $method
      }
   }
   $o->set('recursion-method', $recursion_method);
   eval {
      MasterSlave::check_recursion_method($o->get('recursion-method'));
   };
   if ( $EVAL_ERROR ) {
      $o->save_error($EVAL_ERROR)
   }

   $o->usage_or_errors();


   if ( $o->get('truncate-replicate-table') && $o->get('resume') ) {
       die "--resume and truncate-replicate-table are mutually exclusive";
   }

   if ( $o->get('truncate-replicate-table') && !$o->get('empty-replicate-table') ) {
       die "--resume and --no-empty-replicate-table are mutually exclusive";
   }

   # ########################################################################
   # If --pid, check it first since we'll die if it already exists.
   # ########################################################################
   # We're not daemoninzing, it just handles PID stuff.  Keep $daemon
   # in the the scope of main() because when it's destroyed it automatically
   # removes the PID file.
   my $pid_file = $o->get('pid');
   my $daemon = new Daemon(
      pid_file => $pid_file,
   );
   eval {
      $daemon->run();
   };
   if ( my $e = $EVAL_ERROR ) {
      # TODO quite hackish but it should work for now
      if ( $e =~ m/PID file $pid_file exists/ ) {
         $exit_status |= $PTC_EXIT_STATUS{ALREADY_RUNNING};
         warn "$e\n";
         return $exit_status;
      }
      else {
         die $e;
      }
   }

   # ########################################################################
   # Connect to the master.
   # ########################################################################

   my $set_on_connect = sub {
      my ($dbh) = @_;
      return if $o->get('explain');
      my $sql;

      # https://bugs.launchpad.net/percona-toolkit/+bug/1019479
      # sql_mode ONLY_FULL_GROUP_BY often raises error even when query is
      # safe and deterministic. It's best to turn it off for the session
      # at this point.
      $sql = 'SELECT @@SQL_MODE';
      PTDEBUG && _d($dbh, $sql);
      my ($sql_mode) = eval { $dbh->selectrow_array($sql) };
      if ( $EVAL_ERROR ) {
         die "Error getting the current SQL_MODE: $EVAL_ERROR";
      }
      $sql_mode =~ s/ONLY_FULL_GROUP_BY//i;
      $sql = qq[SET SQL_MODE='$sql_mode'];
      PTDEBUG && _d($dbh, $sql);
      eval { $dbh->do($sql) };
      if ( $EVAL_ERROR ) {
         die "Error setting SQL_MODE"
           . ": $EVAL_ERROR";
      }


      # https://bugs.launchpad.net/percona-toolkit/+bug/919352
      # The tool shouldn't blindly attempt to change binlog_format;
      # instead, it should check if it's already set to STATEMENT.
      # This is becase starting with MySQL 5.1.29, changing the format
      # requires a SUPER user.
      if ( VersionParser->new($dbh) >= '5.1.5' ) {
         $sql = 'SELECT @@binlog_format';
         PTDEBUG && _d($dbh, $sql);
         my ($original_binlog_format) = $dbh->selectrow_array($sql);
         PTDEBUG && _d('Original binlog_format:', $original_binlog_format);
         if ( $original_binlog_format !~ /STATEMENT/i ) {
            $sql = q{/*!50108 SET @@binlog_format := 'STATEMENT'*/};
            eval {
               PTDEBUG && _d($dbh, $sql);
               $dbh->do($sql);
            };
            if ( $EVAL_ERROR ) {
               die "Failed to $sql: $EVAL_ERROR\n"
                  . "This tool requires binlog_format=STATEMENT, "
                  . "but the current binlog_format is set to "
                  ."$original_binlog_format and an error occurred while "
                  . "attempting to change it.  If running MySQL 5.1.29 or newer, "
                  . "setting binlog_format requires the SUPER privilege.  "
                  . "You will need to manually set binlog_format to 'STATEMENT' "
                  . "before running this tool.\n";
            }
         }
      }

      # Set transaction isolation level. We set binlog_format to STATEMENT,
      # but if the transaction isolation level is set to READ COMMITTED and the
      # --replicate table is in InnoDB format, the tool fails with the following
      # message:
      #
      # Binary logging not possible. Message: Transaction level 'READ-COMMITTED'
      # in InnoDB is not safe for binlog mode 'STATEMENT'
      #
      # See also http://code.google.com/p/maatkit/issues/detail?id=720
      $sql = 'SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ';
      eval {
         PTDEBUG && _d($dbh, $sql);
         $dbh->do($sql);
      };
      if ( $EVAL_ERROR ) {
         die "Failed to $sql: $EVAL_ERROR\n"
            . "If the --replicate table is InnoDB and the default server "
            . "transaction isolation level is not REPEATABLE-READ then "
            . "checksumming may fail with errors such as \"Binary logging not "
            . "possible. Message: Transaction level 'READ-COMMITTED' in "
            . "InnoDB is not safe for binlog mode 'STATEMENT'\".  In that "
            . "case you will need to manually set the transaction isolation "
            . "level to REPEATABLE-READ.\n";
      }


      return;
   };

   # Do not call "new Cxn(" directly; use this sub so that set_on_connect
   # is applied to every cxn.
   # TODO: maybe this stuff only needs to be set on master cxn?
   my $make_cxn = sub {
      my (%args) = @_;
      my $cxn = new Cxn(
         %args,
         DSNParser    => $dp,
         OptionParser => $o,
         set          => $args{set_vars} ? $set_on_connect : undef,
      );
      eval { $cxn->connect() };  # connect or die trying
      if ( $EVAL_ERROR ) {
         die ts($EVAL_ERROR);
      }
      return $cxn;
   };

   # The dbh and dsn can be used before checksumming starts, but once
   # inside the main TABLE loop, only use the master cxn because its
   # dbh may be recreated.
   my $master_cxn = $make_cxn->(set_vars => 1, dsn_string => shift @ARGV);
   my $master_dbh = $master_cxn->dbh();  # just for brevity
   my $master_dsn = $master_cxn->dsn();  # just for brevity

   if ($o->get('disable-qrt-plugin')) {
       eval {
           $master_dbh->selectrow_arrayref('SELECT @@query_response_time_session_stats' );
       };
       if ($EVAL_ERROR) {
           $original_qrt_plugin_master_status = undef;
           PTDEBUG && _d('QRT plugin is not installed: '.$EVAL_ERROR);
       } else {
           ($original_qrt_plugin_master_status) = $master_dbh->selectrow_arrayref('SELECT @@query_response_time_stats' );
           PTDEBUG && _d("Disabling qrt plugin on master server");
           $master_dbh->do('SET GLOBAL query_response_time_stats = off');
       }
   }

   my @ignored_engines = keys %{$o->get('ignore-engines')};
   my @rocksdb_ignored = grep(/^ROCKSDB$/i, @ignored_engines);
   if (!@rocksdb_ignored) {
       print STDOUT "Checking if all tables can be checksummed ...\n";
       my $mysql_config = MySQLConfig->new(dbh => $master_dbh);
       my $has_rocksdb = $mysql_config->has_engine('ROCKSDB');
       if ($has_rocksdb) {
           my $sql = "SELECT DISTINCT `table_name`, `table_schema`, `engine` FROM `information_schema`.`tables` " .
                     " WHERE `table_schema` NOT IN ('mysql', 'information_schema', 'performance_schema') " .
                     "   AND `engine` LIKE 'ROCKSDB'";
           my $rows = $master_dbh->selectall_arrayref($sql, {Slice=>{}});
           my $not_ignored_rocks_db_tables_count= scalar @$rows;
           if (@$rows) {
               my ($tables_list, $separator) = ('', '');
               for my $row (@$rows) {
                   $tables_list .= $separator.$row->{table_schema}.".".$row->{table_name};
                   $separator = ", ";
                   if ($o->get('ignore-tables')->{"$row->{table_schema}.$row->{table_name}"}) {
                       $not_ignored_rocks_db_tables_count--;
                   }
               }
               if ($not_ignored_rocks_db_tables_count > 0) {
                   print STDERR "\nThe RocksDB storage engine is not supported with pt-table-checksum " .
                                "since RocksDB does not support binlog_format=STATEMENT.\n".
                                "We have identified the following tables using MyRocks storage engine:\n";
                   for my $row (@$rows) {
                      print "$row->{table_schema}.$row->{table_name}\n";
                   }
                   print STDERR "\nPlease add ROCKSDB to the list of --ignore-engines\n";
                   print STDERR "--ignore-engines=FEDERATED,MRG_MyISAM,RocksDB\n";
                   print STDERR "\nConversely exclude the MyRocks tables explicitly:\n";
                   print STDERR "--ignore-tables=$tables_list\n\n";
                   print STDERR "Aborting";
                   exit($PTC_EXIT_STATUS{SKIP_TABLE});
               }
           }
       }
       print STDOUT "Starting checksum ...\n";
   }
   # ########################################################################
   # Set up the run time, if any.  Anything that waits should check this
   # between waits, else this will happen:
   # https://bugs.launchpad.net/percona-toolkit/+bug/1043438
   # ########################################################################
   my $have_time;
   if ( my $run_time = $o->get('run-time') ) {
      my $rt = Runtime->new(
         now      => sub { return time; },
         run_time => $run_time,
      );
      $have_time = sub { return $rt->have_time(); };
   }
   else {
      $have_time = sub { return 1; };
   }

   # ########################################################################
   # Set up PXC stuff.
   # ########################################################################
   my $cluster = Percona::XtraDB::Cluster->new();
   my %cluster_name_for;
   $cluster_name_for{$master_cxn} = $cluster->is_cluster_node($master_cxn);

   if ( $cluster_name_for{$master_cxn} ) {
      # Because of https://bugs.launchpad.net/codership-mysql/+bug/1040108
      # ptc and pt-osc check Threads_running by default for --max-load.
      # Strictly speaking, they can run on 5.5.27 as long as that bug doesn't
      # manifest itself.  If it does, however, then the tools will wait forever.
      my $pxc_version = VersionParser->new($master_dbh);
      if ( $pxc_version < '5.5.28' ) {
         die "Percona XtraDB Cluster 5.5.28 or newer is required to run "
            . "this tool on a cluster, but node " . $master_cxn->name
            . " is running version " . $pxc_version->version
            . ".  Please upgrade the node, or run the tool on a newer node, "
            . "or contact Percona for support.\n";
      }
   }

   # ########################################################################
   # If this is not a dry run (--explain was not specified), then we're
   # going to checksum the tables, so do the necessary preparations and
   # checks.  Else, this all can be skipped because all we need for a
   # dry run is a connection to the master.
   # ########################################################################
   my $q  = new Quoter();
   my $tp = new TableParser(Quoter => $q);
   my $rc = new RowChecksum(Quoter=> $q, OptionParser => $o);
   my $ms = new MasterSlave(
      OptionParser => $o,
      DSNParser    => $dp,
      Quoter       => $q,
      channel      => $o->get('channel')
   );

   my $slaves = [];    # all slaves (that we can find)
   my $slave_lag_cxns; # slaves whose lag we'll check

   # ########################################################################
   # Create --plugin.
   # ########################################################################
   my $plugin;
   if ( my $file = $o->get('plugin') ) {
      die "--plugin file $file does not exist\n" unless -f $file;
      eval {
         require $file;
      };
      die "Error loading --plugin $file: $EVAL_ERROR" if $EVAL_ERROR;
      eval {
         $plugin = pt_table_checksum_plugin->new(
            master_cxn  => $master_cxn,
            explain     => $o->get('explain'),
            quiet       => $o->get('quiet'),
            resume      => $o->get('resume'),
            Quoter      => $q,
            TableParser => $tp,
         );
      };
      die "Error creating --plugin: $EVAL_ERROR" if $EVAL_ERROR;
      print "Created plugin from $file.\n";
   }

   my $replica_lag;    # ReplicaLagWaiter object
   my $replica_lag_pr; # Progress for ReplicaLagWaiter
   my $sys_load;       # MySQLStatusWaiter object
   my $sys_load_pr;    # Progress for MySQLStatusWaiter object

   my $repl_table = $q->quote($q->split_unquote($o->get('replicate')));
   my $fetch_sth;  # fetch chunk from repl table
   my $update_sth; # update master_cnt and master_cnt in repl table
   my $delete_sth; # delete checksums for one db.tbl from repl table

   if ( $o->get('truncate-replicate-table') ) {
       eval {
           $master_dbh->do("TRUNCATE TABLE $repl_table");
       };
       if ($EVAL_ERROR) {
           PTDEBUG && _d( "Cannot truncate replicate table $repl_table. $EVAL_ERROR");
       }
   }

   if ( !$o->get('explain') ) {
      # #####################################################################
      # Find and connect to slaves.
      # #####################################################################
      my $make_cxn_cluster = sub {
         my $cxn = $make_cxn->(@_, prev_dsn => $master_cxn->dsn());
         $cluster_name_for{$cxn} = $cluster->is_cluster_node($cxn);
         return $cxn;
      };

      $slaves = $ms->get_slaves(
         dbh      => $master_dbh,
         dsn      => $master_dsn,
         make_cxn => $make_cxn_cluster,
      );

      my %seen_ids;
      for my $cxn ($master_cxn, @$slaves) {
         my $dbh  = $cxn->dbh();
         # get server/node unique id ( https://bugs.launchpad.net/percona-toolkit/+bug/1217466 )
         my $id = $cxn->get_id();
         $seen_ids{$id}++;
      }

      if ( $autodiscover_cluster ) {
         my @known_nodes = grep { $cluster_name_for{$_} } $master_cxn, @$slaves;
         my $new_cxns = $cluster->autodetect_nodes(
                           nodes       => \@known_nodes,
                           MasterSlave => $ms,
                           DSNParser   => $dp,
                           make_cxn    => $make_cxn_cluster,
                           seen_ids    => \%seen_ids,
                        );
         push @$slaves, @$new_cxns;
      }

      my $trimmed_nodes = Cxn->remove_duplicate_cxns(
         cxns => [ $master_cxn, @$slaves ],
      );
      ($master_cxn, @$slaves) = @$trimmed_nodes;

      # If no slaves or nodes were found, and a recursion method was given
      # (implicitly or explicitly), and that method is not none, then warn
      # and continue but exit non-zero because there won't be any diffs but
      # this could be a false-positive from having no slaves/nodes to check.
      # https://bugs.launchpad.net/percona-toolkit/+bug/1210537
      PTDEBUG && _d(scalar @$slaves, 'slaves found');
      if ( !@$slaves
           && (($o->get('recursion-method')->[0] || '') ne 'none'
               || $autodiscover_cluster))
      {
         $exit_status |= $PTC_EXIT_STATUS{NO_SLAVES_FOUND};
         if ( $o->get('quiet') < 2 ) {
            my $type = $autodiscover_cluster ? 'cluster nodes' : 'slaves';
            warn "Diffs cannot be detected because no $type were found.  "
               . "Please read the --recursion-method documentation for "
               . "information.\n";
         }
      }


      # https://bugs.launchpad.net/percona-toolkit/+bug/938068
      if ( $o->get('check-binlog-format') ) {
         my $master_binlog = 'STATEMENT';
         if ( VersionParser->new($master_dbh) >= '5.1.5' ) {
            ($master_binlog) = $master_dbh->selectrow_array(
               'SELECT @@binlog_format');
         }

         my $err = '';
         for my $slave_cxn ( @$slaves ) {
            # https://bugs.launchpad.net/percona-toolkit/+bug/1080385
            next if $cluster_name_for{$slave_cxn};

            my $slave_binlog = 'STATEMENT';
            if ( VersionParser->new($slave_cxn->dbh) >= '5.1.5' ) {
               ($slave_binlog) = $slave_cxn->dbh->selectrow_array(
                  'SELECT @@binlog_format');
            }

            if ( $master_binlog ne $slave_binlog ) {
               $err .= "Replica " . $slave_cxn->name()
                  . qq{ has binlog_format $slave_binlog which could cause }
                  . qq{pt-table-checksum to break replication.  Please read }
                  . qq{"Replicas using row-based replication" in the }
                  . qq{LIMITATIONS section of the tool's documentation.  }
                  . qq{If you understand the risks, specify }
                  . qq{--no-check-binlog-format to disable this check.\n};
            }
         }
         die $err if $err;
      }

      if ( $cluster_name_for{$master_cxn} ) {
         if ( !@$slaves ) {
            if ( ($o->get('recursion-method')->[0] || '') ne 'none' ) {
               die $master_cxn->name() . " is a cluster node but no other nodes "
                  . "or regular replicas were found.  Use --recursion-method=dsn "
                  . "to specify the other nodes in the cluster.\n";
            }
         }

         # Make sure the master and all node are in the same cluster.
         my @other_cluster;
         foreach my $slave ( @$slaves ) {
            next unless $cluster_name_for{$slave};
            if ( $cluster_name_for{$master_cxn} ne $cluster_name_for{$slave}) {
               push @other_cluster, $slave;
            }
         }
         if ( @other_cluster ) {
            die $master_cxn->name . " is in cluster "
               . $cluster_name_for{$master_cxn} . " but these nodes are "
               . "in other clusters:\n"
               . join("\n",
                  map {'  ' . $_->name . " is in cluster $cluster_name_for{$_}"}
                  @other_cluster) . "\n"
               . "All nodes must be in the same cluster.  "
               . "For more information, please read the Percona XtraDB "
               . "Cluster section of the tool's documentation.\n";
         }
      }
      elsif ( @$slaves ) {
         # master is not a cluster node, but what about the slaves?
         my $direct_slave;  # master -> direct_slave
         my @slaves;        # traditional slaves
         my @nodes;         # cluster nodes
         foreach my $slave ( @$slaves ) {
            if ( !$cluster_name_for{$slave} ) {
               push @slaves, $slave;
               next;
            }

            my $is_master_of = eval {
               $ms->is_master_of($master_cxn->dbh, $slave->dbh);
            };
            if ( $EVAL_ERROR && $EVAL_ERROR =~ m/is not a slave/ ) {
               push @nodes, $slave;
            }
            elsif ( $is_master_of ) {
               $direct_slave = $slave;
            }
            else {
               # Another error could have happened but we don't really
               # care.  We know for sure the slave is a node, so just
               # presume that and carry on.
               push @nodes, $slave;
            }
         }

         my $err = '';
         if ( @nodes ) {
            if ( $direct_slave ) {
               warn "Diffs will only be detected if the cluster is "
                  . "consistent with " . $direct_slave->name . " because "
                  . $master_cxn->name . " is a traditional replication master "
                  . "but these replicas are cluster nodes:\n"
                  . join("\n", map { '  ' . $_->name } @nodes) . "\n"
                  . "For more information, please read the Percona XtraDB "
                  . "Cluster section of the tool's documentation.\n";
            }
            else {
               warn "Diffs may not be detected on these cluster nodes "
                  . "because the direct replica of " . $master_cxn->name
                  . " was not found or specified:\n"
                  . join("\n", map { '  ' . $_->name } @nodes) . "\n"
                  . "For more information, please read the Percona XtraDB "
                  . "Cluster section of the tool's documentation.\n";
            }

            if ( @slaves ) {
               warn "Diffs will only be detected on these replicas if "
                  . "they replicate from " . $master_cxn->name . ":\n"
                  . join("\n", map { '  ' . $_->name } @slaves) . "\n"
                  . "For more information, please read the Percona XtraDB "
                  . "Cluster section of the tool's documentation.\n";
            }
         }
      }

      # don't touch the QRT plugin on the slave unless we asked for it
      # to be disabled.
      if ($o->get('disable-qrt-plugin')) {
          for my $slave (@$slaves) {
              my $qrt_plugin_status;
              eval {
                  ($qrt_plugin_status) = $slave->{dbh}->selectrow_arrayref('SELECT @@QUERY_RESPONSE_TIME_SESSION_STATS' );
              };
              if ($EVAL_ERROR) {
                  PTDEBUG && _d('QRT plugin is not installed on slave '.$slave->{dsn_name});
                  $slave->{qrt_plugin_status} = undef;
                  next;
              }
              $slave->{qrt_plugin_status} = $qrt_plugin_status->[0];
              if ($slave->{qrt_plugin_status}) {
                  PTDEBUG && _d("Disabling qrt plugin state on slave ".$slave->{dsn_name});
                  $slave->{dbh}->do('SET GLOBAL query_response_time_stats = off');
              }
          }
      }

      if ( $o->get('check-slave-lag') ) {
         PTDEBUG && _d('Will use --check-slave-lag to check for slave lag');
         my $cxn = $make_cxn->(
            dsn_string => $o->get('check-slave-lag'),
            prev_dsn   => $master_cxn->dsn(),
         );
         $slave_lag_cxns = [ $cxn ];
      }
      else {
         PTDEBUG && _d('Will check slave lag on all slaves');
         $slave_lag_cxns = [ map { $_ } @$slaves ];
      }

      # Cluster nodes aren't slaves, so SHOW SLAVE STATUS doesn't work.
      # Nodes shouldn't be out of sync anyway because the cluster is
      # (virtually) synchronous, so waiting for the last checksum chunk
      # to appear should be sufficient.
      @$slave_lag_cxns = grep {
         my $slave_cxn = $_;
         if ( $cluster_name_for{$slave_cxn} ) {
            warn "Not checking replica lag on " . $slave_cxn->name()
               . " because it is a cluster node.\n";
            0;
         }
         else {
            PTDEBUG && _d('May check slave lag on', $slave_cxn->name());
            $slave_cxn;
         }
      } @$slave_lag_cxns;

      if ( $slave_lag_cxns && scalar @$slave_lag_cxns ) {
         if ($o->get('skip-check-slave-lag')) {
             my $slaves_to_skip = $o->get('skip-check-slave-lag');
             my $filtered_slaves = [];
             for my $slave (@$slave_lag_cxns) {
                 my $found=0;
                 for my $slave_to_skip (@$slaves_to_skip) {
                     my $h_eq_h = $slave->{dsn}->{h} eq $slave_to_skip->{h};
                     my $p_eq_p;
                     if (defined($slave->{dsn}->{P}) || defined($slave_to_skip->{P})) {
                       $p_eq_p = $slave->{dsn}->{P} eq $slave_to_skip->{P};
                     } else {
                       PTDEBUG && _d("Both port DSNs are undefined, setting p_eq_p to true");
                       $p_eq_p = 1;
                     }
                     if ($h_eq_h && $p_eq_p) {
                         $found=1;
                     }
                 }
                 if ($found) {
                    printf("Skipping slave %s\n", $slave->name());
                 } else {
                    push @$filtered_slaves, $slave;
                }
             }
             $slave_lag_cxns = $filtered_slaves;
         }
      }

      # #####################################################################
      # Possibly check replication slaves and exit.
      # #####################################################################
      if ( $o->get('replicate-check') && $o->get('replicate-check-only') ) {
         PTDEBUG && _d('Will --replicate-check and exit');

         # --plugin hook
         if ( $plugin && $plugin->can('before_replicate_check') ) {
            $plugin->before_replicate_check();
         }

         foreach my $slave ( @$slaves ) {
            my $diffs = $rc->find_replication_differences(
               dbh        => $slave->dbh(),
               repl_table => $repl_table,
            );
            PTDEBUG && _d(scalar @$diffs, 'checksum diffs on',
               $slave->name());
            $diffs = filter_tables_replicate_check_only($diffs, $o);
            if ( @$diffs ) {
               $exit_status |= $PTC_EXIT_STATUS{TABLE_DIFF};
               if ( $o->get('quiet') < 2 ) {
                  print_checksum_diffs(
                     cxn   => $slave,
                     diffs => $diffs,
                  );
               }
            }
         }

         # --plugin hook
         if ( $plugin && $plugin->can('after_replicate_check') ) {
            $plugin->after_replicate_check();
         }

         PTDEBUG && _d('Exit status', $exit_status, 'oktorun', $oktorun);
         return $exit_status;
      }

      # #####################################################################
      # Check for replication filters.
      # #####################################################################
      if ( $o->get('check-replication-filters') ) {
         PTDEBUG && _d("Checking slave replication filters");
         my @all_repl_filters;
         foreach my $slave ( @$slaves ) {
            my $repl_filters = $ms->get_replication_filters(
               dbh => $slave->dbh(),
            );
            if ( keys %$repl_filters ) {
               push @all_repl_filters,
                  { name    => $slave->name(),
                    filters => $repl_filters,
                  };
            }
         }
         if ( @all_repl_filters ) {
            my $msg = "Replication filters are set on these hosts:\n";
            foreach my $host ( @all_repl_filters ) {
               my $filters = $host->{filters};
               $msg .= "  $host->{name}\n"
                     . join("\n", map { "    $_ = $host->{filters}->{$_}" }
                            keys %{$host->{filters}})
                     . "\n";
            }
            $msg .= "Please read the --check-replication-filters documentation "
                  . "to learn how to solve this problem.";
            die ts($msg);
         }
      }

      # #####################################################################
      # Check that the replication table exists, or possibly create it.
      # #####################################################################
      eval {
         check_repl_table(
            dbh          => $master_dbh,
            repl_table   => $repl_table,
            slaves       => $slaves,
            have_time    => $have_time,
            OptionParser => $o,
            TableParser  => $tp,
            Quoter       => $q,
         );
      };
      if ( $EVAL_ERROR ) {
         die ts($EVAL_ERROR);
      }

      # #####################################################################
      # Make a ReplicaLagWaiter to help wait for slaves after each chunk.
      # #####################################################################
      my $sleep = sub {
         # Don't let the master dbh die while waiting for slaves because we
         # may wait a very long time for slaves.

         # This is called from within the main TABLE loop, so use the
         # master cxn; do not use $master_dbh.
         my $dbh = $master_cxn->dbh();
         if ( !$dbh || !$dbh->ping() ) {
            PTDEBUG && _d('Lost connection to master while waiting for slave lag');
            eval { $dbh = $master_cxn->connect() };  # connect or die trying
            if ( $EVAL_ERROR ) {
               $oktorun = 0;  # Fatal error
               chomp $EVAL_ERROR;
               die "Lost connection to master while waiting for replica lag "
                  . "($EVAL_ERROR)";
            }
         }
         $dbh->do("SELECT 'pt-table-checksum keepalive'");
         sleep $o->get('check-interval');
         return;
      };

      my $get_lag;
      # The plugin is able to override the slavelag check so tools like
      # pt-heartbeat or other replicators (Tungsten...) can be used to
      # measure replication lag
      if ( $plugin && $plugin->can('get_slave_lag') ) {
         $get_lag = $plugin->get_slave_lag(oktorun => \$oktorun);
      } else {
         $get_lag = sub {
            my ($cxn) = @_;
            my $dbh = $cxn->dbh();
            if ( !$dbh || !$dbh->ping() ) {
               PTDEBUG && _d('Lost connection to slave', $cxn->name(),
                  'while waiting for slave lag');
               eval { $dbh = $cxn->connect() };
               if ( $EVAL_ERROR ) {
                  PTDEBUG && _d('Failed to connect to slave', $cxn->name(),
                     ':', $EVAL_ERROR);
                  return; # keep waiting and trying to reconnect
               }
            }
            my $slave_lag;
            eval {
               $slave_lag = $ms->get_slave_lag($dbh);
            };
            if ( $EVAL_ERROR ) {
               PTDEBUG && _d('Error getting slave lag', $cxn->name(),
                  ':', $EVAL_ERROR);
               return; # keep waiting and trying to reconnect
            }
            return $slave_lag;
         };
      }

      $replica_lag = new ReplicaLagWaiter(
         slaves   => $slave_lag_cxns,
         max_lag  => $o->get('max-lag'),
         oktorun  => sub { return $oktorun && $have_time->(); },
         get_lag  => $get_lag,
         sleep    => $sleep,
         fail_on_stopped_replication => $o->get('fail-on-stopped-replication'),
      );

      my $get_status;
      {
         my $sql = "SHOW GLOBAL STATUS LIKE ?";
         my $sth = $master_cxn->dbh()->prepare($sql);

         $get_status = sub {
            my ($var) = @_;
            PTDEBUG && _d($sth->{Statement}, $var);
            $sth->execute($var);
            my (undef, $val) = $sth->fetchrow_array();
            return $val;
         };
      }

      eval {
         $sys_load = new MySQLStatusWaiter(
            max_spec   => $o->get('max-load'),
            get_status => $get_status,
            oktorun    => sub { return $oktorun && $have_time->(); },
            sleep      => $sleep,
         );
      };
      if ( $EVAL_ERROR ) {
         chomp $EVAL_ERROR;
         die "Error checking --max-load: $EVAL_ERROR.  "
            . "Check that the variables specified for --max-load "
            . "are spelled correctly and exist in "
            . "SHOW GLOBAL STATUS.  Current value for this option is:\n"
            . "  --max-load " . (join(',', @{$o->get('max-load')})) . "\n";
      }

      if ( $o->get('progress') ) {
         $replica_lag_pr = new Progress(
            jobsize => scalar @$slaves,
            spec    => $o->get('progress'),
            name    => "Waiting for replicas to catch up",  # not used
         );

         $sys_load_pr = new Progress(
            jobsize => scalar @{$o->get('max-load')},
            spec    => $o->get('progress'),
            name    => "Waiting for --max-load", # not used
         );
      }

      # #####################################################################
      # Prepare statement handles to update the repl table on the master.
      # #####################################################################
      $fetch_sth = $master_dbh->prepare(
         "SELECT this_crc, this_cnt FROM $repl_table "
         . "WHERE db = ? AND tbl = ? AND chunk = ?");
      $update_sth = $master_dbh->prepare(
         "UPDATE $repl_table SET chunk_time = ?, master_crc = ?, master_cnt = ? "
         . "WHERE db = ? AND tbl = ? AND chunk = ?");
      $delete_sth = $master_dbh->prepare(
         "DELETE FROM $repl_table WHERE db = ? AND tbl = ?");
   } # !$o->get('explain')

   # ########################################################################
   # Do the version-check
   # ########################################################################
   if ( $o->get('version-check') && (!$o->has('quiet') || !$o->get('quiet')) ) {
      VersionCheck::version_check(
         force     => $o->got('version-check'),
         instances => [
            { dbh => $master_dbh, dsn => $master_dsn },
            map({ +{ dbh => $_->dbh(), dsn => $_->dsn() } } @$slaves)
         ],
      );
   }

   # ########################################################################
   # Checksum args and the DMS part of the checksum query for each table.
   # ########################################################################
   my %crc_args     = $rc->get_crc_args(dbh => $master_dbh);
   my $checksum_dml = "REPLACE INTO $repl_table "
                    . "(db, tbl, chunk, chunk_index,"
                    . " lower_boundary, upper_boundary, this_cnt, this_crc) "
                    . "SELECT"
                    . ($cluster->is_cluster_node($master_cxn) ? ' /*!99997*/' : '')
                    . " ?, ?, ?, ?, ?, ?,";
   my $past_cols    = " COUNT(*), '0'";

   # ########################################################################
   # Get last chunk for --resume.
   # ########################################################################
   my $last_chunk;
   if ( $o->get('resume') ) {
      $last_chunk = last_chunk(
         dbh        => $master_dbh,
         repl_table => $repl_table,
      );
   }

   my $schema_iter = new SchemaIterator(
      dbh          => $master_dbh,
      resume       => $last_chunk ? $q->quote(@{$last_chunk}{qw(db tbl)})
                                  : "",
      OptionParser => $o,
      TableParser  => $tp,
      Quoter       => $q,
   );

   if ( $last_chunk &&
        !$schema_iter->table_is_allowed(@{$last_chunk}{qw(db tbl)}) ) {
      PTDEBUG && _d('Ignoring last table', @{$last_chunk}{qw(db tbl)},
         'and resuming from next table');
      $last_chunk = undef;
   }

   # ########################################################################
   # Various variables and modules for checksumming the tables.
   # ########################################################################
   my $total_rows = 0;
   my $total_time = 0;
   my $total_rate = 0;
   my $tn         = new TableNibbler(TableParser => $tp, Quoter => $q);
   my $retry      = new Retry();

   # --chunk-size-limit has two purposes.  The 1st, as documented, is
   # to prevent oversized chunks when the chunk index is not unique.
   # The 2nd is to determine if the table can be processed in one chunk
   # (WHERE 1=1 instead of nibbling).  This creates a problem when
   # the user does --chunk-size-limit=0 to disable the 1st, documented
   # purpose because, apparently, they're using non-unique indexes and
   # they don't care about potentially large chunks.  But disabling the
   # 1st purpose adversely affects the 2nd purpose becuase 0 * the chunk size
   # will always be zero, so tables will only be single-chunked if EXPLAIN
   # says there are 0 rows, but sometimes EXPLAIN says there is 1 row
   # even when the table is empty.  This wouldn't matter except that nibbling
   # an empty table doesn't currently work becuase there are no boundaries,
   # so no checksum is written for the empty table.  To fix this and
   # preserve the two purposes of this option, usages of the 2nd purpose
   # do || 1 so the limit is never 0 and empty tables are single-chunked.
   # See:
   #   https://bugs.launchpad.net/percona-toolkit/+bug/987393
   #   https://bugs.launchpad.net/percona-toolkit/+bug/938660
   #   https://bugs.launchpad.net/percona-toolkit/+bug/987495
   # This is used for the 2nd purpose:
   my $chunk_size_limit = $o->get('chunk-size-limit') || 1;

   # ########################################################################
   # Callbacks for each table's nibble iterator.  All checksum work is done
   # in these callbacks and the subs that they call.
   # ########################################################################
   my $callbacks = {
      init => sub {
         my (%args) = @_;
         my $tbl         = $args{tbl};
         my $nibble_iter = $args{NibbleIterator};
         my $statements  = $nibble_iter->statements();
         my $oktonibble  = 1;

         if ( $last_chunk ) { # resuming
            if ( have_more_chunks(%args, last_chunk => $last_chunk) ) {
               $nibble_iter->set_nibble_number($last_chunk->{chunk});
               PTDEBUG && _d('Have more chunks; resuming from',
                  $last_chunk->{chunk}, 'at', $last_chunk->{ts});
               if ( !$o->get('quiet') ) {
                  print "Resuming from $tbl->{db}.$tbl->{tbl} chunk "
                     . "$last_chunk->{chunk}, timestamp $last_chunk->{ts}\n";
               }
            }
            else {
               # Problem resuming or no next lower boundary.
               PTDEBUG && _d('No more chunks; resuming from next table');
               $oktonibble = 0; # don't nibble table; next table
            }

            # Just need to call us once to kick-start the resume process.
            $last_chunk = undef;
         }

         if ( $o->get('check-slave-tables') ) {
            eval {
               check_slave_tables(
                  slaves        => $slaves,
                  db            => $tbl->{db},
                  tbl           => $tbl->{tbl},
                  checksum_cols => $tbl->{checksum_cols},
                  have_time     => $have_time,
                  TableParser   => $tp,
                  OptionParser  => $o,
               );
            };
            if ( $EVAL_ERROR ) {
               my $msg
                  = "Skipping table $tbl->{db}.$tbl->{tbl} because it has "
                  . "problems on these replicas:\n"
                  . $EVAL_ERROR
                  . "This can break replication.  If you understand the risks, "
                  . "specify --no-check-slave-tables to disable this check.\n";
               warn ts($msg);
               $exit_status |= $PTC_EXIT_STATUS{SKIP_TABLE};
               $oktonibble = 0;
           }
         }

         if ( $o->get('explain') ) {
            # --explain level 1: print the checksum and next boundary
            # statements.
            print "--\n",
                  "-- $tbl->{db}.$tbl->{tbl}\n",
                  "--\n\n";

            foreach my $sth ( sort keys %$statements ) {
               next if $sth =~ m/^explain/;
               if ( $statements->{$sth} ) {
                  print $statements->{$sth}->{Statement}, "\n\n";
               }
            }

            if ( $o->get('explain') < 2 ) {
               $oktonibble = 0; # don't nibble table; next table
            }
         }
         else {
            if ( $nibble_iter->one_nibble() ) {
               my @too_large;
               SLAVE:
               foreach my $slave ( @$slaves ) {
                  PTDEBUG && _d('Getting table row estimate on', $slave->name());
                  my $have_warned = 0;
                  while ( $oktorun && $have_time->() )  {
                     my $n_rows;
                     eval {
                        # TODO: This duplicates NibbleIterator::can_nibble();
                        # probably best to have 1 code path to determine if
                        # a given table is oversized on a given host.
                        ($n_rows) = NibbleIterator::get_row_estimate(
                           Cxn   => $slave,
                           tbl   => $tbl,
                           where => $o->get('where'),
                        );
                     };
                     if ( my $e = $EVAL_ERROR ) {
                        if ( $slave->lost_connection($e) ) {
                           PTDEBUG && _d($e);
                           eval { $slave->connect() };
                           if ( $EVAL_ERROR ) {
                              PTDEBUG && _d('Failed to connect to slave', $slave->name(),
                                 ':', $EVAL_ERROR);
                              if ( !$have_warned && $o->get('quiet') < 2 ) {
                                 my $msg = "Trying to connect to replica "
                                    . $slave->name() . " to get row count of"
                                    . " table $tbl->{db}.$tbl->{tbl}...\n";
                                 warn ts($msg);
                                 $have_warned = 1;
                              }
                              sleep 2;
                           }
                           next; # try again
                        }
                        die "Error getting row count estimate of table"
                           . " $tbl->{db}.$tbl->{tbl} on replica "
                           . $slave->name() . ": $e";
                     }
                     PTDEBUG && _d('Table on', $slave->name(), 'has', $n_rows, 'rows');
                     my $slave_skip_tolerance = $o->get('slave-skip-tolerance') || 1;
                     if ( $n_rows
                          && $n_rows > ($tbl->{chunk_size} * $chunk_size_limit) * $slave_skip_tolerance )
                     {
                        PTDEBUG && _d('Table too large on', $slave->name());
                        push @too_large, [$slave->name(), $n_rows || 0];
                     }
                     next SLAVE;
                  }
               }
               if ( @too_large ) {
                  if ( $o->get('quiet') < 2 ) {
                     my $msg
                        = "Skipping table $tbl->{db}.$tbl->{tbl} because"
                        . " on the master it would be checksummed in one chunk"
                        . " but on these replicas it has too many rows:\n";
                     foreach my $info ( @too_large ) {
                        $msg .= "  $info->[1] rows on $info->[0]\n";
                     }
                     $msg .= "The current chunk size limit is "
                           . ($tbl->{chunk_size} * $chunk_size_limit)
                           . " rows (chunk size=$tbl->{chunk_size}"
                           . " * chunk size limit=$chunk_size_limit).\n";
                     warn ts($msg);
                  }
                  $exit_status |= $PTC_EXIT_STATUS{SKIP_TABLE};
                  $oktonibble = 0;
               }
            }
            else { # chunking the table
               if ( $o->get('check-plan') ) {
                  my $idx_len = new IndexLength(Quoter => $q);
                  my ($key_len, $key) = $idx_len->index_length(
                     Cxn          => $args{Cxn},
                     tbl          => $tbl,
                     index        => $nibble_iter->nibble_index(),
                     n_index_cols => $o->get('chunk-index-columns'),
                  );
                  if ( !$key || lc($key) ne lc($nibble_iter->nibble_index()) ) {
                     die "Cannot determine the key_len of the chunk index "
                        . "because MySQL chose "
                        . ($key ? "the $key" : "no") . " index "
                        . "instead of the " . $nibble_iter->nibble_index()
                        . " index for the first lower boundary statement.  "
                        . "See --[no]check-plan in the documentation for more "
                        . "information.";
                  }
                  elsif ( !$key_len ) {
                     die "The key_len of the $key index is "
                        . (defined $key_len ? "zero" : "NULL")
                        . ", but this should not be possible.  "
                        . "See --[no]check-plan in the documentation for more "
                        . "information.";
                  }
                  $tbl->{key_len} = $key_len;
               }
            }

            if ( $oktonibble && $o->get('empty-replicate-table') ) {
               use_repl_db(
                  dbh          => $master_cxn->dbh(),
                  repl_table   => $repl_table,
                  OptionParser => $o,
                  Quoter       => $q,
               );
               PTDEBUG && _d($delete_sth->{Statement});
               $delete_sth->execute($tbl->{db}, $tbl->{tbl});
            }

            # USE the correct db while checksumming this table.  The "correct"
            # db is a complicated subject; see sub for comments.
            use_repl_db(
               dbh          => $master_cxn->dbh(),
               tbl          => $tbl, # XXX working on this table
               repl_table   => $repl_table,
               OptionParser => $o,
               Quoter       => $q,
            );
            # #########################################################
            # XXX DO NOT CHANGE THE DB UNTIL THIS TABLE IS FINISHED XXX
            # #########################################################
         }

         return $oktonibble; # continue nibbling table?
      },
      next_boundaries => sub {
         my (%args) = @_;
         my $tbl         = $args{tbl};
         my $nibble_iter = $args{NibbleIterator};
         my $sth         = $nibble_iter->statements();
         my $boundary    = $nibble_iter->boundaries();

         return 1 if $nibble_iter->one_nibble();

         # Check that MySQL will use the nibble index for the next upper
         # boundary sql.  This check applies to the next nibble.  So if
         # the current nibble number is 5, then nibble 5 is already done
         # and we're checking nibble number 6.

         # XXX This call and others like it are relying on a Perl oddity.
         # See https://bugs.launchpad.net/percona-toolkit/+bug/987393
         my $expl = explain_statement(
            tbl  => $tbl,
            sth  => $sth->{explain_upper_boundary},
            vals => [ @{$boundary->{lower}}, $nibble_iter->limit() ],
         );
         if (   lc($expl->{key} || '')
             ne lc($nibble_iter->nibble_index() || '') ) {
            PTDEBUG && _d('Cannot nibble next chunk, aborting table');
            if ( $o->get('quiet') < 2 ) {
               warn ts("Aborting table $tbl->{db}.$tbl->{tbl} at chunk "
                  . ($nibble_iter->nibble_number() + 1)
                  . " because it is not safe to chunk.  Chunking should "
                  . "use the "
                  . ($nibble_iter->nibble_index() || '?')
                  . " index, but MySQL chose "
                  . ($expl->{key} ? "the $expl->{key}" : "no")
                  . " index.\n");
            }
            $tbl->{checksum_results}->{errors}++;
            return 0; # stop nibbling table
         }

         # Once nibbling begins for a table, control does not return to this
         # tool until nibbling is done because, as noted above, all work is
         # done in these callbacks.  This callback is the only place where we
         # can prematurely stop nibbling by returning false.  This allows
         # Ctrl-C to stop the tool between nibbles instead of between tables.
         return $oktorun && $have_time->(); # continue nibbling table?
      },
      exec_nibble => sub {
         my (%args) = @_;
         my $tbl         = $args{tbl};
         my $nibble_iter = $args{NibbleIterator};
         my $sth         = $nibble_iter->statements();
         my $boundary    = $nibble_iter->boundaries();

         # Count every chunk, even if it's ultimately skipped, etc.
         $tbl->{checksum_results}->{n_chunks}++;

         # Reset the nibble_time because this nibble hasn't been
         # executed yet.  If nibble_time is undef, then it's marked
         # as skipped in after_nibble.
         $tbl->{nibble_time} = undef;

         # --explain level 2: print chunk,lower boundary values,upper
         # boundary values.
         if ( $o->get('explain') > 1 ) {
            my $chunk = $nibble_iter->nibble_number();
            if ( $nibble_iter->one_nibble() ) {
               printf "%d 1=1\n", $chunk;
            }
            else {
               # XXX This call and others like it are relying on a Perl oddity.
               # See https://bugs.launchpad.net/percona-toolkit/+bug/987393
               my $lb_quoted = join(
                  ',', map { defined $_ ? $_ : 'NULL'} @{$boundary->{lower}});
               my $ub_quoted = join(
                  ',', map { defined $_ ? $_ : 'NULL'} @{$boundary->{upper}});
               printf "%d %s %s\n", $chunk, $lb_quoted, $ub_quoted;
            }
            if ( !$nibble_iter->more_boundaries() ) {
               print "\n"; # blank line between this table and the next table
            }
            return 0;  # next boundary
         }

         # Skip this nibble unless it's safe.
         return 0 unless nibble_is_safe(
            %args,
            OptionParser => $o,
         );

         # Exec and time the nibble.
         $tbl->{nibble_time} = exec_nibble(
            %args,
            Retry        => $retry,
            Quoter       => $q,
            OptionParser => $o,
         );
         PTDEBUG && _d('Nibble time:', $tbl->{nibble_time});

         # We're executing REPLACE queries which don't return rows.
         # Returning 0 from this callback causes the nibble iter to
         # get the next boundaries/nibble.
         return 0;
      },
      after_nibble => sub {
         my (%args) = @_;
         my $tbl         = $args{tbl};
         my $nibble_iter = $args{NibbleIterator};

         # Don't need to do anything here if we're just --explain'ing.
         return if $o->get('explain');

         # Chunk/nibble number that we just inserted or skipped.
         my $chunk = $nibble_iter->nibble_number();

         # Nibble time will be zero if the chunk was skipped.
         if ( !defined $tbl->{nibble_time} ) {
            PTDEBUG && _d('Skipping chunk', $chunk);
            $exit_status |= $PTC_EXIT_STATUS{SKIP_CHUNK};
            $tbl->{checksum_results}->{skipped}++;
            return;
         }

         # Max chunk number that worked.  This may be less than the total
         # number of chunks if, for example, chunk 16 of 16 times out, but
         # chunk 15 worked.  The max chunk is used for checking for diffs
         # on the slaves, in the done callback.
         $tbl->{max_chunk} = $chunk;

         # Fetch the checksum that we just executed from the replicate table.
         $fetch_sth->execute(@{$tbl}{qw(db tbl)}, $chunk);
         my ($crc, $cnt) = $fetch_sth->fetchrow_array();

         $tbl->{checksum_results}->{n_rows} += $cnt || 0;

         # We're working on the master, so update the checksum's master_cnt
         # and master_crc.
         $update_sth->execute(
            # UPDATE repl_table SET
            sprintf('%.6f', $tbl->{nibble_time}), # chunk_time
            $crc,                                 # master_crc
            $cnt,                                 # master_cnt
            # WHERE
            $tbl->{db},
            $tbl->{tbl},
            $chunk,
         );

         # Should be done automatically, but I like to be explicit.
         $fetch_sth->finish();
         $update_sth->finish();
         $delete_sth->finish();

         # Update rate, chunk size, and progress if the nibble actually
         # selected some rows.
         if ( ($cnt || 0) > 0 ) {
            # Update the rate of rows per second for the entire server.
            # This is used for the initial chunk size of the next table.
            $total_rows += $cnt;
            $total_time += ($tbl->{nibble_time} || 0);
            $total_rate  = $total_time ? int($total_rows / $total_time) : 0;
            PTDEBUG && _d('Total avg rate:', $total_rate);

            # Adjust chunk size.  This affects the next chunk.
            if ( $o->get('chunk-time') ) {
               $tbl->{chunk_size} = $tbl->{nibble_time}
                                  ? $tbl->{rate}->update($cnt, $tbl->{nibble_time})
                                  : $o->get('chunk-time');

               if ( $tbl->{chunk_size} < 1 ) {
                  # This shouldn't happen.  WeightedAvgRate::update() may return
                  # a value < 1, but minimum chunk size is 1.
                  $tbl->{chunk_size} = 1;

                  # This warning is printed once per table.
                  if ( !$tbl->{warned}->{slow}++ && $o->get('quiet') < 2 ) {
                     warn ts("Checksum queries for table "
                        . "$tbl->{db}.$tbl->{tbl} are executing very slowly.  "
                        . "--chunk-size has been automatically reduced to 1.  "
                        . "Check that the server is not being overloaded, "
                        . "or increase --chunk-time.  The last chunk, number "
                        . "$chunk of table $tbl->{db}.$tbl->{tbl}, "
                        . "selected $cnt rows and took "
                        . sprintf('%.3f', $tbl->{nibble_time} || 0)
                        . " seconds to execute.\n");
                  }
               }

               # Update chunk-size based on rows/s checksum rate.
               $nibble_iter->set_chunk_size($tbl->{chunk_size});
               PTDEBUG && _d('Updated chunk size: '.$tbl->{chunk_size});
            }

            # Every table should have a Progress obj; update it.
            if ( my $tbl_pr = $tbl->{progress} ) {
               $tbl_pr->update(sub {return $tbl->{checksum_results}->{n_rows}});
            }
         }

         # Wait forever for slaves to catch up.
         $replica_lag_pr->start() if $replica_lag_pr;
         $replica_lag->wait(Progress => $replica_lag_pr);

         # Wait forever for system load to abate.
         $sys_load_pr->start() if $sys_load_pr;
         $sys_load->wait(Progress => $sys_load_pr);

         return;
      },
      done => sub { # done nibbling table
         my (%args) = @_;
         my $tbl         = $args{tbl};
         my $nibble_iter = $args{NibbleIterator};
         my $max_chunk   = $tbl->{max_chunk};

         # Don't need to do anything here if we're just --explain'ing.
         return if $o->get('explain');

         # Wait for all slaves to run all checksum chunks,
         # then check for differences.
         if ( $max_chunk && $o->get('replicate-check') && scalar @$slaves ) {
            PTDEBUG && _d('Checking slave diffs');

            my $check_pr;
            if ( $o->get('progress') ) {
               $check_pr = new Progress(
                  jobsize => $max_chunk,
                  spec    => $o->get('progress'),
                  name    => "Waiting to check replicas for differences",
               );
            }

            # Wait for the last checksum of this table to replicate
            # to each slave.
            # MySQL 8+ replication is slower than 5.7 and the old wait_for_last_checksum alone
            # was failing. The new wait_for_slaves checks that Read_Master_Log_Pos on slaves is
            # greather or equal Position in the master
            if (!$args{Cxn}->is_cluster_node()) {
                wait_for_slaves(master_dbh => $args{Cxn}->dbh(), master_slave => $ms, slaves => $slaves);
            }
            wait_for_last_checksum(
               tbl          => $tbl,
               repl_table   => $repl_table,
               slaves       => $slaves,
               max_chunk    => $max_chunk,
               check_pr     => $check_pr,
               have_time    => $have_time,
               OptionParser => $o,
            );

            # Check each slave for checksum diffs.
            my %diff_chunks;
            foreach my $slave ( @$slaves ) {
               eval {
                  my $diffs = $rc->find_replication_differences(
                     dbh        => $slave->dbh(),
                     repl_table => $repl_table,
                     where      => "db='$tbl->{db}' AND tbl='$tbl->{tbl}'",
                  );
                  PTDEBUG && _d(scalar @$diffs, 'checksum diffs on',
                     $slave->name());
                  # Save unique chunks that differ.
                  # https://bugs.launchpad.net/percona-toolkit/+bug/1030031
                  if ( scalar @$diffs ) {
                     # "chunk" is the chunk number.  See the SELECT
                     # statement in RowChecksum::find_replication_differences()
                     # for the full list of columns.
                     map { $diff_chunks{ $_->{chunk} }++ } @$diffs;
                     $exit_status |= $PTC_EXIT_STATUS{TABLE_DIFF};
                  }

                  my $max_cnt_diff=0;
                  for my $diff (@$diffs) {
                     if (abs($diff->{cnt_diff}) > $max_cnt_diff) {
                         $tbl->{checksum_results}->{max_rows_cnt_diff} = abs($diff->{cnt_diff});
                     }
                  }
               };
               if ($EVAL_ERROR) {
                  if ( $o->get('quiet') < 2 ) {
                     warn ts("Error checking for checksum differences of table "
                        . "$tbl->{db}.$tbl->{tbl} on replica " . $slave->name()
                        . ": $EVAL_ERROR\n"
                        . "Check that the replica is running and has the "
                        . "replicate table $repl_table.\n");
                  }
                  $tbl->{checksum_results}->{errors}++;
               }
            }
            $tbl->{checksum_results}->{diffs} = scalar keys %diff_chunks;
         }

         # Print table's checksum results if we're not being quiet,
         # else print if table has diffs and we're not being completely
         # quiet.
         if ( !$o->get('quiet')
              || $o->get('quiet') < 2 &&  $tbl->{checksum_results}->{diffs} ) {
            print_checksum_results(tbl => $tbl);
         }

         return;
      },
   };

   # ########################################################################
   # Init the --plugin.
   # ########################################################################

   # --plugin hook
   if ( $plugin && $plugin->can('init') ) {
      $plugin->init(
         slaves         => $slaves,
         slave_lag_cxns => $slave_lag_cxns,
         repl_table     => $repl_table,
      );
   }

   # ########################################################################
   # Checksum each table.
   # ########################################################################

   TABLE:
   while ( $oktorun && $have_time->() && (my $tbl = $schema_iter->next()) ) {
      eval {
         # Results, stats, and info related to checksumming this table can
         # be saved here.  print_checksum_results() uses this info.
         $tbl->{checksum_results} = {};

         # Set table's initial chunk size.  If this is the first table,
         # then total rate will be zero, so use --chunk-size.  Or, if
         # --chunk-time=0, then only use --chunk-size for every table.
         # Else, the initial chunk size is based on the total rates of
         # rows/s from all previous tables.  If --chunk-time is really
         # small, like 0.001, then Perl int() will probably round the
         # chunk size to zero, which is invalid, so we default to 1.
         my $chunk_time = $o->get('chunk-time');
         my $chunk_size = $chunk_time && $total_rate
                        ? int($total_rate * $chunk_time) || 1
                        : $o->get('chunk-size');
         $tbl->{chunk_size} = $chunk_size;

         # Make a nibble iterator for this table.  This should only fail
         # if the table has no indexes and is too large to checksum in
         # one chunk.
         my $checksum_cols = eval {
            $rc->make_chunk_checksum(
               dbh => $master_cxn->dbh(),
               tbl => $tbl,
               %crc_args
            );
         };

         if ( $EVAL_ERROR ) {
            warn ts("Skipping table $tbl->{db}.$tbl->{tbl} because "
                  . "$EVAL_ERROR\n");
            $exit_status |= $PTC_EXIT_STATUS{SKIP_TABLE};
            return;
         }

         my $nibble_iter;
         eval {
            $nibble_iter = new OobNibbleIterator(
               Cxn                => $master_cxn,
               tbl                => $tbl,
               chunk_size         => $tbl->{chunk_size},
               chunk_index        => $o->get('chunk-index'),
               n_chunk_index_cols => $o->get('chunk-index-columns'),
               dml                => $checksum_dml,
               select             => $checksum_cols,
               past_dml           => $checksum_dml,
               past_select        => $past_cols,
               callbacks          => $callbacks,
               resume             => $last_chunk,
               OptionParser       => $o,
               Quoter             => $q,
               TableNibbler       => $tn,
               TableParser        => $tp,
               RowChecksum        => $rc,
               comments           => {
                  bite   => "checksum table",
                  nibble => "checksum chunk",
               },
            );
         };
         if ( $EVAL_ERROR ) {
            if ( $o->get('quiet') < 2 ) {
               warn ts("Cannot checksum table $tbl->{db}.$tbl->{tbl}: "
                  . "$EVAL_ERROR\n");
            }
            $tbl->{checksum_results}->{errors}++;
         }
         else {
            # Init a new weighted avg rate calculator for the table.
            $tbl->{rate} = new WeightedAvgRate(target_t => $chunk_time);

            # Make a Progress obj for this table.  It may not be used;
            # depends on how many rows, chunk size, how fast the server
            # is, etc.  But just in case, all tables have a Progress obj.
            if ( $o->get('progress')
                 && !$nibble_iter->one_nibble()
                 &&  $nibble_iter->row_estimate() )
            {
               $tbl->{progress} = new Progress(
                  jobsize => $nibble_iter->row_estimate(),
                  spec    => $o->get('progress'),
                  name    => "Checksumming $tbl->{db}.$tbl->{tbl}",
               );
            }

            # Make a list of the columns being checksummed.  As the option's
            # docs note, this really only makes sense when checksumming one
            # table, unless the tables have a common set of columns.
            # TODO: this now happens in 3 places, search for 'columns'.
            my $tbl_struct = $tbl->{tbl_struct};
            my $ignore_col = $o->get('ignore-columns') || {};
            my $all_cols   = $o->get('columns')        || $tbl_struct->{non_generated_cols};
            my @cols       = map  { lc $_ }
                             grep { !$ignore_col->{$_} }
                             @$all_cols;
            $tbl->{checksum_cols} = \@cols;

            # --plugin hook
            if ( $plugin && $plugin->can('before_checksum_table') ) {
               $plugin->before_checksum_table(
                  tbl   => $tbl);
            }

            # Finally, checksum the table.
            # The "1 while" loop is necessary because we're executing REPLACE
            # statements which don't return rows and NibbleIterator only
            # returns if it has rows to return.  So all the work is done via
            # the callbacks. -- print_checksum_results(), which is called
            # from the done callback, uses this start time.
            $tbl->{checksum_results}->{start_time} = time;
            1 while $nibble_iter->next();

            # --plugin hook
            if ( $plugin && $plugin->can('after_checksum_table') ) {
               $plugin->after_checksum_table();
            }
         }
      };
      if ( $EVAL_ERROR ) {
          if ($EVAL_ERROR =~ m/replication/) {
                   exit($PTC_EXIT_STATUS{REPLICATION_STOPPED});
          }
         # This should not happen.  If it does, it's probably some bug
         # or error that we're not catching.
         warn ts(($oktorun ? "Error " : "Fatal error ")
            . "checksumming table $tbl->{db}.$tbl->{tbl}: "
            . "$EVAL_ERROR\n");
         $tbl->{checksum_results}->{errors}++;

         # Print whatever checksums results we got before dying, regardless
         # of --quiet because at this point we need all the info we can get.
         print_checksum_results(tbl => $tbl);
      }

      # Update the tool's exit status.
      if ( $tbl->{checksum_results}->{errors} ) {
         $exit_status |= $PTC_EXIT_STATUS{ERROR};
      }
   }

   # Restore origin QRT pligin state
   if ($o->get('disable-qrt-plugin')) {
       eval {
           if ($original_qrt_plugin_master_status) {
               PTDEBUG && _d("Restoring qrt plugin state on master server");
               $master_dbh->do("SET GLOBAL query_response_time_stats = $original_qrt_plugin_master_status->[0]");
           }
           for my $slave (@$slaves) {
               if ($slave->{qrt_plugin_status}) {
                   PTDEBUG && _d("Restoring qrt plugin state on slave ".$slave->{dsn_name});
                   $slave->{dbh}->do("SET GLOBAL query_response_time_stats = $slave->{qrt_plugin_status}");
               }
           }
       };
       if ($EVAL_ERROR) {
           warn "Cannot restore qrt_plugin status: $EVAL_ERROR";
       }
   }

   PTDEBUG && _d('Exit status', $exit_status,
                 'oktorun',     $oktorun,
                 'have time',   $have_time->());
   return $exit_status;
}

# ############################################################################
# Subroutines
# ############################################################################
sub ts {
   my ($msg) = @_;
   my ($s, $m, $h, $d, $M) = localtime;
   my $ts = sprintf('%02d-%02dT%02d:%02d:%02d', $M+1, $d, $h, $m, $s);
   return $msg ? "$ts $msg" : $ts;
}


sub nibble_is_safe {
   my (%args) = @_;
   my @required_args = qw(Cxn tbl NibbleIterator OptionParser);
   foreach my $arg ( @required_args ) {
      die "I need a $arg argument" unless $args{$arg};
   }
   my ($cxn, $tbl, $nibble_iter, $o)= @args{@required_args};

   # EXPLAIN the checksum chunk query to get its row estimate and index.
   # XXX This call and others like it are relying on a Perl oddity.
   # See https://bugs.launchpad.net/percona-toolkit/+bug/987393
   my $sth      = $nibble_iter->statements();
   my $boundary = $nibble_iter->boundaries();
   if (!defined($boundary) || !$boundary || (!$boundary->{lower} || !$boundary->{upper})) {
        return 0;
   }
   my $expl     = explain_statement(
      tbl  => $tbl,
      sth  => $sth->{explain_nibble},
      vals => [ @{$boundary->{lower}}, @{$boundary->{upper}} ],
   );

   # Ensure that MySQL is using the chunk index if the table is being chunked.
   if ( !$nibble_iter->one_nibble()
        && lc($expl->{key} || '') ne lc($nibble_iter->nibble_index() || '') ) {
      if ( !$tbl->{warned}->{not_using_chunk_index}++
           && $o->get('quiet') < 2 ) {
         warn ts("Skipping chunk " . $nibble_iter->nibble_number()
            . " of $tbl->{db}.$tbl->{tbl} because MySQL chose "
            . ($expl->{key} ? "the $expl->{key}" : "no") . " index "
            . " instead of the " . $nibble_iter->nibble_index() . " index.\n");
      }
      $exit_status |= $PTC_EXIT_STATUS{SKIP_CHUNK};
      return 0; # not safe
   }

   # Ensure that the chunk isn't too large if there's a --chunk-size-limit.
   # If single-chunking the table, this has already been checked, so it
   # shouldn't have changed.  If chunking the table with a non-unique key,
   # oversize chunks are possible.
   if ( my $limit = $o->get('chunk-size-limit') ) {
      my $oversize_chunk = ($expl->{rows} || 0) >= $tbl->{chunk_size} * $limit;
      if ( $oversize_chunk
           && $nibble_iter->identical_boundaries($boundary->{upper},
                                                 $boundary->{next_lower}) ) {
         if ( !$tbl->{warned}->{oversize_chunk}++
              && $o->get('quiet') < 2 ) {
            warn ts("Skipping chunk " . $nibble_iter->nibble_number()
               . " of $tbl->{db}.$tbl->{tbl} because it is oversized.  "
               . "The current chunk size limit is "
               . ($tbl->{chunk_size} * $limit)
               . " rows (chunk size=$tbl->{chunk_size}"
               . " * chunk size limit=$limit), but MySQL estimates "
               . "that there are " . ($expl->{rows} || 0)
               . " rows in the chunk.\n");
         }
         $exit_status |= $PTC_EXIT_STATUS{SKIP_CHUNK};
         return 0; # not safe
      }
   }

   # Ensure that MySQL is still using the entire index.
   # https://bugs.launchpad.net/percona-toolkit/+bug/1010232
   if ( !$nibble_iter->one_nibble()
        && $tbl->{key_len}
        && ($expl->{key_len} || 0) < $tbl->{key_len} ) {
      if ( !$tbl->{warned}->{key_len}++
           && $o->get('quiet') < 2 ) {
         warn ts("Skipping chunk " . $nibble_iter->nibble_number()
            . " of $tbl->{db}.$tbl->{tbl} because MySQL used "
            . "only " . ($expl->{key_len} || 0) . " bytes "
            . "of the " . ($expl->{key} || '?') . " index instead of "
            . $tbl->{key_len} . ".  See the --[no]check-plan documentation "
            . "for more information.\n");
      }
      $exit_status |= $PTC_EXIT_STATUS{SKIP_CHUNK};
      return 0; # not safe
   }

   return 1; # safe
}

sub exec_nibble {
   my (%args) = @_;
   my @required_args = qw(Cxn tbl NibbleIterator Retry Quoter OptionParser);
   foreach my $arg ( @required_args ) {
      die "I need a $arg argument" unless $args{$arg};
   }
   my ($cxn, $tbl, $nibble_iter, $retry, $q, $o)= @args{@required_args};

   my $dbh         = $cxn->dbh();
   my $sth         = $nibble_iter->statements();
   my $boundary    = $nibble_iter->boundaries();
   # XXX This call and others like it are relying on a Perl oddity.
   # See https://bugs.launchpad.net/percona-toolkit/+bug/987393
   my $lb_quoted   = $q->serialize_list(@{$boundary->{lower}});
   my $ub_quoted   = $q->serialize_list(@{$boundary->{upper}});
   my $chunk       = $nibble_iter->nibble_number();
   my $chunk_index = $nibble_iter->nibble_index();

   return $retry->retry(
      tries => $o->get('retries'),
      wait  => sub { return; },
      try   => sub {
         # ###################################################################
         # Start timing the checksum query.
         # ###################################################################
         my $t_start = time;

         # Execute the REPLACE...SELECT checksum query.
         # XXX This call and others like it are relying on a Perl oddity.
         # See https://bugs.launchpad.net/percona-toolkit/+bug/987393
         PTDEBUG && _d($sth->{nibble}->{Statement},
            'lower boundary:', @{$boundary->{lower}},
            'upper boundary:', @{$boundary->{upper}});
         $sth->{nibble}->execute(
            # REPLACE INTO repl_table SELECT
            $tbl->{db},             # db
            $tbl->{tbl},            # tbl
            $chunk,                 # chunk (number)
            $chunk_index,           # chunk_index
            $lb_quoted,             # lower_boundary
            $ub_quoted,             # upper_boundary
            # this_cnt, this_crc WHERE
            @{$boundary->{lower}},  # upper boundary values
            @{$boundary->{upper}},  # lower boundary values
         );

         my $t_end = time;
         # ###################################################################
         # End timing the checksum query.
         # ###################################################################

         # Check if checksum query caused any warnings.
         my $sql_warn = 'SHOW WARNINGS';
         PTDEBUG && _d($sql_warn);
         my $warnings = $dbh->selectall_arrayref($sql_warn, { Slice => {} } );
         foreach my $warning ( @$warnings ) {
            my $code    = ($warning->{code} || 0);
            my $message = $warning->{message};
            if ( $ignore_code{$code} ) {
               PTDEBUG && _d('Ignoring warning:', $code, $message);
               next;
            }
            elsif ( $warn_code{$code}
                    && (!$warn_code{$code}->{pattern}
                        || $message =~ m/$warn_code{$code}->{pattern}/) )
            {
               if ( !$tbl->{warned}->{$code}++ ) {  # warn once per table
                  if ( $o->get('quiet') < 2 ) {
                     warn ts("Checksum query for table $tbl->{db}.$tbl->{tbl} "
                        . "caused MySQL error $code: "
                        . ($warn_code{$code}->{message}
                           ? $warn_code{$code}->{message}
                           : $message)
                        . "\n");
                  }
                  $tbl->{checksum_results}->{errors}++;
               }
            }
            else {
               # This die will propagate to fail which will return 0
               # and propagate it to final_fail which will die with
               # this error message.  (So don't wrap it in ts().)
               die "Checksum query for table $tbl->{db}.$tbl->{tbl} "
                  . "caused MySQL error $code:\n"
                  . "    Level: " . ($warning->{level}   || '') . "\n"
                  . "     Code: " . ($warning->{code}    || '') . "\n"
                  . "  Message: " . ($warning->{message} || '') . "\n"
                  . "    Query: " . $sth->{nibble}->{Statement} . "\n";
            }
         }

         # Success: no warnings, no errors.  Return nibble time.
         return $t_end - $t_start;
      },
      fail => sub {
         my (%args) = @_;
         my $error = $args{error};

         if (   $error =~ m/Lock wait timeout exceeded/
             || $error =~ m/Query execution was interrupted/
             || $error =~ m/Deadlock found/
         ) {
            # These errors/warnings can be retried, so don't print
            # a warning yet; do that in final_fail.
            return 1;
         }
         elsif (   $error =~ m/MySQL server has gone away/
                || $error =~ m/Lost connection to MySQL server/
         ) {
            # The 2nd pattern means that MySQL itself died or was stopped.
            # The 3rd pattern means that our cxn was killed (KILL <id>).
            eval { $dbh = $cxn->connect(); };
            return 1 unless $EVAL_ERROR; # reconnected, retry checksum query
            $oktorun = 0;                # failed to reconnect, exit tool
         }

         # At this point, either the error/warning cannot be retried,
         # or we failed to reconnect.  So stop trying and call final_fail.
         return 0;
      },
      final_fail => sub {
         my (%args) = @_;
         my $error = $args{error};

         if (   $error =~ m/Lock wait timeout exceeded/
             || $error =~ m/Query execution was interrupted/
             || $error =~ m/Deadlock found/
         ) {
            # These errors/warnings are not fatal but only cause this
            # nibble to be skipped.
            my $err = $error =~ /Lock wait timeout exceeded/
                    ? 'lock_wait_timeout'
                    : 'query_interrupted';
            if ( !$tbl->{warned}->{$err}++ && $o->get('quiet') < 2 ) {
               my $msg = "Skipping chunk " . ($nibble_iter->nibble_number() || '?')
                       . " of $tbl->{db}.$tbl->{tbl} because $error.\n";
               warn ts($msg);
            }
            $exit_status |= $PTC_EXIT_STATUS{SKIP_CHUNK};
            return;  # skip this nibble
         }

         # This die will be caught by the eval inside the TABLE loop.
         # Checksumming for this table will stop, which is probably
         # good because by this point the error or warning indicates
         # that something fundamental is broken or wrong.  Checksumming
         # will continue with the next table, unless the fail code set
         # oktorun=0, in which case the error/warning is fatal.
         die "Error executing checksum query: $args{error}\n";
      }
   );
}

{
my $line_fmt = "%14s %6s %6s %8s % 10s %7s %7s %7s %-s\n";
my @headers  =  qw(TS ERRORS DIFFS ROWS DIFF_ROWS CHUNKS SKIPPED TIME TABLE);

sub print_checksum_results {
   my (%args) = @_;
   my @required_args = qw(tbl);
   foreach my $arg ( @required_args ) {
      die "I need a $arg argument" unless $args{$arg};
   }
   my ($tbl) = @args{@required_args};

   if ($print_header) {
      printf $line_fmt, @headers;
      $print_header = 0;
   }

   my $res = $tbl->{checksum_results};
   printf $line_fmt,
      ts(),
      $res->{errors}   || 0,
      $res->{diffs}    || 0,
      $res->{n_rows}   || 0,
      $tbl->{checksum_results}->{max_rows_cnt_diff} || 0,
      $res->{n_chunks} || 0,
      $res->{skipped}  || 0,
      sprintf('%.3f', $res->{start_time} ? time - $res->{start_time} : 0),
      "$tbl->{db}.$tbl->{tbl}";

   return;
}
}

{
my @headers = qw(table chunk cnt_diff crc_diff chunk_index lower_boundary upper_boundary);

sub print_checksum_diffs {
   my ( %args ) = @_;
   my @required_args = qw(cxn diffs);
   foreach my $arg ( @required_args ) {
      die "I need a $arg argument" unless $args{$arg};
   }
   my ($cxn, $diffs) = @args{@required_args};

   print "Differences on ", $cxn->name(), "\n";
   print join(' ', map { uc $_ } @headers), "\n";
   foreach my $diff ( @$diffs ) {
      print join(' ', map { defined $_ ? $_ : '' } @{$diff}{@headers}), "\n";
   }
   print "\n";

   return;
}
}

sub filter_tables_replicate_check_only {
   my ($diffs, $o) = @_;
   my @filtered_diffs;

   # TODO: SchemaIterator has the methods to filter the dbs & tables,
   # but we don't actually need a real iterator beyond that
   my $filter = new SchemaIterator(
      file_itr     => "Fake",
      OptionParser => $o,
      Quoter       => "Quoter",
      TableParser  => "TableParser",
   );

   for my $diff (@$diffs) {
      my ($db, $table) = Quoter->split_unquote($diff->{table});
      next unless $filter->database_is_allowed($db)
               && $filter->table_is_allowed($db, $table);
      push @filtered_diffs, $diff;

   }

   return \@filtered_diffs;
}

sub check_repl_table {
   my ( %args ) = @_;
   my @required_args = qw(dbh repl_table slaves have_time
                          OptionParser TableParser Quoter);
   foreach my $arg ( @required_args ) {
      die "I need a $arg argument" unless $args{$arg};
   }
   my ($dbh, $repl_table, $slaves, $have_time, $o, $tp, $q) = @args{@required_args};

   PTDEBUG && _d('Checking --replicate table', $repl_table);

   # ########################################################################
   # Create the --replicate database.
   # ########################################################################

   # If the repl db doesn't exit, auto-create it, maybe.
   my ($db, $tbl) = $q->split_unquote($repl_table);
   my $show_db_sql = "SHOW DATABASES LIKE '$db'";
   PTDEBUG && _d($show_db_sql);
   my @db_exists = $dbh->selectrow_array($show_db_sql);
   if ( !@db_exists && !$o->get('create-replicate-table') ) {
      die "--replicate database $db does not exist and "
         . "--no-create-replicate-table was specified.  You need "
         . "to create the database.\n";
   }

   if ( $o->get('create-replicate-table') ) {
      # Even if the db already exists, do this in case it does not exist
      # on a slave.
      my $create_db_sql
         = "CREATE DATABASE IF NOT EXISTS "
         . $q->quote($db)
         . " /* pt-table-checksum */";
      PTDEBUG && _d($create_db_sql);
      eval {
         $dbh->do($create_db_sql);
      };
      if ( $EVAL_ERROR ) {
         # CREATE DATABASE IF NOT EXISTS failed but the db could already
         # exist and the error could be due, for example, to the user not
         # having privs to create it, but they still have privs to use it.

         if ( @db_exists ) {
            # Repl db already exists on the master, so check if it's also
            # on all slaves.  If not, and given that creating it failed,
            # we'll die because we can't be sure if it's ok on all slaves.
            # The user can verify and disable this check if it's ok.
            my $e = $EVAL_ERROR;  # CREATE DATABASE error
            my @slaves_missing_db;
            foreach my $slave ( @$slaves ) {
               PTDEBUG && _d($show_db_sql, 'on', $slave->name());
               my @db_exists_in_slave
                  = $slave->dbh->selectrow_array($show_db_sql);
               if ( !@db_exists_in_slave ) {
                  push @slaves_missing_db, $slave;
               }
            }
            if ( @slaves_missing_db ) {
               warn $e;  # CREATE DATABASE error
               die "The --replicate database $db exists on the master but "
                 . "$create_db_sql on the master failed (see the error above) "
                 . "and the database does not exist on these replicas:\n"
                 . join("\n", map { "  " . $_->name() } @slaves_missing_db)
                 . "\nThis can break replication.  If you understand "
                 . "the risks, specify --no-create-replicate-table to disable "
                 . "this check.\n";
            }
         }
         else {
            warn $EVAL_ERROR;
            die "--replicate database $db does not exist and it cannot be "
               . "created automatically.  You need to create the database.\n";
         }
      }
   }


   # USE the correct db (probably the repl db, but maybe --replicate-database).
   use_repl_db(%args);

   # ########################################################################
   # Create the --replicate table.
   # ########################################################################

   # Check if the repl table exists; if not, create it, maybe.
   my $tbl_exists = $tp->check_table(
      dbh => $dbh,
      db  => $db,
      tbl => $tbl,
   );
   PTDEBUG && _d('--replicate table exists:', $tbl_exists ? 'yes' : 'no');

   if ( !$tbl_exists && !$o->get('create-replicate-table') ) {
      die "--replicate table $repl_table does not exist and "
        . "--no-create-replicate-table was specified.  "
        . "You need to create the table.\n";
   }


   # We used to check the table privs here, but:
   # https://bugs.launchpad.net/percona-toolkit/+bug/916168

   # Always create the table, unless --no-create-replicate-table
   # was passed in; see https://bugs.launchpad.net/percona-toolkit/+bug/950294
   if ( $o->get('create-replicate-table') ) {
      eval {
         create_repl_table(%args);
      };
      if ( $EVAL_ERROR ) {
         # CREATE TABLE IF NOT EXISTS failed but the table could already
         # exist and the error could be due, for example, to the user not
         # having privs to create it, but they still have privs to use it.

         if ( $tbl_exists ) {
            # Repl table already exists on the master, so check if it's also
            # on all slaves.  If not, and given that creating it failed,
            # we'll die because we can't be sure if it's ok on all slaves.
            # The user can verify and disable this check if it's ok.
            my $e          = $EVAL_ERROR;  # CREATE TABLE error
            my $ddl        = $tp->get_create_table($dbh, $db, $tbl);
            my $tbl_struct = $tp->parse($ddl);
            eval {
               check_slave_tables(
                  slaves        => $slaves,
                  db            => $db,
                  tbl           => $tbl,
                  checksum_cols => $tbl_struct->{cols},
                  have_time     => $have_time,
                  TableParser   => $tp,
                  OptionParser  => $o,
               );
            };
            if ( $EVAL_ERROR ) {
               warn $e;  # CREATE TABLE error
               die "The --replicate table $repl_table exists on the master but "
                 . "but it has problems on these replicas:\n"
                 . $EVAL_ERROR
                 . "\nThis can break replication.  If you understand "
                 . "the risks, specify --no-create-replicate-table to disable "
                 . "this check.\n";
            }
         }
         else {
            warn $EVAL_ERROR;
            die "--replicate table $tbl does not exist and it cannot be "
               . "created automatically.  You need to create the table.\n"
         }
      }
   }

   # Check and wait for the repl table to appear on all slaves.
   # https://bugs.launchpad.net/percona-toolkit/+bug/1008778
   if ( scalar @$slaves ) {
      my $waiting_for;
      my $pr;
      if ( $o->get('progress') ) {
         $pr = new Progress(
            jobsize  => scalar @$slaves,
            spec     => $o->get('progress'),
            callback => sub {
               print STDERR "Waiting for the --replicate table to replicate to "
                  . $waiting_for->name() . "...\n";
            },
         );
         $pr->start();
      }

      foreach my $slave ( @$slaves ) {
         PTDEBUG && _d('Checking if', $slave->name(), 'has repl table');
         $waiting_for = $slave;
         my $slave_has_repl_table = $tp->check_table(
               dbh => $slave->dbh(),
               db  => $db,
               tbl => $tbl,
         );
         while ( !$slave_has_repl_table ) {
            $pr->update(sub { return 0; }) if $pr;
            sleep 0.5;
            $slave_has_repl_table = $tp->check_table(
               dbh => $slave->dbh(),
               db  => $db,
               tbl => $tbl,
            );
         }
      }
   }

   if ( $o->get('binary-index') ) {
      PTDEBUG && _d('--binary-index : checking if replicate table has binary type columns');
      my $create_table = $tp->get_create_table( $dbh, $db, $tbl );
      if (  $create_table !~ /lower_boundary`?\s+BLOB/si
         || $create_table !~ /upper_boundary`?\s+BLOB/si )
      {
         die "--binary-index was specified but the current checksum table ($db.$tbl) uses"
         ." TEXT columns. To use BLOB columns, drop the current checksum table, then recreate"
         ." it by specifying --create-replicate-table --binary-index.";
      }
   }

   return;  # success, repl table is ready to go
}

# Check that db.tbl exists on all slaves and has the checksum cols,
# else when we check for diffs we'll break replication by selecting
# a nonexistent column.
sub check_slave_tables {
   my (%args) = @_;
   my @required_args = qw(slaves db tbl checksum_cols have_time TableParser OptionParser);
   foreach my $arg ( @required_args ) {
      die "I need a $arg argument" unless $args{$arg};
   }
   my ($slaves, $db, $tbl, $checksum_cols, $have_time, $tp, $o) = @args{@required_args};

   my @problems;
   SLAVE:
   foreach my $slave ( @$slaves ) {
      my $slave_has_table = 0;
      my $have_warned     = 0;
      while ( $oktorun && $have_time->() )  {
         eval {
            # TableParser::check_table() does not die on error, it sets
            # check_table_error and return 0.
            $slave_has_table = $tp->check_table(
                  dbh => $slave->dbh,
                  db  => $db,
                  tbl => $tbl,
            );
            die $tp->{check_table_error} if defined $tp->{check_table_error};
            if ( !$slave_has_table ) {
               push @problems, "Table $db.$tbl does not exist on replica "
                  . $slave->name;
            }
            else {
               # TableParser::get_create_table() will die on error.
               my $slave_tbl_struct = $tp->parse(
                  $tp->get_create_table($slave->dbh, $db, $tbl)
               );
               my @slave_missing_cols;
               foreach my $col ( @$checksum_cols ) {
                  if ( !$slave_tbl_struct->{is_col}->{$col} ) {
                     push @slave_missing_cols, $col;
                  }
               }
               if ( @slave_missing_cols ) {
                  push @problems, "Table $db.$tbl on replica " . $slave->name
                     . " is missing these columns: "
                     . join(", ", @slave_missing_cols);
               }
            }
         };
         if ( my $e = $EVAL_ERROR ) {
            PTDEBUG && _d($e);
            if ( !$slave->lost_connection($e) ) {
               push @problems, "Error checking table $db.$tbl on replica "
                  . $slave->name . ": $e";
               next SLAVE;
            }

            # Lost connection to slave. Reconnect and try again.
            eval { $slave->connect() };
            if ( $EVAL_ERROR ) {
               PTDEBUG && _d('Failed to connect to slave', $slave->name(),
                  ':', $EVAL_ERROR);
               if ( !$have_warned && $o->get('quiet') < 2 ) {
                  my $msg = "Trying to connect to replica "
                     . $slave->name() . " to check $db.$tbl...\n";
                  warn ts($msg);
                  $have_warned = 1;
               }
               sleep 2; # wait between failed reconnect attempts
            }
            next; # try again
         } # eval error

         # No error, so we successfully queried this slave.
         next SLAVE;

      } # while oktorun && have_time
   } # foreach slave

   die join("\n", @problems) . "\n" if @problems;

   return;
}

# Sub: use_repl_db
#   USE the correct database for the --replicate table.
#   This sub must be called before any work is done with the --replicatte
#   table because replication filters can really complicate replicating the
#   checksums.  The originally issue is,
#   http://code.google.com/p/maatkit/issues/detail?id=982,
#   but here's what you need to know:
#   - If there is no active DB, then if there's any do-db or ignore-db
#     settings, the checksums will get filtered out of replication. So we
#     have to have some DB be the current one.
#   - Other places in the code may change the DB and we might not know it.
#     Opportunity for bugs.  The SHOW CREATE TABLE, for example. In the
#     end, a bunch of USE statements isn't a big deal, it just looks noisy
#     when you analyze the logs this tool creates. But it's better to just
#     have them even if they're no-op.
#   - We need to always let the user specify, because there are so many
#     possibilities that the tool can't guess the right thing in all of
#     them.
#   - The right default behavior, which the user can override, is:
#       * When running queries on the --replicate table itself, such as
#         emptying it, USE that table's database.
#       * When running checksum queries, USE the database of the table that's
#         being checksummed.
#       * When the user specifies --replicate-database, in contrast, always
#         USE that database.
#   - This behavior is the best compromise by default, because users who
#     explicitly replicate some databases and filter out others will be
#     very likely to run pt-table-checksum and limit its checksumming to
#     only the databases that are replicated. I've seen people do this,
#     including Peter. In this case, the tool will work okay even without
#     an explicit --replicate-database setting.
#
# Required Arguments:
#   dbh          - dbh
#   repl_table   - Full quoted --replicate table name
#   OptionParser - <OptionParser>
#   Quoter       - <Quoter>
#
# Optional Arguments:
#   tbl - Standard tbl hashref of table being checksummed
#
# Returns:
#   Nothing or dies on error
sub use_repl_db {
   my ( %args ) = @_;
   my @required_args = qw(dbh repl_table OptionParser Quoter);
   foreach my $arg ( @required_args ) {
      die "I need a $arg argument" unless $args{$arg};
   }
   my ($dbh, $repl_table, $o, $q) = @args{@required_args};
   PTDEBUG && _d('use_repl_db');

   my ($db, $tbl) = $q->split_unquote($repl_table);
   if ( my $tbl = $args{tbl} ) {
      # If there's a tbl arg then its db will be used unless
      # --replicate-database was specified.  A tbl arg means
      # we're checksumming that table.  Other callers won't
      # pass a tbl arg when they're just doing something to
      # the --replicate table.
      $db = $o->get('replicate-database') ? $o->get('replicate-database')
          :                                 $tbl->{db};
   }
   else {
      # Caller is doing something just to the --replicate table.
      # Use the db from --replicate db.tbl (gotten earlier) unless
      # --replicate-database is in effect.
      $db = $o->get('replicate-database') if $o->get('replicate-database');
   }

   eval {
      my $sql = "USE " . $q->quote($db);
      PTDEBUG && _d($sql);
      $dbh->do($sql);
   };
   if ( $EVAL_ERROR ) {
      # Report which option db really came from.
      my $opt = $o->get('replicate-database') ? "--replicate-database"
              :                                 "--replicate database";
      if ( $EVAL_ERROR =~ m/unknown database/i ) {
         die "$opt $db does not exist.  You need to create the "
            . "database or specify a database for $opt that exists.\n";
      }
      else {
         die "Error using $opt $db: $EVAL_ERROR\n";
      }
   }

   return;
}

sub create_repl_table {
   my ( %args ) = @_;
   my @required_args = qw(dbh repl_table OptionParser);
   foreach my $arg ( @required_args ) {
      die "I need a $arg argument" unless $args{$arg};
   }
   my ($dbh, $repl_table, $o) = @args{@required_args};
   PTDEBUG && _d('Creating --replicate table', $repl_table);
   my $sql = $o->read_para_after(__FILE__, qr/MAGIC_create_replicate/);
   $sql =~ s/CREATE TABLE checksums/CREATE TABLE IF NOT EXISTS $repl_table/;
   $sql =~ s/;$//;
   if ( $o->get('binary-index') ) {
      $sql =~ s/`?lower_boundary`?\s+TEXT/`lower_boundary` BLOB/is;
      $sql =~ s/`?upper_boundary`?\s+TEXT/`upper_boundary` BLOB/is;
   }
   PTDEBUG && _d($dbh, $sql);
   eval {
      $dbh->do($sql);
   };
   if ( $EVAL_ERROR ) {
      die ts("--create-replicate-table failed: $EVAL_ERROR");
   }

   return;
}

# Sub: explain_statement
#   EXPLAIN a statement.
#
# Required Arguments:
#   * tbl  - Standard tbl hashref
#   * sth  - Sth with EXLAIN <statement>
#   * vals - Values for sth, if any
#
# Returns:
#   Hashref with EXPLAIN plan
sub explain_statement {
   my ( %args ) = @_;
   my @required_args = qw(tbl sth vals);
   foreach my $arg ( @required_args ) {
      die "I need a $arg argument" unless defined $args{$arg};
   }
   my ($tbl, $sth, $vals) = @args{@required_args};

   my $expl;
   eval {
      PTDEBUG && _d($sth->{Statement}, 'params:', @$vals);
      $sth->execute(@$vals);
      $expl = $sth->fetchrow_hashref();
      $sth->finish();
   };
   if ( $EVAL_ERROR ) {
      # This shouldn't happen.
      warn ts("Error executing " . $sth->{Statement} . ": $EVAL_ERROR\n");
      $tbl->{checksum_results}->{errors}++;
   }
   PTDEBUG && _d('EXPLAIN plan:', Dumper($expl));
   return $expl;
}

sub last_chunk {
   my (%args) = @_;
   my @required_args = qw(dbh repl_table);
   foreach my $arg ( @required_args ) {
      die "I need a $arg argument" unless $args{$arg};
   }
   my ($dbh, $repl_table, $q) = @args{@required_args};
   PTDEBUG && _d('Getting last chunk for --resume');

   my $sql = "SELECT * FROM $repl_table FORCE INDEX (ts_db_tbl) "
           . "WHERE master_cnt IS NOT NULL "
           . "ORDER BY ts DESC, db DESC, tbl DESC LIMIT 1";
   PTDEBUG && _d($sql);
   my $sth = $dbh->prepare($sql);
   $sth->execute();
   my $last_chunk = $sth->fetchrow_hashref();
   $sth->finish();
   PTDEBUG && _d('Last chunk:', Dumper($last_chunk));

   if ( !$last_chunk || !$last_chunk->{ts} ) {
      PTDEBUG && _d('Replicate table is empty; will not resume');
      return;
   }

   return $last_chunk;
}

sub have_more_chunks {
   my (%args) = @_;
   my @required_args = qw(tbl last_chunk NibbleIterator);
   foreach my $arg ( @required_args ) {
      die "I need a $arg argument" unless $args{$arg};
   }
   my ($tbl, $last_chunk, $nibble_iter) = @args{@required_args};
   PTDEBUG && _d('Checking for more chunks beyond last chunk');

   # If there's no next lower boundary, then this is the last
   # chunk of the table.
   if ( !$nibble_iter->more_boundaries() ) {
      PTDEBUG && _d('No more boundaries');
      return 0;
   }

   # The previous chunk index must match the current chunk index,
   # else we don't know what to do.
   my $chunk_index = lc($nibble_iter->nibble_index() || '');
   if (lc($last_chunk->{chunk_index}   || '') ne $chunk_index) {
      warn ts("Cannot resume from table $tbl->{db}.$tbl->{tbl} chunk "
         . "$last_chunk->{chunk} because the chunk indexes are different: "
         . ($last_chunk->{chunk_index} ? $last_chunk->{chunk_index}
                                       : "no index")
         . " was used originally but "
         . ($chunk_index ? $chunk_index : "no index")
         . " is used now.  If the table has not changed significantly, "
         . "this may be caused by running the tool with different command "
         . "line options.  This table will be skipped and checksumming "
         . "will resume with the next table.\n");
      $tbl->{checksum_results}->{errors}++;
      return 0;
   }

   return 1; # more chunks
}

sub wait_for_slaves {
   my (%args) = @_;
   my @required_args = qw(master_dbh master_slave slaves);
   foreach my $arg ( @required_args ) {
       die "I need a $arg argument" unless $args{$arg};
   }
   my ($master_dbh, $ms, $slaves) = @args{@required_args};

   foreach my $slave ( @$slaves ) {
       my $dp = $slave->{DSNParser};
       my $mdbh = $dp->get_dbh($dp->get_cxn_params($slave->{parent}));
       my $master_status = $ms->get_master_status($mdbh);
       $ms->wait_for_master(master_status => $master_status, slave_dbh => $slave->dbh());
       $dp->disconnect($mdbh);
   }
}

sub wait_for_last_checksum {
   my (%args) = @_;
   my @required_args = qw(tbl repl_table slaves max_chunk have_time OptionParser);
   foreach my $arg ( @required_args ) {
      die "I need a $arg argument" unless defined $args{$arg};
   }
   my ($tbl, $repl_table, $slaves, $max_chunk, $have_time, $o) = @args{@required_args};
   my $check_pr = $args{check_pr};

   # Requiring "AND master_crc IS NOT NULL" avoids a race condition
   # when the system is fast but replication is slow.  In such cases,
   # we can select on the slave before the update for $update_sth
   # replicates; this causes a false-positive diff.
   my $sql = "SELECT MAX(chunk) FROM $repl_table "
           . "WHERE db='$tbl->{db}' AND tbl='$tbl->{tbl}' "
           . "AND master_crc IS NOT NULL";
   PTDEBUG && _d($sql);

   my $sleep_time = 0;
   my $n_slaves   = scalar @$slaves - 1;
   my @chunks;
   my %skip_slave;
   my %have_warned;
   my $checked_all;
   while ( $oktorun && $have_time->() && (!$checked_all || (($chunks[0] || 0) < $max_chunk)) ) {
      @chunks      = ();
      $checked_all = 1;
      for my $i ( 0..$n_slaves ) {
         my $slave = $slaves->[$i];
         if ( $skip_slave{$i} ) {
            PTDEBUG && _d('Skipping slave', $slave->name(),
               'due to previous error it caused');
            next;
         }
         PTDEBUG && _d('Getting last checksum on', $slave->name());
         eval {
            my ($chunk) = $slave->dbh()->selectrow_array($sql);
            PTDEBUG && _d($slave->name(), 'max chunk:', $chunk);
            push @chunks, $chunk || 0;
         };
         if (my $e = $EVAL_ERROR) {
            PTDEBUG && _d($e);
            if ( $slave->lost_connection($e) ) {
               if ( !$have_warned{$i} && $o->get('quiet') < 2 ) {
                  warn ts("Lost connection to " .  $slave->name() . " while "
                     . "waiting for the last checksum of table "
                     . "$tbl->{db}.$tbl->{tbl} to replicate. Will reconnect "
                     . "and try again. No more warnings for this replica will "
                     . "be printed.\n");
                  $have_warned{$i}++;
               }
               eval { $slave->connect() };
               if ( $EVAL_ERROR ) {
                  PTDEBUG && _d($EVAL_ERROR);
                  sleep 1; # wait between failed reconnect attempts
               }
               $checked_all = 0;
            }
            else {
               if ( $o->get('quiet') < 2 ) {
                  warn ts("Error waiting for the last checksum of table "
                        . "$tbl->{db}.$tbl->{tbl} to replicate to "
                        . "replica " . $slave->name() . ": $e\n"
                        . "Check that the replica is running and has the "
                        . "replicate table $repl_table.  Checking the replica "
                        . "for checksum differences will probably cause "
                        . "another error.\n");
               }
               $tbl->{checksum_results}->{errors}++;
               $skip_slave{$i} = 1;
            }
            next;
         }
      }

      # If we have no chunks, which can happen if the slaves
      # were skipped due to errors, then @chunks will be empty
      # and nothing of the following applies. In fact, it
      # leads to an uninit warning because of $chunks[0]; See
      # https://bugs.launchpad.net/percona-toolkit/+bug/1052475
      next unless @chunks;
      @chunks = sort { $a <=> $b } @chunks;
      if ( $chunks[0] < $max_chunk ) {
         if ( $check_pr ) {
            $check_pr->update(sub { return $chunks[0]; });
         }

         # We shouldn't wait long here because we already waited
         # for all slaves to catchup at least until --max-lag.
         $sleep_time += 0.25 if $sleep_time <= $o->get('max-lag');
         PTDEBUG && _d('Sleep', $sleep_time, 'waiting for chunks');
         sleep $sleep_time;
      }
   }
   return;
}

# Catches signals so we can exit gracefully.
sub sig_int {
   my ( $signal ) = @_;
   $exit_status |= $PTC_EXIT_STATUS{CAUGHT_SIGNAL};
   if ( $oktorun ) {
      warn "# Caught SIG$signal.\n";
      $oktorun = 0;
   }
   else {
      warn "# Exiting on SIG$signal.\n";
      exit $exit_status;
   }
}

sub _d {
   my ($package, undef, $line) = caller 0;
   @_ = map { (my $temp = $_) =~ s/\n/\n# /g; $temp; }
        map { defined $_ ? $_ : 'undef' }
        @_;
   print STDERR "# $package:$line $PID ", join(' ', @_), "\n";
}

# ############################################################################
# Run the program.
# ############################################################################

# https://bugs.launchpad.net/percona-toolkit/+bug/916999
# http://www.mysqlperformanceblog.com/2012/02/21/dbd-mysql-4-014-breaks-pt-table-checksum-2-0/
eval {
   require DBD::mysql;
};
if ( !$EVAL_ERROR && $DBD::mysql::VERSION eq "4.014" ) {
   die "DBD::mysql v4.014 is installed, but it has as bug which causes "
     . "pt-table-checksum to fail.  Please upgrade DBD::mysql to any "
     . "newer version.\n"
}

if ( !caller ) { exit main(@ARGV); }

1; # Because this is a module as well as a script.

# ############################################################################
# Documentation
# ############################################################################
=pod

=head1 NAME

pt-table-checksum - Verify MySQL replication integrity.

=head1 SYNOPSIS

Usage: pt-table-checksum [OPTIONS] [DSN]

pt-table-checksum performs an online replication consistency check by executing
checksum queries on the master, which produces different results on replicas
that are inconsistent with the master.  The optional DSN specifies the master
host.  The tool's L<"EXIT STATUS"> is non-zero if any differences are found,
or if any warnings or errors occur.

The following command will connect to the replication master on localhost,
checksum every table, and report the results on every detected replica:

   pt-table-checksum

This tool is focused on finding data differences efficiently.  If any data is
different, you can resolve the problem with pt-table-sync.

=head1 RISKS

Percona Toolkit is mature, proven in the real world, and well tested,
but all database tools can pose a risk to the system and the database
server.  Before using this tool, please:

=over

=item * Read the tool's documentation

=item * Review the tool's known L<"BUGS">

=item * Test the tool on a non-production server

=item * Backup your production server and verify the backups

=back

See also L<"LIMITATIONS">.

=head1 DESCRIPTION

pt-table-checksum is designed to do the right thing by default in almost every
case.  When in doubt, use L<"--explain"> to see how the tool will checksum a
table.  The following is a high-level overview of how the tool functions.

In contrast to older versions of pt-table-checksum, this tool is focused on a
single purpose, and does not have a lot of complexity or support many different
checksumming techniques.  It executes checksum queries on only one server, and
these flow through replication to re-execute on replicas.  If you need the older
behavior, you can use Percona Toolkit version 1.0.

pt-table-checksum connects to the server you specify, and finds databases and
tables that match the filters you specify (if any).  It works one table at a
time, so it does not accumulate large amounts of memory or do a lot of work
before beginning to checksum.  This makes it usable on very large servers. We
have used it on servers with hundreds of thousands of databases and tables, and
trillions of rows.  No matter how large the server is, pt-table-checksum works
equally well.

One reason it can work on very large tables is that it divides each table into
chunks of rows, and checksums each chunk with a single REPLACE..SELECT query.
It varies the chunk size to make the checksum queries run in the desired amount
of time.  The goal of chunking the tables, instead of doing each table with a
single big query, is to ensure that checksums are unintrusive and don't cause
too much replication lag or load on the server.  That's why the target time for
each chunk is 0.5 seconds by default.

The tool keeps track of how quickly the server is able to execute the queries,
and adjusts the chunks as it learns more about the server's performance.  It
uses an exponentially decaying weighted average to keep the chunk size stable,
yet remain responsive if the server's performance changes during checksumming
for any reason.  This means that the tool will quickly throttle itself if your
server becomes heavily loaded during a traffic spike or a background task, for
example.

Chunking is accomplished by a technique that we used to call "nibbling" in other
tools in Percona Toolkit.  It is the same technique used for pt-archiver, for
example.  The legacy chunking algorithms used in older versions of
pt-table-checksum are removed, because they did not result in predictably sized
chunks, and didn't work well on many tables.  All that is required to divide a
table into chunks is an index of some sort (preferably a primary key or unique
index).  If there is no index, and the table contains a suitably small number of
rows, the tool will checksum the table in a single chunk.

pt-table-checksum has many other safeguards to ensure that it does not interfere
with any server's operation, including replicas.  To accomplish this,
pt-table-checksum detects replicas and connects to them automatically.  (If this
fails, you can give it a hint with the L<"--recursion-method"> option.)

The tool monitors replicas continually.  If any replica falls too far behind in
replication, pt-table-checksum pauses to allow it to catch up.  If any replica
has an error, or replication stops, pt-table-checksum pauses and waits.  In
addition, pt-table-checksum looks for common causes of problems, such as
replication filters, and refuses to operate unless you force it to.  Replication
filters are dangerous, because the queries that pt-table-checksum executes could
potentially conflict with them and cause replication to fail.

pt-table-checksum verifies that chunks are not too large to checksum safely. It
performs an EXPLAIN query on each chunk, and skips chunks that might be larger
than the desired number of rows. You can configure the sensitivity of this
safeguard with the L<"--chunk-size-limit"> option. If a table will be
checksummed in a single chunk because it has a small number of rows, then
pt-table-checksum additionally verifies that the table isn't oversized on
replicas.  This avoids the following scenario: a table is empty on the master
but is very large on a replica, and is checksummed in a single large query,
which causes a very long delay in replication.

There are several other safeguards. For example, pt-table-checksum sets its
session-level innodb_lock_wait_timeout to 1 second, so that if there is a lock
wait, it will be the victim instead of causing other queries to time out.
Another safeguard checks the load on the database server, and pauses if the load
is too high. There is no single right answer for how to do this, but by default
pt-table-checksum will pause if there are more than 25 concurrently executing
queries.  You should probably set a sane value for your server with the
L<"--max-load"> option.

Checksumming usually is a low-priority task that should yield to other work on
the server. However, a tool that must be restarted constantly is difficult to
use.  Thus, pt-table-checksum is very resilient to errors.  For example, if the
database administrator needs to kill pt-table-checksum's queries for any reason,
that is not a fatal error.  Users often run pt-kill to kill any long-running
checksum queries. The tool will retry a killed query once, and if it fails
again, it will move on to the next chunk of that table.  The same behavior
applies if there is a lock wait timeout.  The tool will print a warning if such
an error happens, but only once per table.  If the connection to any server
fails, pt-table-checksum will attempt to reconnect and continue working.

If pt-table-checksum encounters a condition that causes it to stop completely,
it is easy to resume it with the L<"--resume"> option. It will begin from the
last chunk of the last table that it processed.  You can also safely stop the
tool with CTRL-C.  It will finish the chunk it is currently processing, and then
exit.  You can resume it as usual afterwards.

After pt-table-checksum finishes checksumming all of the chunks in a table, it
pauses and waits for all detected replicas to finish executing the checksum
queries.  Once that is finished, it checks all of the replicas to see if they
have the same data as the master, and then prints a line of output with the
results.  You can see a sample of its output later in this documentation.

The tool prints progress indicators during time-consuming operations.  It prints
a progress indicator as each table is checksummed.  The progress is computed by
the estimated number of rows in the table. It will also print a progress report
when it pauses to wait for replication to catch up, and when it is waiting to
check replicas for differences from the master.  You can make the output less
verbose with the L<"--quiet"> option.

If you wish, you can query the checksum tables manually to get a report of which
tables and chunks have differences from the master.  The following query will
report every database and table with differences, along with a summary of the
number of chunks and rows possibly affected:

  SELECT db, tbl, SUM(this_cnt) AS total_rows, COUNT(*) AS chunks
  FROM percona.checksums
  WHERE (
   master_cnt <> this_cnt
   OR master_crc <> this_crc
   OR ISNULL(master_crc) <> ISNULL(this_crc))
  GROUP BY db, tbl;

The table referenced in that query is the checksum table, where the checksums
are stored.  Each row in the table contains the checksum of one chunk of data
from some table in the server.

Version 2.0 of pt-table-checksum is not backwards compatible with pt-table-sync
version 1.0.  In some cases this is not a serious problem.  Adding a
"boundaries" column to the table, and then updating it with a manually generated
WHERE clause, may suffice to let pt-table-sync version 1.0 interoperate with
pt-table-checksum version 2.0.  Assuming an integer primary key named 'id', You
can try something like the following:

  ALTER TABLE checksums ADD boundaries VARCHAR(500);
  UPDATE checksums
   SET boundaries = COALESCE(CONCAT('id BETWEEN ', lower_boundary,
      ' AND ', upper_boundary), '1=1');

Take into consideration that by default, pt-table-checksum use C<CRC32> checksums.
C<CRC32> is not a cryptographic algorithm and for that reason it is prone to have
collisions. On the other hand, C<CRC32> algorithm is faster and less CPU-intensive
than C<MD5> and C<SHA1>.

Related reading material:
Percona Toolkit UDFs: L<https://www.percona.com/doc/percona-server/LATEST/management/udf_percona_toolkit.html>
How to avoid hash collisions when using MySQL’s CRC32 function: L<https://www.percona.com/blog/2014/10/13/how-to-avoid-hash-collisions-when-using-mysqls-crc32-function/>

=head1 LIMITATIONS

=over

=item Replicas using row-based replication

pt-table-checksum requires statement-based replication, and it sets
C<binlog_format=STATEMENT> on the master, but due to a MySQL limitation
replicas do not honor this change.  Therefore, checksums will not replicate
past any replicas using row-based replication that are masters for
further replicas.

The tool automatically checks the C<binlog_format> on all servers.
See L<"--[no]check-binlog-format"> .

(L<Bug 899415|https://bugs.launchpad.net/percona-toolkit/+bug/899415>)

=item Schema and table differences

The tool presumes that schemas and tables are identical on the master and
all replicas.  Replication will break if, for example, a replica does not
have a schema that exists on the master (and that schema is checksummed),
or if the structure of a table on a replica is different than on the master.

=back

=head1 Percona XtraDB Cluster

pt-table-checksum works with Percona XtraDB Cluster (PXC) 5.5.28-23.7 and newer.
The number of possible Percona XtraDB Cluster setups is large given that
it can be used with regular replication as well.  Therefore, only the setups
listed below are supported and known to work.  Other setups, like cluster
to cluster, are not support and probably don't work.

Except where noted, all of the following supported setups require that you
use the C<dsn> method for L<"--recursion-method"> to specify cluster nodes.
Also, the lag check (see L<"REPLICA CHECKS">) is not performed for cluster
nodes.

=over

=item Single cluster

The simplest PXC setup is a single cluster: all servers are cluster nodes,
and there are no regular replicas.  If all nodes are specified in the
DSN table (see L<"--recursion-method">), then you can run the tool on any
node and any diffs on any other nodes will be detected.

All nodes must be in the same cluster (have the same C<wsrep_cluster_name>
value), else the tool exits with an error.  Although it's possible to have
different clusters with the same name, this should not be done and is not
supported.  This applies to all supported setups.

=item Single cluster with replicas

Cluster nodes can also be regular masters and replicate to regular replicas.
However, the tool can only detect diffs on a replica if ran on the replica's
"master node".  For example, if the cluster setup is,

   node1 <-> node2 <-> node3
               |         |
               |         +-> replica3
               +-> replica2

you can detect diffs on replica3 by running the tool on node3, but to detect
diffs on replica2 you must run the tool again on node2.  If you run the tool
on node1, it will not detect diffs on either replica.

Currently, the tool does not detect this setup or warn about replicas that
cannot be checked (e.g. replica2 when running on node3).

Replicas in this setup are still subject to L<"--[no]check-binlog-format">.

=item Master to single cluster

It is possible for a regular master to replicate to a cluster, as if the
cluster were one logical slave, like:

   master -> node1 <-> node2 <-> node3

The tool supports this setup but only if ran on the master and if all nodes
in the cluster are consistent with the "direct replica" (node1 in this example)
of the master.  For example, if all nodes have value "foo" for row 1 but
the master has value "bar" for the same row, this diff will be detected.
Or if only node1 has this diff, it will also be detected.  But if only node2
or node3 has this diff, it will not be detected.  Therefore, this setup is
used to check that the master and the cluster as a whole are consistent.

In this setup, the tool can automatically detect the "direct replica" (node1)
when ran on the master, so you do not have to use the C<dsn> method for
L<"--recursion-method"> because node1 will represent the entire cluster,
which is why all other nodes must be consistent with it.

The tool warns when it detects this setup to remind you that it only works
when used as described above.  These warnings do not affect the exit status
of the tool; they're only reminders to help avoid false-positive results.

=item RocksDB support

Due to the limitations in the RocksDB engine like not suporting binlog_format=STATEMENT
or they way RocksDB handles Gap locks, pt-table-cheksum will skip tables using RocksDB engine.
More Information: (L<https://www.percona.com/doc/percona-server/LATEST/myrocks/limitations.html>)

=back

=head1 OUTPUT

The tool prints tabular results, one line per table:

              TS ERRORS  DIFFS  ROWS  DIFF_ROWS CHUNKS SKIPPED    TIME TABLE
  10-20T08:36:50      0      0   200      0       1       0   0.005 db1.tbl1
  10-20T08:36:50      0      0   603      3       7       0   0.035 db1.tbl2
  10-20T08:36:50      0      0    16      0       1       0   0.003 db2.tbl3
  10-20T08:36:50      0      0   600      0       6       0   0.024 db2.tbl4

Errors, warnings, and progress reports are printed to standard error.  See also
L<"--quiet">.

Each table's results are printed when the tool finishes checksumming the table.
The columns are as follows:

=over

=item TS

The timestamp (without the year) when the tool finished checksumming the table.

=item ERRORS

The number of errors and warnings that occurred while checksumming the table.
Errors and warnings are printed to standard error while the table is in
progress.

=item DIFFS

The number of chunks that differ from the master on one or more replicas.
If C<--no-replicate-check> is specified, this column will always have zeros.
If L<"--replicate-check-only"> is specified, then only tables with differences
are printed.

=item ROWS

The number of rows selected and checksummed from the table.  It might be
different from the number of rows in the table if you use the --where option.

=item DIFF_ROWS

The maximum number of differences per chunk. If a chunk has 2 different rows and
another chunk has 3 different rows, this value will be 3.

=item CHUNKS

The number of chunks into which the table was divided.

=item SKIPPED

The number of chunks that were skipped due one or more of these problems:

   * MySQL not using the --chunk-index
   * MySQL not using the full chunk index (--[no]check-plan)
   * Chunk size is greater than --chunk-size * --chunk-size-limit
   * Lock wait timeout exceeded (--retries)
   * Checksum query killed (--retries)

As of pt-table-checksum 2.2.5, skipped chunks cause a non-zero L<"EXIT STATUS">.

=item TIME

The time elapsed while checksumming the table.

=item TABLE

The database and table that was checksummed.

=back

If L<"--replicate-check-only"> is specified, only checksum differences on
detected replicas are printed.  The output is different: one paragraph per
replica, one checksum difference per line, and values are separated by spaces:

  Differences on h=127.0.0.1,P=12346
  TABLE CHUNK CNT_DIFF CRC_DIFF CHUNK_INDEX LOWER_BOUNDARY UPPER_BOUNDARY
  db1.tbl1 1 0 1 PRIMARY 1 100
  db1.tbl1 6 0 1 PRIMARY 501 600

  Differences on h=127.0.0.1,P=12347
  TABLE CHUNK CNT_DIFF CRC_DIFF CHUNK_INDEX LOWER_BOUNDARY UPPER_BOUNDARY
  db1.tbl1 1 0 1 PRIMARY 1 100
  db2.tbl2 9 5 0 PRIMARY 101 200

The first line of a paragraph indicates the replica with differences.
In this example there are two: h=127.0.0.1,P=12346 and h=127.0.0.1,P=12347.
The columns are as follows:

=over

=item TABLE

The database and table that differs from the master.

=item CHUNK

The chunk number of the table that differs from the master.

=item CNT_DIFF

The number of chunk rows on the replica minus the number of chunk rows
on the master.

=item CRC_DIFF

1 if the CRC of the chunk on the replica is different than the CRC of the
chunk on the master, else 0.

=item CHUNK_INDEX

The index used to chunk the table.

=item LOWER_BOUNDARY

The index values that define the lower boundary of the chunk.

=item UPPER_BOUNDARY

The index values that define the upper boundary of the chunk.

=back

=head1 EXIT STATUS

pt-table-checksum has three possible exit statuses: zero, 255, and any other
value is a bitmask with flags for different problems.

A zero exit status indicates no errors, warnings, or checksum differences,
or skipped chunks or tables.

A 255 exit status indicates a fatal error.  In other words: the tool died
or crashed.  The error is printed to C<STDERR>.

If the exit status is not zero or 255, then its value functions as a bitmask
with these flags:

   FLAG              BIT VALUE  MEANING
   ================  =========  ==========================================
   ERROR                     1  A non-fatal error occurred
   ALREADY_RUNNING           2  --pid file exists and the PID is running
   CAUGHT_SIGNAL             4  Caught SIGHUP, SIGINT, SIGPIPE, or SIGTERM
   NO_SLAVES_FOUND           8  No replicas or cluster nodes were found
   TABLE_DIFF               16  At least one diff was found
   SKIP_CHUNK               32  At least one chunk was skipped
   SKIP_TABLE               64  At least one table was skipped
   REPLICATION_STOPPED     128  Replica is down or stopped

If any flag is set, the exit status will be non-zero.  Use the bitwise C<AND>
operation to check for a particular flag.  For example, if C<$exit_status & 16>
is true, then at least one diff was found.

As of pt-table-checksum 2.2.5, skipped chunks cause a non-zero exit status.
An exit status of zero or 32 is equivalent to a zero exit status with skipped
chunks in previous versions of the tool.

=head1 OPTIONS

This tool accepts additional command-line arguments.  Refer to the
L<"SYNOPSIS"> and usage information for details.

=over

=item --ask-pass

group: Connection

Prompt for a password when connecting to MySQL.

=item --channel

type: string

Channel name used when connected to a server using replication channels.
Suppose you have two masters, master_a at port 12345, master_b at port 1236 and
a slave connected to both masters using channels chan_master_a and chan_master_b.
If you want to run pt-table-sync to synchronize the slave against master_a, pt-table-sync
won't be able to determine what's the correct master since SHOW SLAVE STATUS
will return 2 rows. In this case, you can use --channel=chan_master_a to specify
the channel name to use in the SHOW SLAVE STATUS command.

=item --[no]check-binlog-format

default: yes

Check that the C<binlog_format> is the same on all servers.

See "Replicas using row-based replication" under L<"LIMITATIONS">.

=item --binary-index

This option modifies the behavior of L<"--create-replicate-table"> such that the
replicate table's upper and lower boundary columns are created with the BLOB
data type.
This is useful in cases where you have trouble checksumming tables with keys that 
include a binary data type or that have non-standard character sets.
See L<"--replicate">.

=item --check-interval

type: time; default: 1; group: Throttle

Sleep time between checks for L<"--max-lag">.

=item --[no]check-plan

default: yes

Check query execution plans for safety. By default, this option causes
pt-table-checksum to run EXPLAIN before running queries that are meant to access
a small amount of data, but which could access many rows if MySQL chooses a bad
execution plan. These include the queries to determine chunk boundaries and the
chunk queries themselves. If it appears that MySQL will use a bad query
execution plan, the tool will skip the chunk of the table.

The tool uses several heuristics to determine whether an execution plan is bad.
The first is whether EXPLAIN reports that MySQL intends to use the desired index
to access the rows. If MySQL chooses a different index, the tool considers the
query unsafe.

The tool also checks how much of the index MySQL reports that it will use for
the query. The EXPLAIN output shows this in the key_len column. The tool
remembers the largest key_len seen, and skips chunks where MySQL reports that it
will use a smaller prefix of the index. This heuristic can be understood as
skipping chunks that have a worse execution plan than other chunks.

The tool prints a warning the first time a chunk is skipped due to
a bad execution plan in each table. Subsequent chunks are skipped silently,
although you can see the count of skipped chunks in the SKIPPED column in
the tool's output.

This option adds some setup work to each table and chunk. Although the work is
not intrusive for MySQL, it results in more round-trips to the server, which
consumes time. Making chunks too small will cause the overhead to become
relatively larger. It is therefore recommended that you not make chunks too
small, because the tool may take a very long time to complete if you do.

=item --[no]check-replication-filters

default: yes; group: Safety

Do not checksum if any replication filters are set on any replicas.
The tool looks for server options that filter replication, such as
binlog_ignore_db and replicate_do_db.  If it finds any such filters,
it aborts with an error.

If the replicas are configured with any filtering options, you should be careful
not to checksum any databases or tables that exist on the master and not the
replicas.  Changes to such tables might normally be skipped on the replicas
because of the filtering options, but the checksum queries modify the contents
of the table that stores the checksums, not the tables whose data you are
checksumming.  Therefore, these queries will be executed on the replica, and if
the table or database you're checksumming does not exist, the queries will cause
replication to fail.  For more information on replication rules, see
L<http://dev.mysql.com/doc/en/replication-rules.html>.

Replication filtering makes it impossible to be sure that the checksum queries
won't break replication (or simply fail to replicate).  If you are sure that
it's OK to run the checksum queries, you can negate this option to disable the
checks.  See also L<"--replicate-database">.

See also L<"REPLICA CHECKS">.

=item --check-slave-lag

type: string; group: Throttle

Pause checksumming until this replica's lag is less than L<"--max-lag">.  The
value is a DSN that inherits properties from the master host and the connection
options (L<"--port">, L<"--user">, etc.).  By default, pt-table-checksum
monitors lag on all connected replicas, but this option limits lag monitoring
to the specified replica.  This is useful if certain replicas are intentionally
lagged (with L<pt-slave-delay> for example), in which case you can specify
a normal replica to monitor.

See also L<"REPLICA CHECKS">.

=item --[no]check-slave-tables

default: yes; group: Safety

Checks that tables on slaves exist and have all the checksum L<"--columns">.
Tables missing on slaves or not having all the checksum L<"--columns"> can
cause the tool to break replication when it tries to check for differences.
Only disable this check if you are aware of the risks and are sure that all
tables on all slaves exist and are identical to the master.

=item --chunk-index

type: string

Prefer this index for chunking tables.  By default, pt-table-checksum chooses
the most appropriate index for chunking.  This option lets you specify the index
that you prefer.  If the index doesn't exist, then pt-table-checksum will fall
back to its default behavior of choosing an index.  pt-table-checksum adds the
index to the checksum SQL statements in a C<FORCE INDEX> clause.  Be careful
when using this option; a poor choice of index could cause bad performance.
This is probably best to use when you are checksumming only a single table, not
an entire server.

=item --chunk-index-columns

type: int

Use only this many left-most columns of a L<"--chunk-index">.  This works
only for compound indexes, and is useful in cases where a bug in the MySQL
query optimizer (planner) causes it to scan a large range of rows instead
of using the index to locate starting and ending points precisely.  This
problem sometimes occurs on indexes with many columns, such as 4 or more.
If this happens, the tool might print a warning related to the
L<"--[no]check-plan"> option.  Instructing the tool to use only the first
N columns of the index is a workaround for the bug in some cases.

=item --chunk-size

type: size; default: 1000

Number of rows to select for each checksum query.  Allowable suffixes are
k, M, G.  You should not use this option in most cases; prefer L<"--chunk-time">
instead.

This option can override the default behavior, which is to adjust chunk size
dynamically to try to make chunks run in exactly L<"--chunk-time"> seconds.
When this option isn't set explicitly, its default value is used as a starting
point, but after that, the tool ignores this option's value.  If you set this
option explicitly, however, then it disables the dynamic adjustment behavior and
tries to make all chunks exactly the specified number of rows.

There is a subtlety: if the chunk index is not unique, then it's possible that
chunks will be larger than desired. For example, if a table is chunked by an
index that contains 10,000 of a given value, there is no way to write a WHERE
clause that matches only 1,000 of the values, and that chunk will be at least
10,000 rows large.  Such a chunk will probably be skipped because of
L<"--chunk-size-limit">.

Selecting a small chunk size will cause the tool to become much slower, in part
because of the setup work required for L<"--[no]check-plan">.

=item --chunk-size-limit

type: float; default: 2.0; group: Safety

Do not checksum chunks this much larger than the desired chunk size.

When a table has no unique indexes, chunk sizes can be inaccurate.  This option
specifies a maximum tolerable limit to the inaccuracy.  The tool uses <EXPLAIN>
to estimate how many rows are in the chunk.  If that estimate exceeds the
desired chunk size times the limit (twice as large, by default), then the tool
skips the chunk.

The minimum value for this option is 1, which means that no chunk can be larger
than L<"--chunk-size">.  You probably don't want to specify 1, because rows
reported by EXPLAIN are estimates, which can be different from the real number
of rows in the chunk.  If the tool skips too many chunks because they are
oversized, you might want to specify a value larger than the default of 2.

You can disable oversized chunk checking by specifying a value of 0.

=item --chunk-time

type: float; default: 0.5

Adjust the chunk size dynamically so each checksum query takes this long to execute.

The tool tracks the checksum rate (rows per second) for all tables and each
table individually.  It uses these rates to adjust the chunk size after each
checksum query, so that the next checksum query takes this amount of time (in
seconds) to execute.

The algorithm is as follows: at the beginning of each table, the chunk size is
initialized from the overall average rows per second since the tool began
working, or the value of L<"--chunk-size"> if the tool hasn't started working
yet. For each subsequent chunk of a table, the tool adjusts the chunk size to
try to make queries run in the desired amount of time.  It keeps an
exponentially decaying moving average of queries per second, so that if the
server's performance changes due to changes in server load, the tool adapts
quickly.  This allows the tool to achieve predictably timed queries for each
table, and for the server overall.

If this option is set to zero, the chunk size doesn't auto-adjust, so query
checksum times will vary, but query checksum sizes will not. Another way to do
the same thing is to specify a value for L<"--chunk-size"> explicitly, instead
of leaving it at the default.

=item --columns

short form: -c; type: array; group: Filter

Checksum only this comma-separated list of columns.  If a table doesn't have
any of the specified columns it will be skipped.

This option applies to all tables, so it really only makes sense when
checksumming one table unless the tables have a common set of columns.

=item --config

type: Array; group: Config

Read this comma-separated list of config files; if specified, this must be the
first option on the command line.

See the L<"--help"> output for a list of default config files.

=item --[no]create-replicate-table

default: yes

Create the L<"--replicate"> database and table if they do not exist.
The structure of the replicate table is the same as the suggested table
mentioned in L<"--replicate">.

=item --databases

short form: -d; type: hash; group: Filter

Only checksum this comma-separated list of databases.

=item --databases-regex

type: string; group: Filter

Only checksum databases whose names match this Perl regex. This is matched
against the lowercase table name. This is the bare regex; it should not be
enclosed in slashes.

=item --defaults-file

short form: -F; type: string; group: Connection

Only read mysql options from the given file.  You must give an absolute
pathname.

=item --disable-qrt-plugin

Disable the QRT (Query Response Time) plugin if it is enabled.

=item --[no]empty-replicate-table

default: yes

Delete previous checksums for each table before checksumming the table.  This
option does not truncate the entire table, it only deletes rows (checksums) for
each table just before checksumming the table.  Therefore, if checksumming stops
prematurely and there was preexisting data, there will still be rows for tables
that were not checksummed before the tool was stopped.

If you're resuming from a previous checksum run, then the checksum records for
the table from which the tool resumes won't be emptied.

To empty the entire replicate table, you must manually execute C<TRUNCATE TABLE>
before running the tool.

=item --engines

short form: -e; type: hash; group: Filter

Only checksum tables which use these storage engines.

=item --explain

cumulative: yes; default: 0; group: Output

Show, but do not execute, checksum queries (disables
L<"--[no]empty-replicate-table">).  If specified twice, the tool actually
iterates through the chunking algorithm, printing the upper and lower boundary
values for each chunk, but not executing the checksum queries.

=item --fail-on-stopped-replication

If replication is stopped, fail with an error (exit status 128) instead of waiting
until replication is restarted.

=item --float-precision

type: int

Precision for FLOAT and DOUBLE number-to-string conversion.  Causes FLOAT
and DOUBLE values to be rounded to the specified number of digits after the
decimal point, with the ROUND() function in MySQL.  This can help avoid
checksum mismatches due to different floating-point representations of the same
values on different MySQL versions and hardware.  The default is no rounding;
the values are converted to strings by the CONCAT() function, and MySQL chooses
the string representation.  If you specify a value of 2, for example, then the
values 1.008 and 1.009 will be rounded to 1.01, and will checksum as equal.

=item --function

type: string

Hash function for checksums (FNV1A_64, MURMUR_HASH, SHA1, MD5, CRC32, etc).

The default is to use CRC32(), but MD5() and SHA1() also work, and you
can use your own function, such as a compiled UDF, if you wish.  The
function you specify is run in SQL, not in Perl, so it must be available
to MySQL.

MySQL doesn't have good built-in hash functions that are fast.  CRC32() is too
prone to hash collisions, and MD5() and SHA1() are very CPU-intensive. The
FNV1A_64() UDF that is distributed with Percona Server is a faster alternative.
It is very simple to compile and install; look at the header in the source code
for instructions.  If it is installed, it is preferred over MD5().  You can also
use the MURMUR_HASH() function if you compile and install that as a UDF; the
source is also distributed with Percona Server, and it might be better than
FNV1A_64().

=item --help

group: Help

Show help and exit.

=item --host

short form: -h; type: string; default: localhost; group: Connection

Host to connect to.

=item --ignore-columns

type: Hash; group: Filter

Ignore this comma-separated list of columns when calculating the checksum.
If a table has all of its columns filtered by --ignore-columns, it will
be skipped.

=item --ignore-databases

type: Hash; group: Filter

Ignore this comma-separated list of databases.

=item --ignore-databases-regex

type: string; group: Filter

Ignore databases whose names match this Perl regex.

=item --ignore-engines

type: Hash; default: FEDERATED,MRG_MyISAM; group: Filter

Ignore this comma-separated list of storage engines.

=item --ignore-tables

type: Hash; group: Filter

Ignore this comma-separated list of tables.  Table names may be qualified with
the database name.  The L<"--replicate"> table is always automatically ignored.

=item --ignore-tables-regex

type: string; group: Filter

Ignore tables whose names match the Perl regex. This is matched
against the lowercase table name. This is the bare regex; it should not be
enclosed in slashes.

=item --max-lag

type: time; default: 1s; group: Throttle

Pause checksumming until all replicas' lag is less than this value.  After each
checksum query (each chunk), pt-table-checksum looks at the replication lag of
all replicas to which it connects, using Seconds_Behind_Master. If any replica
is lagging more than the value of this option, then pt-table-checksum will sleep
for L<"--check-interval"> seconds, then check all replicas again.  If you
specify L<"--check-slave-lag">, then the tool only examines that server for
lag, not all servers.

The tool waits forever for replicas to stop lagging.  If any replica is
stopped, the tool waits forever until the replica is started.  Checksumming
continues once all replicas are running and not lagging too much.

The tool prints progress reports while waiting.  If a replica is stopped, it
prints a progress report immediately, then again at every progress report
interval.

See also L<"REPLICA CHECKS">.

=item --max-load

type: Array; default: Threads_running=25; group: Throttle

Examine SHOW GLOBAL STATUS after every chunk, and pause if any status variables
are higher than the threshold.  The option accepts a comma-separated list of
MySQL status variables to check for a threshold.  An optional C<=MAX_VALUE> (or
C<:MAX_VALUE>) can follow each variable.  If not given, the tool determines a
threshold by examining the current value and increasing it by 20%.

For example, if you want the tool to pause when Threads_connected gets too high,
you can specify "Threads_connected", and the tool will check the current value
when it starts working and add 20% to that value.  If the current value is 100,
then the tool will pause when Threads_connected exceeds 120, and resume working
when it is below 120 again.  If you want to specify an explicit threshold, such
as 110, you can use either "Threads_connected:110" or "Threads_connected=110".

The purpose of this option is to prevent the tool from adding too much load to
the server. If the checksum queries are intrusive, or if they cause lock waits,
then other queries on the server will tend to block and queue. This will
typically cause Threads_running to increase, and the tool can detect that by
running SHOW GLOBAL STATUS immediately after each checksum query finishes.  If
you specify a threshold for this variable, then you can instruct the tool to
wait until queries are running normally again.  This will not prevent queueing,
however; it will only give the server a chance to recover from the queueing.  If
you notice queueing, it is best to decrease the chunk time.

=item --password

short form: -p; type: string; group: Connection

Password to use when connecting.
If password contains commas they must be escaped with a backslash: "exam\,ple"

=item --pause-file

type: string

Execution will be paused while the file specified by this param exists.

=item --pid

type: string

Create the given PID file.  The tool won't start if the PID file already
exists and the PID it contains is different than the current PID.  However,
if the PID file exists and the PID it contains is no longer running, the
tool will overwrite the PID file with the current PID.  The PID file is
removed automatically when the tool exits.

=item --plugin

type: string

Perl module file that defines a C<pt_table_checksum_plugin> class.
A plugin allows you to write a Perl module that can hook into many parts
of pt-table-checksum.  This requires a good knowledge of Perl and
Percona Toolkit conventions, which are beyond this scope of this
documentation.  Please contact Percona if you have questions or need help.

See L<"PLUGIN"> for more information.

=item --port

short form: -P; type: int; group: Connection

Port number to use for connection.

=item --progress

type: array; default: time,30

Print progress reports to STDERR.

The value is a comma-separated list with two parts.  The first part can be
percentage, time, or iterations; the second part specifies how often an update
should be printed, in percentage, seconds, or number of iterations.  The tool
prints progress reports for a variety of time-consuming operations, including
waiting for replicas to catch up if they become lagged.

=item --quiet

short form: -q; cumulative: yes; default: 0

Print only the most important information (disables L<"--progress">).
Specifying this option once causes the tool to print only errors, warnings, and
tables that have checksum differences.

Specifying this option twice causes the tool to print only errors.  In this
case, you can use the tool's exit status to determine if there were any warnings
or checksum differences.

=item --recurse

type: int

Number of levels to recurse in the hierarchy when discovering replicas.
Default is infinite.  See also L<"--recursion-method"> and L<"REPLICA CHECKS">.

=item --recursion-method

type: array; default: processlist,hosts

Preferred recursion method for discovering replicas.  pt-table-checksum
performs several L<"REPLICA CHECKS"> before and while running.

Although replicas are not required to run pt-table-checksum, the tool
cannot detect diffs on slaves that it cannot discover.  Therefore,
a warning is printed and the L<"EXIT STATUS"> is non-zero if no replicas
are found and the method is not C<none>.  If this happens, try a different
recursion method, or use the C<dsn> method to specify the replicas to check.

Possible methods are:

  METHOD       USES
  ===========  =============================================
  processlist  SHOW PROCESSLIST
  hosts        SHOW SLAVE HOSTS
  cluster      SHOW STATUS LIKE 'wsrep\_incoming\_addresses'
  dsn=DSN      DSNs from a table
  none         Do not find slaves

The C<processlist> method is the default, because C<SHOW SLAVE HOSTS> is not
reliable.  However, if the server uses a non-standard port (not 3306), then
the C<hosts> method becomes the default because it works better in this case.

The C<hosts> method requires replicas to be configured with C<report_host>,
C<report_port>, etc.

The C<cluster> method requires a cluster based on Galera 23.7.3 or newer,
such as Percona XtraDB Cluster versions 5.5.29 and above.  This will
auto-discover nodes in a cluster using
C<SHOW STATUS LIKE 'wsrep\_incoming\_addresses'>.  You can combine C<cluster>
with C<processlist> and C<hosts> to auto-discover cluster nodes and replicas,
but this functionality is experimental.

The C<dsn> method is special: rather than automatically discovering replicas,
this method specifies a table with replica DSNs.  The tool will only connect
to these replicas.  This method works best when replicas do not use the same
MySQL username or password as the master, or when you want to prevent the tool
from connecting to certain replicas.  The C<dsn> method is specified like:
C<--recursion-method dsn=h=host,D=percona,t=dsns>.  The specified DSN must
have D and t parts, or just a database-qualified t part, which specify the
DSN table.  The DSN table must have the following structure:

  CREATE TABLE `dsns` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `parent_id` int(11) DEFAULT NULL,
    `dsn` varchar(255) NOT NULL,
    PRIMARY KEY (`id`)
  );

DSNs are ordered by C<id>, but C<id> and C<parent_id> are otherwise ignored.
The C<dsn> column contains a replica DSN like it would be given on the command
line, for example: C<"h=replica_host,u=repl_user,p=repl_pass">.

The C<none> method makes the tool ignore all slaves and cluster nodes. This
method is not recommended because it effectively disables the
L<"REPLICA CHECKS"> and no differences can be found. It is useful, however, if
you only need to write checksums on the master or a single cluster node. The
safer alternative is C<--no-replicate-check>: the tool finds replicas and
cluster nodes, performs the L<"REPLICA CHECKS">, but does not check for
differences. See L<"--[no]replicate-check">.

=item --replicate

type: string; default: percona.checksums

Write checksum results to this table.  The replicate table must have this
structure (MAGIC_create_replicate):

  CREATE TABLE checksums (
     db             CHAR(64)     NOT NULL,
     tbl            CHAR(64)     NOT NULL,
     chunk          INT          NOT NULL,
     chunk_time     FLOAT            NULL,
     chunk_index    VARCHAR(200)     NULL,
     lower_boundary TEXT             NULL,
     upper_boundary TEXT             NULL,
     this_crc       CHAR(40)     NOT NULL,
     this_cnt       INT          NOT NULL,
     master_crc     CHAR(40)         NULL,
     master_cnt     INT              NULL,
     ts             TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
     PRIMARY KEY (db, tbl, chunk),
     INDEX ts_db_tbl (ts, db, tbl)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8;

Note: lower_boundary and upper_boundary data type can be BLOB. See L<"--binary-index">.

By default, L<"--create-replicate-table"> is true, so the database and
the table specified by this option are created automatically if they do not
exist.

Be sure to choose an appropriate storage engine for the replicate table.  If you
are checksumming InnoDB tables, and you use MyISAM for this table, a deadlock
will break replication, because the mixture of transactional and
non-transactional tables in the checksum statements will cause it to be written
to the binlog even though it had an error.  It will then replay without a
deadlock on the replicas, and break replication with "different error on master
and slave."  This is not a problem with pt-table-checksum; it's a problem with
MySQL replication, and you can read more about it in the MySQL manual.

The replicate table is never checksummed (the tool automatically adds this
table to L<"--ignore-tables">).

=item --[no]replicate-check

default: yes

Check replicas for data differences after finishing each table.  The tool finds
differences by executing a simple SELECT statement on all detected replicas.
The query compares the replica's checksum results to the master's checksum
results.  It reports differences in the DIFFS column of the output.

=item --replicate-check-only

Check replicas for consistency without executing checksum queries.
This option is used only with L<"--[no]replicate-check">.  If specified,
pt-table-checksum doesn't checksum any tables.  It checks replicas for
differences found by previous checksumming, and then exits.  It might be useful
if you run pt-table-checksum quietly in a cron job, for example, and later want
a report on the results of the cron job, perhaps to implement a Nagios check.

=item --replicate-check-retries

type: int; default: 1

Retry checksum comparison this many times when a difference is encountered.
Only when a difference persists after this number of checks is it considered valid.
Using this option with a value of 2 or more alleviates spurious differences that
arise when using the --resume option.

=item --replicate-database

type: string

USE only this database.  By default, pt-table-checksum executes USE to select
the database that contains the table it's currently working on.  This is is a
best effort to avoid problems with replication filters such as binlog_ignore_db
and replicate_ignore_db.  However, replication filters can create a situation
where there simply is no one right way to do things.  Some statements might not
be replicated, and others might cause replication to fail.  In such cases, you
can use this option to specify a default database that pt-table-checksum selects
with USE, and never changes.  See also L<"--[no]check-replication-filters">.

=item --resume

Resume checksumming from the last completed chunk (disables
L<"--[no]empty-replicate-table">).  If the tool stops before it checksums all
tables, this option makes checksumming resume from the last chunk of the last
table that it finished.

=item --retries

type: int; default: 2

Retry a chunk this many times when there is a nonfatal error.  Nonfatal errors
are problems such as a lock wait timeout or the query being killed.

=item --run-time

type: time

How long to run.  Default is to run until all tables have been checksummed.
These time value suffixes are allowed: s (seconds), m (minutes), h (hours),
and d (days).  Combine this option with L<"--resume"> to checksum as many
tables within an allotted time, resuming from where the tool left off next
time it is ran.

=item --separator

type: string; default: #

The separator character used for CONCAT_WS().  This character is used to join
the values of columns when checksumming.

=item --skip-check-slave-lag

type: DSN; repeatable: yes

DSN to skip when checking slave lag. It can be used multiple times.
Example: --skip-check-slave-lag h=127.1,P=12345 --skip-check-slave-lag h=127.1,P=12346

=item --slave-user

type: string

Sets the user to be used to connect to the slaves.
This parameter allows you to have a different user with less privileges on the
slaves but that user must exist on all slaves.

=item --slave-password

type: string

Sets the password to be used to connect to the slaves.
It can be used with --slave-user and the password for the user must be the same
on all slaves.

=item --set-vars

type: Array; group: Connection

Set the MySQL variables in this comma-separated list of C<variable=value> pairs.

By default, the tool sets:

=for comment ignore-pt-internal-value
MAGIC_set_vars

   wait_timeout=10000
   innodb_lock_wait_timeout=1

Variables specified on the command line override these defaults.  For
example, specifying C<--set-vars wait_timeout=500> overrides the defaultvalue of C<10000>.

The tool prints a warning and continues if a variable cannot be set.

=item --socket

short form: -S; type: string; group: Connection

Socket file to use for connection.

=item --slave-skip-tolerance

type: float; default: 1.0

When a master table is marked to be checksumed in only one chunk but a slave
table exceeds the maximum accepted size for this, the table is skipped.
Since number of rows are often rough estimates, many times tables are skipped
needlessly for very small differences.
This option provides a max row excess tolerance to prevent this.
For example a value of 1.2 will tolerate slave tables with up to 20% excess rows.

=item --tables

short form: -t; type: hash; group: Filter

Checksum only this comma-separated list of tables.
Table names may be qualified with the database name.

=item --tables-regex

type: string; group: Filter

Checksum only tables whose names match this Perl regex.

=item --trim

Add TRIM() to VARCHAR columns (helps when comparing 4.1 to >= 5.0).
This is useful when you don't care about the trailing space differences between
MySQL versions that vary in their handling of trailing spaces. MySQL 5.0 and
later all retain trailing spaces in VARCHAR, while previous versions would
remove them.  These differences will cause false checksum differences.

=item --truncate-replicate-table

Truncate the replicate table before starting the checksum.
This parameter differs from L<--empty-replicate-table> which only deletes the rows
for the table being checksumed when starting the checksum for that table, while
L<--truncate-replicate-table> will truncate the replicate table at the beginning of the
process and thus, all previous checksum information will be losti, even if the process
stops due to an error.

=item --user

short form: -u; type: string; group: Connection

User for login if not current user.

=item --version

group: Help

Show version and exit.

=item --[no]version-check

default: yes

Check for the latest version of Percona Toolkit, MySQL, and other programs.

This is a standard "check for updates automatically" feature, with two
additional features.  First, the tool checks its own version and also the
versions of the following software: operating system, Percona Monitoring and
Management (PMM), MySQL, Perl, MySQL driver for Perl (DBD::mysql), and
Percona Toolkit. Second, it checks for and warns about versions with known
problems. For example, MySQL 5.5.25 had a critical bug and was re-released
as 5.5.25a.

A secure connection to Percona’s Version Check database server is done to
perform these checks. Each request is logged by the server, including software
version numbers and unique ID of the checked system. The ID is generated by the
Percona Toolkit installation script or when the Version Check database call is
done for the first time.

Any updates or known problems are printed to STDOUT before the tool's normal
output.  This feature should never interfere with the normal operation of the
tool.

For more information, visit L<https://www.percona.com/doc/percona-toolkit/LATEST/version-check.html>.

=item --where

type: string

Do only rows matching this WHERE clause.  You can use this option to limit
the checksum to only part of the table.  This is particularly useful if you have
append-only tables and don't want to constantly re-check all rows; you could run
a daily job to just check yesterday's rows, for instance.

This option is much like the -w option to mysqldump.  Do not specify the WHERE
keyword.  You might need to quote the value.  Here is an example:

  pt-table-checksum --where "ts > CURRENT_DATE - INTERVAL 1 DAY"

=back

=head1 REPLICA CHECKS

By default, pt-table-checksum attempts to find and connect to all replicas
connected to the master host.  This automated process is called
"slave recursion" and is controlled by the L<"--recursion-method"> and
L<"--recurse"> options.  The tool performs these checks on all replicas:

=over

=item 1. L<"--[no]check-replication-filters">

pt-table-checksum checks for replication filters on all replicas because
they can complicate or break the checksum process.  By default, the tool
will exit if any replication filters are found, but this check can be
disabled by specifying C<--no-check-replication-filters>.

=item 2. L<"--replicate"> table

pt-table-checksum checks that the L<"--replicate"> table exists on all
replicas, else checksumming can break replication when updates to the table
on the master replicate to a replica that doesn't have the table.  This
check cannot be disabled, and the tool waits forever until the table
exists on all replicas, printing L<"--progress"> messages while it waits.

=item 3. Single chunk size

If a table can be checksummed in a single chunk on the master,
pt-table-checksum will check that the table size on all replicas is less than
L<"--chunk-size"> * L<"--chunk-size-limit">. This prevents a rare problem
where the table on the master is empty or small, but on a replica it is much
larger. In this case, the single chunk checksum on the master would overload
the replica.

Another rare problem occurs when the table size on a replica is close to
L<"--chunk-size"> * L<"--chunk-size-limit">. In such cases, the table is more
likely to be skipped even though it's safe to checksum in a single chunk.
This happens because table sizes are estimates. When those estimates and
L<"--chunk-size"> * L<"--chunk-size-limit"> are almost equal, this check
becomes more sensitive to the estimates' margin of error rather than actual
significant differences in table sizes. Specifying a larger value for
L<"--chunk-size-limit"> helps avoid this problem.

This check cannot be disabled.

=item 4. Lag

After each chunk, pt-table-checksum checks the lag on all replicas, or only
the replica specified by L<"--check-slave-lag">.  This helps the tool
not to overload the replicas with checksum data.  There is no way to
disable this check, but you can specify a single replica to check with
L<"--check-slave-lag">, and if that replica is the fastest, it will help
prevent the tool from waiting too long for replica lag to abate.

=item 5. Checksum chunks

When pt-table-checksum finishes checksumming a table, it waits for the last
checksum chunk to replicate to all replicas so it can perform the
L<"--[no]replicate-check">.  Disabling that option by specifying
L<--no-replicate-check> disables this check, but it also disables
immediate reporting of checksum differences, thereby requiring a second run
of the tool with L<"--replicate-check-only"> to find and print checksum
differences.

=back

=head1 PLUGIN

The file specified by L<"--plugin"> must define a class (i.e. a package)
called C<pt_table_checksum_plugin> with a C<new()> subroutine.
The tool will create an instance of this class and call any hooks that
it defines.  No hooks are required, but a plugin isn't very useful without
them.

These hooks, in this order, are called if defined:

   init
   before_replicate_check
   after_replicate_check
   get_slave_lag
   before_checksum_table
   after_checksum_table

Each hook is passed different arguments.  To see which arguments are passed
to a hook, search for the hook's name in the tool's source code, like:

   # --plugin hook
   if ( $plugin && $plugin->can('init') ) {
      $plugin->init(
         slaves         => $slaves,
         slave_lag_cxns => $slave_lag_cxns,
         repl_table     => $repl_table,
      );
   }

The comment C<# --plugin hook> precedes every hook call.

Please contact Percona if you have questions or need help.

=head1 DSN OPTIONS

These DSN options are used to create a DSN.  Each option is given like
C<option=value>.  The options are case-sensitive, so P and p are not the
same option.  There cannot be whitespace before or after the C<=> and
if the value contains whitespace it must be quoted.  DSN options are
comma-separated.  See the L<percona-toolkit> manpage for full details.

=over

=item * A

dsn: charset; copy: yes

Default character set.

=item * D

copy: no

DSN table database.

=item * F

dsn: mysql_read_default_file; copy: yes

Defaults file for connection values.

=item * h

dsn: host; copy: yes

Connect to host.

=item * p

dsn: password; copy: yes

Password to use when connecting.
If password contains commas they must be escaped with a backslash: "exam\,ple"

=item * P

dsn: port; copy: yes

Port number to use for connection.

=item * S

dsn: mysql_socket; copy: no

Socket file to use for connection.

=item * t

copy: no

DSN table table.

=item * u

dsn: user; copy: yes

User for login if not current user.

=back

=head1 ENVIRONMENT

The environment variable C<PTDEBUG> enables verbose debugging output to STDERR.
To enable debugging and capture all output to a file, run the tool like:

   PTDEBUG=1 pt-table-checksum ... > FILE 2>&1

Be careful: debugging output is voluminous and can generate several megabytes
of output.

=head1 ATTENTION

Using <PTDEBUG> might expose passwords. When debug is enabled, all command line 
parameters are shown in the output.

=head1 SYSTEM REQUIREMENTS

You need Perl, DBI, DBD::mysql, and some core packages that ought to be
installed in any reasonably new version of Perl.

=head1 BUGS

For a list of known bugs, see L<https://jira.percona.com/projects/PT/issues>.

Please report bugs at L<https://jira.percona.com/projects/PT>.
Include the following information in your bug report:

=over

=item * Complete command-line used to run the tool

=item * Tool L<"--version">

=item * MySQL version of all servers involved

=item * Output from the tool including STDERR

=item * Input files (log/dump/config files, etc.)

=back

If possible, include debugging output by running the tool with C<PTDEBUG>;
see L<"ENVIRONMENT">.

=head1 DOWNLOADING

Visit L<http://www.percona.com/software/percona-toolkit/> to download the
latest release of Percona Toolkit.  Or, get the latest release from the
command line:

   wget percona.com/get/percona-toolkit.tar.gz

   wget percona.com/get/percona-toolkit.rpm

   wget percona.com/get/percona-toolkit.deb

You can also get individual tools from the latest release:

   wget percona.com/get/TOOL

Replace C<TOOL> with the name of any tool.

=head1 AUTHORS

Baron Schwartz and Daniel Nichter

=head1 ACKNOWLEDGMENTS

Claus Jeppesen, Francois Saint-Jacques, Giuseppe Maxia, Heikki Tuuri,
James Briggs, Martin Friebe, and Sergey Zhuravlev

=head1 ABOUT PERCONA TOOLKIT

This tool is part of Percona Toolkit, a collection of advanced command-line
tools for MySQL developed by Percona.  Percona Toolkit was forked from two
projects in June, 2011: Maatkit and Aspersa.  Those projects were created by
Baron Schwartz and primarily developed by him and Daniel Nichter.  Visit
L<http://www.percona.com/software/> to learn about other free, open-source
software from Percona.

=head1 COPYRIGHT, LICENSE, AND WARRANTY

This program is copyright 2011-2021 Percona LLC and/or its affiliates,
2007-2011 Baron Schwartz.

THIS PROGRAM IS PROVIDED "AS IS" AND WITHOUT ANY EXPRESS OR IMPLIED
WARRANTIES, INCLUDING, WITHOUT LIMITATION, THE IMPLIED WARRANTIES OF
MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation, version 2; OR the Perl Artistic License.  On UNIX and similar
systems, you can issue `man perlgpl' or `man perlartistic' to read these
licenses.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc., 59 Temple
Place, Suite 330, Boston, MA  02111-1307  USA.

=head1 VERSION

pt-table-checksum 3.5.1

=cut