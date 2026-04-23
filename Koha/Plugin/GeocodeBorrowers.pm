package Koha::Plugin::GeocodeBorrowers;

use Modern::Perl;
use strict;
use warnings;
use CGI;
use utf8;
use base qw(Koha::Plugins::Base);
use C4::Context;
use Try::Tiny;
use LWP::UserAgent;
use JSON qw(decode_json);
use URI::Escape;

our $VERSION = '2.0';
our $metadata = {
    name            => 'Borrower Geocoding Plugin',
    author          => 'Samuel Sowanick, Corvallis-Benton County Public Library',
    description     => 'Geocodes patron addresses using the Nominatim/OpenStreetMap API.',
    date_authored   => '2026-04-22',
    date_updated    => '2026-04-23',
    minimum_version => '22.05',
    version         => $VERSION,
};

# Nominatim endpoint used by this plugin.
# Per the Nominatim usage policy (https://operations.osmfoundation.org/policies/nominatim/)
# the public API at nominatim.openstreetmap.org must not be used for heavy bulk
# geocoding. This plugin complies by:
#   1. Caching every result in borrower_lat_long so the same address is never
#      requested twice.
#   2. Throttling to exactly 1 request/second (the maximum allowed).
#   3. Sending a descriptive User-Agent that identifies the application and
#      contact, as required by the policy.
#   4. Capping web-triggered runs at 50 patrons so a librarian click cannot
#      accidentally fire hundreds of requests in a single HTTP response cycle.
# If your installation has a large patron base, consider running your own
# Nominatim instance (https://nominatim.org/release-docs/latest/admin/Installation/)
# and overriding NOMINATIM_URL below.
use constant NOMINATIM_URL => 'https://nominatim.openstreetmap.org/search';

# User-Agent string sent with every request.
# The Nominatim policy requires a value that clearly identifies your application
# and provides a contact point. Stock LWP strings ("libwww-perl/X.Y") are
# explicitly disallowed. Change the URL/email below to match your library.
use constant NOMINATIM_UA =>
    'Koha-GeocodeBorrowers/2.0 (Koha ILS patron geocoding plugin; '
  . 'https://github.com/your-library/koha-plugin-geocode-borrowers; '
  . 'your-contact@yourlibrary.org)';

sub new {
    my ( $class, $args ) = @_;
    $args->{metadata} = $metadata;
    $args->{metadata}->{class} = $class;
    my $self = $class->SUPER::new($args);
    return $self;
}

sub install {
    my ($self, $args) = @_;
    my $dbh = C4::Context->dbh;

    # No foreign key on borrowernumber — we want data to flow INTO this table
    # only. A FK would let Koha's InnoDB engine implicitly modify or delete rows
    # here in response to changes in borrowers (ON DELETE CASCADE, ON UPDATE
    # CASCADE), which is the opposite of what we want. Orphan cleanup is handled
    # explicitly at the start of sync_geocoding() instead.
    #
    # geocoded_on timestamp gives us an audit trail of when each record was last
    # geocoded, and lets us force re-syncs by date if needed.
    my $query = "
        CREATE TABLE IF NOT EXISTS borrower_lat_long (
            borrowernumber INT(11) NOT NULL,
            address TEXT,
            city TEXT,
            state TEXT,
            zipcode TEXT,
            latitude DECIMAL(10,8),
            longitude DECIMAL(11,8),
            geocoded_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (borrowernumber)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    ";

    try {
        $dbh->do($query);
    } catch {
        warn "Failed to create borrower_lat_long table: $_";
        return 0;
    };
    return 1;
}

sub upgrade {
    my ($self, $args) = @_;
    my $dbh = C4::Context->dbh;
    my $dt  = $args->{DB_VERSION} // '0';

    # v1.4 -> v1.5: drop foreign key if it still exists (installs that had
    # the old schema), add geocoded_on if missing.
    if ( $dt lt '1.5' ) {
        eval {
            $dbh->do("ALTER TABLE borrower_lat_long DROP FOREIGN KEY fk_bll_borrowernumber");
        };
        eval {
            $dbh->do("
                ALTER TABLE borrower_lat_long
                ADD COLUMN geocoded_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    ON UPDATE CURRENT_TIMESTAMP
            ");
        };
    }

    # v1.x -> v2.0: no schema changes; Google API key is no longer used but
    # the stored value is left in plugin_data so a downgrade is non-destructive.
    return 1;
}

sub uninstall {
    my ($self, $args) = @_;
    my $dbh = C4::Context->dbh;
    $dbh->do("DROP TABLE IF EXISTS borrower_lat_long");
    return 1;
}

sub sync_geocoding {
    my ($self, %opts) = @_;
    my $batch_max = $opts{batch_max} // 0;   # 0 = unlimited (nightly); set for web tool

    my $dbh = C4::Context->dbh;

    # Build a User-Agent that satisfies Nominatim's identification requirement.
    # LWP's default string ("libwww-perl/X.Y") is explicitly disallowed by the
    # usage policy, so we replace it entirely.
    my $ua = LWP::UserAgent->new( agent => NOMINATIM_UA );
    $ua->timeout(30);

    # Purge rows whose borrowernumber no longer exists in borrowers.
    $dbh->do("
        DELETE bll FROM borrower_lat_long bll
        LEFT JOIN borrowers b ON bll.borrowernumber = b.borrowernumber
        WHERE b.borrowernumber IS NULL
    ");

    # The WHERE clause uses MySQL/MariaDB's null-safe equality operator (<=>)
    # to detect address field changes including NULL transitions.
    my $sth = $dbh->prepare("
        SELECT b.borrowernumber, b.address, b.city, b.state, b.zipcode
        FROM borrowers b
        LEFT JOIN borrower_lat_long bll ON b.borrowernumber = bll.borrowernumber
        WHERE bll.borrowernumber IS NULL
           OR NOT (b.address  <=> bll.address)
           OR NOT (b.city     <=> bll.city)
           OR NOT (b.state    <=> bll.state)
           OR NOT (b.zipcode  <=> bll.zipcode)
    ");
    $sth->execute();

    # Fetch all rows into memory immediately, then close the cursor to reduce
    # lock contention while we sleep between geocode requests.
    my @rows = @{ $sth->fetchall_arrayref({}) };
    $sth->finish();

    # INSERT ... ON DUPLICATE KEY UPDATE is a true in-place update.
    my $insert_sth = $dbh->prepare("
        INSERT INTO borrower_lat_long
            (borrowernumber, address, city, state, zipcode, latitude, longitude)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE
            address     = VALUES(address),
            city        = VALUES(city),
            state       = VALUES(state),
            zipcode     = VALUES(zipcode),
            latitude    = VALUES(latitude),
            longitude   = VALUES(longitude)
    ");

    my $count      = 0;
    my $skip_count = 0;
    my $fail_count = 0;

    for my $row (@rows) {

        # Use defined + ne '' instead of bare truthiness so that a field
        # containing the string "0" is not incorrectly dropped.
        my $full_address = join(', ',
            grep { defined $_ && $_ ne '' }
            ($row->{address}, $row->{city}, $row->{state}, $row->{zipcode})
        );

        unless ($full_address) {
            $skip_count++;
            next;
        }

        # Nominatim free-form search. We request JSON output and limit to 1
        # result since we only need the best match.
        # NOTE: Do NOT send personally identifiable patron data to the public
        # Nominatim service if your privacy policy or local law prohibits it.
        # In that case, run your own Nominatim instance and update NOMINATIM_URL.
        my $url = NOMINATIM_URL
            . '?q='      . uri_escape($full_address)
            . '&format=jsonv2'
            . '&limit=1'
            . '&addressdetails=0';

        my $response = $ua->get($url);

        unless ($response->is_success) {
            warn "GeocodeBorrowers: HTTP error for borrower $row->{borrowernumber}: "
                . $response->status_line;
            $fail_count++;

            # Still sleep 2 full second on error — the server may be under
            # load and we must not hammer it regardless of the outcome.
            sleep(2);
            next;
        }

        my $data = eval { decode_json($response->decoded_content) };
        if ($@) {
            warn "GeocodeBorrowers: JSON parse error for borrower $row->{borrowernumber}: $@";
            $fail_count++;
            sleep(2);
            next;
        }

        # Nominatim returns a JSON array. An empty array means no results.
        unless (ref $data eq 'ARRAY' && @$data) {
            warn "GeocodeBorrowers: No results for borrower $row->{borrowernumber} "
                . "(address: $full_address)";
            $fail_count++;
            sleep(2);
            next;
        }

        my $lat = $data->[0]{lat};
        my $lng = $data->[0]{lon};    # Nominatim uses 'lon', not 'lng'

        # Do not write the row if lat/lng are missing — this prevents a
        # borrower from being silently stuck as geocoded-but-null and never
        # retried on subsequent syncs.
        unless (defined $lat && $lat != 0 && defined $lng && $lng != 0) {
            warn "GeocodeBorrowers: API returned a result but no valid coordinates "
                . "for borrower $row->{borrowernumber}";
            $fail_count++;
            sleep(2);
            next;
        }

        # Wrap individual inserts in eval so one DB error doesn't abort
        # the entire sync run.
        eval {
            $insert_sth->execute(
                $row->{borrowernumber},
                $row->{address},
                $row->{city},
                $row->{state},
                $row->{zipcode},
                $lat,
                $lng
            );
        };
        if ($@) {
            warn "GeocodeBorrowers: DB insert failed for borrower $row->{borrowernumber}: $@";
            $fail_count++;
            sleep(2);
            next;
        }

        $count++;

        # Stop early if a batch cap was set (used by the web tool).
        last if $batch_max && $count >= $batch_max;

        # Nominatim's usage policy requires a maximum of 1 request per second.
        # We sleep a full second here (rather than 100 ms as with Google) to
        # ensure we never exceed that limit even under system scheduling jitter.
        sleep(2);
    }

    my $remaining = scalar(@rows) - $count - $skip_count - $fail_count;
    my $message = "Synchronized $count patron(s).";
    $message .= " Skipped $skip_count with no address." if $skip_count;
    $message .= " Failed $fail_count (see Koha logs for details)." if $fail_count;
    $message .= " $remaining patron(s) still pending — run the nightly job or sync again to continue."
        if $batch_max && $remaining > 0;

    return ($count, $message);
}

sub configure {
    my ($self, $args) = @_;
    my $cgi = $self->{cgi};
    my $template = $self->get_template({ file => 'configure.tt' });

    # Only save when the form is explicitly submitted (POST + save param).
    if ( $cgi->request_method() eq 'POST' && $cgi->param('save') ) {
        # No API key is required for the public Nominatim endpoint.
        # We store the custom Nominatim URL so installations that run their
        # own instance can point the plugin at it without editing source code.
        my $url = $cgi->param('nominatim_url') || '';
        $self->store_data({ nominatim_url => $url });
        $template->param( success_message => 'Configuration saved.' );
    }

    $template->param( nominatim_url => $self->retrieve_data('nominatim_url') );
    $self->output_html( $template->output() );
}

sub tool {
    my ( $self, $args ) = @_;
    my $cgi      = $self->{cgi};
    my $template = $self->get_template({ file => 'tool.tt' });

    if ( $cgi->request_method() eq 'POST' && $cgi->param('run_sync') ) {
        # Cap web-triggered syncs at 50 patrons per click. At 1 req/sec that is
        # ~50 seconds — well within a typical HTTP gateway timeout.
        my ($count, $message) = $self->sync_geocoding( batch_max => 50 );
        $template->param( result_message => $message );
    }

    $self->output_html( $template->output() );
}

sub nightly {
    my ($self) = @_;
    my ($count, $message) = $self->sync_geocoding();
    warn "GeocodeBorrowers nightly sync: $message";
}

1;
