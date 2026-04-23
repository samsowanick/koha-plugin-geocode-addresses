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

our $VERSION = '1.5';
our $metadata = {
    name            => 'Borrower Geocoding Plugin',
    author          => 'Samuel Sowanick, Corvallis-Benton County Public Library',
    description     => 'Geocodes patron addresses.',
    date_authored   => '2026-04-22',
    date_updated    => '2026-04-22',
    minimum_version => '22.05',
    version         => $VERSION,
};

# Removed unused 'use DBI' — C4::Context->dbh handles all DB connections.

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
        # Drop FK constraint if present — ignore errors if it was never there.
        eval {
            $dbh->do("ALTER TABLE borrower_lat_long DROP FOREIGN KEY fk_bll_borrowernumber");
        };
        # Add geocoded_on column if missing.
        eval {
            $dbh->do("
                ALTER TABLE borrower_lat_long
                ADD COLUMN geocoded_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    ON UPDATE CURRENT_TIMESTAMP
            ");
        };
    }
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
    my $api_key   = $self->retrieve_data('google_api_key');
    my $batch_max = $opts{batch_max} // 0;   # 0 = unlimited (nightly); set for web tool
    return (0, "Google API Key is not configured.") unless $api_key;

    my $dbh = C4::Context->dbh;

    # Increased timeout to 30s to be more tolerant under load.
    my $ua = LWP::UserAgent->new;
    $ua->timeout(30);

    # Purge rows whose borrowernumber no longer exists in borrowers.
    # Without a FK cascade this must be done manually. Doing it here — rather
    # than via a trigger — ensures all writes to borrower_lat_long are
    # controlled exclusively by this plugin.
    $dbh->do("
        DELETE bll FROM borrower_lat_long bll
        LEFT JOIN borrowers b ON bll.borrowernumber = b.borrowernumber
        WHERE b.borrowernumber IS NULL
    ");

    # The WHERE clause uses MySQL/MariaDB's null-safe equality operator (<=>)
    # to detect address field changes including NULL transitions.
    # Note: <=> in Perl is the numeric spaceship/sort operator — a different
    # thing. The <=> here is inside a SQL string, interpreted by the DB engine.
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

    # Fetch all rows into memory immediately, then close the cursor.
    # This avoids holding an open read cursor against the borrowers table
    # for the entire duration of the sync (which sleeps 100ms per row),
    # reducing lock contention during busy periods.
    my @rows = @{ $sth->fetchall_arrayref({}) };
    $sth->finish();

    # INSERT ... ON DUPLICATE KEY UPDATE is a true in-place update.
    # REPLACE INTO (the previous approach) does a DELETE + INSERT under the
    # hood, which could silently remove rows from any future table that
    # references borrower_lat_long. ON DUPLICATE KEY UPDATE never deletes.
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

        my $safe_address = uri_escape($full_address);
        my $url = "https://maps.googleapis.com/maps/api/geocode/json?address=$safe_address&key=$api_key";

        # Attempt the geocode request with exponential backoff on rate-limit
        # responses. Google returns OVER_QUERY_LIMIT when the per-second or
        # per-day quota is exceeded. We retry up to 3 times with increasing
        # delays (2s, 4s, 8s) before giving up on this borrower.
        my $data;
        my $max_retries = 3;
        my $retry_delay = 2;
        my $gave_up     = 0;

        for my $attempt ( 1 .. $max_retries ) {
            my $response = $ua->get($url);

            unless ($response->is_success) {
                warn "GeocodeBorrowers: HTTP error for borrower $row->{borrowernumber} "
                    . "(attempt $attempt): " . $response->status_line;
                # Non-200 HTTP errors are not retryable — move on immediately.
                $gave_up = 1;
                last;
            }

            $data = eval { decode_json($response->decoded_content) };
            if ($@) {
                warn "GeocodeBorrowers: JSON parse error for borrower $row->{borrowernumber}: $@";
                $gave_up = 1;
                last;
            }

            if ( $data->{status} eq 'OVER_QUERY_LIMIT' ) {
                warn "GeocodeBorrowers: OVER_QUERY_LIMIT on attempt $attempt for borrower "
                    . "$row->{borrowernumber} — waiting ${retry_delay}s before retry.";
                sleep($retry_delay);
                $retry_delay *= 2;
                $data = undef;
                next;
            }

            # Any other status (OK, ZERO_RESULTS, INVALID_REQUEST, etc.)
            # is a definitive answer — no point retrying.
            last;
        }

        unless (defined $data) {
            warn "GeocodeBorrowers: Giving up on borrower $row->{borrowernumber} after retries.";
            $fail_count++;
            next;
        }

        if ($gave_up) {
            $fail_count++;
            next;
        }

        unless ($data->{status} eq 'OK' && @{$data->{results}}) {
            warn "GeocodeBorrowers: No results for borrower $row->{borrowernumber} "
                . "(status: $data->{status}, address: $full_address)";
            $fail_count++;
            next;
        }

        my $lat = $data->{results}[0]{geometry}{location}{lat};
        my $lng = $data->{results}[0]{geometry}{location}{lng};

        # Do not write the row if lat/lng are missing — this prevents a
        # borrower from being silently stuck as geocoded-but-null and never
        # retried on subsequent syncs (address fields would match, so the
        # WHERE clause would skip them forever).
        unless (defined $lat && $lat != 0 && defined $lng && $lng != 0) {
            warn "GeocodeBorrowers: API returned OK but no valid coordinates "
                . "for borrower $row->{borrowernumber}";
            $fail_count++;
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
            next;
        }

        $count++;

        # Stop early if a batch cap was set (used by the web tool to avoid
        # hitting the server's HTTP gateway timeout). The nightly job passes
        # no limit and runs until all patrons are processed.
        last if $batch_max && $count >= $batch_max;

        # Throttle to ~10 req/sec to stay within Google's rate limits
        # and avoid hammering their API during a bulk sync.
        select(undef, undef, undef, 0.1);
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
    # Previously 'save' was a hidden field present on every page load, meaning a
    # GET request to the configure page would overwrite stored data.
    if ( $cgi->request_method() eq 'POST' && $cgi->param('save') ) {
        my $key = $cgi->param('google_api_key') || '';
        $self->store_data({ google_api_key => $key });
        $template->param( success_message => 'Configuration saved.' );
    }

    $template->param( google_api_key => $self->retrieve_data('google_api_key') );
    $self->output_html( $template->output() );
}

sub tool {
    my ( $self, $args ) = @_;
    my $cgi      = $self->{cgi};
    my $template = $self->get_template({ file => 'tool.tt' });

    # Use Koha's template system for both GET and POST so that csrf-token.inc
    # is rendered by the same mechanism Koha uses everywhere else in the staff
    # interface. Printing raw HTML bypasses Koha's CSRF middleware entirely,
    # which is why the POST was being rejected with "No CSRF token passed".
    if ( $cgi->request_method() eq 'POST' && $cgi->param('run_sync') ) {
        # Cap web-triggered syncs at 50 patrons per click to avoid hitting
        # the server's HTTP gateway timeout. The nightly job runs unlimited.
        # The result message will tell the librarian if more remain.
        my ($count, $message) = $self->sync_geocoding( batch_max => 50 );
        $template->param( result_message => $message );
    }

    $self->output_html( $template->output() );
}


sub nightly {
    my ($self) = @_;

    # Log the result so failures are visible in Koha's plack/starman logs
    # rather than silently discarded.
    my ($count, $message) = $self->sync_geocoding();
    warn "GeocodeBorrowers nightly sync: $message";
}

1;