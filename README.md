# SondeHub+ Local

Local web dashboard for radiosonde monitoring.

It combines information from:

- SondeHub
- Radiosonde Watch
- local radiosonde_auto_rx logs

The application provides a browser interface and local HTTP API.

## Requirements

- Linux
- Python 3
- Python venv
- network access to SondeHub
- optional Radiosonde Watch
- optional radiosonde_auto_rx
- systemd if using the included service template

Python dependencies are listed in requirements.txt.

## Configuration

Configuration is provided through environment variables.

See .env.example.

Available variables:

SONDEHUB_PLUS_PORT
SONDEHUB_PLUS_BIND
SONDEHUB_LISTENER_CALLSIGN
RADIOSONDE_WATCH_URL
RADIOSONDE_WATCH_LOCAL_ENDPOINT
RADIOSONDE_AUTO_RX_DIR
SONDEHUB_API_URL
SONDEHUB_PLUS_USER_AGENT

Set SONDEHUB_LISTENER_CALLSIGN to your own SondeHub
listener/station callsign.

## Network access

The example configuration uses:

SONDEHUB_PLUS_BIND=127.0.0.1

This means the web server is accessible only from the local machine.

For access from a trusted LAN you can use:

SONDEHUB_PLUS_BIND=0.0.0.0

The application does not provide built-in authentication or TLS.

Do not expose it directly to the public Internet.

For remote access use an authenticated reverse proxy, VPN,
or another trusted access layer.

## Installation

First check the repository without making system changes:

    ./install.sh --check

Install dependencies and systemd files:

    sudo ./install.sh

The installer does not automatically start or enable the service.

Edit:

    /etc/default/sondehub-plus

Then start:

    sudo systemctl enable --now sondehub-plus.service

Health check:

    curl http://127.0.0.1:8093/healthz

Logs:

    journalctl -u sondehub-plus.service -f

## Radiosonde Watch

If RADIOSONDE_WATCH_URL points to localhost or 127.0.0.1,
the browser link to Radiosonde Watch automatically uses the hostname
through which SondeHub+ was opened.

This avoids embedding a machine-specific hostname in app.py.

## Local receiver endpoint

Some Radiosonde Watch installations expose a dedicated endpoint
containing live receiver state, including fields such as:

    received_serials
    state
    auto_rx_running

Configure the endpoint with:

    RADIOSONDE_WATCH_LOCAL_ENDPOINT=/api/your-local-endpoint

If this variable is not configured, SondeHub+ continues to operate,
but dedicated live local-receiver status is unavailable.

## Memory guards

The supplied systemd example contains:

    MemoryHigh=350M
    MemoryMax=500M
    MemorySwapMax=64M

These are emergency safety limits, not expected normal memory usage.

## Repository layout

    app.py
    static/
    requirements.txt
    .env.example
    install.sh
    systemd/
        sondehub-plus.service.example
    THIRD_PARTY.md

## Development

The application remains primarily a single Python file.

Large refactors should be separated from the first public release
so that the tested runtime behaviour is not changed unnecessarily.

## License

SondeHub+ Local project code is distributed under the
GNU General Public License version 3 or later
(GPL-3.0-or-later).

See the LICENSE file for the full license text.

Third-party components and services remain subject to their
respective licenses and terms. See THIRD_PARTY.md.
