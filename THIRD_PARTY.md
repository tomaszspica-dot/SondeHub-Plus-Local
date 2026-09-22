# Third-party components

This project uses external open-source software and services.

## Python dependency

### sondehub 0.3.2

Direct Python dependency used by app.py.

License:

GPL-3.0-or-later

The package is installed through pip from requirements.txt and is
not vendored in this repository.

It declares additional transitive Python dependencies. Those
dependencies are resolved by pip and remain subject to their own
licenses.


## Upstream projects and integrations

### projecthorus/pysondehub

Repository:

https://github.com/projecthorus/pysondehub

Relationship to this project:

Direct Python dependency used by `app.py` for SondeHub realtime functionality. The package is installed through `requirements.txt`; it is not vendored here.

### projecthorus/radiosonde_auto_rx

Repository:

https://github.com/projecthorus/radiosonde_auto_rx

Relationship to this project:

Optional upstream radiosonde receiver software. SondeHub+ Local can read local `radiosonde_auto_rx` log files and derive local reception/flight statistics from them. The upstream receiver application is not vendored in this repository.

### projecthorus/sondehub-tracker / SondeHub ecosystem

Repository:

https://github.com/projecthorus/sondehub-tracker

Relationship to this project:

SondeHub+ Local uses SondeHub APIs, data structures and services. The SondeHub Tracker project is listed here as a canonical public project in the same upstream ecosystem; this dashboard is a separate project.

### Radiosonde Watch

Relationship to this project:

Optional local companion service consumed over HTTP. SondeHub+ Local can read its status/radiosonde endpoints and redirect the browser to its local UI.

A canonical public upstream repository for the particular Radiosonde Watch installation used during development is not documented in this repository, so no source URL is asserted here.

### Provenance note

The repository history available here does not establish exact line-by-line copied source provenance from the upstream projects listed above. The relationships documented above are dependencies, integrations, API/data use, or runtime interoperability unless a future entry explicitly states that a file or fragment was copied/adapted.

If an exact copied or adapted source fragment is identified, add the upstream repository URL, source path/commit, license and the corresponding local file here.

## Browser libraries

### Leaflet 1.9.4

Used for interactive maps.

License:

BSD-2-Clause

Leaflet is loaded at runtime from a public CDN and is not vendored
in this repository.

### Chart.js 4.4.3

Used for the altitude chart.

License:

MIT

Chart.js is loaded at runtime from a public CDN and is not vendored
in this repository.

## OpenStreetMap

The application displays map tiles based on OpenStreetMap data.

OpenStreetMap data is made available under the Open Database
License (ODbL).

The interactive map includes visible OpenStreetMap contributor
attribution linking to the OpenStreetMap copyright and licensing
information.

## Repository-local static files

The files:

    static/rw-altitude-chart.css
    static/rw-altitude-chart.js

are repository-local application files.

The external Chart.js library referenced by the JavaScript file is
not embedded in that file; it is loaded separately at runtime.

## Project license

The project itself is distributed under:

GPL-3.0-or-later

See LICENSE.

Third-party names, trademarks, software, data and services remain
the property of their respective owners.
