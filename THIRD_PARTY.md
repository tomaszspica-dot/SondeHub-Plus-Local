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
