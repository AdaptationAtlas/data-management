# Adaptation Atlas STAC Catalog

This repository contains code to generate the root STAC catalog and key sub-catalogs for the **Adaptation Atlas**. While this centralizes catalog construction for now, portions of the code may eventually be moved to their respective data repositories to simplify maintenance and dataset-specific updates.

## Running Locally

To build and view the catalog locally:

1. **Build the catalog**
   Run the provided scripts to generate the STAC catalog structure under the `dev-stac/` folder.

2. **Serve the catalog**
   Make sure `serve_stac` is executable:

   ```bash
   chmod +x serve_stac
   ```

   Then start the local server:

   ```bash
   ./serve_stac
   ```

3. **View the catalog**

   * In your browser:
     [http://localhost:8000/dev-stac/catalog.json](http://localhost:8000/dev-stac/catalog.json)

   * In STAC Browser:
     [https://radiantearth.github.io/stac-browser/#/external/http:/localhost:8000/dev-stac/catalog.json](https://radiantearth.github.io/stac-browser/#/external/http:/localhost:8000/dev-stac/catalog.json)
