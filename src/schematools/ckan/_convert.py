from __future__ import annotations

import re
from time import strftime
from typing import Any

from schematools.types import DatasetSchema


def from_dataset(ds: DatasetSchema, path: str) -> dict[str, Any]:
    """Convert a dataset to the CKAN format variant used by data.overheid.nl.

    path should be the dataset path as used in DSO-API, e.g., "beheerkaart/cbs_grid"
    for the dataset with id "beheerkaartCbsGrid".

    The output, as JSON, can be used in a CKAN create_package request.

    A limited amount of documentation for the format can be pieced together from:
    * https://docs.ckan.org/en/2.9/api/index.html
    * https://ckanext-dcatdonl.readthedocs.io/en/latest/usage-creationandupdating.html
    * https://waardelijsten.dcat-ap-donl.nl/
    """

    # XXX CKAN allows a dataset to have multiple resources associated with it,
    # which we could use to represent the tables, but data.overheid.nl doesn't
    # seem to support that (or it's broken).

    url = "https://api.data.amsterdam.nl/v1/" + path

    has_geo = any(table.has_geometry_fields for table in ds.tables)
    theme = THEME_RUIMTE if has_geo else THEME_BESTUUR

    # Apparently, titles must be globally unique. If they're not, we get an error message that
    # complains about non-unique identifiers. So, add "_amsterdam_dso-api" to prevent clashes.
    #
    # TODO if we upload anything with uppercase letters or spaces, we get an error message.
    # But most datasets in the catalog at data.overheid.nl do use those. Figure out how to
    # preserve uppercase and spaces.
    title = (
        ds.title.lower().replace(" ", "_")
        if ds.title is not None and ALMOST_NAME.match(ds.title)
        else ds.id.lower()
    ) + "_amsterdam_dso-api"

    data = {
        "authority": "http://standaarden.overheid.nl/owms/terms/Amsterdam",
        "contact_point_name": "Gemeente Amsterdam",
        "contact_point_email": "datapunt@amsterdam.nl",
        "contact_point_website": "https://data.amsterdam.nl",
        "format": JSON,
        "identifier": url,
        "language": [NLD],
        "license_id": LICENSES.get(ds.license, LICENSE_UNKNOWN),
        "metadata_language": NLD,
        "mimetype": "application/json",
        "modified": strftime("%Y-%m-%dT%H:%M:%S"),
        "name": title,
        "notes": ds.description or title,  # Must not be empty.
        "owner_org": "gemeente-amsterdam",
        "publisher": "http://standaarden.overheid.nl/owms/terms/Amsterdam",
        "theme": [theme],
        "title": title,
        "url": url,
    }

    # XXX I can't find what the inverse is called, so we omit this field for
    # niet_beschikbaar. The uploader in dcatd does the same.
    if ds.status == DatasetSchema.Status.beschikbaar:
        data["dataset_status"] = "http://data.overheid.nl/status/beschikbaar"

    return data


# RE that describes valid CKAN names and titles, except that it also allows lowercase and space.
# We map the latter to _.
ALMOST_NAME = re.compile(r"^[A-Za-z0-9_ -]+$")

# JSON is our primary file type. Not sure if we can submit multiple types for a dataset.
# Keep this consistent with the mimetype.
# From https://waardelijsten.dcat-ap-donl.nl/mdr_filetype_nal.json
JSON = "http://publications.europa.eu/resource/authority/file-type/JSON"

# From https://waardelijsten.dcat-ap-donl.nl/overheid_license.json
LICENSES = {
    "Creative Commons, Naamsvermelding": "http://creativecommons.org/licenses/by/4.0/deed.nl",
    "public": "http://creativecommons.org/publicdomain/mark/1.0/deed.nl",
    "Gesloten": "http://standaarden.overheid.nl/owms/terms/geslotenlicentie",
}
LICENSE_UNKNOWN = "http://standaarden.overheid.nl/owms/terms/licentieonbekend"

# From https://waardelijsten.dcat-ap-donl.nl/donl_language.json
NLD = "http://publications.europa.eu/resource/authority/language/NLD"

# From
# https://github.com/dataoverheid/donlsync-mappings/blob/1c7b23f3f6dfc623/value-mappings/Eindhoven__Dataset__AccessRights.json
THEME_BESTUUR = "http://standaarden.overheid.nl/owms/terms/Bestuur"
THEME_RUIMTE = "http://standaarden.overheid.nl/owms/terms/Ruimte_en_infrastructuur"
