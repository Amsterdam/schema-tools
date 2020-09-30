# Profile Based Authorization

This document describes how Profile Based authorization is working.

## Dataset Level Configuration

Each dataset defines minimal required permissions per dataset/table/field, if needed.

This is done via `auth` parameter on each level respectively.

### Example of Dataset Level Authorization:

    {
      "type": "dataset",
      "id": "brp",
      ...
      "auth": "BRP/R"
      "tables": [...]
    }


### Example of Table Level Authorization:
    
    {
      "type": "dataset",
      "id": "brp",
      ...
      "tables": [
        {
          "id": "ingeschrevenpersonen",
          "type": "table",
          "auth": "BRP/R",
          "schema": {
            ...
          }
        }
      ]
    }
    
### Example of Field Level Authorization:

    {
      "type": "dataset",
      "id": "brp",
      ...
      "tables": [
        {
          "id": "ingeschrevenpersonen",
          "type": "table",
          "schema": {
            ...,
            "properties": {
              "schema": {
                "$ref": "https://schemas.data.amsterdam.nl/schema@v1.1.1#/definitions/schema"
              },
              "id": {
                "auth": "BRP/R"
                "type": "integer"
              }
            }
          }
        }
      ]
    }
    
All of above require `BRP/R` permission in order to access data.

## Additional Profile Authorization

In addition to Dataset Level Configuration we may define Profiles that expand access rights for given scope.

Each request may contain one or multiple profiles, this is controlled by `scopes` definition of each profile.

NOTE: profile is assigned to request only if ALL profile scopes are subset of request scopes. Profile with no scopes will be assigned to all requests.

### Profile Structure

* name: Human Friendly profile name
* scopes: List of scopes required to use this profile
* datasets: Dictionary with details per dataset

#### Dataset Details Structure

Each dataset may define either `permissions` or `tables` definition that will apply to it's tables.

* permissions: string, defines permission to access all tables and all fields within dataset, overwriting dataset level configuration. (possible value: `read`)
* tables: dictionary, defines detailed configuration for table or fields

#### Table Details Structure

Each dataset table may define either `permissions` or `fields` definition that will apply to it's fields.

* permissions: string, defines permission to access all fields within table, overwriting dataset level configuration. (possible value: `read`)
* fields: dictionary, defines detailed configuration for each field

#### Field Permissions

Each field can define representation level to be presented to user.

* read: plain text representation of field
* encoded: encoded string

### Example Profile

    {
      "name": "medewerker",
      "scopes": ["FP/MD"],
      "datasets": {
        "parkeervakken": {
          "permisssions": "read"
        },
        "klantbeeld": {
          "tables": {
            "BvAdresBewonershistorie": {
              "fields": {
                "bsn": "encoded",
                "einddatum_bewoning": "read"
              }
            }
          }
        }
      }
    }


## Profile Combinations

Each request may contain one or multiple Profiles assigned, each profile may only extend permissions given to user.

This means if there are 2 profiles, one of which allows access to one field only, while another allows access to all fields - user will see all fields.

Representations will be merged in favour of highest as well. Given 2 profiles with `encoded` and `read` permissions, user will see `read` representation.


## Example of compined configuration

    # dataset configuration
    {
      "type": "dataset",
      "id": "brp",
      "auth": "BRP/R"
      ...
      "tables": [
        {
          "id": "ingeschrevenpersonen",
          "type": "table",
          "schema": {
            ...,
            "properties": {
              "id": {
                "type": "integer"
              }
              "bsn": {
                "auth": "BRP/RS"
                "type": "string"
              },
              ...
            }
          }
        }
      ]
    }
    
    # profile configuration for BRP/RS
    {
      "name": "medewerker",
      "scopes": ["BRP/RS"],
      "datasets": {
        "brp": {
          "tables": {
            "ingeschrevenpersonen": {
              "fields": {
                "bsn": "encoded"
              }
            }
          }
        }
      }
    }
    
    # profile configuration for BRP/RSN
    {
      "name": "medewerker+",
      "scopes": ["BRP/RSN"],
      "datasets": {
        "brp": {
          "tables": {
            "ingeschrevenpersonen": {
              "fields": {
                "bsn": "read"
              }
            }
          }
        }
      }
    }


In example above Dataset is not available to general public and requires `BRP/R` scope to be given to request.

Requests with `BPR/R` scope will see following fields in API output:

    {
      "id": 1
    }

This is because field `bsn` requires additional permission `BRP/RS` to be seen.

Profile definition for `BRP/RS` controls how `bsn` field is seen, namely encodes field for this profile.
Requests wuth `BRP/RS` scope will see following fields in API output. NOTE: `BRP/R` scope is not required to be present in request.

    {
      "id": 1,
      "bsn": "eabef034"
    }
    
Profile definition for `BRP/RSN` allows `bsn` field to be seen in plain text, therefore requests with `BRP/RSN` scope present will see plain text bsn:

    {
      "id": 1,
      "bsn": 908923894
    }
