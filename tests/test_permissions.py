from __future__ import annotations

import logging

import pytest
from psycopg.errors import DuplicateObject
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError

from schematools.importer.ndjson import NDJSONImporter
from schematools.permissions.db import (
    apply_profile_permissions,
    apply_schema_and_profile_permissions,
)
from schematools.types import DatasetSchema, ProfileSchema, Scope


# In test files we use a lot of non-existent scopes, so instead of writing scope
# json files we monkeypatch this method.
@pytest.fixture(autouse=True)
def patch_find_scope_by_id(monkeypatch):
    monkeypatch.setattr(DatasetSchema, "_find_scope_by_id", Scope.from_string)


@pytest.fixture()
def gebieden_profiles(profile_loader) -> list[ProfileSchema]:
    return [profile_loader.get_profile("gebieden_test")]


class TestReadPermissions:
    def test_auto_permissions(self, engine, gebieden_schema_auth, gebieden_profiles, dbsession):
        """
        Prove that roles are automatically created for each scope in the schema
        LEVEL/A --> scope_level_a
        LEVEL/B --> scope_level_b
        LEVEL/C --> scope_level_c
        """
        importer = NDJSONImporter(gebieden_schema_auth, engine)
        importer.generate_db_objects("bouwblokken", truncate=True, ind_extra_index=False)
        importer.generate_db_objects("buurten", truncate=True, ind_extra_index=False)

        # Apply the permissions from Schema and Profiles.
        apply_schema_and_profile_permissions(
            engine,
            gebieden_schema_auth,
            gebieden_profiles,
            create_roles=True,
        )
        _check_select_permission_granted(engine, "scope_level_a", "gebieden_buurten_v1")
        _check_select_permission_granted(
            engine, "scope_level_b", "gebieden_bouwblokken_v1", "id, eind_geldigheid"
        )
        _check_select_permission_granted(
            engine, "scope_level_c", "gebieden_bouwblokken_v1", "begin_geldigheid"
        )

    def test_roles_created(self, engine, gebieden_schema_auth):
        scope = Scope.from_string("certain_role")
        scope2 = Scope.from_string("another_role")
        apply_schema_and_profile_permissions(
            engine,
            gebieden_schema_auth,
            profiles=[],
            create_roles=True,
            all_scopes=[scope, scope2],
        )
        _check_role_exists(engine, "scope_certain_role")
        _check_role_exists(engine, "scope_another_role")

    def test_all_profile_permissions(
        self,
        engine,
        profile_loader,
        verblijfsobjecten_schema,
        brk_schema,
        brp_schema,
        gebieden_schema_scopes,
        parkeervakken_schema,
        gebieden_schema_auth,
        caplog,
    ):
        """Prove that the profiles are properly included in the grants."""
        all_schemas = {
            schema.id: schema
            for schema in (
                verblijfsobjecten_schema,
                gebieden_schema_scopes,
                brk_schema,
                brp_schema,
                parkeervakken_schema,
            )
        }
        all_profiles = profile_loader.get_all_profiles()

        for schema in all_schemas.values():
            importer = NDJSONImporter(schema, engine)
            for table in schema.get_tables(include_nested=True, include_through=True):
                importer.generate_db_objects(table.id, truncate=True, ind_extra_index=False)
                importer.generate_db_objects(table.id, truncate=True, ind_extra_index=False)

        # Apply the permissions from profiles alone.
        with (
            caplog.at_level(logging.INFO, logger="schematools.permissions.db"),
            engine.connect() as conn,
        ):
            apply_profile_permissions(
                conn,
                all_profiles,
                all_schemas,
                create_roles=True,
                verbose=1,
            )

        grants = _filter_grant_statements(caplog)
        assert grants == [
            'CREATE ROLE "scope_brk_encoded"',
            'CREATE ROLE "scope_brk_encoded.filtered" IN ROLE "scope_brk_encoded"',
            'CREATE ROLE "scope_brk_rid"',
            'CREATE ROLE "scope_brk_rid.filtered" IN ROLE "scope_brk_rid"',
            'CREATE ROLE "scope_brp_rname"',
            'CREATE ROLE "scope_brp_rname.filtered" IN ROLE "scope_brp_rname"',
            'CREATE ROLE "scope_fp_md"',
            'CREATE ROLE "scope_fp_md.filtered" IN ROLE "scope_fp_md"',
            "GRANT SELECT ON SEQUENCE public.parkeervakken_parkeervakken_regimes_v1_id_seq TO scope_fp_md",
            "GRANT SELECT ON TABLE public.brk_kadastraleobjecten_v1 TO scope_brk_encoded",
            "GRANT SELECT ON TABLE public.brk_kadastraleobjecten_v1 TO scope_brk_rid",
            'GRANT SELECT ON TABLE public.brp_ingeschrevenpersonen_v1 TO "scope_brp_rname.filtered"',
            "GRANT SELECT ON TABLE public.gebieden_bouwblokken_v1 TO scope_fp_md",
            "GRANT SELECT ON TABLE public.parkeervakken_parkeervakken_regimes_v1 TO scope_fp_md",
            "GRANT SELECT ON TABLE public.parkeervakken_parkeervakken_v1 TO scope_fp_md",
        ]

    def test_auto_permissions_with_scopes(
        self, engine, gebieden_schema_scopes, gebieden_profiles, dbsession
    ):
        """
        Prove that roles are automatically created for each Scope objects.

        Scope objects are properly resolved and permissions granted.
        Using Scope objects in the scopes/HARRY folder on dataset, table, and field level.
        Ensure we can still use auth strings in the same schema.
        """
        importer = NDJSONImporter(gebieden_schema_scopes, engine)
        importer.generate_db_objects("bouwblokken", truncate=True, ind_extra_index=False)
        importer.generate_db_objects("buurten", truncate=True, ind_extra_index=False)

        # Apply the permissions from Schema and Profiles.
        apply_schema_and_profile_permissions(
            engine,
            gebieden_schema_scopes,
            gebieden_profiles,
            create_roles=True,
        )
        _check_select_permission_granted(engine, "scope_harry_one", "gebieden_buurten_v1")
        _check_select_permission_granted(
            engine, "scope_harry_two", "gebieden_bouwblokken_v1", "id, eind_geldigheid"
        )
        _check_select_permission_granted(
            engine, "scope_harry_three", "gebieden_bouwblokken_v1", "begin_geldigheid"
        )

        # Check if auth strings in the same schema also work.
        _check_select_permission_granted(
            engine,
            "scope_level_d",
            "gebieden_bouwblokken_v1",
            "ligt_in_buurt_id, ligt_in_buurt_loose_id",
        )

    def test_nm_relations_permissions(self, engine, kadastraleobjecten_schema, dbsession, caplog):
        importer = NDJSONImporter(kadastraleobjecten_schema, engine)
        importer.generate_db_objects("kadastraleobjecten", truncate=True, ind_extra_index=False)

        # Setup schema and profile
        # This schema has auth on dataset level, not on table
        ams_schema = {kadastraleobjecten_schema.id: kadastraleobjecten_schema}

        _create_role(engine, "openbaar")
        _create_role(engine, "brk_rsn")
        _create_role(engine, "brk_ro")

        # Check if the roles exist, the tables exist,
        # and the roles have no read privilege on the tables.
        _check_select_permission_denied(engine, "openbaar", "brk_kadastraleobjecten_v1")
        _check_select_permission_denied(engine, "brk_rsn", "brk_kadastraleobjecten_v1")
        _check_select_permission_denied(engine, "brk_ro", "brk_kadastraleobjecten_v1")

        # make sure role 'write_brk' exists with create_roles=True
        # The role exists now for all test following this statement
        with caplog.at_level(logging.INFO, logger="schematools.permissions.db"):
            apply_schema_and_profile_permissions(
                engine,
                ams_schema,
                None,
                only_role="openbaar",
                only_scope="OPENBAAR",
                create_roles=True,
                verbose=1,
            )
            apply_schema_and_profile_permissions(
                engine, ams_schema, None, only_role="brk_rsn", only_scope="BRK/RSN", verbose=1
            )
            apply_schema_and_profile_permissions(
                engine, ams_schema, None, only_role="brk_ro", only_scope="BRK/RO", verbose=1
            )

        grants = _filter_grant_statements(caplog)
        assert grants == [
            'CREATE ROLE "write_brk"',
            "GRANT SELECT (begin_geldigheid) ON TABLE public.brk_kadastraleobjecten_v1 TO brk_rsn",
            "GRANT SELECT (eind_geldigheid) ON TABLE public.brk_kadastraleobjecten_v1 TO brk_rsn",
            "GRANT SELECT (id) ON TABLE public.brk_kadastraleobjecten_v1 TO brk_rsn",
            "GRANT SELECT (identificatie) ON TABLE public.brk_kadastraleobjecten_v1 TO brk_rsn",
            "GRANT SELECT (koopsom) ON TABLE public.brk_kadastraleobjecten_v1 TO brk_ro",
            "GRANT SELECT (neuron_id) ON TABLE public.brk_kadastraleobjecten_v1 TO brk_rsn",
            "GRANT SELECT (registratiedatum) ON TABLE public.brk_kadastraleobjecten_v1 TO brk_rsn",
            "GRANT SELECT (soort_cultuur_onbebouwd_code) ON TABLE public.brk_kadastraleobjecten_v1 TO brk_ro",
            "GRANT SELECT (soort_cultuur_onbebouwd_omschrijving) ON TABLE public.brk_kadastraleobjecten_v1 TO brk_ro",
            "GRANT SELECT (soort_grootte) ON TABLE public.brk_kadastraleobjecten_v1 TO brk_rsn",
            "GRANT SELECT (volgnummer) ON TABLE public.brk_kadastraleobjecten_v1 TO brk_rsn",
            "GRANT SELECT ON SEQUENCE public.brk_kadastraleobjecten_is_ontstaan_uit_kadastraalobject__id_seq TO brk_rsn",
            "GRANT SELECT ON TABLE public.brk_kadastraleobjecten_is_ontstaan_uit_kadastraalobject_v1 TO brk_rsn",
            "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.brk_kadastraleobjecten_is_ontstaan_uit_kadastraalobject_v1 TO write_brk",
            "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.brk_kadastraleobjecten_is_ontstaan_uit_kadastraalobject_v1 TO write_brk",
            "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.brk_kadastraleobjecten_is_ontstaan_uit_kadastraalobject_v1 TO write_brk",
            "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.brk_kadastraleobjecten_v1 TO write_brk",
            "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.brk_kadastraleobjecten_v1 TO write_brk",
            "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.brk_kadastraleobjecten_v1 TO write_brk",
            "GRANT USAGE ON SEQUENCE public.brk_kadastraleobjecten_is_ontstaan_uit_kadastraalobject__id_seq TO write_brk",
            "GRANT USAGE ON SEQUENCE public.brk_kadastraleobjecten_is_ontstaan_uit_kadastraalobject__id_seq TO write_brk",
            "GRANT USAGE ON SEQUENCE public.brk_kadastraleobjecten_is_ontstaan_uit_kadastraalobject__id_seq TO write_brk",
        ]

        # table denied
        _check_select_permission_denied(engine, "openbaar", "brk_kadastraleobjecten_v1")
        _check_select_permission_denied(engine, "openbaar", "brk_kadastraleobjecten_v1", "koopsom")

        # table denied, column granted, auth level dataset
        _check_select_permission_denied(engine, "brk_rsn", "brk_kadastraleobjecten_v1")
        _check_select_permission_denied(engine, "brk_rsn", "brk_kadastraleobjecten_v1", "koopsom")
        _check_select_permission_granted(
            engine, "brk_rsn", "brk_kadastraleobjecten_v1", "identificatie"
        )

        # table denied, column granted, auth level field
        _check_select_permission_denied(engine, "brk_ro", "brk_kadastraleobjecten_v1")
        _check_select_permission_granted(engine, "brk_ro", "brk_kadastraleobjecten_v1", "koopsom")

        # nm relations table tests, should have dataset auth level: brk_rsn
        _check_select_permission_denied(
            engine, "openbaar", "brk_kadastraleobjecten_is_ontstaan_uit_kadastraalobject_v1"
        )
        _check_select_permission_denied(
            engine, "brk_ro", "brk_kadastraleobjecten_is_ontstaan_uit_kadastraalobject_v1"
        )
        _check_select_permission_granted(
            engine, "brk_rsn", "brk_kadastraleobjecten_is_ontstaan_uit_kadastraalobject_v1"
        )

    def test_brk_permissions(self, engine, brk_schema_without_bag_relations, dbsession, caplog):
        """Prove that a dataset with many nested tables get the proper permissions."""
        importer = NDJSONImporter(brk_schema_without_bag_relations, engine)
        for table in brk_schema_without_bag_relations.get_tables():
            importer.generate_db_objects(table.id, truncate=True, ind_extra_index=False)

        # Setup schema and profile
        # This schema has auth on dataset level, not on table
        ams_schema = {brk_schema_without_bag_relations.id: brk_schema_without_bag_relations}

        # make sure role 'write_brk' exists with create_roles=True
        # The role exists now for all test following this statement
        with caplog.at_level(logging.INFO, logger="schematools.permissions.db"):
            apply_schema_and_profile_permissions(
                engine,
                ams_schema,
                None,
                create_roles=True,
                verbose=1,
            )

        grants = _filter_grant_statements(caplog)
        assert grants == [
            'CREATE ROLE "scope_brk_rsn"',
            'CREATE ROLE "scope_brk_rsn.filtered" IN ROLE "scope_brk_rsn"',
            "GRANT SELECT ON SEQUENCE public.brk_aantekeningenkadastraleobjecten_heeft_betrokken_pers_id_seq TO scope_brk_rsn",
            "GRANT SELECT ON SEQUENCE public.brk_aantekeningenrechten_heeft_betrokken_persoon_v1_id_seq TO scope_brk_rsn",
            "GRANT SELECT ON SEQUENCE public.brk_aantekeningenrechten_is_gbsd_op_sdl_v1_id_seq TO scope_brk_rsn",
            "GRANT SELECT ON SEQUENCE public.brk_kadastraleobjecten_hft_rel_mt_vot_v1_id_seq TO scope_brk_rsn",
            "GRANT SELECT ON SEQUENCE public.brk_kadastraleobjecten_soort_cultuur_bebouwd_v1_id_seq TO scope_brk_rsn",
            "GRANT SELECT ON SEQUENCE public.brk_stukdelen_is_bron_voor_aantekening_kadastraal_object_id_seq TO scope_brk_rsn",
            "GRANT SELECT ON SEQUENCE public.brk_stukdelen_is_bron_voor_aantekening_recht_v1_id_seq TO scope_brk_rsn",
            "GRANT SELECT ON SEQUENCE public.brk_stukdelen_is_bron_voor_zakelijk_recht_v1_id_seq TO scope_brk_rsn",
            "GRANT SELECT ON TABLE public.brk_aantekeningenkadastraleobjecten_heeft_betrokken_persoon_v1 TO scope_brk_rsn",
            "GRANT SELECT ON TABLE public.brk_aantekeningenkadastraleobjecten_v1 TO scope_brk_rsn",
            "GRANT SELECT ON TABLE public.brk_aantekeningenrechten_heeft_betrokken_persoon_v1 TO scope_brk_rsn",
            "GRANT SELECT ON TABLE public.brk_aantekeningenrechten_is_gbsd_op_sdl_v1 TO scope_brk_rsn",
            "GRANT SELECT ON TABLE public.brk_aantekeningenrechten_v1 TO scope_brk_rsn",
            "GRANT SELECT ON TABLE public.brk_aardzakelijkerechten_v1 TO scope_brk_rsn",
            "GRANT SELECT ON TABLE public.brk_gemeentes_v1 TO scope_brk_rsn",
            "GRANT SELECT ON TABLE public.brk_kadastralegemeentecodes_v1 TO scope_brk_rsn",
            "GRANT SELECT ON TABLE public.brk_kadastralegemeentes_v1 TO scope_brk_rsn",
            "GRANT SELECT ON TABLE public.brk_kadastraleobjecten_hft_rel_mt_vot_v1 TO scope_brk_rsn",
            "GRANT SELECT ON TABLE public.brk_kadastraleobjecten_soort_cultuur_bebouwd_v1 TO scope_brk_rsn",
            "GRANT SELECT ON TABLE public.brk_kadastraleobjecten_v1 TO scope_brk_rsn",
            "GRANT SELECT ON TABLE public.brk_kadastralesecties_v1 TO scope_brk_rsn",
            "GRANT SELECT ON TABLE public.brk_kadastralesubjecten_v1 TO scope_brk_rsn",
            "GRANT SELECT ON TABLE public.brk_meta_v1 TO scope_brk_rsn",
            "GRANT SELECT ON TABLE public.brk_stukdelen_is_bron_voor_aantekening_kadastraal_object_v1 TO scope_brk_rsn",
            "GRANT SELECT ON TABLE public.brk_stukdelen_is_bron_voor_aantekening_recht_v1 TO scope_brk_rsn",
            "GRANT SELECT ON TABLE public.brk_stukdelen_is_bron_voor_zakelijk_recht_v1 TO scope_brk_rsn",
            "GRANT SELECT ON TABLE public.brk_stukdelen_v1 TO scope_brk_rsn",
            "GRANT SELECT ON TABLE public.brk_tenaamstellingen_v1 TO scope_brk_rsn",
            "GRANT SELECT ON TABLE public.brk_zakelijkerechten_v1 TO scope_brk_rsn",
            "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.brk_aantekeningenkadastraleobjecten_heeft_betrokken_persoon_v1 TO write_brk",
            "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.brk_aantekeningenkadastraleobjecten_v1 TO write_brk",
            "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.brk_aantekeningenrechten_heeft_betrokken_persoon_v1 TO write_brk",
            "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.brk_aantekeningenrechten_is_gbsd_op_sdl_v1 TO write_brk",
            "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.brk_aantekeningenrechten_v1 TO write_brk",
            "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.brk_aardzakelijkerechten_v1 TO write_brk",
            "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.brk_gemeentes_v1 TO write_brk",
            "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.brk_kadastralegemeentecodes_v1 TO write_brk",
            "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.brk_kadastralegemeentes_v1 TO write_brk",
            "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.brk_kadastraleobjecten_hft_rel_mt_vot_v1 TO write_brk",
            "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.brk_kadastraleobjecten_soort_cultuur_bebouwd_v1 TO write_brk",
            "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.brk_kadastraleobjecten_v1 TO write_brk",
            "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.brk_kadastralesecties_v1 TO write_brk",
            "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.brk_kadastralesubjecten_v1 TO write_brk",
            "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.brk_meta_v1 TO write_brk",
            "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.brk_stukdelen_is_bron_voor_aantekening_kadastraal_object_v1 TO write_brk",
            "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.brk_stukdelen_is_bron_voor_aantekening_recht_v1 TO write_brk",
            "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.brk_stukdelen_is_bron_voor_zakelijk_recht_v1 TO write_brk",
            "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.brk_stukdelen_v1 TO write_brk",
            "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.brk_tenaamstellingen_v1 TO write_brk",
            "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.brk_zakelijkerechten_v1 TO write_brk",
            "GRANT USAGE ON SEQUENCE public.brk_aantekeningenkadastraleobjecten_heeft_betrokken_pers_id_seq TO write_brk",
            "GRANT USAGE ON SEQUENCE public.brk_aantekeningenrechten_heeft_betrokken_persoon_v1_id_seq TO write_brk",
            "GRANT USAGE ON SEQUENCE public.brk_aantekeningenrechten_is_gbsd_op_sdl_v1_id_seq TO write_brk",
            "GRANT USAGE ON SEQUENCE public.brk_kadastraleobjecten_hft_rel_mt_vot_v1_id_seq TO write_brk",
            "GRANT USAGE ON SEQUENCE public.brk_kadastraleobjecten_soort_cultuur_bebouwd_v1_id_seq TO write_brk",
            "GRANT USAGE ON SEQUENCE public.brk_stukdelen_is_bron_voor_aantekening_kadastraal_object_id_seq TO write_brk",
            "GRANT USAGE ON SEQUENCE public.brk_stukdelen_is_bron_voor_aantekening_recht_v1_id_seq TO write_brk",
            "GRANT USAGE ON SEQUENCE public.brk_stukdelen_is_bron_voor_zakelijk_recht_v1_id_seq TO write_brk",
        ]

    def test_openbaar_permissions(
        self, engine, afval_schema, gebieden_profiles, dbsession, caplog
    ):
        """
        Prove that the default auth scope is "OPENBAAR".
        """

        importer = NDJSONImporter(afval_schema, engine)
        importer.generate_db_objects("containers", truncate=True, ind_extra_index=False)
        importer.generate_db_objects("clusters", truncate=True, ind_extra_index=False)

        # Create postgres roles
        _create_role(engine, "openbaar")
        _create_role(engine, "bag_r")
        # Check if the roles exist, the tables exist,
        # and the roles have no read privilige on the tables.
        _check_select_permission_denied(engine, "openbaar", "afvalwegingen_containers_v1")
        _check_select_permission_denied(engine, "bag_r", "afvalwegingen_clusters_v1")

        with caplog.at_level(logging.INFO, logger="schematools.permissions.db"):
            apply_schema_and_profile_permissions(
                engine=engine,
                schemas=afval_schema,
                profiles=gebieden_profiles,
                only_role="openbaar",
                only_scope="OPENBAAR",
                create_roles=True,
            )
            apply_schema_and_profile_permissions(
                engine=engine,
                schemas=afval_schema,
                profiles=gebieden_profiles,
                only_role="bag_r",
                only_scope="BAG/R",
                create_roles=True,
                verbose=1,
            )

        grants = _filter_grant_statements(caplog)
        assert grants == [
            'CREATE ROLE "bag_r"',
            'CREATE ROLE "bag_r.filtered" IN ROLE "bag_r"',
            "GRANT SELECT ON TABLE public.afvalwegingen_clusters_v1 TO bag_r",
            "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.afvalwegingen_clusters_v1 TO write_afvalwegingen",
            "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.afvalwegingen_containers_v1 TO write_afvalwegingen",
        ]

        _check_select_permission_granted(engine, "openbaar", "afvalwegingen_containers_v1")
        _check_select_permission_denied(engine, "openbaar", "afvalwegingen_clusters_v1")

        for bag_r_role in ("bag_r", "bag_r.filtered"):
            _check_select_permission_denied(engine, bag_r_role, "afvalwegingen_containers_v1")
            _check_select_permission_granted(engine, bag_r_role, "afvalwegingen_clusters_v1")

    def test_interacting_permissions(
        self, engine, gebieden_schema_auth, gebieden_profiles, dbsession
    ):
        """
        Prove that dataset, table, and field permissions are set
        according to the "OF-OF" Exclusief principle:

        * Een user met scope LEVEL/A mag alles uit de dataset gebieden zien,
          behalve tabel bouwblokken.
        * Een user met scope LEVEL/B mag alle velden van tabel bouwblokken zien,
          behalve beginGeldigheid.
        * Een user met scope LEVEL/C mag veld beginGeldigheid zien.
        """

        importer = NDJSONImporter(gebieden_schema_auth, engine)
        importer.generate_db_objects("bouwblokken", truncate=True, ind_extra_index=False)
        importer.generate_db_objects("buurten", truncate=True, ind_extra_index=False)

        # Create postgres roles
        test_roles = ["level_a", "level_b", "level_c"]
        for test_role in test_roles:
            _create_role(engine, test_role)

        # Check if the roles exist, the tables exist,
        # and the roles have no read privilige on the tables.
        for test_role in test_roles:
            for table in ["gebieden_bouwblokken_v1", "gebieden_buurten_v1"]:
                _check_select_permission_denied(engine, test_role, table)

        # Apply the permissions from Schema and Profiles.
        apply_schema_and_profile_permissions(
            engine,
            gebieden_schema_auth,
            gebieden_profiles,
            only_role="level_a",
            only_scope="LEVEL/A",
        )
        apply_schema_and_profile_permissions(
            engine,
            gebieden_schema_auth,
            gebieden_profiles,
            only_role="level_b",
            only_scope="LEVEL/B",
        )
        apply_schema_and_profile_permissions(
            engine,
            gebieden_schema_auth,
            gebieden_profiles,
            only_role="level_c",
            only_scope="LEVEL/C",
        )

        # Check if the read priviliges are correct
        _check_select_permission_denied(engine, "level_a", "gebieden_bouwblokken_v1")
        _check_select_permission_granted(engine, "level_a", "gebieden_buurten_v1")

        _check_select_permission_granted(
            engine, "level_b", "gebieden_bouwblokken_v1", "id, eind_geldigheid"
        )
        _check_select_permission_denied(
            engine, "level_b", "gebieden_bouwblokken_v1", "begin_geldigheid"
        )
        _check_select_permission_denied(engine, "level_b", "gebieden_buurten_v1")

        _check_select_permission_denied(
            engine, "level_c", "gebieden_bouwblokken_v1", "id, eind_geldigheid"
        )
        _check_select_permission_granted(
            engine, "level_c", "gebieden_bouwblokken_v1", "begin_geldigheid"
        )
        _check_select_permission_denied(engine, "level_c", "gebieden_buurten_v1")

    def test_auth_list_permissions(
        self, engine, gebieden_schema_auth_list, gebieden_profiles, dbsession, caplog
    ):
        """
        Prove that dataset, table, and field permissions are set,
        according to the "OF-OF" Exclusief principle.
        Prove that when the auth property is a list of scopes, this is interpreted as "OF-OF".

        * Een user met scope LEVEL/A1 of LEVEL/A2 mag alles uit de dataset gebieden zien,
          behalve tabel bouwblokken.
        * Een user met scope LEVEL/B1 of LEVEL/B2 mag alle velden van tabel bouwblokken zien,
          behalve beginGeldigheid.
        * Een user met scope LEVEL/C1 of LEVEL/B2 mag veld beginGeldigheid zien.
        """
        importer = NDJSONImporter(gebieden_schema_auth_list, engine)
        importer.generate_db_objects("bouwblokken", truncate=True, ind_extra_index=False)
        importer.generate_db_objects("buurten", truncate=True, ind_extra_index=False)

        # Create postgres roles
        test_roles = [
            "level_a1",
            "level_a2",
            "level_b1",
            "level_b2",
            "level_c1",
            "level_c2",
        ]
        for test_role in test_roles:
            _create_role(engine, test_role)

        # Check if the roles exist, the tables exist,
        # and the roles have no read privilige on the tables.
        for test_role in test_roles:
            for table in ["gebieden_bouwblokken_v1", "gebieden_buurten_v1"]:
                _check_select_permission_denied(engine, test_role, table)

        # Apply the permissions from Schema and Profiles.
        with caplog.at_level(logging.INFO, logger="schematools.permissions.db"):
            apply_schema_and_profile_permissions(
                engine,
                gebieden_schema_auth_list,
                gebieden_profiles,
                only_role="level_a1",
                only_scope="LEVEL/A1",
                verbose=1,
            )
            grants = _filter_grant_statements(caplog)
            assert grants == [
                "GRANT SELECT ON SEQUENCE public.gebieden_buurten_ligt_in_wijk_v1_id_seq TO level_a1",
                "GRANT SELECT ON TABLE public.gebieden_buurten_ligt_in_wijk_v1 TO level_a1",
                "GRANT SELECT ON TABLE public.gebieden_buurten_v1 TO level_a1",
                "GRANT SELECT ON TABLE public.gebieden_wijken_v1 TO level_a1",
                "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.gebieden_bouwblokken_v1 TO write_gebieden",
                "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.gebieden_buurten_ligt_in_wijk_v1 TO write_gebieden",
                "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.gebieden_buurten_v1 TO write_gebieden",
                "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.gebieden_wijken_v1 TO write_gebieden",
                "GRANT USAGE ON SEQUENCE public.gebieden_buurten_ligt_in_wijk_v1_id_seq TO write_gebieden",
            ]

            apply_schema_and_profile_permissions(
                engine,
                gebieden_schema_auth_list,
                gebieden_profiles,
                only_role="level_b1",
                only_scope="LEVEL/B1",
                verbose=1,
            )
            grants = _filter_grant_statements(caplog)
            assert grants == [
                "GRANT SELECT (eind_geldigheid) ON TABLE public.gebieden_bouwblokken_v1 TO level_b1",
                "GRANT SELECT (id) ON TABLE public.gebieden_bouwblokken_v1 TO level_b1",
                "GRANT SELECT (ligt_in_buurt_id) ON TABLE public.gebieden_bouwblokken_v1 TO level_b1",
                "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.gebieden_bouwblokken_v1 TO write_gebieden",
                "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.gebieden_buurten_ligt_in_wijk_v1 TO write_gebieden",
                "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.gebieden_buurten_v1 TO write_gebieden",
                "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.gebieden_wijken_v1 TO write_gebieden",
                "GRANT USAGE ON SEQUENCE public.gebieden_buurten_ligt_in_wijk_v1_id_seq TO write_gebieden",
            ]

            apply_schema_and_profile_permissions(
                engine,
                gebieden_schema_auth_list,
                gebieden_profiles,
                only_role="level_c1",
                only_scope="LEVEL/C1",
                verbose=1,
            )
            grants = _filter_grant_statements(caplog)
            assert grants == [
                "GRANT SELECT (begin_geldigheid) ON TABLE public.gebieden_bouwblokken_v1 TO level_c1",
                "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.gebieden_bouwblokken_v1 TO write_gebieden",
                "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.gebieden_buurten_ligt_in_wijk_v1 TO write_gebieden",
                "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.gebieden_buurten_v1 TO write_gebieden",
                "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.gebieden_wijken_v1 TO write_gebieden",
                "GRANT USAGE ON SEQUENCE public.gebieden_buurten_ligt_in_wijk_v1_id_seq TO write_gebieden",
            ]

            apply_schema_and_profile_permissions(
                engine,
                gebieden_schema_auth_list,
                gebieden_profiles,
                only_role="level_a2",
                only_scope="LEVEL/A2",
                verbose=1,
            )
            grants = _filter_grant_statements(caplog)
            assert grants == [
                "GRANT SELECT ON SEQUENCE public.gebieden_buurten_ligt_in_wijk_v1_id_seq TO level_a2",
                "GRANT SELECT ON TABLE public.gebieden_buurten_ligt_in_wijk_v1 TO level_a2",
                "GRANT SELECT ON TABLE public.gebieden_buurten_v1 TO level_a2",
                "GRANT SELECT ON TABLE public.gebieden_wijken_v1 TO level_a2",
                "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.gebieden_bouwblokken_v1 TO write_gebieden",
                "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.gebieden_buurten_ligt_in_wijk_v1 TO write_gebieden",
                "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.gebieden_buurten_v1 TO write_gebieden",
                "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.gebieden_wijken_v1 TO write_gebieden",
                "GRANT USAGE ON SEQUENCE public.gebieden_buurten_ligt_in_wijk_v1_id_seq TO write_gebieden",
            ]

            apply_schema_and_profile_permissions(
                engine,
                gebieden_schema_auth_list,
                gebieden_profiles,
                only_role="level_b2",
                only_scope="LEVEL/B2",
                verbose=1,
            )
            grants = _filter_grant_statements(caplog)
            assert grants == [
                "GRANT SELECT (eind_geldigheid) ON TABLE public.gebieden_bouwblokken_v1 TO level_b2",
                "GRANT SELECT (id) ON TABLE public.gebieden_bouwblokken_v1 TO level_b2",
                "GRANT SELECT (ligt_in_buurt_id) ON TABLE public.gebieden_bouwblokken_v1 TO level_b2",
                "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.gebieden_bouwblokken_v1 TO write_gebieden",
                "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.gebieden_buurten_ligt_in_wijk_v1 TO write_gebieden",
                "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.gebieden_buurten_v1 TO write_gebieden",
                "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.gebieden_wijken_v1 TO write_gebieden",
                "GRANT USAGE ON SEQUENCE public.gebieden_buurten_ligt_in_wijk_v1_id_seq TO write_gebieden",
            ]

            apply_schema_and_profile_permissions(
                engine,
                gebieden_schema_auth_list,
                gebieden_profiles,
                only_role="level_c2",
                only_scope="LEVEL/C2",
                verbose=1,
            )
            grants = _filter_grant_statements(caplog)
            assert grants == [
                "GRANT SELECT (begin_geldigheid) ON TABLE public.gebieden_bouwblokken_v1 TO level_c2",
                "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.gebieden_bouwblokken_v1 TO write_gebieden",
                "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.gebieden_buurten_ligt_in_wijk_v1 TO write_gebieden",
                "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.gebieden_buurten_v1 TO write_gebieden",
                "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.gebieden_wijken_v1 TO write_gebieden",
                "GRANT USAGE ON SEQUENCE public.gebieden_buurten_ligt_in_wijk_v1_id_seq TO write_gebieden",
            ]

        # Check if the read priviliges are correct
        _check_select_permission_denied(engine, "level_a1", "gebieden_bouwblokken_v1")
        _check_select_permission_granted(engine, "level_a1", "gebieden_buurten_v1")
        _check_select_permission_denied(engine, "level_a2", "gebieden_bouwblokken_v1")
        _check_select_permission_granted(engine, "level_a2", "gebieden_buurten_v1")

        _check_select_permission_granted(
            engine, "level_b1", "gebieden_bouwblokken_v1", "id, eind_geldigheid"
        )
        _check_select_permission_denied(
            engine, "level_b1", "gebieden_bouwblokken_v1", "begin_geldigheid"
        )
        _check_select_permission_denied(engine, "level_b1", "gebieden_buurten_v1")
        _check_select_permission_granted(
            engine, "level_b2", "gebieden_bouwblokken_v1", "id, eind_geldigheid"
        )
        _check_select_permission_denied(
            engine, "level_b2", "gebieden_bouwblokken_v1", "begin_geldigheid"
        )
        _check_select_permission_denied(engine, "level_b2", "gebieden_buurten_v1")

        _check_select_permission_denied(
            engine, "level_c1", "gebieden_bouwblokken_v1", "id, eind_geldigheid"
        )
        _check_select_permission_granted(
            engine, "level_c1", "gebieden_bouwblokken_v1", "begin_geldigheid"
        )
        _check_select_permission_denied(engine, "level_c1", "gebieden_buurten_v1")

        _check_select_permission_denied(
            engine, "level_c2", "gebieden_bouwblokken_v1", "id, eind_geldigheid"
        )
        _check_select_permission_granted(
            engine, "level_c2", "gebieden_bouwblokken_v1", "begin_geldigheid"
        )
        _check_select_permission_denied(engine, "level_c2", "gebieden_buurten_v1")

        # Check that there are no INSERT, UPDATE, TRUNCATE, DELETE privileges
        _check_insert_permission_denied(
            engine, "level_b1", "gebieden_bouwblokken_v1", "id", "'abc'"
        )
        _check_update_permission_denied(
            engine, "level_b1", "gebieden_bouwblokken_v1", "id", "'def'", "id = 'abc'"
        )
        _check_delete_permission_denied(
            engine, "level_b1", "gebieden_bouwblokken_v1", "id = 'abc'"
        )
        _check_truncate_permission_denied(engine, "level_b1", "gebieden_bouwblokken_v1")

    def test_auto_create_roles(
        self, engine, gebieden_schema_auth, gebieden_profiles, dbsession, caplog
    ):
        """
        Prove that dataset, table, and field permissions are set according,
        to the "OF-OF" Exclusief principle:

        * Een user met scope LEVEL/A mag alles uit de dataset gebieden zien,
          behalve tabel bouwblokken.
        * Een user met scope LEVEL/B mag alle velden van tabel bouwblokken zien,
          behalve beginGeldigheid.
        * Een user met scope LEVEL/C mag veld beginGeldigheid zien.

        Drie corresponderende users worden automatisch aangemaakt:
        'scope_level_a', 'scope_level_b', en 'scope_level_c;
        """

        importer = NDJSONImporter(gebieden_schema_auth, engine)
        importer.generate_db_objects("bouwblokken", truncate=True, ind_extra_index=False)
        importer.generate_db_objects("buurten", truncate=True, ind_extra_index=False)

        # These tests commented out due to: Error when trying to teardown test databases
        # Roles may still exist from previous test run. Uncomment when fixed:
        # _check_role_does_not_exist(engine, "scope_level_a")
        # _check_role_does_not_exist(engine, "scope_level_b")
        # _check_role_does_not_exist(engine, "scope_level_c")

        # Apply the permissions from Schema and Profiles.
        with caplog.at_level(logging.INFO, logger="schematools.permissions.db"):
            apply_schema_and_profile_permissions(
                engine,
                gebieden_schema_auth,
                gebieden_profiles,
                create_roles=True,
                verbose=1,
            )

        grants = _filter_grant_statements(caplog)
        assert grants == [
            "GRANT SELECT (begin_geldigheid) ON TABLE public.gebieden_bouwblokken_v1 TO scope_level_c",
            "GRANT SELECT (begingeldigheid) ON TABLE public.gebieden_ggwgebieden_v1 TO scope_level_a",
            "GRANT SELECT (eind_geldigheid) ON TABLE public.gebieden_bouwblokken_v1 TO scope_level_b",
            "GRANT SELECT (eindgeldigheid) ON TABLE public.gebieden_ggwgebieden_v1 TO scope_level_a",
            "GRANT SELECT (id) ON TABLE public.gebieden_bouwblokken_v1 TO scope_level_b",
            "GRANT SELECT (id) ON TABLE public.gebieden_ggwgebieden_v1 TO scope_level_a",
            "GRANT SELECT (identificatie) ON TABLE public.gebieden_ggwgebieden_v1 TO scope_level_a",
            "GRANT SELECT (ligt_in_buurt_id) ON TABLE public.gebieden_bouwblokken_v1 TO scope_level_d",
            "GRANT SELECT (ligt_in_buurt_identificatie) ON TABLE public.gebieden_bouwblokken_v1 TO scope_level_d",
            "GRANT SELECT (ligt_in_buurt_loose_id) ON TABLE public.gebieden_bouwblokken_v1 TO scope_level_d",
            "GRANT SELECT (ligt_in_buurt_volgnummer) ON TABLE public.gebieden_bouwblokken_v1 TO scope_level_d",
            "GRANT SELECT (volgnummer) ON TABLE public.gebieden_ggwgebieden_v1 TO scope_level_a",
            "GRANT SELECT ON SEQUENCE public.gebieden_bouwblokken_ligt_in_buurt_v1_id_seq TO scope_level_d",
            "GRANT SELECT ON SEQUENCE public.gebieden_buurten_ligt_in_wijk_v1_id_seq TO scope_level_a",
            "GRANT SELECT ON SEQUENCE public.gebieden_ggwgebieden_bestaat_uit_buurten_v1_id_seq TO scope_level_e",
            "GRANT SELECT ON SEQUENCE public.gebieden_ggwgebieden_gebieds_grenzen_v1_id_seq TO scope_level_f",
            "GRANT SELECT ON TABLE public.gebieden_bouwblokken_ligt_in_buurt_v1 TO scope_level_d",
            "GRANT SELECT ON TABLE public.gebieden_buurten_ligt_in_wijk_v1 TO scope_level_a",
            "GRANT SELECT ON TABLE public.gebieden_buurten_v1 TO scope_level_a",
            "GRANT SELECT ON TABLE public.gebieden_ggwgebieden_bestaat_uit_buurten_v1 TO scope_level_e",
            "GRANT SELECT ON TABLE public.gebieden_ggwgebieden_gebieds_grenzen_v1 TO scope_level_f",
            "GRANT SELECT ON TABLE public.gebieden_wijken_v1 TO scope_level_a",
            "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.gebieden_bouwblokken_ligt_in_buurt_v1 TO write_gebieden",
            "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.gebieden_bouwblokken_v1 TO write_gebieden",
            "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.gebieden_buurten_ligt_in_wijk_v1 TO write_gebieden",
            "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.gebieden_buurten_v1 TO write_gebieden",
            "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.gebieden_ggwgebieden_bestaat_uit_buurten_v1 TO write_gebieden",
            "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.gebieden_ggwgebieden_gebieds_grenzen_v1 TO write_gebieden",
            "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.gebieden_ggwgebieden_v1 TO write_gebieden",
            "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.gebieden_wijken_v1 TO write_gebieden",
            "GRANT USAGE ON SEQUENCE public.gebieden_bouwblokken_ligt_in_buurt_v1_id_seq TO write_gebieden",
            "GRANT USAGE ON SEQUENCE public.gebieden_buurten_ligt_in_wijk_v1_id_seq TO write_gebieden",
            "GRANT USAGE ON SEQUENCE public.gebieden_ggwgebieden_bestaat_uit_buurten_v1_id_seq TO write_gebieden",
            "GRANT USAGE ON SEQUENCE public.gebieden_ggwgebieden_gebieds_grenzen_v1_id_seq TO write_gebieden",
        ]

        # Check if roles exist and the read priviliges are correct
        _check_select_permission_denied(engine, "scope_level_a", "gebieden_bouwblokken_v1")
        _check_select_permission_granted(engine, "scope_level_a", "gebieden_buurten_v1")

        _check_select_permission_granted(
            engine, "scope_level_b", "gebieden_bouwblokken_v1", "id, eind_geldigheid"
        )
        _check_select_permission_denied(
            engine, "scope_level_b", "gebieden_bouwblokken_v1", "begin_geldigheid"
        )
        _check_select_permission_denied(engine, "scope_level_b", "gebieden_buurten_v1")

        _check_select_permission_denied(
            engine, "scope_level_c", "gebieden_bouwblokken_v1", "id, eind_geldigheid"
        )
        _check_select_permission_granted(
            engine, "scope_level_c", "gebieden_bouwblokken_v1", "begin_geldigheid"
        )
        _check_select_permission_granted(
            engine, "scope_level_d", "gebieden_bouwblokken_v1", "ligt_in_buurt_loose_id"
        )
        _check_select_permission_granted(
            engine, "scope_level_d", "gebieden_bouwblokken_v1", "ligt_in_buurt_id"
        )
        _check_select_permission_granted(
            engine, "scope_level_d", "gebieden_bouwblokken_v1", "ligt_in_buurt_identificatie"
        )
        _check_select_permission_granted(
            engine, "scope_level_d", "gebieden_bouwblokken_v1", "ligt_in_buurt_volgnummer"
        )
        _check_select_permission_denied(engine, "scope_level_c", "gebieden_buurten_v1")
        _check_select_permission_denied(engine, "scope_level_d", "gebieden_buurten_v1")

        # Check the through table, for all columns
        _check_select_permission_granted(
            engine, "scope_level_e", "gebieden_ggwgebieden_bestaat_uit_buurten_v1"
        )
        # Check the nested table, for all columns
        _check_select_permission_granted(
            engine, "scope_level_f", "gebieden_ggwgebieden_gebieds_grenzen_v1"
        )
        # Check the through table
        _check_select_permission_denied(
            engine, "scope_level_a", "gebieden_ggwgebieden_bestaat_uit_buurten_v1"
        )
        # Check the nested table
        _check_select_permission_denied(
            engine, "scope_level_a", "gebieden_ggwgebieden_gebieds_grenzen_v1"
        )

    def test_single_dataset_permissions(
        self, engine, gebieden_schema_auth, meetbouten_schema, dbsession
    ):
        """
        Prove when revoking grants on one dataset, other datasets are unaffected.
        """

        # dataset 1: gebieden
        importer = NDJSONImporter(gebieden_schema_auth, engine)
        importer.generate_db_objects("bouwblokken", truncate=True, ind_extra_index=False)
        importer.generate_db_objects("buurten", truncate=True, ind_extra_index=False)
        importer.generate_db_objects("wijken", truncate=True, ind_extra_index=False)

        # dataset 2: meetbouten
        importer = NDJSONImporter(meetbouten_schema, engine)
        importer.generate_db_objects("meetbouten", truncate=True, ind_extra_index=False)
        importer.generate_db_objects("metingen", truncate=True, ind_extra_index=False)
        importer.generate_db_objects("referentiepunten", truncate=True, ind_extra_index=False)

        # Apply the permissions to gebieden
        apply_schema_and_profile_permissions(engine, gebieden_schema_auth, None, create_roles=True)
        # Check perms on gebieden
        _check_select_permission_granted(engine, "scope_level_a", "gebieden_buurten_v1")
        _check_select_permission_granted(
            engine, "scope_level_b", "gebieden_bouwblokken_v1", "id, eind_geldigheid"
        )
        _check_select_permission_granted(
            engine, "scope_level_c", "gebieden_bouwblokken_v1", "begin_geldigheid"
        )

        # Apply the permissions to meetbouten
        apply_schema_and_profile_permissions(engine, meetbouten_schema, None, create_roles=True)
        # Check perms on meetbouten
        _check_select_permission_granted(engine, "scope_openbaar", "meetbouten_meetbouten_v1")

        # Revoke permissions for dataset gebieden and set grant again
        apply_schema_and_profile_permissions(
            engine,
            schemas=gebieden_schema_auth,
            profiles=None,
            create_roles=True,
            revoke=True,
        )
        # Check perms again on meetbouten
        _check_select_permission_granted(engine, "scope_openbaar", "meetbouten_meetbouten_v1")

    def test_permissions_support_shortnames(self, engine, hr_schema_auth, dbsession, caplog):
        """
        Prove that table, and field permissions are set on the shortnamed field.
        """

        importer = NDJSONImporter(hr_schema_auth, engine)
        importer.generate_db_objects("sbiactiviteiten", truncate=True, ind_extra_index=False)

        # Setup schema and profile
        ams_schema = {hr_schema_auth.id: hr_schema_auth}

        # Apply the permissions from Schema and Profiles.
        with caplog.at_level(logging.INFO, logger="schematools.permissions.db"):
            apply_schema_and_profile_permissions(
                engine,
                ams_schema,
                None,
                only_role="level_b",
                only_scope="LEVEL/B",
                create_roles=True,
                verbose=1,
            )
            apply_schema_and_profile_permissions(
                engine,
                ams_schema,
                None,
                only_role="level_c",
                only_scope="LEVEL/C",
                create_roles=True,
                verbose=1,
            )

        grants = _filter_grant_statements(caplog)
        assert grants == [
            'CREATE ROLE "level_b"',
            'CREATE ROLE "level_b.filtered" IN ROLE "level_b"',
            'CREATE ROLE "level_c"',
            'CREATE ROLE "level_c.filtered" IN ROLE "level_c"',
            'CREATE ROLE "write_hr"',
            "GRANT SELECT (identifier) ON TABLE public.hr_sbi_ac_v1 TO level_b",
            "GRANT SELECT (sbi_ac_naam) ON TABLE public.hr_sbi_ac_v1 TO level_b",
            "GRANT SELECT (sbi_ac_no) ON TABLE public.hr_sbi_ac_v1 TO level_c",
            "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.hr_sbi_ac_v1 TO write_hr",
            "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.hr_sbi_ac_v1 TO write_hr",
        ]

        # Check if the read priviliges are correct
        for level_b_role in ("level_b", "level_b.filtered"):
            _check_select_permission_granted(engine, level_b_role, "hr_sbi_ac_v1", "sbi_ac_naam")
            _check_select_permission_denied(engine, level_b_role, "hr_sbi_ac_v1", "sbi_ac_no")

        for level_c_role in ("level_c", "level_c.filtered"):
            _check_select_permission_denied(engine, level_c_role, "hr_sbi_ac_v1", "sbi_ac_naam")
            _check_select_permission_granted(engine, level_c_role, "hr_sbi_ac_v1", "sbi_ac_no")


class TestWritePermissions:
    def test_dataset_write_role(self, engine, gebieden_schema_auth):
        """
        Prove that a write role with name write_{dataset.id} is created with DML rights
        Check INSERT, UPDATE, DELETE, TRUNCATE permissions
        Check that for SELECT permissions you need an additional scope role.
        """
        importer = NDJSONImporter(gebieden_schema_auth, engine)
        importer.generate_db_objects("bouwblokken", truncate=True, ind_extra_index=False)
        importer.generate_db_objects("buurten", truncate=True, ind_extra_index=False)

        # Setup schema
        ams_schema = {gebieden_schema_auth.id: gebieden_schema_auth}

        # The write_ roles do not have SELECT permissions
        _check_insert_permission_denied(
            engine, "write_gebieden", "gebieden_bouwblokken_v1", "id", "'abc'"
        )

        apply_schema_and_profile_permissions(
            engine=engine,
            schemas=ams_schema,
            profiles=None,
            set_read_permissions=True,
            set_write_permissions=True,
            create_roles=True,
            revoke=True,
        )

        # Drop testuser in case previous tests did not terminate correctly
        with engine.begin() as connection:
            connection.execute(text("DROP ROLE IF EXISTS testuser"))

        _create_role(engine, "testuser")

        with engine.begin() as connection:
            connection.execute(text("GRANT write_gebieden TO testuser"))

        #  It is now possible to INSERT data into the dataset tables
        _check_insert_permission_granted(
            engine, "testuser", "gebieden_bouwblokken_v1", "id", "'abc'"
        )

        #  The write_ roles do have SELECT permissions, therefore testuser should not have it
        _check_select_permission_granted(engine, "testuser", "gebieden_bouwblokken_v1")

        #  With SELECT it is possible to UPDATE or DELETE on given condition
        _check_update_permission_granted(
            engine, "testuser", "gebieden_bouwblokken_v1", "id", "'def'", "id = 'abc'"
        )
        _check_delete_permission_granted(
            engine, "testuser", "gebieden_bouwblokken_v1", "id = 'abc'"
        )

        # Add SELECT permissions by granting the appropriate scope to the user
        with engine.begin() as connection:
            connection.execute(text("GRANT scope_level_b TO testuser"))

        # But now it's possible to SELECT the columns within scope level_b
        _check_select_permission_granted(
            engine, "testuser", "gebieden_bouwblokken_v1", "id, eind_geldigheid"
        )

        # And it's also possible to UPDATE and DELETE,
        # if the column for the condition is within scope
        _check_update_permission_granted(
            engine, "testuser", "gebieden_bouwblokken_v1", "id", "'def'", "id = 'abc'"
        )
        _check_delete_permission_granted(
            engine, "testuser", "gebieden_bouwblokken_v1", "id = 'def'"
        )

        # TRUNCATE is also allowed, even though the table is already empty by now
        _check_truncate_permission_granted(engine, "testuser", "gebieden_bouwblokken_v1")

    def test_multiple_datasets_write_roles(self, engine, parkeervakken_schema, afval_schema):
        """
        Prove that the write_{dataset.id} roles only have DML rights for their associated
        dataset tables.
        """

        importer = NDJSONImporter(parkeervakken_schema, engine)
        importer.generate_db_objects("parkeervakken", truncate=True, ind_extra_index=False)
        importer = NDJSONImporter(afval_schema, engine)
        importer.generate_db_objects("containers", truncate=True, ind_extra_index=False)
        importer.generate_db_objects("clusters", truncate=True, ind_extra_index=False)

        # Setup schema and profile
        ams_schema = {afval_schema.id: afval_schema, parkeervakken_schema.id: parkeervakken_schema}

        apply_schema_and_profile_permissions(
            engine=engine,
            schemas=ams_schema,
            profiles=None,
            set_read_permissions=True,
            set_write_permissions=True,
            create_roles=True,
            revoke=True,
        )

        # Drop testuser in case previous tests did not terminate correctly
        with engine.begin() as connection:
            connection.execute(text("DROP ROLE IF EXISTS parkeer_tester"))
            connection.execute(text("DROP ROLE IF EXISTS afval_tester"))

        _create_role(engine, "parkeer_tester")
        _create_role(engine, "afval_tester")

        with engine.begin() as connection:
            connection.execute(text("GRANT write_parkeervakken TO parkeer_tester"))
            connection.execute(text("GRANT write_afvalwegingen TO afval_tester"))

        #  parkeer_tester has INSERT permission on parkeervakken datasets
        _check_insert_permission_granted(
            engine, "parkeer_tester", "parkeervakken_parkeervakken_v1", "id", "'abc'"
        )
        #  afval_tester has INSERT permission on afvalwegingen datasets
        _check_insert_permission_granted(
            engine, "afval_tester", "afvalwegingen_containers_v1", "id", "3"
        )
        #  parkeer_tester has NO INSERT permission on afvalwegingen datasets
        _check_insert_permission_denied(
            engine, "parkeer_tester", "afvalwegingen_containers_v1", "id", "3"
        )
        #  afval_tester has NO INSERT permission on parkeervakken datasets
        _check_insert_permission_denied(
            engine, "afval_tester", "parkeervakken_parkeervakken_v1", "id", "'abc'"
        )

    def test_permissions_support_shortnames(self, engine, hr_schema_auth, dbsession):
        """
        Prove that table, and field permissions are set on the shortnamed field.
        """
        importer = NDJSONImporter(hr_schema_auth, engine)
        importer.generate_db_objects("sbiactiviteiten", truncate=True, ind_extra_index=False)

        # Setup schema and profile
        ams_schema = {hr_schema_auth.id: hr_schema_auth}

        # Apply the permissions from Schema and Profiles.
        apply_schema_and_profile_permissions(engine, ams_schema, None, create_roles=True)

        # Check if the write priviliges are correct
        _check_insert_permission_granted(
            engine,
            "write_hr",
            "hr_sbi_ac_v1",
            "sbi_ac_naam,sbi_ac_no,identifier",
            "'berry','14641','15101051'",
        )

    def test_setting_additional_grants(self, engine, meetbouten_schema, dbsession, caplog):
        """
        Prove that additional grants can be set using the extra argument.
        """

        importer = NDJSONImporter(meetbouten_schema, engine)
        importer.generate_db_objects("meetbouten", truncate=True, ind_extra_index=False)
        importer.generate_db_objects("metingen", truncate=True, ind_extra_index=False)
        importer.generate_db_objects("referentiepunten", truncate=True, ind_extra_index=False)

        # Create the datasets_dataset table
        with engine.begin() as connection:
            connection.execute(text("CREATE TABLE datasets_dataset (id integer)"))

        # Apply the permissions to meetbouten and add the extra grants to datasets_dataset
        with caplog.at_level(logging.INFO, logger="schematools.permissions.db"):
            apply_schema_and_profile_permissions(
                engine,
                meetbouten_schema,
                None,
                create_roles=True,
                verbose=1,
                additional_grants=("datasets_dataset:SELECT;scope_openbaar",),
            )

        grants = _filter_grant_statements(caplog)
        assert grants == [
            "GRANT SELECT ON SEQUENCE public.meetbouten_meetbouten_ligt_in_buurt_v1_id_seq TO scope_openbaar",
            "GRANT SELECT ON SEQUENCE public.meetbouten_metingen_refereertaanreferentiepunten_v1_id_seq TO scope_openbaar",
            "GRANT SELECT ON TABLE public.datasets_dataset TO scope_openbaar",
            "GRANT SELECT ON TABLE public.meetbouten_meetbouten_ligt_in_buurt_v1 TO scope_openbaar",
            "GRANT SELECT ON TABLE public.meetbouten_meetbouten_v1 TO scope_openbaar",
            "GRANT SELECT ON TABLE public.meetbouten_metingen_refereertaanreferentiepunten_v1 TO scope_openbaar",
            "GRANT SELECT ON TABLE public.meetbouten_metingen_v1 TO scope_openbaar",
            "GRANT SELECT ON TABLE public.meetbouten_referentiepunten_v1 TO scope_openbaar",
            "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.meetbouten_meetbouten_ligt_in_buurt_v1 TO write_meetbouten",
            "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.meetbouten_meetbouten_v1 TO write_meetbouten",
            "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.meetbouten_metingen_refereertaanreferentiepunten_v1 TO write_meetbouten",
            "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.meetbouten_metingen_v1 TO write_meetbouten",
            "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES ON TABLE public.meetbouten_referentiepunten_v1 TO write_meetbouten",
            "GRANT USAGE ON SEQUENCE public.meetbouten_meetbouten_ligt_in_buurt_v1_id_seq TO write_meetbouten",
            "GRANT USAGE ON SEQUENCE public.meetbouten_metingen_refereertaanreferentiepunten_v1_id_seq TO write_meetbouten",
        ]

        # Check perms on the datasets_dataset table
        _check_select_permission_granted(engine, "scope_openbaar", "datasets_dataset")


def _create_role(engine, role):
    """Create role. If role already exists just fail and ignore.
    This may happen if a previous pytest did not terminate correctly.
    """
    try:
        with engine.begin() as conn:
            conn.execute(text(f'CREATE ROLE "{role}"'))
    except ProgrammingError as e:
        if not isinstance(e.orig, DuplicateObject):
            raise


def _check_role_exists(engine, role):
    """Check if role does not exist"""
    with engine.begin() as connection:
        result = connection.execute(text(f"SELECT rolname FROM pg_roles WHERE rolname='{role}'"))
        rows = list(result)
        assert len(rows) == 1


def _check_role_does_not_exist(engine, role):
    """Check if role does not exist"""
    with engine.begin() as connection:
        result = connection.execute(text("SELECT rolname FROM pg_roles WHERE rolname=%s"), role)
        rows = list(result)
        assert len(rows) == 0


def _check_select_permission_denied(engine, role, table, column="*"):
    """Check if role has no SELECT permission on table.
    Fail if role, table or column does not exist.
    """
    with (
        pytest.raises(Exception, match=f"permission denied for table {table}"),
        engine.begin() as connection,
    ):
        connection.execute(text(f'SET ROLE "{role}"'))
        connection.execute(text(f"SELECT {column} FROM {table}"))
        connection.execute(text("RESET ROLE"))


def _check_select_permission_granted(engine, role, table, column="*"):
    """Check if role has SELECT permission on table.
    Fail if role, table or column does not exist.
    """
    with engine.begin() as connection:
        connection.execute(text(f'SET ROLE "{role}"'))
        result = connection.execute(text(f"SELECT {column} FROM {table}"))
        connection.execute(text("RESET ROLE"))
    assert result


def _check_insert_permission_granted(engine, role, table, column, value):
    """Check if role has INSERT permission on table.
    Fail if role, table or column does not exist, or value mismatches in datatype.
    """
    with engine.begin() as connection:
        connection.execute(text(f'SET ROLE "{role}"'))
        result = connection.execute(text(f"INSERT INTO {table} ({column}) VALUES ({value})"))
        connection.execute(text("RESET ROLE"))
    assert result


def _check_insert_permission_denied(engine, role, table, column, value):
    """Check if role has no INSERT permission on table.
    Fail if role, table or column does not exist.
    """
    with pytest.raises(Exception) as e_info, engine.begin() as connection:
        connection.execute(text(f'SET ROLE "{role}"'))
        connection.execute(text(f"INSERT INTO {table} ({column}) VALUES ({value})"))
        connection.execute(text("RESET ROLE"))
    assert f"permission denied for table {table}" in str(e_info)


def _check_update_permission_granted(engine, role, table, column, value, condition):
    """Check if role has UPDATE permission on table.
    Fail if role, table or column does not exist, or value mismatches in datatype.
    """
    with engine.begin() as connection:
        connection.execute(text(f'SET ROLE "{role}"'))
        result = connection.execute(
            text(f"UPDATE {table} SET {column} =  {value} WHERE {condition}")
        )
        connection.execute(text("RESET ROLE"))
    assert result


def _check_update_permission_denied(engine, role, table, column, value, condition):
    """Check if role has no UPDATE permission on table.
    Fail if role, table or column does not exist, or value mismatches in datatype.
    """
    with pytest.raises(Exception) as e_info, engine.begin() as connection:
        connection.execute(text(f'SET ROLE "{role}"'))
        connection.execute(text(f"UPDATE {table} SET {column} =  {value} WHERE {condition}"))
        connection.execute(text("RESET ROLE"))
    assert f"permission denied for table {table}" in str(e_info)


def _check_delete_permission_granted(engine, role, table, condition):
    """Check if role has DELETE permission on table.
    Fail if role, table or column does not exist, or value mismatches in datatype."""
    with engine.begin() as connection:
        connection.execute(text(f'SET ROLE "{role}"'))
        result = connection.execute(text(f"DELETE FROM {table} WHERE {condition}"))  # noqa: S608
        connection.execute(text("RESET ROLE"))
    assert result


def _check_delete_permission_denied(engine, role, table, condition):
    """Check if role has no DELETE permission on table.
    Fail if role, table or column does not exist, or value mismatches in datatype."""
    with pytest.raises(Exception) as e_info, engine.begin() as connection:
        connection.execute(text(f'SET ROLE "{role}"'))
        connection.execute(text(f"DELETE FROM {table} WHERE {condition}"))  # noqa: S608
        connection.execute(text("RESET ROLE"))
    assert f"permission denied for table {table}" in str(e_info)


def _check_truncate_permission_granted(engine, role, table):
    """Check if role has TRUNCATE permission on table.
    Fail if role or table does not exist.
    """
    with engine.begin() as connection:
        connection.execute(text(f'SET ROLE "{role}"'))
        result = connection.execute(text(f"TRUNCATE {table}"))
        connection.execute(text("RESET ROLE"))
    assert result


def _check_truncate_permission_denied(engine, role, table):
    """Check if role has no TRUNCATE permission on table.
    Fail if role or table does not exist.
    """
    with pytest.raises(Exception) as e_info, engine.begin() as connection:
        connection.execute(text(f'SET ROLE "{role}"'))
        connection.execute(text(f"TRUNCATE {table}"))
        connection.execute(text("RESET ROLE"))
    assert f"permission denied for table {table}" in str(e_info)


def _filter_grant_statements(caplog):
    grants = sorted(
        m.replace("Executed --> ", "")
        for m in caplog.messages
        # Be specific in what is excluded, so unexpected notices can be detected.
        if not m.endswith('" already exists, skipping')
    )

    # Writes are seen multple times, because they use a single role.
    seen = set()
    seen_twice = {m for m in grants if (" TO write_" not in m) and (m in seen or seen.add(m))}
    newline = "\n"  # Python 3.10 f-string syntax doesn't support \
    assert not seen_twice, f"Duplicate grants: {newline.join(seen_twice)}"

    caplog.clear()
    return grants
