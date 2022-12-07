from django.core.management import BaseCommand
from django.db import connection, models
from django.db.migrations import Migration
from django.db.migrations.autodetector import MigrationAutodetector
from django.db.migrations.graph import MigrationGraph
from django.db.migrations.questioner import InteractiveMigrationQuestioner
from django.db.migrations.state import ModelState, ProjectState


class Command(BaseCommand):
    """Demonstration of Django migration bug/crash."""

    requires_system_checks = []

    def handle(self, *args, **options):
        fields1 = {
            "field1": models.CharField(max_length=200, blank=True),
        }

        fields2 = {
            "field1": models.CharField(max_length=300),
            "field2": models.IntegerField(default=0),
        }

        state1 = self._get_state(fields1)
        state2 = self._get_state(fields2)

        detector = MigrationAutodetector(
            from_state=state1,
            to_state=state2,
            questioner=InteractiveMigrationQuestioner(specified_apps=["dummy_demo"]),
        )
        dependency_graph = MigrationGraph()
        changes: dict[str, list[Migration]] = detector.changes(
            dependency_graph,
            trim_to_apps=["dummy_demo"],
            migration_name="dummy",
        )
        if not changes:
            self.stdout.write("No changes detected")
            return

        # Print changes
        start_state = state1
        for app_label, app_migrations in changes.items():
            for migration in app_migrations:
                start_state = self._print_sql(start_state, migration)

    def _print_sql(self, start_state: ProjectState, migration: Migration) -> ProjectState:
        """Print the SQL statements for a migration"""
        self.stdout.write(f"-- Migration: {migration.name}")
        for operation in migration.operations:
            self.stdout.write(f"--   {operation}")

        with connection.schema_editor(collect_sql=True, atomic=migration.atomic) as schema_editor:
            # TODO/FIXME: currently the start_state gets bound models after an operation.
            try:
                start_state = migration.apply(start_state, schema_editor, collect_sql=True)
            except Exception:
                # On crashes, still show the generated statements so far
                self.stdout.write("\n".join(schema_editor.collected_sql))
                raise

        self.stdout.write("\n".join(schema_editor.collected_sql))
        return start_state

    def _get_state(self, fields: dict) -> ProjectState:
        """Approach 1 to create a project state (from an existing model)"""
        ModelClass = type(
            "MyModel",
            (models.Model,),
            {
                "__module__": "dummy_demo.models",
                "Meta": type("Meta", (), {"app_label": "dummy_demo"}),
                **fields,
            },
        )

        state = ProjectState(real_apps=[])
        state.add_model(ModelState.from_model(ModelClass))
        return state

    def _get_state_raw(self, fields: dict) -> ProjectState:
        """Approach 2 to create a project state (from raw unbound fields)."""
        state = ProjectState(real_apps=[])
        state.add_model(
            ModelState(
                app_label="dummy_demo",
                name="MyModel",
                fields=fields,
            )
        )
        return state
