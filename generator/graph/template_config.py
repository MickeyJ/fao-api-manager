class TemplateConfig:
    """
    Configuration for templates used in the graph generator.
    This class provides methods to retrieve template names and filenames
    for various components of the graph migration process.
    """

    # -----------------------
    # Node Migration Template
    @property
    def node_migration_template(self) -> str:
        return "yaml_node_migration.cypher.sql.jinja2"

    def node_migration_filename(self, node_name: str) -> str:
        return f"{node_name}.cypher.sql"

    # -----------------------

    # Node Indexes Template
    @property
    def node_indexes_template(self) -> str:
        return "yaml_node_indexes.sql.jinja2"

    def node_indexes_filename(self, node_name: str) -> str:
        return f"{node_name}_indexes.sql"

    # -----------------------

    # Node Verify Queries Template
    @property
    def node_verify_template(self) -> str:
        return "yaml_node_verify.cypher.sql.jinja2"

    def node_verify_filename(self, node_name: str) -> str:
        return f"{node_name}_verify.cypher.sql"

    # -----------------------

    # Node/Relationship Main Template
    @property
    def pipeline_main_template(self) -> str:
        return "yaml_pipeline_main.py.jinja2"

    def pipeline_main_filename(self) -> str:
        return "__main__.py"

    # -----------------------

    # Relationship Migration Template
    @property
    def relationship_migration_template(self) -> str:
        return "yaml_relationship_migration.sql.jinja2"

    def relationship_migration_filename(self, pipeline_name: str) -> str:
        return f"{pipeline_name}.sql"

    # -----------------------

    # Relationship Verify Queries Template
    @property
    def relationship_verify_template(self) -> str:
        return "yaml_relationship_verify.cypher.sql.jinja2"

    def relationship_verify_filename(self, pipeline_name: str) -> str:
        return f"{pipeline_name}_verify.cypher.sql"

    # -----------------------

    # Node/Relationship Migrator Template
    @property
    def migrator_template(self) -> str:
        return "yaml_migrator.py.jinja2"

    def migrator_filename(self, name: str) -> str:
        return f"{name}.py"

    # -----------------------

    # Global Indexes Template
    @property
    def global_indexes_template(self) -> str:
        return "yaml_global_indexes.sql.jinja2"

    def global_indexes_filename(self) -> str:
        return "create_global_indexes.sql"

    # -----------------------

    # Orchestration Template
    @property
    def orchestrate_migration_template(self) -> str:
        return "yaml_orchestrate_migration.py.jinja2"

    def orchestrate_migration_filename(self) -> str:
        return "orchestrate_migration.py"


template_config = TemplateConfig()
