from jinja2 import Environment, FileSystemLoader
from pathlib import Path
from typing import Dict, List, Optional


class TemplateRenderer:
    def __init__(self, project_name: str, template_dir: str = "templates"):
        self.template_dir = Path(template_dir)
        self.project_name = project_name

        # Set up Jinja2 environment
        self.jinja_env = Environment(
            loader=FileSystemLoader(self.template_dir),
            trim_blocks=True,
            lstrip_blocks=True,
        )

    def render_empty_init_template(self) -> str:
        """Render __init__.py template"""
        template = self.jinja_env.get_template("__init__empty.py.jinja2")
        return template.render()

    def render_init_template(self, directory_name: str) -> str:
        """Render __init__.py template"""
        template = self.jinja_env.get_template("__init__.py.jinja2")
        return template.render(directory_name=directory_name, project_name=self.project_name)

    def render_main_template(self, pipeline_name: str, modules: List[str]) -> str:
        """Render __main__.py template"""
        template = self.jinja_env.get_template("__main__.py.jinja2")
        return template.render(pipeline_name=pipeline_name, modules=modules, project_name=self.project_name)

    def render_pipelines_main_template(self, pipeline_names: List[str]) -> str:
        """Render pipelines_main__.py template"""
        template = self.jinja_env.get_template("pipelines__main__.py.jinja2")
        return template.render(pipeline_names=pipeline_names, project_name=self.project_name)  # Named parameters

    def render_models_init_template(
        self,
        imports: list[Dict],
    ) -> str:
        """Render __init__.py template"""
        template = self.jinja_env.get_template("models__init__.py.jinja2")
        return template.render(imports=imports, project_name=self.project_name)

    def render_module_template(
        self,
        csv_files: List[str],
        model_name: str,
        table_name: str,
        csv_analysis: Dict | None = None,
        specs: Dict | None = None,
    ) -> str:
        """Render pipeline_module template (e.g., items.py, areas.py)"""
        template = self.jinja_env.get_template("pipeline_module.py.jinja2")
        return template.render(
            csv_files=csv_files,
            model_name=model_name,
            table_name=table_name,
            csv_analysis=csv_analysis,
            specs=specs,
            project_name=self.project_name,
        )

    def render_model_template(self, model_name: str, table_name: str, csv_analysis: Dict, specs: Dict) -> str:
        """Render SQLAlchemy model template"""
        template = self.jinja_env.get_template("model.py.jinja2")
        return template.render(
            model_name=model_name,
            table_name=table_name,
            csv_analysis=csv_analysis,
            specs=specs,
            project_name=self.project_name,
        )

    def render_api_router_template(self, router: dict) -> str:
        """Render SQLAlchemy model template"""
        template = self.jinja_env.get_template("api_router.py.jinja2")
        return template.render(
            router=router,
            project_name=self.project_name,
        )

    def render_api_main_template(self, routers: list[dict]) -> str:
        """Render SQLAlchemy model template"""
        template = self.jinja_env.get_template("api__main__.py.jinja2")
        return template.render(
            routers=routers,
            project_name=self.project_name,
        )

    def render_api_init_template(self) -> str:
        """Render SQLAlchemy model template"""
        template = self.jinja_env.get_template("api__init__.py.jinja2")
        return template.render(
            project_name=self.project_name,
        )

    def render_database_template(
        self,
    ) -> str:
        """Render database file template"""
        template = self.jinja_env.get_template("database.py.jinja2")
        return template.render()

    def render_database_utils_template(
        self,
    ) -> str:
        """Render db utils template"""
        template = self.jinja_env.get_template("db.utils.py.jinja2")
        return template.render()

    def render_requirements_template(
        self,
    ) -> str:
        """Render SQLAlchemy model template"""
        template = self.jinja_env.get_template("requirements.in.jinja2")
        return template.render()

    def render_makefile_template(
        self,
    ) -> str:
        """Render SQLAlchemy model template"""
        template = self.jinja_env.get_template("Makefile.jinja2")
        return template.render()

    def render_env_templates(
        self,
    ) -> List[Dict]:
        """Render .env template"""
        return [
            {
                "file_name": ".env",
                "content": self.jinja_env.get_template(".env.jinja2").render(),
            },
            {
                "file_name": "local.env",
                "content": self.jinja_env.get_template("local.env.jinja2").render(),
            },
            {
                "file_name": "local-admin.env",
                "content": self.jinja_env.get_template("local-admin.env.jinja2").render(),
            },
            {
                "file_name": "remote.env",
                "content": self.jinja_env.get_template("remote.env.jinja2").render(),
            },
        ]
