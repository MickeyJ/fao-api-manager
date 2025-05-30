from jinja2 import Environment, FileSystemLoader
from pathlib import Path
from typing import Dict, List, Optional


class TemplateRenderer:
    def __init__(self, template_dir: str = "templates"):
        self.template_dir = Path(template_dir)

        # Set up Jinja2 environment
        self.jinja_env = Environment(
            loader=FileSystemLoader(self.template_dir),
            trim_blocks=True,
            lstrip_blocks=True,
        )

    def render_init_template(
        self,
        directory_name: str,
    ) -> str:
        """Render __init__.py template"""
        template = self.jinja_env.get_template("__init__.py.jinja2")
        return template.render(
            directory_name=directory_name,
        )

    def render_main_template(self, pipeline_name: str, modules: List[str]) -> str:
        """Render __main__.py template"""
        template = self.jinja_env.get_template("__main__.py.jinja2")
        return template.render(pipeline_name=pipeline_name, modules=modules)

    def render_module_template(
        self,
        csv_filename: str,
        model_name: str,
        table_name: str,
        csv_analysis: Optional[Dict] = None,
    ) -> str:
        """Render module template (e.g., items.py, areas.py)"""
        template = self.jinja_env.get_template("module.py.jinja2")
        return template.render(
            csv_filename=csv_filename,
            model_name=model_name,
            table_name=table_name,
            csv_analysis=csv_analysis,
        )

    def render_model_template(
        self, model_name: str, table_name: str, csv_analysis: Dict
    ) -> str:
        """Render SQLAlchemy model template"""
        template = self.jinja_env.get_template("model.py.jinja2")
        return template.render(
            model_name=model_name, table_name=table_name, csv_analysis=csv_analysis
        )
