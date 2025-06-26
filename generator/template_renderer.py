from jinja2 import Environment, FileSystemLoader
from pathlib import Path
from typing import Dict, List, Optional
from collections import defaultdict


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
        return template.render(project_name=self.project_name)

    def render_pipeline_init_template(self, directory_name: str, modules) -> str:
        """Render __init__.py template"""
        template = self.jinja_env.get_template("pipeline__init__.py.jinja2")
        return template.render(directory_name=directory_name, project_name=self.project_name, modules=modules)

    def render_pipeline_main_template(self, pipeline_name: str, modules: List[str]) -> str:
        """Render __main__.py template"""
        template = self.jinja_env.get_template("pipeline__main__.py.jinja2")
        return template.render(pipeline_name=pipeline_name, project_name=self.project_name, modules=modules)

    def render_pipelines_main_template(self, pipeline_names: List[str]) -> str:
        """Render pipelines_main__.py template"""
        template = self.jinja_env.get_template("pipelines__main__.py.jinja2")
        return template.render(pipeline_names=pipeline_names, project_name=self.project_name)  # Named parameters

    def render_pipelines_init_template(
        self,
    ) -> str:
        """Render __init__.py template"""
        template = self.jinja_env.get_template("pipelines__init__.py.jinja2")
        return template.render(project_name=self.project_name)

    def render_all_model_imports_template(
        self,
        imports: list[Dict],
    ) -> str:
        """Render __init__.py template"""
        template = self.jinja_env.get_template("all_model_imports.py.jinja2")
        return template.render(imports=imports, project_name=self.project_name)

    def render_base_modules_template(
        self,
        base_chunk_size: Optional[int] = 40000,
    ) -> str:
        """Render SQLAlchemy model template"""
        template = self.jinja_env.get_template("base_modules.py.jinja2")
        return template.render(base_chunk_size=base_chunk_size, project_name=self.project_name)

    def render_reference_module_template(
        self,
        module: Dict,
    ) -> str:
        """Render pipeline_module template (e.g., items.py, areas.py)"""
        template = self.jinja_env.get_template("reference_module.py.jinja2")
        return template.render(
            module=module,
            project_name=self.project_name,
        )

    def render_dataset_module_template(
        self,
        module: Dict,
    ) -> str:
        """Render pipeline_module template (e.g., items.py, areas.py)"""
        template = self.jinja_env.get_template("dataset_module.py.jinja2")
        return template.render(
            module=module,
            project_name=self.project_name,
        )

    def render_model_template(self, module: Dict, safe_index_name) -> str:
        """Render SQLAlchemy model template"""
        template = self.jinja_env.get_template("model.py.jinja2")
        return template.render(
            module=module,
            safe_index_name=safe_index_name,
            project_name=self.project_name,
        )

    def render_dataset_router_config_template(self, router: dict, reference_modules: dict) -> str:
        """Render SQLAlchemy model template"""
        template = self.jinja_env.get_template("api/dataset_router_config.py.jinja2")
        return template.render(
            router=router,
            reference_modules=reference_modules,
            project_name=self.project_name,
        )

    def render_api_router_template(self, router: dict, reference_modules: dict) -> str:
        """Render SQLAlchemy model template"""
        template = self.jinja_env.get_template("api/api_router.py.jinja2")
        return template.render(
            router=router,
            reference_modules=reference_modules,
            project_name=self.project_name,
        )

    def render_api_main_template(self, routers: defaultdict) -> str:
        """Render SQLAlchemy model template"""
        template = self.jinja_env.get_template("api__main__.py.jinja2")
        return template.render(
            routers=routers,
            project_name=self.project_name,
        )

    def render_api_router_group_init_template(self, group_name: str, router_group: defaultdict) -> str:
        """Render SQLAlchemy model template"""
        template = self.jinja_env.get_template("api_router_group__init__.py.jinja2")
        return template.render(
            group_name=group_name,
            router_group=router_group,
            project_name=self.project_name,
        )

    def render_api_init_template(self, routers: defaultdict) -> str:
        """Render SQLAlchemy model template"""
        template = self.jinja_env.get_template("api__init__.py.jinja2")
        return template.render(
            routers=routers,
            project_name=self.project_name,
        )

    def render_project_main_template(self) -> str:
        """Render SQLAlchemy model template"""
        template = self.jinja_env.get_template("project__main__.py.jinja2")
        return template.render(
            project_name=self.project_name,
        )

    def render_core_error_codes_template(self, reference_modules: dict) -> str:
        """Render core/error_codes.py template"""
        template = self.jinja_env.get_template("error_codes.py.jinja2")
        return template.render(
            project_name=self.project_name,
            reference_modules=reference_modules,
        )

    def render_core_exceptions_template(self, reference_modules: dict) -> str:
        """Render core/exceptions.py template"""
        template = self.jinja_env.get_template("exceptions.py.jinja2")
        return template.render(
            project_name=self.project_name,
            reference_modules=reference_modules,
        )

    def render_core_validation_template(self, reference_modules: dict) -> str:
        """Render core/validation.py template"""
        template = self.jinja_env.get_template("validation.py.jinja2")
        return template.render(
            project_name=self.project_name,
            reference_modules=reference_modules,
        )
