from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dbt import tracking
from dbt.lib import get_dbt_config
from dbt.parser.manifest import ManifestLoader as DbtManifestLoader, Manifest as DbtManifest

from metricflow.model.model_transformer import ModelTransformer
from metricflow.model.parsing.dir_to_model import ModelBuildResult, parse_directory_of_yaml_files_to_model
from metricflow.model.transformations.yet_another_dbt_to_metricflow import YetAnotherDbtManifestTransformer


@dataclass
class DbtProfileArgs:
    """Class to represent dbt profile arguments

    dbt's get_dbt_config uses `getattr` to get values out of the passed in args.
    We cannot pass a simple dict, because `getattr` doesn't work for keys of a
    dictionary. Thus we create a simple object that `getattr` will work on.
    """

    profile: Optional[str] = None
    target: Optional[str] = None


def get_dbt_project_manifest(
    directory: str, profile: Optional[str] = None, target: Optional[str] = None
) -> DbtManifest:
    """Builds the dbt Manifest object from the dbt project"""

    profile_args = DbtProfileArgs(profile=profile, target=target)
    dbt_config = get_dbt_config(project_dir=directory, args=profile_args)
    # If we don't disable tracking, we have to setup a full
    # dbt User object to build the manifest
    tracking.disable_tracking()
    return DbtManifestLoader.get_full_manifest(config=dbt_config)


def parse_dbt_project_to_model(
    directory: str, profile: Optional[str] = None, target: Optional[str] = None
) -> ModelBuildResult:
    """Parse dbt model files in the given directory to a UserConfiguredModel."""
    manifest = get_dbt_project_manifest(directory=directory, profile=profile, target=target)
    dbt_build_result = YetAnotherDbtManifestTransformer(manifest=manifest).build_user_configured_model()

    mf_metrics_dir = str(Path(directory).joinpath("metrics"))
    mf_build_result = parse_directory_of_yaml_files_to_model(mf_metrics_dir)
    dbt_build_result.model.metrics = mf_build_result.model.metrics

    transformed_model = ModelTransformer.transform(model=dbt_build_result.model)
    return ModelBuildResult(model=transformed_model, issues=dbt_build_result.issues)
