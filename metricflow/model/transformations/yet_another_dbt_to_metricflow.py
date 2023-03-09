from dataclasses import dataclass
from typing import List

from dbt.contracts.graph.manifest import Manifest as DbtManifest

from metricflow.aggregation_properties import AggregationType
from metricflow.model.objects.data_source import DataSource
from metricflow.model.objects.elements.dimension import Dimension, DimensionType, DimensionTypeParams
from metricflow.model.objects.elements.identifier import Identifier, IdentifierType
from metricflow.model.objects.elements.measure import Measure
from metricflow.model.objects.user_configured_model import UserConfiguredModel
from metricflow.model.parsing.dir_to_model import ModelBuildResult
from metricflow.model.validations.validator_helpers import ModelValidationResults, ValidationError, ValidationIssue


@dataclass
class PartialDataSource:  # noqa: D
    sql_table: str
    issues: List[ValidationIssue]
    measures: List[Measure]
    identifiers: List[Identifier]
    dimensions: List[Dimension]


class YetAnotherDbtManifestTransformer:
    """The DbtManifestTransform is a class used to transform dbt Manifests into MetricFlow UserConfiguredModels

    This helps keep track of state objects while transforming the Manifest into a
    UserConfiguredModel, ensuring like dbt Node elements are rendered only once and
    allowing us to pass around fewer arguments (reducing the mental load)
    """

    def __init__(self, manifest: DbtManifest) -> None:
        """Constructor.

        Args:
            manifest: A dbt Manifest object
        """
        self.manifest = manifest

    def build_user_configured_model(self) -> ModelBuildResult:
        issues = []
        data_sources = []
        for model in self.__dbt_models():
            if model.meta.get('mf') is None:
                continue

            data_source, model_issues = self.__to_data_source(model)
            data_sources.append(data_source)
            issues.extend(model_issues)

        return ModelBuildResult(
            model=UserConfiguredModel(data_sources=data_sources, metrics=[]),
            issues=ModelValidationResults.from_issues_sequence(issues)
        )

    def __to_data_source(self, dbt_model):
        partial_ds = PartialDataSource(
            sql_table=f"db.schema.{dbt_model.name}",  # TODO: find the actual table
            issues=[],
            identifiers=[],
            measures=[],
            dimensions=[]
        )
        self.__fill_dbt_model_meta(partial_ds, dbt_model)
        for col in dbt_model.columns.values():
            self.__fill_col(partial_ds, col)

        return DataSource(
            name=dbt_model.name,
            sql_table=partial_ds.sql_table,
            description=dbt_model.description or dbt_model.name,
            identifiers=partial_ds.identifiers,
            measures=partial_ds.measures,
            dimensions=partial_ds.dimensions,
        ), partial_ds.issues

    def __fill_dbt_model_meta(self, partial_ds, dbt_model):
        mf = dbt_model.meta.get('mf')
        if mf is None:
            return None

        partial_ds.sql_table = mf.get('sql_table', partial_ds.sql_table)
        measures = mf.get('measures')
        if measures is None:
            partial_ds.issues.append(
                ValidationError(message=f'model.meta.mf.measures not found in model: {dbt_model.name}')
            )
            return

        # TODO: Use metricflow mapping mechanisms
        for measure in measures:
            partial_ds.measures.append(
                Measure(
                    name=measure['name'],
                    description=measure.get('description', measure['name']),
                    agg=measure['agg'].upper(),
                    expr=measure.get('expr', measure['name']),
                )
            )

    def __fill_col(self, partial_ds: PartialDataSource, col):
        mf = col.meta.get('mf')
        if mf is None:
            return None

        mf_type = mf.get('mf_type')
        if mf.get('mf_type') is None:
            partial_ds.issues.append(ValidationError(message=f'meta.mf.mf_type not found for col: {col.name}'))

        name = mf.get('name', col.name)
        description = col.description or col.name
        expr = mf.get('expr', col.name)
        type_name = mf.get('type')
        if mf_type == 'identifier':
            if type_name is None:
                partial_ds.issues.append(ValidationError(message=f'meta.mf.type not found for col: {col.name}'))
                return None

            partial_ds.identifiers.append(
                Identifier(
                    name=name,
                    description=description,
                    expr=expr,
                    type=IdentifierType.for_name(type_name.upper()),
                )
            )
        elif mf_type == 'dimension':
            if type_name is None:
                partial_ds.issues.append(ValidationError(message=f'meta.mf.type not found for col: {col.name}'))
                return None

            type_params_dict = mf.get('type_params')
            type_params = None
            if type_params_dict is not None:
                type_params = DimensionTypeParams.parse_obj(type_params_dict)

            partial_ds.dimensions.append(
                Dimension(
                    name=name,
                    description=name,
                    expr=expr,
                    type=DimensionType.for_name(type_name.upper()),
                    type_params=type_params,
                )
            )
        elif mf_type == 'measure':
            agg = mf.get('agg')
            if agg is None:
                partial_ds.issues.append(ValidationError(message=f'meta.mf.agg not found for col: {col.name}'))
                return None

            partial_ds.measures.append(
                Measure(
                    name=name,
                    description=description,
                    expr=expr,
                    agg=AggregationType.for_name(agg),
                )
            )
        else:
            partial_ds.issues.append(ValidationError(message=f'invalid meta.mf.mf_type for col: {col.name}, value: {mf_type}'))

    def __dbt_models(self):
        for name, model in self.manifest.nodes.items():
            if name.startswith('model.'):
                yield model
