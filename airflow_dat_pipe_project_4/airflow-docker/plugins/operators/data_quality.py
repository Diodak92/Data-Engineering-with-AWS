from typing import Any

from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator


class DataQualityOperator(RedshiftDataOperator):

    ui_color = '#89DA59'

    def __init__(
                self,
                workgroup_name: str | None = None,
                cluster_identifier: str | None = None,
                database: str | None = None,
                db_user: str | None = None,
                sql_check: str | list[str] = "",
                logic_test: str | None = None,
                aws_conn_id: str = "aws_default",
                poll_interval: int = 10,
                wait_for_completion: bool = True,
                **kwargs,
                ):
        
        if workgroup_name and cluster_identifier:
            raise ValueError("Provide only one of workgroup_name or cluster_identifier for DataQualityOperator.")

        # Only pass the configured Redshift target to avoid Data API validation errors.
        redshift_target: dict[str, str] = {}
        if workgroup_name:
            redshift_target["workgroup_name"] = workgroup_name
        if cluster_identifier:
            redshift_target["cluster_identifier"] = cluster_identifier
        if not redshift_target:
            raise ValueError("DataQualityOperator requires either workgroup_name (serverless) or cluster_identifier (provisioned).")
        if not sql_check:
            raise ValueError("DataQualityOperator requires at least one SQL check to run.")
        
        self.logic_test = logic_test

        super().__init__(
            database=database,
            db_user=db_user,
            sql=sql_check,
            aws_conn_id=aws_conn_id,
            poll_interval=poll_interval,
            wait_for_completion=wait_for_completion,
            return_sql_result = True,
            **redshift_target,
            **kwargs,
        )

    def execute(self, context: dict[str, Any]) -> Any:
        """Run the configured query then validate the returned results."""
        results = super().execute(context)
        self.log.info(f"Data quality query returned: {results}")
        if results is None:
            raise ValueError("Data quality check failed! Query returned no results.")

        first_value = self._first_cell(results)

        if self.logic_test is None:
            if not first_value:
                raise ValueError("Data quality check failed!")
            self.log.info("Data quality check passed.")
            return results

        passed = str(first_value) == str(self.logic_test)

        if not passed:
            raise ValueError(f"Data quality check failed! Expected {self.logic_test}, got {first_value}")

        self.log.info("Data quality check passed.")
        return results

    @staticmethod
    def _first_cell(result: Any) -> Any:
        """Return the first cell from a Redshift Data"""
        records = result.get("records") if isinstance(result, dict) else result
        if not records:
            return None
        cell = records[0]
        if isinstance(cell, (list, tuple)):
            cell = cell[0] if cell else None
        if isinstance(cell, dict):
            cell = next(iter(cell.values()), None)
        return cell
