from typing import Any
import pandas as pd
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
                logic_test: Any | None = None,
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
            retries=0,
            **redshift_target,
            **kwargs,
        )

    def execute(self, context: dict[str, Any]) -> Any:
        """Run the configured query then validate the returned results."""
        df = self._to_dataframe(super().execute(context))
        self._log_frame(df)
        first_value = df.iloc[0, 0] if not df.empty else None

        if self.logic_test is None:
            return self._handle_missing_logic(df, first_value)

        # Allow dynamic checks like "value > 0" or "value == True" via eval.
        eval_statement = f"{first_value} {self.logic_test}"
        self.log.info(f"Evaluating test: {eval_statement}")
        try:
            passed = bool(eval(eval_statement))
        except Exception as exc:
            raise ValueError(f"Failed to evaluate logic_test '{self.logic_test}': {exc}") from exc

        if not passed:
            raise ValueError(f"Data quality check failed! Expected {self.logic_test}, got {first_value}")

        self.log.info("Data quality check passed.")
        return df

    def _handle_missing_logic(self, df: pd.DataFrame, first_value: Any) -> pd.DataFrame:
        """Default behaviour when no logic_test is provided."""
        if df.empty:
            self.log.info("Data quality check passed (empty result set).")
            return df

        if first_value:
            raise ValueError("Data quality check failed!")

        self.log.info("Data quality check passed.")
        return df

    @staticmethod
    def _coerce_cell(cell: Any) -> Any:
        """Normalize a Redshift Data API cell dict into a native Python value."""
        if isinstance(cell, dict):
            for key in ("stringValue", "longValue", "doubleValue", "booleanValue", "isNull"):
                if key in cell:
                    return None if key == "isNull" else cell[key]
            return next(iter(cell.values()), None)
        return cell

    @staticmethod
    def _to_dataframe(result: Any) -> pd.DataFrame:
        """Convert Redshift Data API result(s) into a pandas DataFrame."""
        if isinstance(result, pd.DataFrame):
            return result

        # RedshiftDataOperator may return a single result dict or a list (sub-statements).
        results = result if isinstance(result, list) else [result]
        frames: list[pd.DataFrame] = []
        for res in results:
            frames.append(DataQualityOperator._single_result_to_frame(res))

        if not frames:
            return pd.DataFrame()
        if len(frames) == 1:
            return frames[0]
        return pd.concat(frames, ignore_index=True)

    @staticmethod
    def _single_result_to_frame(result: Any) -> pd.DataFrame:
        """Convert Redshift Data API result dict into a DataFrame."""
        if isinstance(result, pd.DataFrame):
            return result
        if result is None:
            return pd.DataFrame()

        records: list[Any] | None = None
        columns = None
        if isinstance(result, dict):
            # Only use the SQL rows; ignore other metadata keys from the Data API response.
            records = result.get("Records") or result.get("records") or []
            metadata = result.get("ColumnMetadata") or result.get("column_metadata") or []
            if metadata:
                columns = [col.get("label") or col.get("name") for col in metadata]
        if records is None:
            records = []

        rows: list[list[Any]] = []
        for row in records or []:
            if isinstance(row, dict):
                rows.append([DataQualityOperator._coerce_cell(v) for v in row.values()])
            else:
                rows.append([DataQualityOperator._coerce_cell(cell) for cell in (row if isinstance(row, (list, tuple)) else [row])])

        if not rows:
            return pd.DataFrame(columns=columns)

        if columns:
            widths = {len(r) for r in rows}
            if widths != {len(columns)}:
                columns = None

        return pd.DataFrame(rows, columns=columns)

    def _log_frame(self, df: pd.DataFrame) -> None:
        """Print SQL query reults if available."""
        self.log.info(f"Data quality query returned {len(df)} rows and {len(df.columns)} columns.")
        if not df.empty:
            self.log.info("SQL query returned:")
            self.log.info(df)
 