import os
import great_expectations as gx
from great_expectations import expectations as gxe
from spark_utils import get_spark_session

spark = get_spark_session("ge-smoke-test")

df = spark.createDataFrame([(1, "a"), (2, "b"), (None, "c")], ["id", "label"])

context = gx.get_context()
data_source = context.data_sources.add_spark(name="smoke_test_source")
data_asset = data_source.add_dataframe_asset(name="smoke_test_asset")
batch_def = data_asset.add_batch_definition_whole_dataframe("smoke_test_batch")
batch = batch_def.get_batch(batch_parameters={"dataframe": df})

result = batch.validate(gxe.ExpectColumnValuesToNotBeNull(column="id"))
print("Success:", result.success)
print(result)

spark.stop()
os._exit(0)