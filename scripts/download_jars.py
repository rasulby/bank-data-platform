"""Download fixed Spark dependencies during the image build, never during a DAG run."""

from pathlib import Path
from urllib.request import urlretrieve
import pyspark

artifacts = [
    ("org.apache.iceberg", "iceberg-spark-runtime-3.5_2.12", "1.7.1"),
    ("org.apache.iceberg", "iceberg-aws-bundle", "1.7.1"),
    ("org.postgresql", "postgresql", "42.7.4"),
    ("org.apache.spark", "spark-sql-kafka-0-10_2.12", "3.5.3"),
    ("org.apache.spark", "spark-token-provider-kafka-0-10_2.12", "3.5.3"),
    ("org.apache.kafka", "kafka-clients", "3.4.1"),
    ("org.apache.commons", "commons-pool2", "2.11.1"),
]
jars = Path(pyspark.__file__).parent / "jars"
for group, name, version in artifacts:
    filename = f"{name}-{version}.jar"
    urlretrieve(
        f"https://repo.maven.apache.org/maven2/{group.replace('.', '/')}/{name}/{version}/{filename}",
        jars / filename,
    )
    print(filename)
