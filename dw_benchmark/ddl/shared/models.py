from enum import Enum


class TCPDS100g(str, Enum):
    s3_url: str = "s3://fivetran-benchmark/tpcds_100_dat"
    schema_name: str = "tcpds100g"
    ddl: str = "optimized.sql"


class TCPDS100gUnoptimized(str, Enum):
    s3_url: str = "s3://fivetran-benchmark/tpcds_100_dat"
    schema_name: str = "tcpds100g_unoptimized"
    ddl: str = "not_optimized.sql"


class TCPDS1T(str, Enum):
    s3_url: str = "s3://fivetran-benchmark/tpcds_1000_dat"
    schema_name: str = "tcpds1TB"
    ddl: str = "optimized.sql"


class TCPDS3T(str, Enum):
    s3_url: str = "s3://redshift-downloads/TPC-DS/3TB"
    schema_name: str = "tcpds3TB"
    ddl: str = "optimized.sql"


class TCPDS10T(str, Enum):
    s3_url: str = "s3://redshift-downloads/TPC-DS/10TB"
    schema_name: str = "tcpds10TB"
    ddl: str = "optimized.sql"
