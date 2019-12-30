try:
    import importlib.resources as pkg_resources
except ImportError:
    # Try backported to PY<37 `importlib_resources`.
    import importlib_resources as pkg_resources
from dw_benchmark import templates
from dw_benchmark.ddl.shared.models import TCPDS100g
from dw_benchmark.ddl.shared.models import TCPDS100gUnoptimized
from dw_benchmark.ddl.shared.models import TCPDS1T
from dw_benchmark.ddl.shared.models import TCPDS3T
from dw_benchmark.ddl.shared.models import TCPDS10T
from dw_benchmark.ddl.shared.utils import run_query
from sqlalchemy import create_engine


def get_redshift_engine(host, db, user, pwd):
    return create_engine(f"postgresql://{user}:{pwd}@{host}:5439/{db}")


def generate_ddl(model):
    print(f"Generating DDL for {model.schema_name}")
    ddl = pkg_resources.read_text(templates, model.ddl)
    ddl = ddl.format(schema=model.schema_name)
    return ddl


def run_copy_commands(model, engine, role):
    # the copy parameters depend on the source of data.
    # for fivetran data-sources
    if model in [TCPDS100g, TCPDS100gUnoptimized, TCPDS1T]:
        shared_params = (
            "format delimiter '|' acceptinvchars compupdate on region 'us-east-1'"
        )
    else:
        # for redshift data sources
        shared_params = "gzip delimiter '|' compupdate on region 'us-east-1'"
    tables = [
        "store_sales",
        "catalog_sales",
        "web_sales",
        "web_returns",
        "store_returns",
        "catalog_returns",
        "call_center",
        "catalog_page",
        "customer_address",
        "customer",
        "customer_demographics",
        "date_dim",
        "household_demographics",
        "income_band",
        "inventory",
        "item",
        "promotion",
        "reason",
        "ship_mode",
        "store",
        "time_dim",
        "warehouse",
        "web_page",
        "web_site",
    ]

    for table in tables:
        print(f"Running copy for table {model.schema_name}.{table}")
        statement = f"copy {model.schema_name}.{table} from '{model.s3_url}/{table}/' iam_role '{role}' {shared_params};"
        _ = run_query(engine, statement)


def run_ddl(model, engine) -> None:
    ddl = generate_ddl(model)
    _ = run_query(engine, ddl)


def run_analyze(engine) -> None:
    """
    Compute table statistics on  all the db.
    """
    _ = run_query(engine, "analyze;")
