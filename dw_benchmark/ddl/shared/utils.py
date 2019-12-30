try:
    import importlib.resources as pkg_resources
except ImportError:
    # Try backported to PY<37 `importlib_resources`.
    import importlib_resources as pkg_resources
from dw_benchmark import templates


def run_warmup(model, engine) -> None:
    print("Running warmup by selecting * from all tables in a model")
    warmup = pkg_resources.read_text(templates, "warmup.sql")
    statement = warmup.format(schema=model.schema_name)
    _ = run_query(engine, statement)


def read_query(sql_file_path):
    with open(sql_file_path) as file:
        query = file.read()
    return query


def run_query(engine, statement):
    with engine.connect() as con:
        result = con.execution_options(autocommit=True).execute(statement)
        return result


def create_user_with_permissions(user: str, engine, pw: str = "Password1"):
    try:
        run_query(engine, f"create user if not exists {user} password '{pw}';")
    except:
        print(f"User {user} already exists. Skipping user creation")
    schemas = run_query(
        engine, "select schema_name from information_schema.schemata;"
    ).fetchall()
    for schema in schemas:
        schema_name = schema[0]
        print(f"Granting all permissions to schema {schema_name} to user {user}")
        stmt = f"grant usage on schema {schema_name} to {user}; grant all privileges on all tables in schema {schema_name} to {user};"
        run_query(engine, stmt)
